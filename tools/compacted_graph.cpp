#include <iostream>
#include <math.h>
#include <rapidcsv.h>
#include <string.h>
#include <string>

#include <gflags/gflags.h>

#include "graphs/edgelist.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/atomic.h"
#include "utility/bitmap.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"

using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;

template <typename VID_T, typename VDATA_T>
void CompressEdgelistCSV2EdgeListCSV(const std::string input_pt,
                                     const std::string output_pt,
                                     const size_t cores,
                                     char separator_params = ',',
                                     const size_t read_edges = 0) {
  LOG_INFO("GraphReduce: CSV2CSV");
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);

  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  rapidcsv::Document* doc =
      new rapidcsv::Document(input_pt, rapidcsv::LabelParams((-1, -1)),
                             rapidcsv::SeparatorParams(separator_params));
  std::vector<VID_T>* src = new std::vector<VID_T>();
  *src = doc->GetColumn<VID_T>(0);
  std::vector<VID_T>* dst = new std::vector<VID_T>();
  *dst = doc->GetColumn<VID_T>(1);

  size_t num_edges = src->size();
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);
  VID_T max_vid(0);

  LOG_INFO("Read ", num_edges, " edges");
  LOG_INFO("Run: get maximum vid");
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst, &max_vid,
                        &pending_packages, &finish_cv]() {
      for (size_t j = tid; j < src->size(); j += cores) {
        dst_v[j] = dst->at(j);
        src_v[j] = src->at(j);
        write_max(&max_vid, dst->at(j));
        write_max(&max_vid, src->at(j));
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  auto aligned_max_vid =
      (ceil((float)max_vid / ALIGNMENT_FACTOR)) * ALIGNMENT_FACTOR;
  LOG_INFO("#maximum vid: ", max_vid);
  VID_T* vid_map = (VID_T*)malloc(sizeof(VID_T) * aligned_max_vid);
  memset(vid_map, 0, sizeof(VID_T) * aligned_max_vid);
  Bitmap* visited = new Bitmap(aligned_max_vid);
  visited->clear();

  LOG_INFO("Run: transfer");
  VID_T local_id = 0;

  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &max_vid, &visited, &mtx,
                        &vid_map, &local_id]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        mtx.lock();
        if (!visited->get_bit(src_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(src_v[j]);
          vid_map[src_v[j]] = vid;
        }
        if (!visited->get_bit(dst_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(dst_v[j]);
          vid_map[dst_v[j]] = vid;
        }
        mtx.unlock();
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &visited, &vid_map]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        auto src = src_v[j];
        auto dst = dst_v[j];
        src_v[j] = vid_map[src];
        dst_v[j] = vid_map[dst];
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  std::vector<VID_T>* out_src = new std::vector<VID_T>;
  std::vector<VID_T>* out_dst = new std::vector<VID_T>;
  out_src->reserve(num_edges);
  out_dst->reserve(num_edges);
  for (size_t i = 0; i < num_edges; i++) {
    out_src->push_back(src_v[i]);
    out_dst->push_back(dst_v[i]);
  }

  rapidcsv::Document doc_out(
      "", rapidcsv::LabelParams(-1, -1),
      rapidcsv::SeparatorParams(separator_params, false, false));
  doc_out.SetColumn<VID_T>(0, *out_src);
  doc_out.SetColumn<VID_T>(1, *out_dst);
  doc_out.Save(output_pt);

  free(vid_map);
  delete out_src;
  delete out_dst;
  free(src_v);
  free(dst_v);
  std::cout << "Save at " << output_pt << std::endl;
  return;
}

template <typename VID_T, typename VDATA_T>
void CompressEdgelistCSV2EdgelistBin(const std::string input_pt,
                                     const std::string dst_pt,
                                     const size_t cores,
                                     char separator_params = ',',
                                     const size_t read_num_edges = 0) {
  LOG_INFO("GraphReduce: CSV2Bin, read_edges:", read_num_edges);
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edge_list_io_adapter;
  std::string meta_pt = dst_pt + "minigraph_meta" + ".bin";
  std::string data_pt = dst_pt + "minigraph_data" + ".bin";
  std::string vdata_pt = dst_pt + "minigraph_vdata" + ".bin";
  LOG_INFO("Write: ", meta_pt);
  LOG_INFO("Write: ", data_pt);
  LOG_INFO("Write: ", vdata_pt);

  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);

  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);
  std::vector<size_t>* src = new std::vector<size_t>();
  std::vector<size_t>* dst = new std::vector<size_t>();

  size_t num_edges = 0;
  VID_T* src_v = nullptr;
  VID_T* dst_v = nullptr;
  VID_T max_vid = 0;

  const char* s = &separator_params;
  if (read_num_edges == 0) {
    rapidcsv::Document* doc =
        new rapidcsv::Document(input_pt, rapidcsv::LabelParams(-1, -1),
                               rapidcsv::SeparatorParams(separator_params));
    *src = doc->GetColumn<size_t>(0);
    *dst = doc->GetColumn<size_t>(1);
    num_edges = src->size();
    src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
    dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
    memset(src_v, 0, sizeof(VID_T) * num_edges);
    memset(dst_v, 0, sizeof(VID_T) * num_edges);
    delete doc;

    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst,
                          &pending_packages, &finish_cv, &max_vid]() {
        for (size_t j = tid; j < src->size(); j += cores) {
          dst_v[j] = dst->at(j);
          src_v[j] = src->at(j);
          write_max(&max_vid, dst_v[j]);
          write_max(&max_vid, src_v[j]);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    VID_T aligned_max_vid =
        ceil((float)max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    delete src;
    delete dst;

  } else {
    std::string line;
    std::ifstream in(input_pt);
    num_edges = read_num_edges;
    LOG_INFO("Stream reading.", num_edges);
    src_v = (VID_T*)malloc(sizeof(VID_T) * read_num_edges + 1024);
    dst_v = (VID_T*)malloc(sizeof(VID_T) * read_num_edges + 1024);
    memset(src_v, 0, sizeof(VID_T) * read_num_edges + 1024);
    memset(dst_v, 0, sizeof(VID_T) * read_num_edges + 1024);
    size_t count = 0;
    if (in) {
      while (getline(in, line)) {
        if (count > num_edges) break;
        auto out = SplitEdge(line, &separator_params);
        src_v[count] = out.first;
        dst_v[count] = out.second;
        write_max(&max_vid, dst_v[count]);
        write_max(&max_vid, src_v[count]);
        count++;
      }
    }
  }

  auto graph = new EDGE_LIST_T(0, num_edges, 0);
  LOG_INFO("Read ", num_edges, " edges #");

  VID_T aligned_max_vid =
      (ceil((float)max_vid / ALIGNMENT_FACTOR)) * ALIGNMENT_FACTOR;
  LOG_INFO("Run: get maximum vid", aligned_max_vid);

  LOG_INFO("Max vid: ", aligned_max_vid);
  size_t* vid_map = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(vid_map, 0, sizeof(size_t) * aligned_max_vid);
  Bitmap* visited = new Bitmap(aligned_max_vid);
  visited->clear();

  LOG_INFO("Run: transfer");
  std::atomic local_vid(0);
  VID_T local_id = 0;
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &visited, &vid_map,
                        &local_id, &mtx]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        mtx.lock();
        if (!visited->get_bit(src_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(src_v[j]);
          __sync_bool_compare_and_swap(vid_map + src_v[j], 0, vid);
        }
        if (!visited->get_bit(dst_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(dst_v[j]);
          __sync_bool_compare_and_swap(vid_map + dst_v[j], 0, vid);
        }
        mtx.unlock();
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Run: compacted");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &graph, &vid_map]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        auto src = src_v[j];
        auto dst = dst_v[j];
        src_v[j] = vid_map[src];
        dst_v[j] = vid_map[dst];
        graph->buf_graph_[2 * j] = src_v[j];
        graph->buf_graph_[2 * j + 1] = dst_v[j];
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  graph->max_vid_ = local_id;
  graph->num_vertexes_ = local_id;
  graph->aligned_max_vid_ =
      ceil(float(local_id) / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  graph->vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * graph->get_num_vertexes());
  memset(graph->vdata_, 0, sizeof(VDATA_T) * graph->get_num_vertexes());
  free(vid_map);
  free(src_v);
  free(dst_v);
  graph->ShowGraph(100);
  edge_list_io_adapter.Write(*graph, edgelist_bin, meta_pt, data_pt, vdata_pt);
  std::cout << "Save at " << dst_pt << std::endl;
  return;
}

template <typename VID_T, typename VDATA_T>
void CompressEdgelistBin2EdgelistBin(const std::string input_pt,
                                     const std::string dst_pt,
                                     const size_t cores) {
  LOG_INFO("GraphReduce: Bin2Bin");
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edgelist_io_adapter;
  std::string input_meta_pt = input_pt + "minigraph_meta" + ".bin";
  std::string input_data_pt = input_pt + "minigraph_data" + ".bin";
  std::string input_vdata_pt = input_pt + "minigraph_vdata" + ".bin";
  std::string dst_meta_pt = dst_pt + "minigraph_meta" + ".bin";
  std::string dst_data_pt = dst_pt + "minigraph_data" + ".bin";
  std::string dst_vdata_pt = dst_pt + "minigraph_vdata" + ".bin";
  LOG_INFO("Read: ", input_meta_pt);
  LOG_INFO("Read: ", input_data_pt);
  LOG_INFO("Read: ", input_vdata_pt);
  LOG_INFO("Write: ", dst_meta_pt);
  LOG_INFO("Write: ", dst_data_pt);
  LOG_INFO("Write: ", dst_vdata_pt);

  auto graph = new EDGE_LIST_T;

  edgelist_io_adapter.ReadEdgeListFromBin(
      (GRAPH_BASE_T*)graph, 0, input_meta_pt, input_data_pt, input_vdata_pt);
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);

  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  size_t num_edges = graph->num_edges_;
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);

  VID_T max_vid(0);

  LOG_INFO("Read ", num_edges, " edges");
  LOG_INFO("Run: get maximum vid");
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &graph, &pending_packages,
                        &finish_cv, &max_vid]() {
      for (size_t j = tid; j < graph->num_edges_; j += cores) {
        src_v[j] = graph->buf_graph_[j * 2];
        dst_v[j] = graph->buf_graph_[j * 2 + 1];
        write_max(&max_vid, src_v[j]);
        write_max(&max_vid, dst_v[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  VID_T aligned_max_vid =
      ceil((float)max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
  VID_T* vid_map = (VID_T*)malloc(sizeof(VID_T) * aligned_max_vid);
  memset(vid_map, 0, sizeof(VID_T) * aligned_max_vid);
  Bitmap* visited = new Bitmap(aligned_max_vid);
  visited->clear();

  LOG_INFO("Run: transfer");
  std::atomic local_vid(0);
  VID_T local_id = 0;
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &visited, &vid_map,
                        &local_id, &mtx]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        mtx.lock();
        if (!visited->get_bit(src_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(src_v[j]);
          __sync_bool_compare_and_swap(vid_map + src_v[j], 0, vid);
        }
        if (!visited->get_bit(dst_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(dst_v[j]);
          __sync_bool_compare_and_swap(vid_map + dst_v[j], 0, vid);
        }
        mtx.unlock();
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Run: compacted");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &graph, &vid_map]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        auto src = src_v[j];
        auto dst = dst_v[j];
        src_v[j] = vid_map[src];
        dst_v[j] = vid_map[dst];
        graph->buf_graph_[2 * j] = src_v[j];
        graph->buf_graph_[2 * j + 1] = dst_v[j];
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  free(vid_map);
  free(src_v);
  free(dst_v);
  graph->max_vid_ = local_id;
  graph->num_vertexes_ = local_id;
  graph->aligned_max_vid_ =
      ceil((float)graph->max_vid_ / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  edgelist_io_adapter.Write(*graph, edgelist_bin, dst_meta_pt, dst_data_pt,
                            dst_vdata_pt);
  graph->ShowGraph(1000);
  std::cout << "Save at " << dst_pt << std::endl;
  return;
}

template <typename VID_T, typename VDATA_T>
void CompressEdgeListBin2EdgelistCSV(const std::string input_pt,
                                     const std::string dst_pt,
                                     const size_t cores,
                                     char separator_params = ',') {
  LOG_INFO("GraphReduce: Bin2CSV");
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edgelist_io_adapter;
  std::string input_meta_pt = input_pt + "minigraph_meta" + ".bin";
  std::string input_data_pt = input_pt + "minigraph_data" + ".bin";
  std::string input_vdata_pt = input_pt + "minigraph_vdata" + ".bin";
  LOG_INFO("Read: ", input_meta_pt);
  LOG_INFO("Read: ", input_data_pt);
  LOG_INFO("Read: ", input_vdata_pt);
  LOG_INFO("Write: ", dst_pt);

  auto graph = new EDGE_LIST_T;

  edgelist_io_adapter.ReadEdgeListFromBin(
      (GRAPH_BASE_T*)graph, 0, input_meta_pt, input_data_pt, input_vdata_pt);
  graph->ShowGraph();
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);

  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  size_t num_edges = graph->num_edges_;
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);

  VID_T max_vid(0);

  LOG_INFO("Read ", num_edges, " edges");
  LOG_INFO("Run: get maximum vid");
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &graph, &pending_packages,
                        &finish_cv, &max_vid]() {
      for (size_t j = tid; j < graph->num_edges_; j += cores) {
        src_v[j] = graph->buf_graph_[j * 2];
        dst_v[j] = graph->buf_graph_[j * 2 + 1];
        write_max(&max_vid, src_v[j]);
        write_max(&max_vid, dst_v[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  VID_T aligned_max_vid =
      ceil((float)max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
  VID_T* vid_map = (VID_T*)malloc(sizeof(VID_T) * aligned_max_vid);
  memset(vid_map, 0, sizeof(VID_T) * aligned_max_vid);
  Bitmap* visited = new Bitmap(aligned_max_vid);
  visited->clear();

  LOG_INFO("Run: transfer");
  std::atomic local_vid(0);
  VID_T local_id = 0;
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &visited, &vid_map,
                        &local_id, &mtx]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        mtx.lock();
        if (!visited->get_bit(src_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(src_v[j]);
          __sync_bool_compare_and_swap(vid_map + src_v[j], 0, vid);
        }
        if (!visited->get_bit(dst_v[j])) {
          auto vid = __sync_fetch_and_add(&local_id, 1);
          visited->set_bit(dst_v[j]);
          __sync_bool_compare_and_swap(vid_map + dst_v[j], 0, vid);
        }
        mtx.unlock();
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Run: compacted");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &graph, &vid_map]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        auto src = src_v[j];
        auto dst = dst_v[j];
        src_v[j] = vid_map[src];
        dst_v[j] = vid_map[dst];
        graph->buf_graph_[2 * j] = src_v[j];
        graph->buf_graph_[2 * j + 1] = dst_v[j];
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  free(vid_map);
  free(src_v);
  free(dst_v);
  graph->max_vid_ = local_id;
  graph->num_vertexes_ = local_id;
  graph->aligned_max_vid_ =
      ceil((float)graph->max_vid_ / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  std::vector<VID_T>* out_src = new std::vector<VID_T>;
  std::vector<VID_T>* out_dst = new std::vector<VID_T>;
  out_src->reserve(num_edges);
  out_dst->reserve(num_edges);
  for (size_t i = 0; i < graph->get_num_edges(); i++) {
    out_src->push_back(graph->buf_graph_[i * 2]);
    out_dst->push_back(graph->buf_graph_[i * 2 + 1]);
  }

  rapidcsv::Document doc_out(
      "", rapidcsv::LabelParams(-1, -1),
      rapidcsv::SeparatorParams(separator_params, false, false));
  doc_out.SetColumn<VID_T>(0, *out_src);
  doc_out.SetColumn<VID_T>(1, *out_dst);
  doc_out.Save(dst_pt);
  std::cout << "Save at " << dst_pt << std::endl;
  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  size_t cores = FLAGS_cores;

  assert(FLAGS_i != "" && FLAGS_o != "");
  LOG_INFO("Reduce: ", FLAGS_i);

  if (FLAGS_frombin && FLAGS_tobin && FLAGS_in_type == "edgelist" &&
      FLAGS_out_type == "edgelist")
    CompressEdgelistBin2EdgelistBin<vid_t, vdata_t>(FLAGS_i, FLAGS_o, cores);

  if (FLAGS_frombin == false && FLAGS_tobin && FLAGS_in_type == "edgelist" &&
      FLAGS_out_type == "edgelist")
    CompressEdgelistCSV2EdgelistBin<vid_t, vdata_t>(
        FLAGS_i, FLAGS_o, cores, *FLAGS_sep.c_str(), FLAGS_edges);

  if (FLAGS_frombin && FLAGS_tobin == false && FLAGS_in_type == "edgelist" &&
      FLAGS_out_type == "edgelist")
    CompressEdgeListBin2EdgelistCSV<vid_t, vdata_t>(FLAGS_i, FLAGS_o, cores,
                                                    *FLAGS_sep.c_str());

  if (FLAGS_frombin == false && FLAGS_tobin == false &&
      FLAGS_in_type == "edgelist" && FLAGS_out_type == "edgelist")
    CompressEdgelistCSV2EdgeListCSV<vid_t, vdata_t>(
        FLAGS_i, FLAGS_o, cores, *FLAGS_sep.c_str(), FLAGS_edges);

  gflags::ShutDownCommandLineFlags();
}
