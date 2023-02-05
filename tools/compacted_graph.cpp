#include "graphs/edge_list.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"
#include <gflags/gflags.h>
#include <cstring>
#include <iostream>
#include <math.h>
#include <rapidcsv.h>
#include <string>

using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;

template <typename VID_T>
void GraphReduceCSVToCSV(const std::string input_pt,
                         const std::string output_pt, const size_t cores,
                         char separator_params = ',') {
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);

  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  rapidcsv::Document* doc =
      new rapidcsv::Document(input_pt, rapidcsv::LabelParams(),
                             rapidcsv::SeparatorParams(separator_params));
  std::vector<VID_T>* src = new std::vector<VID_T>();
  *src = doc->GetColumn<VID_T>("src");
  std::vector<VID_T>* dst = new std::vector<VID_T>();
  *dst = doc->GetColumn<VID_T>("dst");

  size_t num_edges = src->size();
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);
  std::atomic<VID_T> max_vid_atom(0);

  LOG_INFO("Read ", num_edges, " edges");
  LOG_INFO("Run: get maximum vid");
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst,
                        &pending_packages, &finish_cv, &max_vid_atom]() {
      for (size_t j = tid; j < src->size(); j += cores) {
        dst_v[j] = dst->at(j);
        src_v[j] = src->at(j);
        if (max_vid_atom.load() < dst->at(j)) max_vid_atom.store(dst->at(j));
        if (max_vid_atom.load() < src->at(j)) max_vid_atom.store(src->at(j));
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  max_vid_atom.store((size_t(max_vid_atom.load() / 64) + 1) * 64);
  LOG_INFO("#maximum vid: ", max_vid_atom.load());
  // delete doc;
  VID_T* vid_map = (VID_T*)malloc(sizeof(VID_T) * max_vid_atom.load());
  memset(vid_map, 0, sizeof(VID_T) * max_vid_atom.load());
  Bitmap* visited = new Bitmap(max_vid_atom.load());
  visited->clear();

  LOG_INFO("Run: transfer");
  std::atomic local_vid(0);
  VID_T local_id = 0;
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &max_vid_atom, &visited,
                        &vid_map, &local_id]() {
      for (size_t j = tid; j < num_edges; j += cores) {
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
      "", rapidcsv::LabelParams(0, -1),
      rapidcsv::SeparatorParams(separator_params, false, false));
  doc_out.SetColumnName(0, "src");
  doc_out.SetColumnName(1, "dst");
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

template <typename VID_T>
void GraphReduceCSVToBin(const std::string input_pt, const std::string dst_pt,
                         const size_t cores, char separator_params = ',') {
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

  rapidcsv::Document* doc =
      new rapidcsv::Document(input_pt, rapidcsv::LabelParams(),
                             rapidcsv::SeparatorParams(separator_params));
  std::vector<size_t>* src = new std::vector<size_t>();
  *src = doc->GetColumn<size_t>("src");
  std::vector<size_t>* dst = new std::vector<size_t>();
  *dst = doc->GetColumn<size_t>("dst");

  size_t num_edges = src->size();
  size_t* src_v = (size_t*)malloc(sizeof(size_t) * num_edges);
  size_t* dst_v = (size_t*)malloc(sizeof(size_t) * num_edges);
  memset(src_v, 0, sizeof(size_t) * num_edges);
  memset(dst_v, 0, sizeof(size_t) * num_edges);
  auto graph = new EDGE_LIST_T(0, num_edges, 0);

  std::atomic<size_t> max_vid_atom(0);

  LOG_INFO("Read ", num_edges, " edges");
  LOG_INFO("Run: get maximum vid");
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst,
                        &pending_packages, &finish_cv, &max_vid_atom]() {
      for (size_t j = tid; j < src->size(); j += cores) {
        dst_v[j] = dst->at(j);
        src_v[j] = src->at(j);
        if (max_vid_atom.load() < dst->at(j)) max_vid_atom.store(dst->at(j));
        if (max_vid_atom.load() < src->at(j)) max_vid_atom.store(src->at(j));
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  max_vid_atom.store((size_t(max_vid_atom.load() / 64) + 1) * 64);
  LOG_INFO("#maximum vid: ", max_vid_atom.load());
  graph->max_vid_ = max_vid_atom.load();
  delete doc;
  delete src;
  delete dst;
  size_t* vid_map = (size_t*)malloc(sizeof(size_t) * max_vid_atom.load());
  memset(vid_map, 0, sizeof(size_t) * max_vid_atom.load());
  Bitmap* visited = new Bitmap(max_vid_atom.load());
  visited->clear();

  LOG_INFO("Run: transfer");
  std::atomic local_vid(0);
  VID_T local_id = 0;
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &visited, &vid_map,
                        &local_id]() {
      for (size_t j = tid; j < num_edges; j += cores) {
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
  graph->ShowGraph(10);
  edge_list_io_adapter.Write(*graph, edge_list_bin, false, meta_pt, data_pt,
                             vdata_pt);
  std::cout << "Save at " << dst_pt << std::endl;
  return;
}

template <typename VID_T>
void GraphReduceBinToBin(const std::string input_pt, const std::string dst_pt,
                         const size_t cores) {
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edge_list_io_adapter;
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
  edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edge_list_bin, ' ', 0,
                            input_meta_pt, input_data_pt, input_vdata_pt);
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

  std::atomic<VID_T> max_vid_atom(0);

  LOG_INFO("Read ", num_edges, " edges");
  LOG_INFO("Run: get maximum vid");
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &graph, &pending_packages,
                        &finish_cv, &max_vid_atom]() {
      for (size_t j = tid; j < graph->num_edges_; j += cores) {
        src_v[j] = graph->buf_graph_[j * 2];
        dst_v[j] = graph->buf_graph_[j * 2 + 1];
        if (max_vid_atom.load() < graph->buf_graph_[j * 2 + 1])
          max_vid_atom.store(graph->buf_graph_[j * 2 + 1]);
        if (max_vid_atom.load() < graph->buf_graph_[j * 2])
          max_vid_atom.store(graph->buf_graph_[j * 2]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  max_vid_atom.store((VID_T(max_vid_atom.load() / 64) + 1) * 64);
  LOG_INFO("#maximum vid: ", max_vid_atom.load());
  graph->max_vid_ = max_vid_atom.load();
  VID_T* vid_map = (VID_T*)malloc(sizeof(VID_T) * max_vid_atom.load());
  memset(vid_map, 0, sizeof(VID_T) * max_vid_atom.load());
  Bitmap* visited = new Bitmap(max_vid_atom.load());
  visited->clear();

  LOG_INFO("Run: transfer");
  std::atomic local_vid(0);
  VID_T local_id = 0;
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges,
                        &pending_packages, &finish_cv, &visited, &vid_map,
                        &local_id]() {
      for (size_t j = tid; j < num_edges; j += cores) {
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
  graph->num_vertexes_ = local_id;
  edge_list_io_adapter.Write(*graph, edge_list_bin, false, dst_meta_pt,
                             dst_data_pt, dst_vdata_pt);
  graph->ShowGraph(10);
  std::cout << "Save at " << dst_pt << std::endl;
  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  size_t cores = FLAGS_cores;

  assert(FLAGS_i != "" && FLAGS_o != "");
  LOG_INFO("Reduce: ", FLAGS_i);
  if (FLAGS_tobin) {
    if (FLAGS_frombin)
      GraphReduceBinToBin<vid_t>(FLAGS_i, FLAGS_o, cores);
    else
      GraphReduceCSVToBin<vid_t>(FLAGS_i, FLAGS_o, cores, *FLAGS_sep.c_str());
  } else {
    GraphReduceCSVToCSV<vid_t>(FLAGS_i, FLAGS_o, cores, *FLAGS_sep.c_str());
  }
  gflags::ShutDownCommandLineFlags();
}