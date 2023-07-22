#include <iostream>
#include <math.h>
#include <string>

#include <gflags/gflags.h>
#include "rapidcsv.h"

#include "graphs/edgelist.h"
#include "portability/sys_types.h"
#include "utility/atomic.h"
#include "utility/bitmap.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"

using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;

template <typename VID_T>
void GetGraphStatisticFromCSV(const std::string input_pt,
                              const std::string output_pt,
                              char separator_params = ',',
                              const size_t cores = 1,
                              const size_t expected_edges = 1073741824) {
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);
  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  rapidcsv::Document* doc =
      new rapidcsv::Document(input_pt, rapidcsv::LabelParams(-1, -1),
                             rapidcsv::SeparatorParams(separator_params));
  std::vector<VID_T>* src = new std::vector<VID_T>();
  std::vector<VID_T>* dst = new std::vector<VID_T>();
  src->reserve(expected_edges);
  dst->reserve(expected_edges);
  *src = doc->GetColumn<VID_T>(0);
  *dst = doc->GetColumn<VID_T>(1);

  size_t num_edges = src->size();
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);

  LOG_INFO("read: ", num_edges, " edges");
  VID_T max_vid = 0;
  size_t max_indegree(0);
  size_t max_outdegree(0);
  size_t max_degree(0);

  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst, &num_edges,
                        &pending_packages, &finish_cv, &max_vid]() {
      for (size_t j = tid; j < num_edges; j += cores) {
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

  auto aligned_max_vid = ceil(max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  Bitmap visited(aligned_max_vid);
  visited.clear();
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges, &visited,
                        &pending_packages, &finish_cv, &max_vid]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        visited.set_bit(dst_v[j]);
        visited.set_bit(src_v[j]);
        write_max(&max_vid, dst_v[j]);
        write_max(&max_vid, src_v[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  size_t num_vertexes = visited.get_num_bit();
  size_t* outdegree = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  size_t* indegree = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(outdegree, 0, sizeof(size_t) * aligned_max_vid);
  memset(indegree, 0, sizeof(size_t) * aligned_max_vid);
  size_t sum_outdegree = 0;
  size_t sum_indegree = 0;

  LOG_INFO("Aggregate indegree and outdegree");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    thread_pool.Commit([i, &cores, &src_v, &dst_v, &src, &dst, &outdegree,
                        &indegree, &num_edges, &pending_packages,
                        &finish_cv]() {
      for (size_t j = i; j < num_edges; j += cores) {
        __sync_fetch_and_add(indegree + dst->at(j), 1);
        __sync_fetch_and_add(outdegree + src->at(j), 1);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Compute Maximum degree");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &indegree, &outdegree, &pending_packages,
                        &sum_indegree, &sum_outdegree, &finish_cv,
                        &aligned_max_vid, &max_indegree, &max_outdegree,
                        &max_degree]() {
      for (size_t j = tid; j < aligned_max_vid; j += cores) {
        write_max(&max_outdegree, outdegree[j]);
        write_max(&max_indegree, indegree[j]);
        write_max(&max_degree, indegree[j] + outdegree[j]);
        write_add(&sum_indegree, indegree[j]);
        write_add(&sum_outdegree, outdegree[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  std::ofstream ofs;
  ofs.open(output_pt, std::ios::out);

  LOG_INFO("#maximum vid: ", max_vid);
  LOG_INFO("#maximum indegree: ", max_indegree);
  LOG_INFO("#maximum outdegree: ", max_outdegree);
  LOG_INFO("#maximum degree: ", max_degree);
  LOG_INFO("#sum outdegree: ", sum_outdegree);
  LOG_INFO("#sum indegree: ", sum_indegree);
  LOG_INFO("#avg outdegree: ", sum_outdegree / num_vertexes);
  LOG_INFO("#avg indegree: ", sum_indegree / num_vertexes);
  LOG_INFO("#num_edges: ", num_edges);
  LOG_INFO("#num_vertexes: ", num_vertexes);

  ofs << "#maximum vid: " << max_vid << std::endl;
  ofs << "#maximum indegree: " << max_indegree << std::endl;
  ofs << "#maximum outdegree: " << max_outdegree << std::endl;
  ofs << "#maximum degree: " << max_degree << std::endl;
  ofs << "#maximum degree: " << max_degree << std::endl;
  ofs << "#sum indegree: " << sum_indegree << std::endl;
  ofs << "#sum outdegree: " << sum_outdegree << std::endl;
  ofs << "#avg_in_degree: " << sum_indegree / num_vertexes << std::endl;
  ofs << "#avg_out_degree: " << sum_outdegree / num_vertexes << std::endl;
  ofs << "#num_edges:" << num_edges << std::endl;
  ofs << "#num_vertexes:" << num_vertexes << std::endl;

  ofs.close();
  return;
}

template <typename VID_T>
void GetGraphStatisticFromBin(const std::string input_pt,
                              const std::string output_pt, const size_t cores) {
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edge_list_io_adapter;
  std::string input_meta_pt = input_pt + "minigraph_meta" + ".bin";
  std::string input_data_pt = input_pt + "minigraph_data" + ".bin";
  std::string input_vdata_pt = input_pt + "minigraph_vdata" + ".bin";
  std::string dst_meta_pt = output_pt + "minigraph_meta" + ".bin";
  std::string dst_data_pt = output_pt + "minigraph_data" + ".bin";
  std::string dst_vdata_pt = output_pt + "minigraph_vdata" + ".bin";
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);
  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  auto graph = new EDGE_LIST_T;
  edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edgelist_bin, ' ', 0,
                            input_meta_pt, input_data_pt, input_vdata_pt);

  size_t num_edges = graph->num_edges_;
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);

  LOG_INFO("read: ", num_edges, " edges");
  VID_T max_vid(0);
  size_t max_indegree(0);
  size_t max_outdegree(0);
  size_t max_degree(0);

  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges, &graph,
                        &pending_packages, &finish_cv, &max_vid]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        src_v[j] = graph->buf_graph_[j * 2];
        dst_v[j] = graph->buf_graph_[j * 2 + 1];
        write_max(&max_vid, graph->buf_graph_[j * 2 + 1]);
        write_max(&max_vid, graph->buf_graph_[j * 2]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  VID_T aligned_max_vid = ceil(max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
  size_t* outdegree = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  size_t* indegree = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
  memset(outdegree, 0, sizeof(size_t) * aligned_max_vid);
  memset(indegree, 0, sizeof(size_t) * aligned_max_vid);
  size_t sum_outdegree = 0;
  size_t sum_indegree = 0;

  Bitmap visited(aligned_max_vid);
  visited.clear();
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &num_edges, &visited,
                        &pending_packages, &finish_cv, &max_vid]() {
      for (size_t j = tid; j < num_edges; j += cores) {
        visited.set_bit(dst_v[j]);
        visited.set_bit(src_v[j]);
        write_max(&max_vid, dst_v[j]);
        write_max(&max_vid, src_v[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  size_t num_vertexes = visited.get_num_bit();
  LOG_INFO("Aggregate indegree and outdegree");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    thread_pool.Commit([i, &cores, &src_v, &dst_v, &outdegree, &indegree,
                        &num_edges, &pending_packages, &finish_cv]() {
      for (size_t j = i; j < num_edges; j += cores) {
        __sync_fetch_and_add(indegree + dst_v[j], 1);
        __sync_fetch_and_add(outdegree + src_v[j], 1);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Compute Maximum degree");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &indegree, &outdegree, &pending_packages,
                        &finish_cv, &aligned_max_vid, &max_indegree,
                        &sum_outdegree, &sum_indegree, &max_outdegree,
                        &max_degree]() {
      for (size_t j = tid; j < aligned_max_vid; j += cores) {
        if (max_indegree < indegree[j]) {
          write_max(&max_indegree, indegree[j]);
        }
        if (max_outdegree < outdegree[j]) {
          write_max(&max_outdegree, outdegree[j]);
        }
        write_add(&sum_indegree, indegree[j]);
        write_add(&sum_outdegree, outdegree[j]);
        write_max(&max_degree, indegree[j] + outdegree[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  std::ofstream ofs;
  ofs.open(output_pt, std::ios::out);

  LOG_INFO("#maximum vid: ", max_vid);
  LOG_INFO("#maximum indegree: ", max_indegree);
  LOG_INFO("#maximum outdegree: ", max_outdegree);
  LOG_INFO("#maximum degree: ", max_degree);
  LOG_INFO("#sum outdegree: ", sum_outdegree);
  LOG_INFO("#sum indegree: ", sum_indegree);
  LOG_INFO("#num_edges: ", num_edges);
  LOG_INFO("#num_vertexes: ", num_vertexes);

  ofs << "#maximum vid: " << max_vid << std::endl;
  ofs << "#maximum indegree: " << max_indegree << std::endl;
  ofs << "#maximum outdegree: " << max_outdegree << std::endl;
  ofs << "#maximum degree: " << max_degree << std::endl;
  ofs << "#maximum degree: " << max_degree << std::endl;
  ofs << "#sum indegree: " << sum_indegree << std::endl;
  ofs << "#sum outdegree: " << sum_outdegree << std::endl;
  ofs << "#num_edges: " << num_edges << std::endl;
  ofs << "#num_vertexes: " << num_vertexes << std::endl;
  ofs << "#avg_in_degree: " << sum_indegree / num_vertexes << std::endl;
  ofs << "#avg_out_degree: " << sum_outdegree / num_vertexes << std::endl;

  ofs.close();
  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  assert(FLAGS_i != "" && FLAGS_o != "");
  size_t cores = FLAGS_cores;

  LOG_INFO("Statistic: ", FLAGS_i);
  if (FLAGS_frombin)
    GetGraphStatisticFromBin<vid_t>(FLAGS_i, FLAGS_o, cores);
  else
    GetGraphStatisticFromCSV<vid_t>(FLAGS_i, FLAGS_o, *(FLAGS_sep.c_str()),
                                    cores, FLAGS_edges);
  gflags::ShutDownCommandLineFlags();
}
