#include "portability/sys_types.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"
#include <gflags/gflags.h>
#include <cstring>
#include <iostream>
#include <math.h>
#include <rapidcsv.h>
#include <string>

template <typename VID_T>
void GetGraphStatistic(const std::string input_pt, const std::string output_pt,
                       const size_t cores, char separator_params = ',',
                       const size_t expected_edges = 1073741824) {
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);
  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);

  rapidcsv::Document* doc =
      new rapidcsv::Document(input_pt, rapidcsv::LabelParams(),
                             rapidcsv::SeparatorParams(separator_params));
  std::vector<VID_T>* src = new std::vector<VID_T>();
  std::vector<VID_T>* dst = new std::vector<VID_T>();
  src->reserve(expected_edges);
  dst->reserve(expected_edges);
  *src = doc->GetColumn<VID_T>("src");
  *dst = doc->GetColumn<VID_T>("dst");

  size_t num_edges = src->size();
  VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
  memset(src_v, 0, sizeof(VID_T) * num_edges);
  memset(dst_v, 0, sizeof(VID_T) * num_edges);

  LOG_INFO("read: ", num_edges, " edges");
  std::atomic<VID_T> max_vid_atom(0);
  std::atomic<size_t> max_indegree(0);
  std::atomic<size_t> max_outdegree(0);
  std::atomic<size_t> max_degree(0);

  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst,
                        &pending_packages, &finish_cv, &max_vid_atom]() {
      for (size_t j = tid; j < src->size(); j += cores) {
        dst_v[j] = dst->at(j);
        src_v[j] = src->at(j);
        if (max_vid_atom.load() < dst_v[j]) max_vid_atom.store(dst_v[j]);
        if (max_vid_atom.load() < src_v[j]) max_vid_atom.store(src_v[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("MAXVID: ", max_vid_atom.load());
  max_vid_atom.store((int(max_vid_atom.load() / 64) + 1) * 64);

  size_t* outdegree = (size_t*)malloc(sizeof(size_t) * max_vid_atom.load());
  size_t* indegree = (size_t*)malloc(sizeof(size_t) * max_vid_atom.load());
  memset(outdegree, 0, sizeof(size_t) * max_vid_atom.load());
  memset(indegree, 0, sizeof(size_t) * max_vid_atom.load());

  LOG_INFO("Aggregate indegree and outdegree");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &src_v, &dst_v, &outdegree, &indegree,
                        &num_edges, &pending_packages, &finish_cv,
                        &max_vid_atom]() {
      for (size_t j = tid; j < num_edges; j += cores) {
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
                        &finish_cv, &max_vid_atom, &max_indegree,
                        &max_outdegree, &max_degree]() {
      for (size_t j = tid; j < max_vid_atom.load(); j += cores) {
        if (indegree[j] > max_indegree.load()) max_indegree.store(indegree[j]);
        if (outdegree[j] > max_outdegree.load())
          max_outdegree.store(outdegree[j]);
        if (outdegree[j] + indegree[j] > max_degree.load())
          max_degree.store(outdegree[j] + indegree[j]);
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  std::ofstream ofs;
  ofs.open(output_pt, std::ios::out);

  LOG_INFO("#maximum vid: ", max_vid_atom.load());
  LOG_INFO("#maximum indegree: ", max_indegree.load());
  LOG_INFO("#maximum outdegree: ", max_outdegree.load());
  LOG_INFO("#maximum degree: ", max_degree.load());
  LOG_INFO("#num_edges: ", num_edges);

  ofs << "#maximum vid: " << max_vid_atom.load() << std::endl;
  ofs << "#maximum indegree: " << max_indegree.load() << std::endl;
  ofs << "#maximum outdegree: " << max_outdegree.load() << std::endl;
  ofs << "#maximum degree: " << max_degree.load() << std::endl;
  ofs << "#num_edges:" << num_edges << std::endl;

  ofs.close();
  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  assert(FLAGS_i != "" && FLAGS_o != "" && FLAGS_edges != 0);
  size_t cores = FLAGS_cores;
  LOG_INFO("Statistic: ", FLAGS_i);
  GetGraphStatistic<vid_t>(FLAGS_i, FLAGS_o, cores, *(FLAGS_sep.c_str()),
                           FLAGS_edges);
  gflags::ShutDownCommandLineFlags();
}