#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <math.h>
#include <random>
#include <rapidcsv.h>
#include <string>
#include <unistd.h>

template <typename VID_T>
void GraphReduce(const std::string input_pt, const std::string output_pt,
                 const size_t cores, char separator_params = ',') {
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
  std::atomic max_vid_atom(0);

  LOG_INFO("Run: get maximum vid");
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
  max_vid_atom.store((int(max_vid_atom.load() / 64) + 1) * 64);
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

  rapidcsv::Document doc_out("", rapidcsv::LabelParams(0, -1),
                             rapidcsv::SeparatorParams(',', false, false));
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
  delete src;
  delete dst;
  std::cout << "Save at " << output_pt << std::endl;
  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  size_t cores = FLAGS_cores;

  assert(FLAGS_i != "" && FLAGS_o != "");
  LOG_INFO("Reduce: ", FLAGS_i);
  GraphReduce<vid_t>(FLAGS_i, FLAGS_o, cores, ',');
  gflags::ShutDownCommandLineFlags();
}