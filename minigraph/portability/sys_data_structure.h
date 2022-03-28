//
// Created by hsiaoko on 2022/3/18.
//
#pragma once

#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"
#include <string>

struct CSRPt {
  std::string vertex_pt;
  std::string meta_in_pt;
  std::string meta_out_pt;
  std::string vdata_pt;
  std::string localid2globalid_pt;
  std::string msg_pt;
};

template <typename AutoApp>
class AppWrapper {
 public:
  AppWrapper(AutoApp* auto_app) { auto_app_ = auto_app; }
  AppWrapper() = default;

  AutoApp* auto_app_ = nullptr;

  void BindThreadPool(minigraph::utility::CPUThreadPool* thread_pool) {
    thread_pool_ = thread_pool;
  };

 private:
  minigraph::utility::CPUThreadPool* thread_pool_ = nullptr;
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat { edge_graph_csv, weight_edge_graph_csv, csr_bin };