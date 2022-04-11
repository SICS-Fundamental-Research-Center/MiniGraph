//
// Created by hsiaoko on 2022/3/18.
//
#pragma once

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"
#include <folly/AtomicHashMap.h>
#include <string>

struct CSRPt {
  std::string vertex_pt;
  std::string meta_in_pt;
  std::string meta_out_pt;
  std::string vdata_pt;
  std::string localid2globalid_pt;
  std::string msg_pt;
};

template <typename AutoApp, typename VID_T, typename GID_T, typename VDATA_T,
          typename EDATA_T>
class AppWrapper {
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  AppWrapper(AutoApp* auto_app) { auto_app_ = auto_app; }
  AppWrapper() = default;

  void InitBorderVertexes(folly::AtomicHashMap<VID_T, std::vector<GID_T>*>*
                              global_border_vertexes) {
    auto_app_->global_border_vertexes_info_ =
        new folly::AtomicHashMap<VID_T, VertexInfo*>(1024);
    auto_app_->global_border_vertexes_ = global_border_vertexes;
  }

  AutoApp* auto_app_ = nullptr;
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat { edge_graph_csv, weight_edge_graph_csv, csr_bin };