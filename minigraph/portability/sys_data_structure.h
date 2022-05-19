#pragma once

#include <condition_variable>
#include <string>

#include <folly/AtomicHashMap.h>

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"

struct CSRPt {
  std::string meta_pt;
  std::string data_pt;
};

template <typename AutoApp, typename GID_T, typename VID_T, typename VDATA_T,
          typename EDATA_T>
class AppWrapper {
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  AppWrapper(AutoApp* auto_app) { auto_app_ = auto_app; }
  AppWrapper() = default;

  void InitBorderVertexes(
      std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes,
      std::unordered_map<VID_T, VertexInfo*>* global_border_vertexes_info) {
    auto_app_->global_border_vertexes_ = global_border_vertexes;
    auto_app_->global_border_vertexes_info_ = global_border_vertexes_info;
  }

  AutoApp* auto_app_ = nullptr;
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat {
  edge_list_csv,
  weight_edge_list_csv,
  edge_list_bin,
  csr_bin,
  immutable_csr_bin
};