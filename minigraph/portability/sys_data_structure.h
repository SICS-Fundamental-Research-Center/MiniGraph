#pragma once

#include <condition_variable>
#include <string>

#include <folly/AtomicHashMap.h>

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "utility/thread_pool.h"

struct CSRPt {
  std::string meta_pt;
  std::string data_pt;
};

template <typename VID_T, typename GID_T>
class VertexDependencies {
 public:
  VID_T vid_;
  std::vector<GID_T>* who_need_ = nullptr;
  std::vector<GID_T>* who_provide_ = nullptr;

  VertexDependencies(const VID_T vid) {
    vid_ = vid;
    who_need_ = new std::vector<GID_T>;
    who_provide_ = new std::vector<GID_T>;
  };
  ~VertexDependencies() = default;

  void ShowVertexDependencies() {
    std::cout << "___vid: " << vid_ << "___" << std::endl;

    if (who_need_ != nullptr) {
      std::cout << "   who need: ";
      for (auto& iter : *who_need_) {
        std::cout << iter << ", ";
      }
      std::cout << std::endl;
    }

    if (who_provide_ != nullptr) {
      std::cout << "   who provide: ";
      for (auto& iter : *who_provide_) {
        std::cout << iter << ", ";
      }
      std::cout << std::endl;
    }
    return;
  }
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
      std::unordered_map<VID_T, VertexInfo*>* global_border_vertexes_info,
      std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>*
          global_border_vertexes_with_dependencies,
      bool* communication_matrix) {
    auto_app_->Bind(global_border_vertexes, global_border_vertexes_info,
                    global_border_vertexes_with_dependencies,
                    communication_matrix);
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