#pragma once

#include <math.h>

#include <condition_variable>
#include <string>
#include <vector>

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "utility/thread_pool.h"
#include <folly/AtomicHashMap.h>


struct CSRPt {
  std::string meta_pt;
  std::string data_pt;
};

struct EdgeListPt {
  std::string edges_pt;
  std::string v_label_pt;
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

template <typename VID_T>
class PartialMatch {
 public:
  size_t x_ = 0;
  size_t y_ = 0;
  VID_T* meta_ = nullptr;
  VID_T* matching_solutions_ = nullptr;

  VID_T* meta_to_add_ = nullptr;

  std::vector<VID_T>* vec_meta_ = nullptr;
  std::vector<std::vector<VID_T>*>* vec_matching_solutions = nullptr;

  PartialMatch() = default;
  PartialMatch(size_t x, size_t y) {
    x_ = x;
    y_ = y;
    meta_ = (VID_T*)malloc(sizeof(VID_T) * x_);
    memset(meta_, 0, sizeof(VID_T) * x_);
    meta_to_add_ = new VID_T;
    *meta_to_add_ = VID_MAX;
    matching_solutions_ = (VID_T*)malloc(sizeof(VID_T) * y * x_);
    memset(matching_solutions_, 0, sizeof(VID_T) * y_ * x_);

    vec_meta_ = new std::vector<VID_T>;
    vec_matching_solutions = new std::vector<std::vector<VID_T>*>;

  };

  ~PartialMatch() = default;

  VID_T GetStateId() {
    VID_T state_id = 0;
    for (size_t i = 0; i < vec_meta_->size(); i++) {
      state_id += vec_meta_->at(i) * std::pow(10, (i + 1));
    }
    return state_id;
  }

  void ShowPartialMatch() {
    LOG_INFO("Show Partial Match -  x: ", x_, " y: ", y_);
    std::cout << "  meta: " << std::endl;
    for (size_t i = 0; i < x_; i++) {
      std::cout << meta_[i] << ", ";
    }
    std::cout << std::endl;
    std::cout << "  solution: " << std::endl;
    for (size_t i = 0; i < y_; i++) {
      for (size_t j = 0; j < x_; j++) {
        std::cout << *(matching_solutions_ + i * x_ + j) << ", ";
      }
      std::cout << std::endl;
    }
  }

  bool IsInMeta(VID_T vid) {
    for (size_t i = 0; i < vec_meta_->size(); i++) {
      if (vec_meta_->at(i) == vid) return true;
    }
    return false;
  };

 // bool IsInCurrentMatchingSolution(VID_T vid) {
 //   for (size_t i = 0; i < current_matching_solution_->size(); i++) {
 //     if (current_matching_solution_->at(i) == vid) return true;
 //   }
 //   return false;
 // }
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat {
  edge_list_csv,
  weight_edge_list_csv,
  edge_list_bin,
  csr_bin,
  immutable_csr_bin
};