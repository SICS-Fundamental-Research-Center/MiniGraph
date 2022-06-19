#pragma once

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "utility/thread_pool.h"
#include <folly/AtomicHashMap.h>
#include <condition_variable>
#include <math.h>
#include <string>
#include <vector>
//#include "message_manager/default_message_manager.h"

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

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class PartialMatch {
 public:
  size_t x_ = 0;
  size_t y_ = 0;
  VID_T* meta_ = nullptr;
  VID_T* matching_solutions_ = nullptr;

  VID_T* meta_to_add_ = nullptr;

  std::vector<VID_T>* vec_meta_ = nullptr;

  // In case of the buffer is full, vec_matching_solutions need to be flushed to
  // the secondary storage.
  std::vector<std::vector<VID_T>*>* vec_matching_solutions = nullptr;

  // persistent in main memory.
  std::unordered_map<
      GID_T,
      std::vector<minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*>*
      partial_result_ = nullptr;

  PartialMatch() {
    vec_meta_ = new std::vector<VID_T>;
    vec_matching_solutions = new std::vector<std::vector<VID_T>*>;
    partial_result_ = new std::unordered_map<
        GID_T,
        std::vector<minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*>;
  }

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

    partial_result_ = new std::unordered_map<
        GID_T,
        std::vector<minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*>;
  }

  PartialMatch(size_t x) {
    x_ = x;
    meta_ = (VID_T*)malloc(sizeof(VID_T) * x_);
    memset(meta_, 0, sizeof(VID_T) * x_);
    meta_to_add_ = new VID_T;
    *meta_to_add_ = VID_MAX;

    vec_meta_ = new std::vector<VID_T>;
    vec_matching_solutions = new std::vector<std::vector<VID_T>*>;
    partial_result_ = new std::unordered_map<
        GID_T,
        std::vector<minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*>;
  }

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
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat {
  edge_list_csv,
  weight_edge_list_csv,
  edge_list_bin,
  csr_bin,
  immutable_csr_bin
};