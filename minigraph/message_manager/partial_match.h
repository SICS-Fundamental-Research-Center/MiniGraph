#ifndef MINIGRAPH_PARTIAL_MATCH_H
#define MINIGRAPH_PARTIAL_MATCH_H

#include <unordered_map>
#include <vector>

#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"


namespace minigraph {
namespace message {

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

  std::unordered_map<GID_T, std::vector<std::vector<VID_T>*>*>*
      partial_matching_solutions_by_gid_ = nullptr;

  GID_T* globalid2gid_ = nullptr;
  size_t num_graphs = 0;
  size_t maximum_vid_ = 0;
  std::mutex* mtx_;

  PartialMatch(GID_T* globalid2gid = nullptr) {
    vec_meta_ = new std::vector<VID_T>;
    vec_matching_solutions = new std::vector<std::vector<VID_T>*>;
    partial_matching_solutions_by_gid_ =
        new std::unordered_map<GID_T, std::vector<std::vector<VID_T>*>*>;
    mtx_ = new std::mutex;
    globalid2gid_ = globalid2gid;
  }

  PartialMatch(size_t x, size_t y, GID_T* globalid2gid = nullptr) {
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

    partial_matching_solutions_by_gid_ =
        new std::unordered_map<GID_T, std::vector<std::vector<VID_T>*>*>;
    mtx_ = new std::mutex;
    globalid2gid_ = globalid2gid;
  }

  ~PartialMatch() = default;

  void BufferPartialResults(
      std::vector<std::vector<VID_T>*>& partial_matching_solutions) {
    mtx_->lock();
    for (size_t i = 0; i < partial_matching_solutions.size(); i++) {
      auto v_vid = partial_matching_solutions.at(i)->back();
      GID_T&& gid = Globalvid2Gid(v_vid);
      auto iter = partial_matching_solutions_by_gid_->find(gid);
      if (iter != partial_matching_solutions_by_gid_->end()) {
        iter->second->push_back(partial_matching_solutions.at(i));
      } else {
        auto partial_matching_solution_of_X =
            new std::vector<std::vector<VID_T>*>;
        partial_matching_solution_of_X->push_back(
            partial_matching_solutions.at(i));
        partial_matching_solutions_by_gid_->insert(
            std::make_pair(gid, partial_matching_solution_of_X));
      }
    }
    mtx_->unlock();
  }

  void BufferResults(std::vector<std::vector<VID_T>*>& matching_solutions) {
    mtx_->lock();
    vec_matching_solutions->insert(vec_matching_solutions->end(),
                                   matching_solutions.begin(),
                                   matching_solutions.end());
    mtx_->unlock();
    return;
  }

  VID_T GetStateId(std::vector<VID_T>& meta) {
    VID_T state_id = 0;
    for (size_t i = 0; i < meta.size(); i++) {
      state_id += meta.at(i) * std::pow(10, (i));
    }
    return state_id;
  }

  std::vector<VID_T> StateId2Meta(VID_T state_id) {
    std::vector<VID_T> meta;
    while (state_id != 0) {
      meta->push_back(state_id % 10);
      state_id /= 10;
    }
    std::reverse(meta.begin(), meta.end());
    return meta;
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

  void ShowMatchingSolutions(size_t count = 10) {
    LOG_INFO("*********Show Matched Solutions: ", vec_matching_solutions->size(), "***************");
    for (auto& iter : *vec_matching_solutions) {
      if(count-- < 0)
        break;
      std::cout << "          ";
      for (size_t i = 0; i < iter->size(); i++) {
        std::cout << iter->at(i) << ", ";
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

  std::vector<std::vector<VID_T>*>* GetPartialMatchingSolutionsofX(GID_T gid) {
    auto iter = partial_matching_solutions_by_gid_->find(gid);
    if (iter != partial_matching_solutions_by_gid_->end()) {
      return iter->second;
    } else {
      return nullptr;
    }
  }

 private:
  GID_T Globalvid2Gid(VID_T vid) {
    if (globalid2gid_ == nullptr)
      LOG_ERROR("segmentatino fault: globalid2gid_ is nullptr");
    return globalid2gid_[vid];
  }
};

}  // namespace message
}  // namespace minigraph

#endif  // MINIGRAPH_PARTIAL_MATCH_H
