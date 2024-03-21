#ifndef MINIGRAPH_GRAPHS_EDGE_LIST_H
#define MINIGRAPH_GRAPHS_EDGE_LIST_H

#include "graphs/graph.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/AtomicUnorderedMap.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Range.h>
#include <folly/portability/Asm.h>
#include <folly/portability/Atomic.h>
#include <folly/portability/SysTime.h>
#include <jemalloc/jemalloc.h>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class EdgeList : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  EdgeList(const GID_T gid = 0, const size_t num_edges = 0,
           const size_t num_vertexes = 0, VID_T max_vid = 0,
           VID_T* buf_graph = nullptr)
      : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    this->gid_ = gid;
    this->num_edges_ = num_edges;
    this->max_vid_ = max_vid;
    this->aligned_max_vid_ =
        ceil(max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    this->num_vertexes_ = num_vertexes;

    if (num_vertexes != 0) {
      this->num_vertexes_ = num_vertexes;
      this->vdata_ =
          (VDATA_T*)malloc(sizeof(VDATA_T) * this->get_num_vertexes());
      memset((char*)this->vdata_, 0,
             sizeof(VDATA_T) * this->get_num_vertexes());
      globalid_by_localid_ =
          (VID_T*)malloc(sizeof(VID_T) * this->get_num_vertexes());
      memset((char*)globalid_by_localid_, 0,
             sizeof(VID_T) * this->get_num_vertexes());
    }
    if (num_edges != 0) {
      this->buf_graph_ =
          (VID_T*)malloc(sizeof(VID_T) * this->get_num_edges() * 2);
      memset((char*)this->buf_graph_, 0,
             sizeof(VID_T) * this->get_num_edges() * 2);
    }
    if (buf_graph != nullptr) {
      memcpy(this->buf_graph_, buf_graph,
             this->get_num_edges() * sizeof(VID_T) * 2);
    }
  }

  ~EdgeList() = default;

  void CleanUp() override {
    if (vertexes_info_ != nullptr) {
      std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*> tmp;
      vertexes_info_->swap(tmp);
      delete vertexes_info_;
      vertexes_info_ = nullptr;
    }
  };

  void ShowGraph(const size_t count = 2) {
    std::cout << "\n\n##### EdgeListGraph GID: " << this->get_gid()
              << ", num_verteses: " << this->get_num_vertexes()
              << ", num_edges: " << this->get_num_edges()
              << ", max_vid: " << this->get_max_vid() << " #####" << std::endl;
    std::cout << ">>>> edges_ " << std::endl;
    for (size_t i = 0; i < this->get_num_edges(); i++) {
      if (i > count) break;
      std::cout << "src: " << *(this->buf_graph_ + i * 2)
                << ", dst: " << *(this->buf_graph_ + i * 2 + 1) << std::endl;
    }
    std::cout << ">>>> vdata_ " << std::endl;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (i > count) break;
      std::cout << "vid: " << i << ", vdata_: " << this->vdata_[i] << std::endl;
    }
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByVid(const VID_T vid) {
    auto iter = vertexes_info_->find(vid);
    if (iter != vertexes_info_->end()) {
      iter->second->vdata = this->vdata_ + index_by_vid_[vid];
      iter->second->state = vertexes_state_ + index_by_vid_[vid];
      return *iter->second;
    } else {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
      return vertex_info;
    }
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByIndex(
      const size_t index) {
    auto vid = vid_by_index_[index];
    auto iter = vertexes_info_->find(vid);
    if (iter != vertexes_info_->end()) {
      iter->second->vdata = this->vdata_ + index;
      iter->second->state = vertexes_state_ + index;
      return *iter->second;
    } else {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
      return vertex_info;
    }
  }

  VID_T globalid2localid(const VID_T vid) const {
    auto local_id_iter = map_globalid2localid_->find(vid);
    if (local_id_iter != map_globalid2localid_->end()) {
      return local_id_iter->second;
    } else {
      return VID_MAX;
    }
  }

  VID_T localid2globalid(const VID_T vid) const {
    if (globalid_by_localid_ == nullptr) {
      LOG_ERROR("segmentation fault: globalid_by_localid_ is nullptr");
    }
    if (vid > this->get_num_vertexes() + 1) return VID_MAX;
    return globalid_by_localid_[vid];
  }

  std::unordered_map<VID_T, GID_T>* GetVertexesThatRequiredByOtherGraphs() {
    std::unordered_map<VID_T, GID_T>* border_vertexes =
        new std::unordered_map<VID_T, GID_T>();

    VID_T local_id = 0;
    for (size_t i = 0; i < this->get_num_edges(); i++) {
      auto src = *(this->buf_graph_ + i * 2);
      if (map_globalid2localid_->find(src) == map_globalid2localid_->end())
        map_globalid2localid_->insert(std::make_pair(src, local_id++));
    }
    for (size_t i = 0; i < this->get_num_edges(); i++) {
      auto src = *(this->buf_graph_ + i * 2);
      auto dst = *(this->buf_graph_ + i * 2 + 1);
      if (map_globalid2localid_->find(dst) == map_globalid2localid_->end())
        border_vertexes->insert(std::make_pair(src, this->get_gid()));
    }
    return border_vertexes;
  }

  EdgeList* GetClassType(void) override { return this; }

 public:
  size_t* index_by_vid_ = nullptr;
  VID_T* vid_by_index_ = nullptr;
  // Bitmap* bitmap_ = nullptr;
  VID_T* globalid_by_localid_ = nullptr;

  char* vertexes_state_ = nullptr;
  bool is_serialized_ = true;

  std::map<VID_T, VertexInfo*>* vertexes_info_ = nullptr;
  std::unordered_map<VID_T, VID_T>* map_globalid2localid_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif
