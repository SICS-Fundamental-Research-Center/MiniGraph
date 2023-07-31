#ifndef MINIGRAPH_GRAPHS_GRAPH_H
#define MINIGRAPH_GRAPHS_GRAPH_H

#include <iostream>
#include <string>
#include <unordered_map>

#include "portability/sys_types.h"
#include "utility/bitmap.h"

#include <folly/AtomicHashMap.h>
#include <folly/FBString.h>
#include <folly/Range.h>

namespace minigraph {
namespace graphs {

template <typename VID_T, typename VDATA_T, typename EDATA_T>
class VertexInfo {
 public:
  VID_T vid;
  size_t outdegree = 0;
  size_t indegree = 0;
  VID_T* in_edges = nullptr;
  VID_T* out_edges = nullptr;
  VDATA_T* vdata = nullptr;
  EDATA_T* edata = nullptr;
  char* state = nullptr;

  VertexInfo() = default;
  ~VertexInfo() = default;

  void ShowVertexAbs(const VID_T& globalid = -1) const {
    if (vdata == nullptr) {
      std::cout << " localid: " << vid << ", globalid: " << globalid
                << ", outdegree: " << outdegree << ", indegree: " << indegree
                << std::endl;
    } else {
      std::cout << " localid: " << vid << ", globalid: " << globalid
                << ", label: " << vdata[0] << ", outdegree: " << outdegree
                << ", indegree: " << indegree << std::endl;
    }
  }
  void ShowVertexInfo(const VID_T& globalid = -1) const {
    if (vdata == nullptr) {
      std::cout << " localid: " << vid << ", globalid: " << globalid
                << ", outdegree: " << outdegree << ", indegree: " << indegree
                << std::endl;
    } else {
      std::cout << " localid: " << vid << ", globalid: " << globalid
                << ", label: " << vdata[0] << ", outdegree: " << outdegree
                << ", indegree: " << indegree << std::endl;
    }
    if (indegree > 0) {
      std::cout << "in_edges: ";
      for (size_t i = 0; i < indegree; i++) std::cout << in_edges[i] << ", ";
      std::cout << std::endl;
    }
    if (outdegree > 0) {
      std::cout << "out_edges: ";
      for (size_t i = 0; i < outdegree; i++) std::cout << out_edges[i] << ", ";
      std::cout << std::endl;
    }
    std::cout << "----------------------------" << std::endl;
  }
};

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class Graph {
 public:
  typedef VID_T vid_t;
  typedef GID_T gid_t;
  typedef VDATA_T vdata_t;
  typedef EDATA_T edata_t;

  explicit Graph(GID_T gid) { gid_ = gid; }
  explicit Graph() {}

  inline bool IsInGraph(const VID_T globalid) const {
    assert(bitmap_ != nullptr);
    if (globalid > bitmap_->size_) {
      return false;
    }
    return bitmap_->get_bit(globalid) != 0;
  }

  void InitVdata2AllX(const VDATA_T init_vdata = 0) {
    if (vdata_ == nullptr) {
      vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * get_num_vertexes());
      if (init_vdata != 0)
        for (size_t i = 0; i < this->get_num_vertexes(); i++)
          vdata_[i] = init_vdata;
      else
        memset(vdata_, 0, sizeof(VDATA_T) * this->get_num_vertexes());
    } else {
      memset(vdata_, 0, sizeof(VDATA_T) * this->get_num_vertexes());
      if (init_vdata != 0)
        for (size_t i = 0; i < this->get_num_vertexes(); i++)
          vdata_[i] = init_vdata;
    }
  }

  void InitVdataByVid() {
    if (vdata_ == nullptr) {
      vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * get_num_vertexes());
      memset(vdata_, 0, sizeof(VDATA_T) * this->get_num_vertexes());
      for (size_t i = 0; i < this->get_num_vertexes(); i++) vdata_[i] = i;

    } else {
      memset(vdata_, 0, sizeof(VDATA_T) * this->get_num_vertexes());
      for (size_t i = 0; i < this->get_num_vertexes(); i++) vdata_[i] = i;
    }
  }

  virtual void CleanUp() = 0;
  virtual ~Graph() = default;
  virtual Graph* GetClassType(void) = 0;

  inline void set_num_edges(const size_t n) { num_edges_ = n; };
  inline void set_num_vertexes(const size_t n) { num_vertexes_ = n; };
  inline GID_T get_gid() const { return gid_; }
  inline size_t get_num_vertexes() const { return num_vertexes_; }
  inline size_t get_num_edges() const { return num_edges_; }
  inline VID_T get_max_vid() const { return max_vid_; }
  inline size_t get_aligned_max_vid() const {
    return ceil(aligned_max_vid_ / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
  }

 public:
  GID_T gid_ = -1;
  size_t num_vertexes_ = 0;
  VID_T max_vid_ = 0;
  VID_T aligned_max_vid_ = 0;
  size_t num_edges_ = 0;
  VDATA_T* vdata_ = nullptr;
  EDATA_T* edata_ = nullptr;
  Bitmap* bitmap_ = nullptr;
  VID_T* buf_graph_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph

#endif  // MINIGRAPH_GRAPHS_GRAPH_H
