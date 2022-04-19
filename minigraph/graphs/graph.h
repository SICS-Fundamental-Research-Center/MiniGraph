
#ifndef MINIGRAPH_GRAPHS_GRAPH_H
#define MINIGRAPH_GRAPHS_GRAPH_H

#include <iostream>
#include <string>
#include <unordered_map>

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
  ~VertexInfo() = default;
  VertexInfo() = default;
  void ShowVertexInfo() {
    std::cout << " vid: " << vid << ", label: " << vdata[0]
              << ", outdegree: " << outdegree << ", indegree: " << indegree
              << std::endl;
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
  void RecycleVertex() {
    if (in_edges != nullptr) {
      free(in_edges);
    }
    if (out_edges != nullptr) {
      free(out_edges);
    }
    if (vdata != nullptr) {
      free(vdata);
    }
    if (edata != nullptr) {
      free(edata);
    }
  }
  VDATA_T get_vdata() { return vdata[0]; }
};

template <typename GID_T, typename VID_T>
struct MSG {
  folly::AtomicHashMap<VID_T, GID_T>* bordet_vertexes = nullptr;  // vertex that
};

template <typename VID_T, typename VDATA_T, typename EDATA_T>
struct Message {
  folly::AtomicHashMap<VID_T, VDATA_T>* msg = nullptr;  // vertex that
  // to add
};

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class Graph {
 public:
  typedef VID_T vid_t;
  typedef GID_T gid_t;
  typedef VDATA_T vdata_t;
  typedef EDATA_T edata_t;

  explicit Graph(GID_T gid) { gid_ = gid; };
  explicit Graph() {}
  inline GID_T get_gid() const { return gid_; }
  virtual size_t get_num_vertexes() const = 0;
  virtual void CleanUp() = 0;

 private:
  GID_T gid_ = -1;
};

}  // namespace graphs
}  // namespace minigraph
#endif
