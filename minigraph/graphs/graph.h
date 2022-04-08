
#ifndef MINIGRAPH_GRAPHS_GRAPH_H
#define MINIGRAPH_GRAPHS_GRAPH_H

#include <folly/AtomicHashMap.h>
#include <folly/FBString.h>
#include <folly/Range.h>
#include <iostream>
#include <string>
#include <unordered_map>

namespace minigraph {
namespace graphs {

template <typename VID_T, typename VDATA_T, typename EDATA_T>
struct VertexInfo {
  VID_T vid;
  size_t outdegree = 0;
  size_t indegree = 0;
  VID_T* in_edges = nullptr;
  VID_T* out_edges = nullptr;
  VDATA_T* vdata = nullptr;
  EDATA_T* edata = nullptr;
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

  explicit Graph(GID_T gid) {
    std::cout << "Graph()" << std::endl;
    gid_ = gid;
  };
  explicit Graph() {}
  inline GID_T get_gid() const { return gid_; }
  virtual size_t get_num_vertexes() const = 0;

 private:
  GID_T gid_ = -1;
};

}  // namespace graphs
}  // namespace minigraph
#endif
