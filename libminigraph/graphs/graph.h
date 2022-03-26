
#ifndef MINIGRAPH_GRAPHS_GRAPH_H
#define MINIGRAPH_GRAPHS_GRAPH_H

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

template <typename VID_T, typename VDATA_T, typename EDATA_T>
struct Message {
  std::shared_ptr<std::unordered_map<VID_T, VDATA_T>> updated_val_by_vid;
  // ... to be filled.
};

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class Graph {
 public:
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
