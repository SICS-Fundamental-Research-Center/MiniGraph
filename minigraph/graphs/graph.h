
#ifndef MINIGRAPH_GRAPHS_GRAPH_H
#define MINIGRAPH_GRAPHS_GRAPH_H

#include <folly/FBString.h>
#include <folly/Range.h>
#include <iostream>
#include <string>
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
struct vertex {
  VID_T& vid;
  size_t& outdegree_ = 0;
  size_t& indegree_ = 0;
  VDATA_T& label_;
  VID_T* edges_;
};

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class Graph {
 public:
  explicit Graph(GID_T gid) {
    std::cout << "Graph()" << std::endl;
    gid_ = gid;
  };
  inline size_t get_num_vertexes() { return num_vertexes_; };
  inline size_t get_num_in_edges() { return num_in_edges_; };
  inline size_t get_num_out_edges() { return num_out_edges_; };

  size_t num_vertexes_ = 0;
  size_t num_in_edges_ = 0;
  size_t num_out_edges_ = 0;
  GID_T gid_ = -1;
  VID_T* vertexes_ = nullptr;
  VID_T* in_offset_ = nullptr;
  VID_T* in_edges_ = nullptr;
  VID_T* out_offset_ = nullptr;
  VID_T* out_edges_ = nullptr;
  VDATA_T* vdata_ = nullptr;
  VID_T* localid_to_globalid_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif