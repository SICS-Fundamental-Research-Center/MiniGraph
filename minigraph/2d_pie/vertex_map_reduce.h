#ifndef MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class VertexMapBase {
 public:
  VertexMapBase() = default;
  VertexMapBase(const CONTEXT_T& context) { context_ = context; };
  ~VertexMapBase() = default;

  void VertexMap(){
  };

 private:
  CONTEXT_T context_;

  //virtual void VertexReduce(const CONTEXT_T& context) = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
