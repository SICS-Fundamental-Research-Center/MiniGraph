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
      // for (size_t tid = 0; tid < graph->get_num_vertexes(); tid +=
      // num_worker_)
      // run vertex centric operations.
      // VertexReduce();
  };

  void Bind(GRAPH_T* graph, utility::CPUThreadPool* cpu_thread_pool) {
    graph_ = graph;
    cpu_thread_pool_ = cpu_thread_pool;
  }

 private:
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;
  GRAPH_T* graph_ = nullptr;
  CONTEXT_T context_;

  virtual void VertexReduce(const CONTEXT_T& context) = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
