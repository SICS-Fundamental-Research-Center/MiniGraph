#ifndef MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"

namespace minigraph {

template <typename GRAPH_T>
class VertexMapBase {
 public:
  VertexMapBase() = default;
  ~VertexMapBase() = default;

  void VertexMap(GRAPH_T* graph) {
    for (size_t tid = 0; tid < graph->get_num_vertexes(); tid += num_worker_)
      // run vertex centric operations.
      VertexReduce();
  };

 private:
  size_t num_worker_;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;

  virtual void VertexReduce() = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2D_PIE_VERTEX_MAP_REDUCE_H
