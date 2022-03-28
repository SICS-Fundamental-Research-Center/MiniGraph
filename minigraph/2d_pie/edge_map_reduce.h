#ifndef MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"

namespace minigraph {

template <typename GRAPH_T>
class EdgeMapBase {
 public:
  EdgeMapBase() = default;
  ~EdgeMapBase() = default;

  void EdgeMap(GRAPH_T* graph) {
    for (size_t tid = 0; tid < graph->get_num_vertexes(); tid += num_worker_)
      // run vertex centric operations.
      EdgeReduce();
  };

 private:
  size_t num_worker_;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;

  virtual void EdgeReduce() = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
