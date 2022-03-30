#ifndef MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H
#define MINIGRAPH_2D_PIE_EDGE_MAP_REDUCE_H

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class EdgeMapBase {
 public:
  EdgeMapBase() = default;
  ~EdgeMapBase() = default;

  void EdgeMap(GRAPH_T* graph, const CONTEXT_T& context) {
    //  run vertex centric operations.
    LOG_INFO("EdgeMap", graph->get_num_vertexes());

    graph->Deserialized();
    graph->ShowGraph();
    // EdgeReduce();
  };

 private:
  size_t num_worker_;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;

  virtual void EdgeReduce(const CONTEXT_T& context) = 0;
};

}  // namespace minigraph
#endif  // MINIGRAPH_2d_PIE_EDGE_MAP_REDUCE_H
