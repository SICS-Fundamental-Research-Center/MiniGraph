#ifndef MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
#define MINIGRAPH_2D_PIE_AUTO_APP_BASE_H

#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"
#include <folly/MPMCQueue.h>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class AutoAppBase {
  using VertexMap_T = VertexMapBase<GRAPH_T, CONTEXT_T>;
  using EdgeMap_T = EdgeMapBase<GRAPH_T, CONTEXT_T>;
  typedef folly::MPMCQueue<typename GRAPH_T::vid_t> Frontier;

 public:
  // AutoAppBase() = default;
  AutoAppBase(VertexMap_T* vertex_map, EdgeMap_T* edge_map,
              const CONTEXT_T& context) {
    vertex_map_ = vertex_map;
    edge_map_ = edge_map;
    context_ = context;
  }
  virtual ~AutoAppBase() = default;

  //
  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The PEval in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph
  virtual void PEval() = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The IncEval in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param graph
  virtual void IncEval() = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The MsgAggr in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param Message
  virtual void MsgAggr() = 0;

  void Bind(GRAPH_T* graph, utility::CPUThreadPool* cpu_thread_pool) {
    graph_ = graph;
    cpu_thread_pool_ = cpu_thread_pool;
    edge_map_->Bind(graph, cpu_thread_pool);
    vertex_map_->Bind(graph, cpu_thread_pool);
  }

  EdgeMapBase<GRAPH_T, CONTEXT_T>* edge_map_ = nullptr;
  VertexMapBase<GRAPH_T, CONTEXT_T>* vertex_map_ = nullptr;
  GRAPH_T* graph_ = nullptr;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;
  CONTEXT_T context_;
};

}  // namespace minigraph

#endif  // MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
