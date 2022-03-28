#ifndef MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
#define MINIGRAPH_2D_PIE_AUTO_APP_BASE_H

#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"

namespace minigraph {

template <typename GRAPH_T>
class AutoAppBase {
  using VertexMap_T = VertexMapBase<GRAPH_T>;
  using EdgeMap_T = EdgeMapBase<GRAPH_T>;

 public:
  AutoAppBase(){};
  AutoAppBase(VertexMap_T* vertex_map, EdgeMap_T* edge_map) {}
  virtual ~AutoAppBase() = default;

  //
  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The PEval in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph
  virtual void PEval(GRAPH_T* graph,
                     utility::CPUThreadPool* cpu_thread_pool) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The IncEval in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param graph
  virtual void IncEval(GRAPH_T* graph,
                       utility::CPUThreadPool* cpu_thread_pool) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The MsgAggr in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param Message
  virtual void MsgAggr() = 0;

 private:
};

}  // namespace minigraph

#endif  // MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
