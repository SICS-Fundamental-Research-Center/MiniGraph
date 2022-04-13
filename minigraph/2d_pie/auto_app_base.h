#ifndef MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
#define MINIGRAPH_2D_PIE_AUTO_APP_BASE_H

#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "executors/scheduled_executor.h"
#include "executors/scheduler.h"
#include "executors/task_runner.h"
#include "executors/throttle.h"
#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"
#include <folly/MPMCQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class AutoAppBase {
  using VertexMap_T = VertexMapBase<GRAPH_T, CONTEXT_T>;
  using EdgeMap_T = EdgeMapBase<GRAPH_T, CONTEXT_T>;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;

 public:
  // AutoAppBase() = default;
  AutoAppBase(VertexMap_T* vertex_map, EdgeMap_T* edge_map,
              const CONTEXT_T& context) {
    vertex_map_ = vertex_map;
    edge_map_ = edge_map;
    context_ = context;
    partial_border_vertexes_info_ =
        new folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>(1024);
  }
  virtual ~AutoAppBase() { free(visited_); };

  typedef folly::DMPMCQueue<VertexInfo, false> Frontier;

  //
  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The PEval in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph
  virtual bool PEval() = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The IncEval in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param graph
  virtual bool IncEval() = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The MsgAggr in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param Message
  virtual void MsgAggr(
      folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>*
          global_border_vertexes_info,
      folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>*
          partial_border_vertexes_info) = 0;

  void Bind(GRAPH_T* graph, executors::TaskRunner* task_runner) {
    graph_ = graph;
    edge_map_->Bind(graph);
    vertex_map_->Bind(graph);
    visited_ = (bool*)malloc(sizeof(bool) * graph_->get_num_vertexes());
    task_runner_ = task_runner;
  }

  void Bind(folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>*
                global_border_vertexes_info,
            folly::AtomicHashMap<typename GRAPH_T::vid_t,
                                 typename GRAPH_T::gid_t>* border_vertexes) {
    global_border_vertexes_info_ = global_border_vertexes_info;
    global_border_vertexes_ = border_vertexes;
  }

  EdgeMapBase<GRAPH_T, CONTEXT_T>* edge_map_ = nullptr;
  VertexMapBase<GRAPH_T, CONTEXT_T>* vertex_map_ = nullptr;
  GRAPH_T* graph_ = nullptr;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;
  CONTEXT_T context_;
  folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>*
      partial_border_vertexes_info_ = nullptr;
  folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>*
      global_border_vertexes_info_ = nullptr;
  folly::AtomicHashMap<typename GRAPH_T::vid_t,
                       std::vector<typename GRAPH_T::gid_t>*>*
      global_border_vertexes_ = nullptr;
  executors::TaskRunner* task_runner_;
  bool* visited_ = nullptr;

 protected:
  bool WriteResult() {
    bool tag = false;
    if (visited_ == nullptr || graph_ == nullptr ||
        partial_border_vertexes_info_ == nullptr ||
        global_border_vertexes_info_ == nullptr) {
      return false;
    }
    for (size_t i = 0; i < graph_->get_num_vertexes(); i++) {
      if (visited_[i] == true) {
        tag == false ? tag = true : 0;
        auto globalid = graph_->localid2globalid(i);
        if (global_border_vertexes_->find(globalid) !=
            global_border_vertexes_->end()) {
          auto iter = global_border_vertexes_info_->find(globalid);
          if (iter == global_border_vertexes_info_->end()) {
            VertexInfo* vertex_info = graph_->CopyVertex(globalid);
            partial_border_vertexes_info_->insert(globalid, vertex_info);
          } else {
            VertexInfo* vertex_info = graph_->CopyVertex(globalid);
            partial_border_vertexes_info_->insert(globalid, vertex_info);
          }
        }
      }
    }
    return tag;
  }
};

}  // namespace minigraph

#endif  // MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
