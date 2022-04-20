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
#include <memory>
#include <unordered_map>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class AutoAppBase {
  using VertexMap_T = VertexMapBase<GRAPH_T, CONTEXT_T>;
  using EdgeMap_T = EdgeMapBase<GRAPH_T, CONTEXT_T>;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

 public:
  // AutoAppBase() = default;
  AutoAppBase(VertexMap_T* vertex_map, EdgeMap_T* edge_map,
              const CONTEXT_T& context) {
    edge_map_ = edge_map;
    vertex_map_ = vertex_map;
    context_ = context;
  }
  virtual ~AutoAppBase() { free(visited_); };

  //
  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The PEval in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph
  virtual bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T& partial_result) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The IncEval in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param graph
  virtual bool IncEval(GRAPH_T& graph, PARTIAL_RESULT_T& partial_result) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The MsgAggr in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param Message
  virtual void MsgAggr(std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>&
                           partial_border_vertexes_info) = 0;

  void Bind(executors::TaskRunner* task_runner) { task_runner_ = task_runner; }

  void Bind(folly::AtomicHashMap<typename GRAPH_T::vid_t, VertexInfo*>*
                global_border_vertexes_info,
            folly::AtomicHashMap<typename GRAPH_T::vid_t,
                                 typename GRAPH_T::gid_t>* border_vertexes) {
    global_border_vertexes_info_ = global_border_vertexes_info;
    global_border_vertexes_ = border_vertexes;
  }

  EdgeMap_T* edge_map_ = nullptr;
  VertexMap_T* vertex_map_ = nullptr;

  CONTEXT_T context_;
  std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>*
      global_border_vertexes_info_ = nullptr;
  std::unordered_map<typename GRAPH_T::vid_t,
                     std::vector<typename GRAPH_T::gid_t>*>*
      global_border_vertexes_ = nullptr;
  executors::TaskRunner* task_runner_;
  bool* visited_ = nullptr;

 protected:
  bool GetPartialBorderResult(GRAPH_T& graph, bool* visited,
                              PARTIAL_RESULT_T& partial_result) {
    assert(visited != nullptr);
    bool tag = false;
    int count = 0;
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      if (visited[i] == true) {
        if (count++ == 0) {
          LOG_INFO(i);
        }
        tag == false ? tag = true : 0;
        auto globalid = graph.localid2globalid(i);
        if (global_border_vertexes_->find(globalid) !=
            global_border_vertexes_->end()) {
          auto iter = global_border_vertexes_info_->find(globalid);
          if (iter == global_border_vertexes_info_->end()) {
            // tag == false ? tag = true : 0;
            VertexInfo* vertex_info = graph.CopyVertex(i);
            partial_result.insert(std::make_pair(globalid, vertex_info));
          } else {
            if (iter->second->get_vdata() == graph.GetVertex(i).get_vdata()) {
              continue;
            } else {
              // tag == false ? tag = true : 0;
              VertexInfo* vertex_info = graph.CopyVertex(i);
              partial_result.insert(std::make_pair(globalid, vertex_info));
            }
          }
        }
      }
    }
    LOG_INFO("tag: ", tag, "count: ", count);
    return tag;
  }
};

}  // namespace minigraph

#endif  // MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
