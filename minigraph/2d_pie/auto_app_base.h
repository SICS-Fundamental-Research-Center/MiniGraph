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
#include "message_manager/default_message_manager.h"
#include "utility/thread_pool.h"
#include <folly/MPMCQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <memory>
#include <unordered_map>

namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class AutoAppBase {
  using VMap_T = VMapBase<GRAPH_T, CONTEXT_T>;
  using EMap_T = EMapBase<GRAPH_T, CONTEXT_T>;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

 public:
  AutoAppBase(VMap_T* vmap, EMap_T* emap, const CONTEXT_T& context) {
    emap_ = emap;
    vmap_ = vmap;
    context_ = context;
  }

  //
  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The Init in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph
  virtual bool Init(GRAPH_T& graph) = 0;

  //
  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The PEval in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph, partial_result
  virtual bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
                     executors::TaskRunner* task_runner) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The IncEval in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param graph, partial_result
  virtual bool IncEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
                       executors::TaskRunner* task_runner) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The MsgAggr in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param Message
  virtual bool MsgAggr(std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>*
                           partial_border_vertexes_info) = 0;

  void Bind(std::unordered_map<typename GRAPH_T::vid_t,
                               std::vector<typename GRAPH_T::gid_t>*>*
                global_border_vertexes,
            std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>*
                global_border_vertexes_info,
            std::unordered_map<typename GRAPH_T::vid_t,
                               VertexDependencies<typename GRAPH_T::vid_t,
                                                  typename GRAPH_T::gid_t>*>*
                global_border_vertexes_with_dependencies,
            bool* communication_matrix) {
    global_border_vertexes_info_ = global_border_vertexes_info;
    global_border_vertexes_ = global_border_vertexes;
    global_border_vertexes_with_dependencies_ =
        global_border_vertexes_with_dependencies;
    communication_matrix_ = communication_matrix;
  }

  void Bind(message::DefaultMessageManager<
            typename GRAPH_T::gid_t, typename GRAPH_T::vid_t,
            typename GRAPH_T::vdata_t, typename GRAPH_T::edata_t>* msg_mngr) {
    msg_mngr_ = msg_mngr;
  }

  EMap_T* emap_ = nullptr;
  VMap_T* vmap_ = nullptr;

  CONTEXT_T context_;
  std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>*
      global_border_vertexes_info_ = nullptr;
  std::unordered_map<typename GRAPH_T::vid_t,
                     std::vector<typename GRAPH_T::gid_t>*>*
      global_border_vertexes_ = nullptr;
  std::unordered_map<
      typename GRAPH_T::vid_t,
      VertexDependencies<typename GRAPH_T::vid_t, typename GRAPH_T::gid_t>*>*
      global_border_vertexes_with_dependencies_ = nullptr;

  message::DefaultMessageManager<
      typename GRAPH_T::gid_t, typename GRAPH_T::vid_t,
      typename GRAPH_T::vdata_t, typename GRAPH_T::edata_t>* msg_mngr_ =
      nullptr;

  bool* communication_matrix_ = nullptr;

 protected:
  bool GetPartialBorderResult(GRAPH_T& graph, bool* visited,
                              PARTIAL_RESULT_T* partial_result) {
    assert(visited != nullptr);
    bool tag = false;
    if (global_border_vertexes_->size() == 0) {
      return true;
    }
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      if (visited[i] == true) {
        tag == false ? tag = true : 0;
        auto globalid = graph.Index2Globalid(i);
        if (global_border_vertexes_->find(globalid) !=
            global_border_vertexes_->end()) {
          partial_result->insert(
              std::make_pair(globalid, graph.GetPVertexByIndex(i)));
        }
      }
    }
    return tag;
  }
};

template <typename AutoApp, typename GID_T, typename VID_T, typename VDATA_T,
          typename EDATA_T>
class AppWrapper {
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  AutoApp* auto_app_ = nullptr;

  message::DefaultMessageManager<GID_T, VID_T, VDATA_T, EDATA_T>* msg_mngr_;

  AppWrapper(AutoApp* auto_app) { auto_app_ = auto_app; }
  AppWrapper() = default;

  void InitBorderVertexes(
      std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes,
      std::unordered_map<VID_T, VertexInfo*>* global_border_vertexes_info,
      std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>*
          global_border_vertexes_with_dependencies,
      bool* communication_matrix) {
    auto_app_->Bind(global_border_vertexes, global_border_vertexes_info,
                    global_border_vertexes_with_dependencies,
                    communication_matrix);
  }

  void InitMsgMngr(message::DefaultMessageManager<GID_T, VID_T, VDATA_T,
                                                  EDATA_T>* msg_mngr) {
    msg_mngr_ = msg_mngr;
    auto_app_->Bind(msg_mngr_);
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
