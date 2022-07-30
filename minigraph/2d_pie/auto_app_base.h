#ifndef MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
#define MINIGRAPH_2D_PIE_AUTO_APP_BASE_H

#include <memory>
#include <unordered_map>

#include <folly/MPMCQueue.h>
#include <folly/concurrency/DynamicBoundedQueue.h>

#include "2d_pie/auto_map.h"
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


namespace minigraph {

template <typename GRAPH_T, typename CONTEXT_T>
class AutoAppBase {
  using VMap_T = VMapBase<GRAPH_T, CONTEXT_T>;
  using EMap_T = EMapBase<GRAPH_T, CONTEXT_T>;
  using AutoMap_T = AutoMapBase<GRAPH_T, CONTEXT_T>;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;

 public:
  AutoAppBase(VMap_T* vmap, EMap_T* emap, const CONTEXT_T& context) {
    emap_ = emap;
    vmap_ = vmap;
    context_ = context;
  }

  AutoAppBase(VMap_T* vmap, EMap_T* emap, AutoMap_T* auto_map,
              const CONTEXT_T& context) {
    emap_ = emap;
    vmap_ = vmap;
    auto_map_ = auto_map;
    context_ = context;
  }

  AutoAppBase(AutoMap_T* auto_map, const CONTEXT_T& context) {
    auto_map_ = auto_map;
    context_ = context;
  }

  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The Init in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph, task_runner
  virtual bool Init(GRAPH_T& graph, executors::TaskRunner* task_runner) = 0;

  // @brief Partial evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The PEval in the inherited apps would be
  // invoked directly, not via virtual functions.
  //
  // @param graph, task_runner
  virtual bool PEval(GRAPH_T& graph, executors::TaskRunner* task_runner) = 0;

  // @brief Incremental evaluation to implement.
  // @note: This pure virtual function works as an interface, instructing users
  // to implement in the specific app. The IncEval in the inherited apps would
  // be invoked directly, not via virtual functions.
  // @param graph, task_runner
  virtual bool IncEval(GRAPH_T& graph, executors::TaskRunner* task_runner) = 0;

  // Bind message manager that necessary to auto app.
  void Bind(message::DefaultMessageManager<GRAPH_T>* msg_mngr) {
    msg_mngr_ = msg_mngr;
  }

  EMap_T* emap_ = nullptr;
  VMap_T* vmap_ = nullptr;
  AutoMap_T* auto_map_ = nullptr;

  CONTEXT_T context_;

  message::DefaultMessageManager<GRAPH_T>* msg_mngr_ = nullptr;
};

template <typename AutoApp, typename GRAPH_T>
class AppWrapper {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  AutoApp* auto_app_ = nullptr;

  message::DefaultMessageManager<GRAPH_T>* msg_mngr_;

  AppWrapper(AutoApp* auto_app) { auto_app_ = auto_app; }
  AppWrapper() = default;

  void InitMsgMngr(message::DefaultMessageManager<GRAPH_T>* msg_mngr) {
    msg_mngr_ = msg_mngr;
    auto_app_->Bind(msg_mngr_);
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_2D_PIE_AUTO_APP_BASE_H
