#ifndef MINIGRAPH_COMPUTING_COMPONENT_H
#define MINIGRAPH_COMPUTING_COMPONENT_H

#include "components/component_base.h"
#include "graphs/immutable_csr.h"
#include "utility/io/data_mngr.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <memory>

namespace minigraph {
namespace components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T, typename AUTOAPP_T>
class ComputingComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using APP_WARP = AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  ComputingComponent(
      utility::CPUThreadPool* cpu_thread_pool,
      utility::IOThreadPool* io_thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      const size_t& num_wroker_cc,
      AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>* app_wrapper)
      : ComponentBase<GID_T>(cpu_thread_pool, io_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    partial_result_queue_ = partial_result_queue;
    num_idle_workers_ = std::make_unique<std::atomic<size_t>>(num_wroker_cc);
    app_wrapper_ = app_wrapper;
    XLOG(INFO, "Init ComputingComponent: Finish.");
  };

  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid = GID_MAX;
      while (!task_queue_->read(gid)) {
        // spin until we get a value
        if (this->switch_.load(std::memory_order_relaxed) == false) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      LOG_INFO("gid:", gid);
      // auto cpu_thread_pool = new utility::CPUThreadPool(2, 1);
      // auto task = std::bind(
      //     &components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T,
      //                                     GRAPH_T, AUTOAPP_T>::ProcessGraph,
      //     this, gid, cpu_thread_pool, partial_result_queue_, data_mngr_,
      //     app_wrapper_, this->state_machine_);
      // this->cpu_thread_pool_->Commit(task);
      ProcessGraph(gid, this->cpu_thread_pool_, partial_result_queue_,
                   data_mngr_, app_wrapper_, this->state_machine_);
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  // configuration
  std::unique_ptr<std::atomic<size_t>> num_idle_workers_ = nullptr;

  // global message in shared memory
  // graphs::Message<VID_T, VDATA_T, EDATA_T>* global_msg_;

  // task_queue
  folly::ProducerConsumerQueue<GID_T>* task_queue_;

  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_;

  // data manager
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;

  std::atomic<bool> switch_ = true;

  // 2D-PIE>
  APP_WARP* app_wrapper_ = nullptr;

  void ProcessGraph(
      const GID_T& gid, utility::CPUThreadPool* cpu_thread_pool,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      APP_WARP* app_wrapper, utility::StateMachine<GID_T>* state_machine) {
    utility::CPUThreadPool* thread_pool = new utility::CPUThreadPool(2, 1);
    app_wrapper->auto_app_->Bind((GRAPH_T*)data_mngr->GetGraph(gid),
                                 thread_pool);
    if (this->get_superstep_via_gid(gid) == 0) {
      app_wrapper->auto_app_->PEval()
          ? state_machine->ProcessEvent(gid, CHANGED)
          : state_machine->ProcessEvent(gid, NOTHINGCHANGED);
    } else {
      app_wrapper->auto_app_->IncEval()
          ? state_machine->ProcessEvent(gid, CHANGED)
          : state_machine->ProcessEvent(gid, NOTHINGCHANGED);
    }
    this->add_superstep_via_gid(gid);
    app_wrapper->auto_app_->MsgAggr(
        app_wrapper->auto_app_->global_border_vertexes_info_,
        app_wrapper->auto_app_->partial_border_vertexes_info_);

    if (state_machine->GraphIs(gid, RC)) {
      partial_result_queue->write(gid);
    }
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
