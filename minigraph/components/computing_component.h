#ifndef MINIGRAPH_COMPUTING_COMPONENT_H
#define MINIGRAPH_COMPUTING_COMPONENT_H

#include <condition_variable>
#include <memory>

#include <folly/ProducerConsumerQueue.h>

#include "components/component_base.h"
#include "executors/scheduled_executor.h"
#include "executors/scheduler.h"
#include "executors/task_runner.h"
#include "graphs/immutable_csr.h"
#include "utility/io/data_mngr.h"
#include "utility/thread_pool.h"


namespace minigraph {
namespace components {

static const auto kTotalParallelism = std::thread::hardware_concurrency();

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T, typename AUTOAPP_T>
class ComputingComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using APP_WARP = AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

 public:
  ComputingComponent(
      utility::CPUThreadPool* cpu_thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>* app_wrapper)
      : ComponentBase<GID_T>(cpu_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    partial_result_queue_ = partial_result_queue;
    app_wrapper_ = app_wrapper;
    scheduled_executor_ =
        std::make_unique<executors::ScheduledExecutor>(kTotalParallelism);
    XLOG(INFO, "Init ComputingComponent: Finish. TotalParallelism: ",
         kTotalParallelism);
  };

  ~ComputingComponent() = default;

  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid = GID_MAX;
      while (!task_queue_->read(gid)) {
        // spin until we get a value
        if (this->switch_.load(std::memory_order_relaxed) == false) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      auto task = std::bind(
          &components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T,
                                          GRAPH_T, AUTOAPP_T>::ProcessGraph,
          this, gid, partial_result_queue_, data_mngr_, app_wrapper_,
          this->state_machine_);
      this->cpu_thread_pool_->Commit(task);
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  std::atomic<bool> switch_ = true;

  // task_queue
  folly::ProducerConsumerQueue<GID_T>* task_queue_;

  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_;

  // data manager
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;

  // 2D-PIE>
  APP_WARP* app_wrapper_ = nullptr;

  std::unique_ptr<executors::ScheduledExecutor> scheduled_executor_ = nullptr;

  void ProcessGraph(
      const GID_T& gid,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      APP_WARP* app_wrapper, utility::StateMachine<GID_T>* state_machine) {
    executors::TaskRunner* task_runner =
        scheduled_executor_->RequestTaskRunner(this, {});
    LOG_INFO("ComputingComponent: Process ", gid, "-th graph.");
    app_wrapper->auto_app_->Bind(task_runner);
    GRAPH_T* graph = (GRAPH_T*)data_mngr->GetGraph(gid);
    PARTIAL_RESULT_T* partial_result = new PARTIAL_RESULT_T;
    if (this->get_superstep_via_gid(gid) == 0) {
      app_wrapper->auto_app_->PEval(*graph, partial_result)
          ? state_machine->ProcessEvent(gid, CHANGED)
          : state_machine->ProcessEvent(gid, NOTHINGCHANGED);
    } else {
      app_wrapper->auto_app_->IncEval(*graph, partial_result)
          ? state_machine->ProcessEvent(gid, CHANGED)
          : state_machine->ProcessEvent(gid, NOTHINGCHANGED);
    }
    this->add_superstep_via_gid(gid);
    if (state_machine->GraphIs(gid, RC)) {
      partial_result_queue->write(gid);
    }
    scheduled_executor_->RecycleTaskRunner(this, task_runner);
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
