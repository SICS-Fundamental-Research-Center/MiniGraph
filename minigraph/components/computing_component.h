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
  using APP_WARP = AppWrapper<AUTOAPP_T>;

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
      const size_t& num_wroker_cc, AppWrapper<AUTOAPP_T>* app_wrapper)
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
    XLOG(INFO, "COMPUTING COMPONENT: RUN()");
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid;
      while (!task_queue_->read(gid)) {
        if (this->switch_.load(std::memory_order_relaxed) == false) {
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      GRAPH_T* graph = (GRAPH_T*)data_mngr_->GetGraph(gid);

      // RUN APP
      ProcessGraph(gid, graph, 5, this->get_superstep_via_gid(gid),
                   this->cpu_thread_pool_, app_wrapper_);
    };
  }

  void Stop() override { this->switch_.store(false); }

 private:
  // configuration
  std::unique_ptr<std::atomic<size_t>> num_idle_workers_ = nullptr;

  // global message in shared memory
  graphs::Message<VID_T, VDATA_T, EDATA_T>* global_msg_;

  // task_queue
  folly::ProducerConsumerQueue<GID_T>* task_queue_;

  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_;

  // data manager
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;

  std::atomic<bool> switch_ = true;

  // 2D-PIE>
  APP_WARP* app_wrapper_ = nullptr;

  void ProcessGraph(const GID_T& gid, GRAPH_T* graph, const size_t& num_workers,
                    const size_t& step, utility::CPUThreadPool* cpu_thread_pool,
                    APP_WARP* app_warpper) {
    utility::CPUThreadPool* thread_pool = new utility::CPUThreadPool(2, 1);
    app_warpper->auto_app_->Bind(graph, thread_pool);
    LOG_INFO("PROCESS");
    if (step == 0) {
      app_warpper->auto_app_->PEval();
    } else {
      // app_warpper->auto_app_->IncEval();
    }
  }

  void Enqueue(GID_T gid,
               folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
               std::atomic<size_t>* num_idle_workers, APP_WARP* app_wrapper) {
    XLOG(INFO, gid);
  };
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
