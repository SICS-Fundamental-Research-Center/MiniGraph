#ifndef MINIGRAPH_COMPUTING_COMPONENT_H
#define MINIGRAPH_COMPUTING_COMPONENT_H

#include "components/component_base.h"
#include "executors/scheduled_executor.h"
#include "executors/scheduler.h"
#include "executors/task_runner.h"
#include "graphs/immutable_csr.h"
#include "utility/io/data_mngr.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <condition_variable>
#include <memory>

namespace minigraph {
namespace components {

static const auto kTotalParallelism = std::thread::hardware_concurrency();

template <typename GRAPH_T, typename AUTOAPP_T>
class ComputingComponent : public ComponentBase<typename GRAPH_T::gid_t> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using APP_WARP = AppWrapper<AUTOAPP_T, GRAPH_T>;
  using VertexInfo =
      graphs::VertexInfo<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t,
                         typename GRAPH_T::edata_t>;

 public:
  ComputingComponent(
      const size_t num_workers, const size_t num_cores,
      utility::EDFThreadPool* thread_pool,
      std::unordered_map<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      std::queue<GID_T>* partial_result_queue,
      utility::io::DataMngr<GRAPH_T>* data_mngr,
      AppWrapper<AUTOAPP_T, GRAPH_T>* app_wrapper,
      std::unique_lock<std::mutex>* task_queue_lck,
      std::condition_variable* task_queue_cv,
      std::condition_variable* partial_result_cv)
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    num_workers_ = num_workers;
    num_cores_ = num_cores;
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    partial_result_queue_ = partial_result_queue;
    app_wrapper_ = app_wrapper;
    task_queue_lck_ = task_queue_lck;
    task_queue_cv_ = task_queue_cv;
    partial_result_cv_ = partial_result_cv;
    scheduled_executor_ =
        std::make_unique<executors::ScheduledExecutor>(kTotalParallelism);
    p_ = (size_t*)malloc(sizeof(size_t) * superstep_by_gid->size());
    for (size_t i = 0; i < superstep_by_gid->size(); i++)
      p_[i] = (size_t) num_cores / num_workers;

    
    XLOG(INFO,
         "Init ComputingComponent: Finish. TotalParallelism: ", num_cores_);
  };

  ~ComputingComponent() = default;

  void Run() override {
    LOG_INFO("Run CC");
    folly::NativeSemaphore sem(num_workers_);
    std::queue<GID_T> que_gid;
    while (this->switch_.load()) {
      std::vector<GID_T> vec_gid;
      GID_T gid = MINIGRAPH_GID_MAX;
      task_queue_cv_->wait(*task_queue_lck_,
                           [&] { return !task_queue_->isEmpty(); });
      if (!this->switch_.load()) return;
      while (!task_queue_->read(gid))
        ;
      vec_gid.push_back(gid);
      que_gid.push(gid);
      while (!que_gid.empty()) {
        gid = que_gid.front();
        que_gid.pop();
        sem.try_wait();
        auto task = std::bind(
            &components::ComputingComponent<GRAPH_T, AUTOAPP_T>::ProcessGraph,
            this, gid, sem);
        this->thread_pool_->Commit(task);
      }
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  size_t num_workers_ = 0;
  size_t num_cores_ = 0;
  size_t* p_ = nullptr;
  std::atomic<bool> switch_ = true;

  // task_queue.
  folly::ProducerConsumerQueue<GID_T>* task_queue_ = nullptr;
  std::queue<GID_T>* partial_result_queue_ = nullptr;

  // data manager.
  utility::io::DataMngr<GRAPH_T>* data_mngr_ = nullptr;

  // 2D-PIE app wrapper.
  APP_WARP* app_wrapper_ = nullptr;

  // cv && lck.
  std::unique_ptr<executors::ScheduledExecutor> scheduled_executor_ = nullptr;
  std::unique_lock<std::mutex>* task_queue_lck_;
  std::condition_variable* partial_result_cv_ = nullptr;
  std::condition_variable* task_queue_cv_ = nullptr;

  std::unique_ptr<std::mutex> executor_mtx_;

  void ProcessGraph(const GID_T& gid, folly::NativeSemaphore& sem) {
    LOG_INFO("ProcessGraph: ", gid);
    // executor_cv_->wait(*executor_lck_, [&] { return true; });
    GRAPH_T* graph = (GRAPH_T*)data_mngr_->GetGraph(gid);
    executors::TaskRunner* task_runner = scheduled_executor_->RequestTaskRunner(
        {1, (unsigned)p_[gid]});
    if (this->get_superstep_via_gid(gid) == 0) {
      app_wrapper_->auto_app_->Init(*graph, task_runner);
      app_wrapper_->auto_app_->PEval(*graph, task_runner);
      this->state_machine_->ProcessEvent(gid, CHANGED);
    } else {
      app_wrapper_->auto_app_->IncEval(*graph, task_runner)
          ? this->state_machine_->ProcessEvent(gid, CHANGED)
          : this->state_machine_->ProcessEvent(gid, NOTHINGCHANGED);
    }
    scheduled_executor_->RecycleTaskRunner(task_runner);
    this->add_superstep_via_gid(gid);
    partial_result_queue_->push(gid);
    partial_result_cv_->notify_all();
    sem.post();
    return;
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
