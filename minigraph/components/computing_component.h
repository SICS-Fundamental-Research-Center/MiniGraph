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
      const size_t num_workers, utility::EDFThreadPool* thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>* app_wrapper,
      std::unique_lock<std::mutex>* task_queue_lck,
      std::unique_lock<std::mutex>* partial_result_lck,
      std::condition_variable* task_queue_cv,
      std::condition_variable* partial_result_cv

      )
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    num_workers_.store(num_workers);
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    partial_result_queue_ = partial_result_queue;
    app_wrapper_ = app_wrapper;
    task_queue_lck_ = task_queue_lck;
    partial_result_lck_ = partial_result_lck;
    task_queue_cv_ = task_queue_cv;
    partial_result_cv_ = partial_result_cv;
    scheduled_executor_ =
        std::make_unique<executors::ScheduledExecutor>(kTotalParallelism);

    executor_mtx_ = std::make_unique<std::mutex>();
    executor_lck_ =
        std::make_unique<std::unique_lock<std::mutex>>(*executor_mtx_.get());
    executor_cv_ = std::make_unique<std::condition_variable>();
    XLOG(INFO, "Init ComputingComponent: Finish. TotalParallelism: ",
         kTotalParallelism);
  };

  ~ComputingComponent() = default;

  void Run() override {
    while (this->switch_.load(std::memory_order_seq_cst)) {
      std::vector<GID_T> vec_gid;
      GID_T gid = MINIGRAPH_GID_MAX;
      task_queue_cv_->wait(*task_queue_lck_, [&] {
        return !task_queue_->isEmpty() ||
               !this->switch_.load(std::memory_order_relaxed);
      });

      if (this->switch_.load(std::memory_order_relaxed)) {
        while (!task_queue_->isEmpty()) {
          while (!task_queue_->read(gid)) {
            continue;
          }
          vec_gid.push_back(gid);
        }
        task_queue_cv_->notify_all();
        for (size_t i = 0; i < vec_gid.size(); i++) {
          gid = vec_gid.at(i);
          executor_cv_->wait(*executor_lck_,
                             [&] { return this->num_workers_.load() >= 1; });
          auto task = std::bind(
              &components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T,
                                              GRAPH_T, AUTOAPP_T>::ProcessGraph,
              this, gid);
          this->num_workers_.fetch_sub(1);
          this->thread_pool_->Commit(task);
        }
      } else {
        LOG_INFO("CC exit");
        return;
      }
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  std::atomic<size_t> num_workers_;
  std::atomic<bool> switch_ = true;

  // task_queue.
  folly::ProducerConsumerQueue<GID_T>* task_queue_;

  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_;

  // data manager.
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;

  // 2D-PIE app wrapper.
  APP_WARP* app_wrapper_ = nullptr;

  // cv && lck.
  std::unique_ptr<executors::ScheduledExecutor> scheduled_executor_ = nullptr;
  std::unique_lock<std::mutex>* partial_result_lck_;
  std::unique_lock<std::mutex>* task_queue_lck_;
  std::condition_variable* partial_result_cv_;
  std::condition_variable* task_queue_cv_;

  std::unique_ptr<std::mutex> executor_mtx_;
  std::unique_ptr<std::unique_lock<std::mutex>> executor_lck_;
  std::unique_ptr<std::condition_variable> executor_cv_;

  void ProcessGraph(const GID_T& gid) {
    GRAPH_T* graph = (GRAPH_T*)data_mngr_->GetGraph(gid);
    executors::TaskRunner* task_runner =
        scheduled_executor_->RequestTaskRunner({1, kTotalParallelism});

    PARTIAL_RESULT_T* partial_result = new PARTIAL_RESULT_T;
    if (this->get_superstep_via_gid(gid) == 0) {
      app_wrapper_->auto_app_->PEval(*graph, partial_result, task_runner)
          ? this->state_machine_->ProcessEvent(gid, CHANGED)
          : this->state_machine_->ProcessEvent(gid, NOTHINGCHANGED);
    } else {
      app_wrapper_->auto_app_->IncEval(*graph, partial_result, task_runner)
          ? this->state_machine_->ProcessEvent(gid, CHANGED)
          : this->state_machine_->ProcessEvent(gid, NOTHINGCHANGED);
    }
    partial_result_cv_->wait(*partial_result_lck_,
                             [&] { return !partial_result_queue_->isFull(); });
    scheduled_executor_->RecycleTaskRunner(task_runner);
    while (!partial_result_queue_->write(gid)) {
      continue;
    }
    this->add_superstep_via_gid(gid);
    this->num_workers_.fetch_add(1);
    delete partial_result;
    partial_result_cv_->notify_all();
    executor_cv_->notify_all();
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
