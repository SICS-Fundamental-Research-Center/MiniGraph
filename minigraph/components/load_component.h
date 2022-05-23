#ifndef MINIGRAPH_LOAD_COMPONENT_H
#define MINIGRAPH_LOAD_COMPONENT_H

#include <condition_variable>
#include <memory>
#include <string>

#include <folly/ProducerConsumerQueue.h>

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/state_machine.h"
#include "utility/thread_pool.h"


namespace minigraph::components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T>
class LoadComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  LoadComponent(
      const size_t num_workers, utility::EDFThreadPool* thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* read_trigger,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      std::unique_lock<std::mutex>* read_trigger_lck,
      std::unique_lock<std::mutex>* task_queue_lck,
      std::condition_variable* read_trigger_cv,
      std::condition_variable* task_queue_cv)
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    num_workers_.store(num_workers);
    pt_by_gid_ = pt_by_gid;
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    read_trigger_ = read_trigger;
    read_trigger_lck_ = read_trigger_lck;
    task_queue_lck_ = task_queue_lck;
    read_trigger_cv_ = read_trigger_cv;
    task_queue_cv_ = task_queue_cv;

    executor_mtx_ = std::make_unique<std::mutex>();
    executor_lck_ =
        std::make_unique<std::unique_lock<std::mutex>>(*executor_mtx_.get());
    executor_cv_ = std::make_unique<std::condition_variable>();
    XLOG(INFO, "Init LoadComponent: Finish.");
  }

  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed)) {
      std::vector<GID_T> vec_gid;
      GID_T gid = MINIGRAPH_GID_MAX;
      read_trigger_cv_->wait(*read_trigger_lck_, [&] {
        return !this->switch_.load(std::memory_order_relaxed) ||
               !read_trigger_->isEmpty();
      });
      if (this->switch_.load()) {
        while (!read_trigger_->isEmpty()) {
          while (!read_trigger_->read(gid)) {
            continue;
          }
          vec_gid.push_back(gid);
        }
        for (size_t i = 0; i < vec_gid.size(); i++) {
          gid = vec_gid.at(i);
          CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
          executor_cv_->wait(*executor_lck_,
                             [&] { return this->num_workers_.load() >= 1; });
          auto task = std::bind(
              &components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T,
                                         GRAPH_T>::ProcessGraph,
              this, gid, csr_pt);
          this->thread_pool_->Commit(task);
          this->num_workers_.fetch_sub(1);
          executor_cv_->notify_all();
        }
      } else {
        LOG_INFO("LC exit", task_queue_->isEmpty());
        return;
      }
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  std::atomic<size_t> num_workers_;

  folly::ProducerConsumerQueue<GID_T>* read_trigger_;
  folly::ProducerConsumerQueue<GID_T>* task_queue_;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_;
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;
  std::atomic<bool> switch_ = true;
  std::unique_lock<std::mutex>* read_trigger_lck_;
  std::unique_lock<std::mutex>* task_queue_lck_;
  std::condition_variable* read_trigger_cv_;
  std::condition_variable* task_queue_cv_;

  std::unique_ptr<std::mutex> executor_mtx_;
  std::unique_ptr<std::unique_lock<std::mutex>> executor_lck_;
  std::unique_ptr<std::condition_variable> executor_cv_;

  void ProcessGraph(GID_T gid, CSRPt& csr_pt) {
    if (this->data_mngr_->ReadGraph(gid, csr_pt, csr_bin)) {
      executor_cv_->wait(*executor_lck_, [&] { return true; });
      task_queue_cv_->wait(*task_queue_lck_,
                           [&] { return !task_queue_->isFull(); });
      while (!task_queue_->write(gid)) {
        continue;
      }
      task_queue_cv_->notify_one();
      this->state_machine_->ProcessEvent(gid, LOAD);
    } else {
      this->state_machine_->ProcessEvent(gid, UNLOAD);
      LOG_ERROR("Read graph fault: ", gid);
    }
    this->num_workers_.fetch_add(1);
    executor_cv_->notify_all();
    read_trigger_cv_->notify_all();
  }
};

}  // namespace minigraph::components
#endif  // MINIGRAPH_LOAD_COMPONENT_H
