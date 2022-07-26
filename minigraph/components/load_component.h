#ifndef MINIGRAPH_LOAD_COMPONENT_H
#define MINIGRAPH_LOAD_COMPONENT_H

#include <condition_variable>
#include <memory>
#include <queue>
#include <string>

#include <folly/ProducerConsumerQueue.h>
#include <folly/synchronization/NativeSemaphore.h>

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/state_machine.h"
#include "utility/thread_pool.h"


namespace minigraph {
namespace components {

template <typename GRAPH_T>
class LoadComponent : public ComponentBase<typename GRAPH_T::gid_t> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  LoadComponent(
      const size_t num_workers, folly::NativeSemaphore* sem_lc_dc,
      utility::EDFThreadPool* thread_pool,
      std::unordered_map<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      std::queue<GID_T>* read_trigger,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::io::DataMngr<GRAPH_T>* data_mngr,
      std::unique_lock<std::mutex>* read_trigger_lck,
      std::unique_lock<std::mutex>* task_queue_lck,
      std::condition_variable* read_trigger_cv,
      std::condition_variable* task_queue_cv)
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    num_workers_.store(num_workers);
    sem_lc_dc_ = sem_lc_dc;
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
    while (switch_) {
      GID_T gid = MINIGRAPH_GID_MAX;
      read_trigger_cv_->wait(*read_trigger_lck_, [&] { return true; });
      if (!switch_) return;
      while (!read_trigger_->empty()) {
        sem_lc_dc_->wait();
        gid = read_trigger_->front();
        read_trigger_->pop();
        this->ProcessGraph(gid);
      }
    }
  }

  void Stop() override { switch_ = false; }

 private:
  std::atomic<size_t> num_workers_;
  folly::NativeSemaphore* sem_lc_dc_ = nullptr;
  std::queue<GID_T>* read_trigger_ = nullptr;
  folly::ProducerConsumerQueue<GID_T>* task_queue_ = nullptr;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_;
  utility::io::DataMngr<GRAPH_T>* data_mngr_ = nullptr;
  bool switch_ = true;
  std::unique_lock<std::mutex>* read_trigger_lck_;
  std::unique_lock<std::mutex>* task_queue_lck_;
  std::condition_variable* read_trigger_cv_;
  std::condition_variable* task_queue_cv_;

  std::unique_ptr<std::mutex> executor_mtx_;
  std::unique_ptr<std::unique_lock<std::mutex>> executor_lck_;
  std::unique_ptr<std::condition_variable> executor_cv_;

  void ProcessGraph(GID_T gid) {
    CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
    auto tag = false;
    if (IsSameType<GRAPH_T, CSR_T>())
      tag = this->data_mngr_->ReadGraph(gid, csr_pt, csr_bin);
    else if (IsSameType<GRAPH_T, EDGE_LIST_T>())
      tag = this->data_mngr_->ReadGraph(gid, csr_pt, edge_list_bin);
    if (tag) {
      this->state_machine_->ProcessEvent(gid, LOAD);
      while (!task_queue_->write(gid))
        ;
      task_queue_cv_->notify_all();
    } else {
      this->state_machine_->ProcessEvent(gid, UNLOAD);
      LOG_ERROR("Read graph fault: ", gid);
    }
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_LOAD_COMPONENT_H
