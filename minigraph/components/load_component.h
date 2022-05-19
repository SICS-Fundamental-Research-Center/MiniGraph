#ifndef MINIGRAPH_LOAD_COMPONENT_H
#define MINIGRAPH_LOAD_COMPONENT_H

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/state_machine.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <memory>
#include <string>

namespace minigraph::components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T>
class LoadComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  LoadComponent(
      utility::EDFThreadPool* thread_pool,
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
    pt_by_gid_ = pt_by_gid;
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    read_trigger_ = read_trigger;
    read_trigger_lck_ = read_trigger_lck;
    task_queue_lck_ = task_queue_lck;
    read_trigger_cv_ = read_trigger_cv;
    task_queue_cv_ = task_queue_cv;
    XLOG(INFO, "Init LoadComponent: Finish.");
  }

  void Run() override {
<<<<<<< HEAD
    // std::unique_lock<std::mutex> read_trigger_lck(*read_trigger_mtx_);
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid = GID_MAX;
      read_trigger_cv_->wait(*read_trigger_lck_, [&] {
        return read_trigger_->read(gid) ||
               !switch_.load(std::memory_order_relaxed) ||
               read_trigger_->isEmpty();
      });
      read_trigger_cv_->notify_all();
      // if (this->state_machine_->IsTerminated()) {
      if (!this->switch_.load(std::memory_order_relaxed)) {
        return;
      } else {
        if (gid == GID_MAX) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
=======
    while (this->switch_.load(std::memory_order_relaxed) == true) {
      GID_T gid = MINIGRAPH_GID_MAX;
      while (!read_trigger_->read(gid)) {
        if (this->switch_.load(std::memory_order_relaxed) == false) {
          return;
>>>>>>> e5ab45c9b1706f2fdb027c78aa913ac666033748
        }
        CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
        this->ProcessGraph(gid, csr_pt, data_mngr_, task_queue_,
                           this->state_machine_);
      }
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  folly::ProducerConsumerQueue<GID_T>* read_trigger_;
  folly::ProducerConsumerQueue<GID_T>* task_queue_;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_;
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;
  std::atomic<bool> switch_ = true;
  std::unique_lock<std::mutex>* read_trigger_lck_;
  std::unique_lock<std::mutex>* task_queue_lck_;
  std::condition_variable* read_trigger_cv_;
  std::condition_variable* task_queue_cv_;

  void ProcessGraph(
      GID_T gid, CSRPt& csr_pt,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      utility::StateMachine<GID_T>* state_machine) {
    if (data_mngr->ReadGraph(gid, csr_pt, csr_bin)) {
      // data_mngr->GetGraph(gid);
      state_machine->ProcessEvent(gid, LOAD);
      task_queue_cv_->wait(*task_queue_lck_,
                           [&] { return task_queue_->write(gid); });
      task_queue_cv_->notify_all();
    } else {
      state_machine->ProcessEvent(gid, UNLOAD);
    }
  }
};

}  // namespace minigraph::components
#endif  // MINIGRAPH_LOAD_COMPONENT_H
