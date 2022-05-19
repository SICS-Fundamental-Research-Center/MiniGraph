#ifndef MINIGRAPH_DISCHARGE_COMPONENT_H
#define MINIGRAPH_DISCHARGE_COMPONENT_H

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <string>

namespace minigraph {
namespace components {
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T>
class DischargeComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  DischargeComponent(
      utility::EDFThreadPool* thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      folly::ProducerConsumerQueue<GID_T>* read_trigger,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      std::unique_lock<std::mutex>* partial_result_lck,
      std::unique_lock<std::mutex>* read_trigger_lck,
      std::condition_variable* partial_result_cv,
      std::condition_variable* read_trigger_cv,
      std::atomic<bool>* system_switch,
      std::unique_lock<std::mutex>* system_switch_lck,
      std::condition_variable* system_switch_cv)
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    partial_result_queue_ = partial_result_queue;
    pt_by_gid_ = pt_by_gid;
    read_trigger_ = read_trigger;
    data_mngr_ = data_mngr;
    read_trigger_lck_ = read_trigger_lck;
    partial_result_lck_ = partial_result_lck;
    read_trigger_cv_ = read_trigger_cv;
    partial_result_cv_ = partial_result_cv;
    system_switch_ = system_switch;
    system_switch_lck_ = system_switch_lck;
    system_switch_cv_ = system_switch_cv;

    XLOG(INFO, "Init DischargeComponent: Finish.");
  }

  ~DischargeComponent() = default;

  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid = GID_MAX;
      partial_result_cv_->wait(*partial_result_lck_, [&] {
        return partial_result_queue_->read(gid) ||
               this->state_machine_->IsTerminated() ||
               !this->switch_.load(std::memory_order_relaxed) ||
               partial_result_queue_->isEmpty();
      });
      partial_result_cv_->notify_all();
      if (this->state_machine_->IsTerminated()) {
        system_switch_cv_->wait(*system_switch_lck_,
                                [&] { return system_switch_->load(); });
        system_switch_->store(false);
        system_switch_cv_->notify_all();
        return;
      } else {
        if (gid == GID_MAX) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }
        ProcessPartialResult(gid, data_mngr_, pt_by_gid_, this->state_machine_);
      }
    }
  }

  void Stop() override {
    this->switch_.store(false);
  }

  void ProcessPartialResult(
      const GID_T& gid,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::StateMachine<GID_T>* state_machine) {
    data_mngr->GetGraph(gid);
    size_t bound = 1;
    CSRPt& csr_pt = pt_by_gid->find(gid)->second;
    data_mngr->WriteGraph(gid, csr_pt, csr_bin);
    data_mngr->EraseGraph(gid);
    state_machine->ProcessEvent(gid, AGGREGATE);
    if (TrySync()) {
      std::vector<GID_T>&& evoked_gid = state_machine->EvokeAllX(RT);
      for (auto& iter : evoked_gid) {
        auto gid = iter;
        if (state_machine->GraphIs(gid, IDLE) &&
            this->get_superstep_via_gid(gid) <
                this->get_global_superstep() + bound) {
          read_trigger_cv_->wait(*read_trigger_lck_, [&] {
            return read_trigger_->write(gid) ||
                   !switch_.load(std::memory_order_relaxed);
          });
          read_trigger_cv_->notify_all();
        }
      }
    }
    read_trigger_cv_->wait(*read_trigger_lck_, [&] {
      return read_trigger_->write(gid) ||
             !switch_.load(std::memory_order_relaxed);
    });
    read_trigger_cv_->notify_all();
  }

 private:
  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_ = nullptr;
  // data manager
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;
  // read trigger to determine which fragment could be buffered.
  folly::ProducerConsumerQueue<GID_T>* read_trigger_ = nullptr;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;
  std::atomic<bool> switch_ = true;
  std::unique_lock<std::mutex>* read_trigger_lck_;
  std::unique_lock<std::mutex>* partial_result_lck_;
  std::condition_variable* read_trigger_cv_;
  std::condition_variable* partial_result_cv_;
  std::unique_lock<std::mutex>* system_switch_lck_;
  std::condition_variable* system_switch_cv_;
  std::atomic<bool>* system_switch_;
  bool TrySync() {
    size_t count = 0;
    for (auto& iter : *this->superstep_by_gid_) {
      if (iter.second->load() > this->get_global_superstep()) ++count;
    }
    if (count == this->superstep_by_gid_->size()) {
      this->add_global_superstep();
      return true;
    } else {
      return false;
    }
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
