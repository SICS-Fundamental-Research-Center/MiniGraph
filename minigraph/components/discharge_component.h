#ifndef MINIGRAPH_DISCHARGE_COMPONENT_H
#define MINIGRAPH_DISCHARGE_COMPONENT_H

#include <string>

#include <folly/ProducerConsumerQueue.h>

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/thread_pool.h"

namespace minigraph {
namespace components {
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T>
class DischargeComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  DischargeComponent(
      utility::CPUThreadPool* cpu_thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* partial_result_queue,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      folly::ProducerConsumerQueue<GID_T>* read_trigger,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr)
      : ComponentBase<GID_T>(cpu_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    partial_result_queue_ = partial_result_queue;
    pt_by_gid_ = pt_by_gid;
    read_trigger_ = read_trigger;
    data_mngr_ = data_mngr;
    XLOG(INFO, "Init DischargeComponent: Finish.");
  }

  ~DischargeComponent() = default;

  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid;
      while (!partial_result_queue_->read(gid)) {
        // spin until we get a value
        if (this->switch_.load(std::memory_order_relaxed) == false) {
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      ProcessPartialResult(gid, data_mngr_, read_trigger_, pt_by_gid_,
                           this->state_machine_);
      TrySync();
    }
  }

  void Stop() override { this->switch_.store(false); }

  void ProcessPartialResult(
      const GID_T& gid,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      folly::ProducerConsumerQueue<GID_T>* read_trigger,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::StateMachine<GID_T>* state_machine) {
    data_mngr->GetGraph(gid);
    CSRPt& csr_pt = pt_by_gid->find(gid)->second;
    data_mngr->WriteGraph(gid, csr_pt, csr_bin);
    data_mngr->EraseGraph(gid);
    if (state_machine->GraphIs(gid, RC)) {
      state_machine->ShowGraphState(gid);
      state_machine->ProcessEvent(gid, AGGREGATE);
      std::vector<GID_T> evoked_gid = state_machine->Evoke();
      read_trigger->write(gid);
      for (auto iter : evoked_gid) {
        read_trigger->write(iter);
      }
    }
  }

 private:
  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_ = nullptr;
  // data manager
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;
  // read trigger to determine which fragment could be buffered.
  folly::ProducerConsumerQueue<GID_T>* read_trigger_ = nullptr;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;
  std::atomic<bool> switch_ = true;

  bool TrySync() {
    size_t count = 0;
    for (auto& iter : *this->superstep_by_gid_) {
      if (iter.second->load() > this->get_global_superstep()) ++count;
    }
    if (count == this->superstep_by_gid_->size()) {
      this->global_superstep_->fetch_add(1);
      return true;
    } else {
      return false;
    }
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
