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

#define MAX_THREAD 10;
namespace minigraph::components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T>
class LoadComponent : public ComponentBase<GID_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  LoadComponent(
      utility::CPUThreadPool* cpu_thread_pool,
      utility::IOThreadPool* io_thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<GID_T>* read_trigger,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      const size_t& num_wroker_lc)
      : ComponentBase<GID_T>(cpu_thread_pool, io_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    pt_by_gid_ = pt_by_gid;
    data_mngr_ = data_mngr;
    task_queue_ = task_queue;
    read_trigger_ = read_trigger;
    num_idle_workers_ = std::make_unique<std::atomic<size_t>>(num_wroker_lc);
    XLOG(INFO, "Init LoadComponent: Finish.");
  }

  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed) == true) {
      GID_T gid = GID_MAX;
      while (!read_trigger_->read(gid)) {
        if (this->switch_.load(std::memory_order_relaxed) == false) {
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
      this->ProcessGraph(gid, csr_pt, data_mngr_, task_queue_,
                         this->state_machine_);
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  // configuration
  std::unique_ptr<std::atomic<size_t>> num_idle_workers_ = nullptr;

  folly::ProducerConsumerQueue<GID_T>* read_trigger_;

  // task queue
  folly::ProducerConsumerQueue<GID_T>* task_queue_;

  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_;

  // data manager
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;

  std::atomic<bool> switch_ = true;

  void ProcessGraph(
      GID_T gid, CSRPt& csr_pt,
      utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      utility::StateMachine<GID_T>* state_machine) {
    if (data_mngr->LoadGraph(gid, csr_pt)) {
      state_machine->ProcessEvent(gid, LOAD);
      while (!task_queue->write(gid)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        continue;
      }
    } else {
      state_machine->ProcessEvent(gid, UNLOAD);
    }
  }
};

}  // namespace minigraph::components
#endif  // MINIGRAPH_LOAD_COMPONENT_H
