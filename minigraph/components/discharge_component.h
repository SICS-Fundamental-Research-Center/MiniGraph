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
      const size_t num_workers, utility::EDFThreadPool* thread_pool,
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
      std::vector<GID_T> vec_gid;
      GID_T gid = MINIGRAPH_GID_MAX;
      partial_result_cv_->wait(*partial_result_lck_, [&] {
        return !partial_result_queue_->isEmpty();
      });
      while (!partial_result_queue_->isEmpty()) {
        while (!partial_result_queue_->read(gid)) {
          continue;
        }
        vec_gid.push_back(gid);
      }
      for (size_t i = 0; i < vec_gid.size(); i++) {
        gid = vec_gid.at(i);
        if (this->TrySync()) {
          if (this->state_machine_->IsTerminated()) {
            system_switch_cv_->wait(*system_switch_lck_,
                                    [&] { return system_switch_->load(); });
            system_switch_->store(false);
            system_switch_cv_->notify_all();
            LOG_INFO("DC exit");
            return;
          } else {
            WriteAllGraphsBack();
          }
        } else {
          // BUFF GID
        }
      }
    }
  }

  void Stop() override { this->switch_.store(false); }

  void WriteAllGraphsBack() {
    GID_T gid = this->state_machine_->GetXStateOf(RC);
    if (gid != MINIGRAPH_GID_MAX) {
      LOG_INFO("EVOKE ALL");
      auto out_rc = this->state_machine_->EvokeAllX(RC);
      auto out_rt = this->state_machine_->EvokeAllX(RT);
      for (auto& iter : out_rt) {
        GID_T gid = iter;
        data_mngr_->EraseGraph(gid);
        read_trigger_->write(gid);
        read_trigger_cv_->notify_all();
      }
      for (auto& iter : out_rc) {
        GID_T gid = iter;
        CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
        data_mngr_->WriteGraph(gid, csr_pt, csr_bin);
        read_trigger_->write(gid);
        read_trigger_cv_->notify_all();
      }
    }
  }

 private:
  std::atomic<size_t> num_workers_;
  std::atomic<bool> switch_ = true;
  folly::ProducerConsumerQueue<GID_T>* partial_result_queue_ = nullptr;
  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;
  folly::ProducerConsumerQueue<GID_T>* read_trigger_ = nullptr;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;
  std::unique_lock<std::mutex>* read_trigger_lck_;
  std::unique_lock<std::mutex>* partial_result_lck_;
  std::condition_variable* read_trigger_cv_;
  std::condition_variable* partial_result_cv_;
  std::unique_lock<std::mutex>* system_switch_lck_;
  std::condition_variable* system_switch_cv_;
  std::atomic<bool>* system_switch_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
