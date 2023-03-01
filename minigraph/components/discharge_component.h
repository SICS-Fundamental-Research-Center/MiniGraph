#ifndef MINIGRAPH_DISCHARGE_COMPONENT_H
#define MINIGRAPH_DISCHARGE_COMPONENT_H

#include <string>

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/thread_pool.h"


namespace minigraph {
namespace components {

template <typename GRAPH_T>
class DischargeComponent : public ComponentBase<typename GRAPH_T::gid_t> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  DischargeComponent(
      const size_t num_workers, utility::EDFThreadPool* thread_pool,
      std::unordered_map<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      std::queue<GID_T>* partial_result_queue,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      std::queue<GID_T>* read_trigger,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::io::DataMngr<GRAPH_T>* data_mngr,
      message::DefaultMessageManager<GRAPH_T>* msg_mngr,
      std::unique_lock<std::mutex>* partial_result_lck,
      std::condition_variable* partial_result_cv,
      std::condition_variable* task_queue_cv,
      std::condition_variable* read_trigger_cv,
      std::atomic<bool>* system_switch,
      std::unique_lock<std::mutex>* system_switch_lck,
      std::condition_variable* system_switch_cv, const size_t num_iter,
      std::string mode = "Default")
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    task_queue_ = task_queue;
    num_workers_ = num_workers;
    partial_result_queue_ = partial_result_queue;
    pt_by_gid_ = pt_by_gid;
    read_trigger_ = read_trigger;
    data_mngr_ = data_mngr;
    task_queue_cv_ = task_queue_cv;
    partial_result_lck_ = partial_result_lck;
    read_trigger_cv_ = read_trigger_cv;
    partial_result_cv_ = partial_result_cv;
    system_switch_ = system_switch;
    system_switch_lck_ = system_switch_lck;
    system_switch_cv_ = system_switch_cv;
    msg_mngr_ = msg_mngr;
    mode_ = mode;
    communication_matrix_ = this->msg_mngr_->GetCommunicationMatrix();
    num_iter_ = num_iter;
    XLOG(INFO, "Init DischargeComponent: Finish.");
  }

  ~DischargeComponent() = default;

  void Run() override {
    LOG_INFO("Run DC");
    std::queue<GID_T> que_gid;
    folly::NativeSemaphore sem(num_workers_);
    while (this->switch_.load()) {
      GID_T gid = MINIGRAPH_GID_MAX;
      partial_result_cv_->wait(*partial_result_lck_, [&] { return true; });
      while (!partial_result_queue_->empty()) {
        gid = partial_result_queue_->front();
        partial_result_queue_->pop();
        que_gid.push(gid);
      }

      while (!que_gid.empty()) {
        gid = que_gid.front();
        que_gid.pop();
        if (mode_ != "NoShort") CheckRTRule(gid);
        if (this->TrySync()) {
          this->state_machine_->ShowAllState();
          if (this->state_machine_->IsTerminated() ||
              this->get_global_superstep() > num_iter_) {
            auto out_rts = this->state_machine_->EvokeAllX(RTS);
            for (auto& iter : out_rts) {
              CSRPt& csr_pt = pt_by_gid_->find(iter)->second;
              data_mngr_->WriteGraph(gid, csr_pt, csr_bin, true);
            }

            system_switch_cv_->wait(*system_switch_lck_,
                                    [&] { return system_switch_->load(); });
            system_switch_->store(false);
            system_switch_cv_->notify_all();
            LOG_INFO("DC exit");
            return;
          } else {
            WriteAllGraphsBack(gid);
          }
        } else {
          ReleaseGraphX(gid);
        }
      }
    }
  }

  void Stop() override { this->switch_.store(false); }

 private:
  std::atomic<size_t> num_workers_;
  std::atomic<bool> switch_ = true;
  std::queue<GID_T>* partial_result_queue_ = nullptr;
  std::queue<GID_T>* read_trigger_ = nullptr;
  utility::io::DataMngr<GRAPH_T>* data_mngr_ = nullptr;
  message::DefaultMessageManager<GRAPH_T>* msg_mngr_ = nullptr;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;
  std::unique_lock<std::mutex>* partial_result_lck_ = nullptr;
  folly::ProducerConsumerQueue<GID_T>* task_queue_ = nullptr;
  std::condition_variable* read_trigger_cv_ = nullptr;
  std::condition_variable* task_queue_cv_ = nullptr;
  std::condition_variable* partial_result_cv_ = nullptr;
  std::unique_lock<std::mutex>* system_switch_lck_ = nullptr;
  std::condition_variable* system_switch_cv_ = nullptr;
  std::atomic<bool>* system_switch_;
  std::string mode_ = "default";
  bool* communication_matrix_;
  size_t num_iter_ = 0;

  void ReleaseGraphX(const GID_T gid, bool terminate = false) {
    if (IsSameType<GRAPH_T, CSR_T>()) {
      if (this->state_machine_->GraphIs(gid, RTS)) {
        CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
        data_mngr_->WriteGraph(gid, csr_pt, csr_bin, true);
        data_mngr_->EraseGraph(gid);
      } else if (this->state_machine_->GraphIs(gid, RT)) {
        data_mngr_->EraseGraph(gid);
      } else if (this->state_machine_->GraphIs(gid, RC)) {
        CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
        data_mngr_->WriteGraph(gid, csr_pt, csr_bin, true);
        data_mngr_->EraseGraph(gid);
      }
    }
  }

  void WriteAllGraphsBack(const GID_T current_gid) {
    //LOG_INFO("WriteAllGraphsBack triggered by: ", current_gid);
    // Snapshot
    for (GID_T tmp_gid = 0; tmp_gid < pt_by_gid_->size(); tmp_gid++) {
      msg_mngr_->SetStateMatrix(tmp_gid,
                                this->state_machine_->GetState(tmp_gid));
    }

    auto out_rc_ = this->state_machine_->GetAllinStateX(RC);
    auto out_rt_ = this->state_machine_->GetAllinStateX(RT);
    auto out_rts_ = this->state_machine_->GetAllinStateX(RTS);

    for (auto& iter : out_rc_) {
      if (mode_ != "NoShort")
        if (current_gid == iter) continue;
      this->state_machine_->EvokeX(iter, RC);
      GID_T gid = iter;
      CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
      data_mngr_->WriteGraph(gid, csr_pt, edge_list_bin);
      data_mngr_->EraseGraph(gid);
      read_trigger_->push(gid);
    }
    for (auto& iter : out_rt_) {
      if (mode_ != "NoShort")
        if (current_gid == iter) continue;
      GID_T gid = iter;
      this->state_machine_->EvokeX(iter, RT);
      data_mngr_->EraseGraph(gid);
      read_trigger_->push(gid);
    }
    for (auto& iter : out_rts_) {
      if (mode_ != "NoShort")
        if (current_gid == iter) continue;
      GID_T gid = iter;
      this->state_machine_->EvokeX(iter, RTS);
      data_mngr_->EraseGraph(gid);
      read_trigger_->push(gid);
    }

    if (mode_ != "NoShort") {
      if (this->state_machine_->GraphIs(current_gid, RC)) {
        this->state_machine_->ShowGraphState(current_gid);
        this->state_machine_->EvokeX(current_gid, RC);
        this->state_machine_->ProcessEvent(current_gid, LOAD);
        //LOG_INFO("DC short: ", current_gid);
        while (!task_queue_->write(current_gid))
          ;
        task_queue_cv_->notify_all();
      } else if (this->state_machine_->GraphIs(current_gid, RTS)) {
        this->state_machine_->EvokeX(current_gid, RTS);
        data_mngr_->EraseGraph(current_gid);
        read_trigger_->push(current_gid);
      } else if (this->state_machine_->GraphIs(current_gid, RT)) {
        this->state_machine_->EvokeX(current_gid, RT);
        data_mngr_->EraseGraph(current_gid);
        read_trigger_->push(current_gid);
      }
    }

    //LOG_INFO("Evoke all.");
    read_trigger_cv_->notify_all();
  }

  bool CheckRTRule(const GID_T gid) const {
    size_t num_graphs = this->pt_by_gid_->size();
    if (this->state_machine_->GraphIs(gid, RC)) {
      bool tag = true;
      for (size_t j = 0; j < num_graphs; j++) {
        if (*(communication_matrix_ + j * num_graphs + gid) == 1) {
          tag = false;
          break;
        }
      }
      if (tag) {
        this->state_machine_->ProcessEvent(gid, SHORTCUT);
        LOG_INFO("Shortcut: ", gid);
      }
    }
    return false;
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
