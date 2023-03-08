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
#include "scheduler/fifo_scheduler.h"
#include "scheduler/hash_scheduler.h"
#include "scheduler/large_first_scheduler.h"
#include "scheduler/learned_scheduler.h"
#include "scheduler/small_first_scheduler.h"
#include "scheduler/subgraph_scheduler_base.h"
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
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  LoadComponent(
      const size_t num_workers, utility::EDFThreadPool* thread_pool,
      std::unordered_map<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      std::queue<GID_T>* read_trigger,
      folly::ProducerConsumerQueue<GID_T>* task_queue,
      std::queue<GID_T>* partial_result_queue,
      folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid,
      utility::io::DataMngr<GRAPH_T>* data_mngr,
      message::DefaultMessageManager<GRAPH_T>* msg_mngr,
      std::unique_lock<std::mutex>* read_trigger_lck,
      std::condition_variable* read_trigger_cv,
      std::condition_variable* task_queue_cv,
      std::condition_variable* partial_result_cv, std::string mode = "Default",
      std::string scheduler = "FIFO")
      : ComponentBase<GID_T>(thread_pool, superstep_by_gid, global_superstep,
                             state_machine) {
    num_workers_ = num_workers;
    pt_by_gid_ = pt_by_gid;
    data_mngr_ = data_mngr;
    msg_mngr_ = msg_mngr;
    task_queue_ = task_queue;
    partial_result_queue_ = partial_result_queue;
    read_trigger_ = read_trigger;
    read_trigger_lck_ = read_trigger_lck;
    read_trigger_cv_ = read_trigger_cv;
    task_queue_cv_ = task_queue_cv;
    partial_result_cv_ = partial_result_cv;
    mode_ = mode;

    if (scheduler == "FIFO") {
      scheduler_ = new scheduler::FIFOScheduler<GID_T>();
    } else if (scheduler == "learned_model") {
      scheduler_ =
          new scheduler::LearnedScheduler<GID_T>(msg_mngr_->GetStatisticInfo());
    } else if (scheduler == "hash") {
      scheduler_ = new scheduler::HashScheduler<GID_T>();
    } else if (scheduler == "large_first") {
      scheduler_ = new scheduler::LargeFirstScheduler<GID_T>(
          msg_mngr_->GetStatisticInfo());
    } else if (scheduler == "small_first") {
      scheduler_ = new scheduler::SmallFirstScheduler<GID_T>(
          msg_mngr_->GetStatisticInfo());
    } else {
      scheduler_ = new scheduler::FIFOScheduler<GID_T>();
    }

    XLOG(INFO, "Init LoadComponent: Finish.");
  }

  void Run() override {
    LOG_INFO("Run LC");
    folly::NativeSemaphore sem(num_workers_);
    while (switch_) {
      GID_T gid = MINIGRAPH_GID_MAX;
      read_trigger_cv_->wait(*read_trigger_lck_,
                             [&] { return !read_trigger_->empty(); });
      if (!switch_) return;

      std::queue<GID_T> que_gid;
      std::vector<GID_T> vec_gid;
      while (!read_trigger_->empty()) {
        gid = read_trigger_->front();

        LOG_INFO("push:", gid);
        que_gid.push(gid);
        vec_gid.push_back(gid);
        read_trigger_->pop();
      }

      while (!vec_gid.empty()) {
        gid = scheduler_->ChooseOne(vec_gid);
        LOG_INFO("load ", gid);
        sem.try_wait();
        auto task = std::bind(&components::LoadComponent<GRAPH_T>::ProcessGraph,
                              this, gid, sem, mode_);
        this->thread_pool_->Commit(task);
      }
    }
  }

  void Stop() override { switch_ = false; }

 private:
  size_t num_workers_;
  std::queue<GID_T>* read_trigger_ = nullptr;
  folly::ProducerConsumerQueue<GID_T>* task_queue_ = nullptr;
  std::queue<GID_T>* partial_result_queue_ = nullptr;
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;
  utility::io::DataMngr<GRAPH_T>* data_mngr_ = nullptr;
  message::DefaultMessageManager<GRAPH_T>* msg_mngr_ = nullptr;
  bool switch_ = true;
  std::unique_lock<std::mutex>* read_trigger_lck_ = nullptr;
  std::condition_variable* read_trigger_cv_ = nullptr;
  std::condition_variable* task_queue_cv_ = nullptr;
  std::condition_variable* partial_result_cv_ = nullptr;

  std::string mode_ = "default";

  minigraph::scheduler::SubGraphsSchedulerBase<GID_T>* scheduler_ = nullptr;

  void ProcessGraph(GID_T gid, folly::NativeSemaphore& sem,
                    std::string mode = "default") {
    auto read = false;
    if (this->get_global_superstep() == 0) {
      read = true;
    } else {
      for (GID_T y = 0; y < pt_by_gid_->size(); y++) {
        if (this->msg_mngr_->CheckDependenes(gid, y)) {
          if (msg_mngr_->GetStateMatrix(y) == RC) {
            read = true;
            break;
          }
        }
      }
    }

    if (mode_ == "NoShort") read = true;
    if (read) {
      CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
      auto tag = false;
      tag = this->data_mngr_->ReadGraph(gid, csr_pt, csr_bin);
      if (tag) {
        this->state_machine_->ProcessEvent(gid, LOAD);
        while (!task_queue_->write(gid))
          ;
        task_queue_cv_->notify_all();
      } else {
        this->state_machine_->ProcessEvent(gid, UNLOAD);
        LOG_ERROR("Read graph fault: ", gid);
      }
      sem.post();
    } else {
      // LOG_INFO("LC ShortCut", gid);
      this->add_superstep_via_gid(gid);
      partial_result_queue_->push(gid);
      this->state_machine_->ProcessEvent(gid, SHORTCUTREAD);
      partial_result_cv_->notify_all();
    }
  }
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_LOAD_COMPONENT_H
