#ifndef MINIGRAPH_MINIGRAPH_SYS_H
#define MINIGRAPH_MINIGRAPH_SYS_H

#include <condition_variable>
#include <dirent.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <folly/AtomicHashMap.h>
#include <folly/synchronization/NativeSemaphore.h>

#include "2d_pie/auto_app_base.h"
#include "components/computing_component.h"
#include "components/discharge_component.h"
#include "components/load_component.h"
#include "message_manager/default_message_manager.h"
#include "utility/io/data_mngr.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include "utility/state_machine.h"

namespace minigraph {

template <typename GRAPH_T, typename AUTOAPP_T>
class MiniGraphSys {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
  using APP_WRAPPER = AppWrapper<AUTOAPP_T, GRAPH_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  MiniGraphSys(const std::string work_space, const size_t num_workers_lc = 1,
               const size_t num_workers_cc = 1, const size_t num_workers_dc = 1,
               const size_t num_cores = 1, const size_t buffer_size = 0,
               APP_WRAPPER* app_wrapper = nullptr, std::string mode = "Default",
               const size_t num_iter = 30) {
    assert(num_workers_dc > 0 && num_workers_cc > 0 && num_workers_dc > 0 &&
           num_cores / num_workers_cc >= 1);

    // configure sys.
    LOG_INFO("WorkSpace: ", work_space, " num_workers_lc: ", num_workers_lc,
             ", num_workers_cc: ", num_workers_cc,
             ", num_worker_dc: ", num_workers_dc, ", num_threads: ", num_cores);

    num_threads_ = 3;
    InitWorkList(work_space);

    // init Data Manager.
    data_mngr_ = std::make_unique<utility::io::DataMngr<GRAPH_T>>();

    // init Message Manager
    msg_mngr_ = std::make_unique<message::DefaultMessageManager<GRAPH_T>>(
        data_mngr_.get(), work_space, false);
    msg_mngr_->Init(work_space);

    pt_by_gid_ = new folly::AtomicHashMap<GID_T, CSRPt>(8096);
    InitPtByGid(work_space);

    // init global superstep
    global_superstep_ = new std::atomic<size_t>(0);

    // init superstep of fragments as all 0;
    std::vector<GID_T> vec_gid;
    superstep_by_gid_ = new std::unordered_map<GID_T, std::atomic<size_t>*>;
    for (auto& iter : *pt_by_gid_) {
      superstep_by_gid_->insert(
          std::make_pair(iter.first, new std::atomic<size_t>(0)));
      vec_gid.push_back(iter.first);
    }

    // init read_trigger
    read_trigger_ = std::make_unique<std::queue<GID_T>>();
    for (auto& iter : vec_gid) {
      read_trigger_->push(iter);
    }

    // init task queue
    task_queue_ = std::make_unique<folly::ProducerConsumerQueue<GID_T>>(
        num_workers_cc + 2);

    // init partial result queue
    partial_result_queue_ = std::make_unique<std::queue<GID_T>>();

    // init thread pool
    thread_pool_ = std::make_unique<utility::EDFThreadPool>(num_threads_);
    lc_thread_pool_ = std::make_unique<utility::EDFThreadPool>(num_workers_lc);
    cc_thread_pool_ = std::make_unique<utility::EDFThreadPool>(num_workers_cc);
    dc_thread_pool_ = std::make_unique<utility::EDFThreadPool>(num_workers_dc);

    // init state_machine
    state_machine_ = new utility::StateMachine<GID_T>(vec_gid);

    // init auto_app.
    app_wrapper_ = std::make_unique<AppWrapper<AUTOAPP_T, GRAPH_T>>();
    app_wrapper_.reset(app_wrapper);
    app_wrapper_->InitMsgMngr(msg_mngr_.get());

    // init mutex, lck and cv
    read_trigger_mtx_ = std::make_unique<std::mutex>();
    task_queue_mtx_ = std::make_unique<std::mutex>();
    partial_result_mtx_ = std::make_unique<std::mutex>();

    read_trigger_lck_ = std::make_unique<std::unique_lock<std::mutex>>(
        *read_trigger_mtx_.get());
    task_queue_lck_ =
        std::make_unique<std::unique_lock<std::mutex>>(*task_queue_mtx_.get());
    partial_result_lck_ = std::make_unique<std::unique_lock<std::mutex>>(
        *partial_result_mtx_.get());

    read_trigger_cv_ = std::make_unique<std::condition_variable>();
    task_queue_cv_ = std::make_unique<std::condition_variable>();
    partial_result_cv_ = std::make_unique<std::condition_variable>();

    system_switch_ = std::make_unique<std::atomic<bool>>(true);
    system_switch_mtx_ = std::make_unique<std::mutex>();
    system_switch_lck_ = std::make_unique<std::unique_lock<std::mutex>>(
        *system_switch_mtx_.get());
    system_switch_cv_ = std::make_unique<std::condition_variable>();

    auto sem_lc_dc = new folly::NativeSemaphore(buffer_size);

    // init components
    load_component_ = std::make_unique<components::LoadComponent<GRAPH_T>>(
        num_workers_lc, lc_thread_pool_.get(), superstep_by_gid_,
        global_superstep_, state_machine_, read_trigger_.get(),
        task_queue_.get(), partial_result_queue_.get(), pt_by_gid_,
        data_mngr_.get(), msg_mngr_.get(), read_trigger_lck_.get(),
        read_trigger_cv_.get(), task_queue_cv_.get(), partial_result_cv_.get(),
        mode);
    computing_component_ =
        std::make_unique<components::ComputingComponent<GRAPH_T, AUTOAPP_T>>(
            num_workers_cc, num_cores, cc_thread_pool_.get(), superstep_by_gid_,
            global_superstep_, state_machine_, task_queue_.get(),
            partial_result_queue_.get(), data_mngr_.get(), app_wrapper_.get(),
            task_queue_lck_.get(), task_queue_cv_.get(),
            partial_result_cv_.get());
    discharge_component_ =
        std::make_unique<components::DischargeComponent<GRAPH_T>>(
            num_workers_dc, dc_thread_pool_.get(), superstep_by_gid_,
            global_superstep_, state_machine_, partial_result_queue_.get(),
            task_queue_.get(), read_trigger_.get(), pt_by_gid_,
            data_mngr_.get(), msg_mngr_.get(), partial_result_lck_.get(),
            partial_result_cv_.get(), task_queue_cv_.get(),
            read_trigger_cv_.get(), system_switch_.get(),
            system_switch_lck_.get(), system_switch_cv_.get(), num_iter, mode);
    LOG_INFO("Init MiniGraphSys: Finish.");
  };

  ~MiniGraphSys() = default;

  void Stop() {
    LOG_INFO("MiniGraph STOP.");
    load_component_->Stop();
    computing_component_->Stop();
    discharge_component_->Stop();
    read_trigger_cv_->notify_all();
    task_queue_cv_->notify_all();
    partial_result_cv_->notify_all();
    load_component_->~LoadComponent();
    computing_component_->~ComputingComponent();
    discharge_component_->~DischargeComponent();
  }

  bool RunSys() {
    LOG_INFO("START MiniGraph.");
    auto task_lc = std::bind(&components::LoadComponent<GRAPH_T>::Run,
                             load_component_.get());
    auto task_cc =
        std::bind(&components::ComputingComponent<GRAPH_T, AUTOAPP_T>::Run,
                  computing_component_.get());

    auto task_dc = std::bind(&components::DischargeComponent<GRAPH_T>::Run,
                             discharge_component_.get());

    this->thread_pool_->Commit(task_lc);
    this->thread_pool_->Commit(task_cc);
    this->thread_pool_->Commit(task_dc);

    auto start_time = std::chrono::system_clock::now();
    read_trigger_cv_->notify_all();
    task_queue_cv_->notify_all();
    partial_result_cv_->notify_all();
    system_switch_cv_->wait(*system_switch_lck_,
                            [&] { return !system_switch_->load(); });
    auto end_time = std::chrono::system_clock::now();
    // data_mngr_->CleanUp();

    std::cout << "         #### RUNSYS(): Finish"
              << ", Elapse time: "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << ", Superstep: " << this->global_superstep_->load()
              << " ####      " << std::endl;
    this->Stop();
    return true;
  }

  void ShowResult(const size_t num_vertexes_to_show = 20,
                  char separator_params = ',') {
    LOG_INFO("**************Show Result****************");
    for (auto& iter : *pt_by_gid_) {
      GID_T gid = iter.first;
      CSRPt csr_pt = iter.second;
      auto graph = new GRAPH_T;
      if (IsSameType<GRAPH_T, CSR_T>()) {
        data_mngr_->csr_io_adapter_->Read((GRAPH_BASE_T*)graph, csr_bin, gid,
                                          csr_pt.meta_pt, csr_pt.data_pt,
                                          csr_pt.vdata_pt);
        ((CSR_T*)graph)->ShowGraph(num_vertexes_to_show);
      } else if (IsSameType<GRAPH_T, EDGE_LIST_T>()) {
        data_mngr_->edge_list_io_adapter_->Read(
            (GRAPH_BASE_T*)graph, edge_list_bin, separator_params, gid,
            csr_pt.meta_pt, csr_pt.data_pt, csr_pt.vdata_pt);
        ((EDGE_LIST_T*)graph)->ShowGraph(num_vertexes_to_show);
      }
    }
    if (msg_mngr_->GetPartialMatch() != nullptr)
      msg_mngr_->GetPartialMatch()->ShowMatchingSolutions();
  }

  void ShowNumComponents() {
    // vdata_t components = 0;
    for (auto& iter : *pt_by_gid_) {
      GID_T gid = iter.first;
      CSRPt csr_pt = iter.second;
      auto graph = new GRAPH_T;
      data_mngr_->csr_io_adapter_->Read((GRAPH_BASE_T*)graph, csr_bin, gid,
                                        csr_pt.meta_pt, csr_pt.data_pt);
      // auto components_of_X = graph->GetComponents();
      // LOG_INFO(components_of_X, " components found");
    }
  }

  utility::io::DataMngr<GRAPH_T>* GetDataMngr() { return data_mngr_.get(); }

 private:
  // file path by gid.
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;

  // thread pool.
  size_t num_threads_ = 0;
  std::unique_ptr<utility::EDFThreadPool> thread_pool_ = nullptr;
  std::unique_ptr<utility::EDFThreadPool> lc_thread_pool_ = nullptr;
  std::unique_ptr<utility::EDFThreadPool> cc_thread_pool_ = nullptr;
  std::unique_ptr<utility::EDFThreadPool> dc_thread_pool_ = nullptr;

  // superstep.
  std::unordered_map<GID_T, std::atomic<size_t>*>* superstep_by_gid_ = nullptr;
  std::atomic<size_t>* global_superstep_ = nullptr;

  // state machine.
  utility::StateMachine<GID_T>* state_machine_ = nullptr;

  // task queue.
  std::unique_ptr<folly::ProducerConsumerQueue<GID_T>> task_queue_ = nullptr;

  // read trigger queue.
  std::unique_ptr<std::queue<GID_T>> read_trigger_ = nullptr;

  // partial result queue.
  std::unique_ptr<std::queue<GID_T>> partial_result_queue_ = nullptr;

  // components.
  std::unique_ptr<components::LoadComponent<GRAPH_T>> load_component_ = nullptr;
  std::unique_ptr<components::ComputingComponent<GRAPH_T, AUTOAPP_T>>
      computing_component_ = nullptr;
  std::unique_ptr<components::DischargeComponent<GRAPH_T>>
      discharge_component_ = nullptr;

  // data manager
  std::unique_ptr<utility::io::DataMngr<GRAPH_T>> data_mngr_ = nullptr;

  // App wrapper
  std::unique_ptr<AppWrapper<AUTOAPP_T, GRAPH_T>> app_wrapper_ = nullptr;

  std::unique_ptr<message::DefaultMessageManager<GRAPH_T>> msg_mngr_ = nullptr;

  std::unique_ptr<std::mutex> read_trigger_mtx_ = nullptr;
  std::unique_ptr<std::mutex> task_queue_mtx_ = nullptr;
  std::unique_ptr<std::mutex> partial_result_mtx_ = nullptr;
  std::unique_ptr<std::unique_lock<std::mutex>> read_trigger_lck_ = nullptr;
  std::unique_ptr<std::unique_lock<std::mutex>> task_queue_lck_ = nullptr;
  std::unique_ptr<std::unique_lock<std::mutex>> partial_result_lck_ = nullptr;
  std::unique_ptr<std::condition_variable> read_trigger_cv_ = nullptr;
  std::unique_ptr<std::condition_variable> task_queue_cv_ = nullptr;
  std::unique_ptr<std::condition_variable> partial_result_cv_ = nullptr;

  // system switch
  std::unique_ptr<std::atomic<bool>> system_switch_ = nullptr;
  std::unique_ptr<std::mutex> system_switch_mtx_ = nullptr;
  std::unique_ptr<std::unique_lock<std::mutex>> system_switch_lck_ = nullptr;
  std::unique_ptr<std::condition_variable> system_switch_cv_ = nullptr;

  void InitWorkList(const std::string& work_space) {
    std::string meta_root = work_space + "minigraph_meta/";
    std::string data_root = work_space + "minigraph_data/";
    std::string vdata_root = work_space + "minigraph_vdata/";
    if (!data_mngr_->IsExist(meta_root)) data_mngr_->MakeDirectory(meta_root);
    if (!data_mngr_->IsExist(data_root)) data_mngr_->MakeDirectory(data_root);
    if (!data_mngr_->IsExist(vdata_root)) data_mngr_->MakeDirectory(vdata_root);
  }

  bool InitPtByGid(const std::string& work_space) {
    std::string meta_root = work_space + "minigraph_meta/";
    std::string data_root = work_space + "minigraph_data/";
    std::string vdata_root = work_space + "minigraph_vdata/";

    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(meta_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/minigraph_meta/");
      size_t pos2 = path.find(".bin");
      int type_length = std::string("/minigraph_meta/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.meta_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.meta_pt = path;
      }
    }

    for (const auto& entry : std::filesystem::directory_iterator(data_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/minigraph_data/");
      size_t pos2 = path.find(".bin");
      int type_length = std::string("/minigraph_data/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.data_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.data_pt = path;
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator(vdata_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/minigraph_vdata/");
      size_t pos2 = path.find(".bin");
      int type_length = std::string("/minigraph_vdata/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.vdata_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.vdata_pt = path;
      }
    }
    return true;
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_MINIGRAPH_SYS_H
