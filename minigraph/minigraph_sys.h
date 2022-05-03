#ifndef MINIGRAPH_MINIGRAPH_SYS_H
#define MINIGRAPH_MINIGRAPH_SYS_H

#include <dirent.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "components/computing_component.h"
#include "components/discharge_component.h"
#include "components/load_component.h"
#include "utility/io/data_mngr.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include "utility/state_machine.h"
#include <folly/AtomicHashMap.h>

namespace fs = std::filesystem;

namespace minigraph {

template <typename GRAPH_T, typename AUTOAPP_T>
class MiniGraphSys {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  MiniGraphSys(const std::string& raw_data, const std::string& work_space,
               const size_t& num_workers_lc, const size_t& num_workers_cc,
               const size_t& num_workers_dc, const size_t& num_threads_cpu,
               const bool is_partition = true, const size_t& num_partitions = 3,
               AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>*
                   app_wrapper = nullptr) {
    // configure sys.
    LOG_INFO("WorkSpace: ", work_space, " num_workers_lc: ", num_workers_lc,
             ", num_workers_cc: ", num_workers_cc,
             ", num_worker_dc: ", num_workers_dc,
             ", num_threads_cpu: ", num_threads_cpu,
             ", is_partition: ", is_partition);
    InitWorkList(work_space);

    // init Data Manager.
    data_mngr_ = std::make_unique<
        utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>>();

    // init partitioner
    if (is_partition) {
      edge_cut_partitioner_ =
          std::make_unique<minigraph::utility::partitioner::EdgeCutPartitioner<
              GID_T, VDATA_T, VDATA_T, EDATA_T>>(raw_data, work_space);
      edge_cut_partitioner_->RunPartition(num_partitions);
      data_mngr_->global_border_vertexes_.reset(
          edge_cut_partitioner_->GetGlobalBorderVertexes());
      data_mngr_->WriteBorderVertexes(
          *(data_mngr_->global_border_vertexes_.get()),
          work_space + "/border_vertexes/global.bv");
      return;
    } else {
      data_mngr_->global_border_vertexes_.reset(data_mngr_->ReadBorderVertexes(
          work_space + "/border_vertexes/global.bv"));
    }
    // find all files.
    pt_by_gid_ = new folly::AtomicHashMap<GID_T, CSRPt>(64);
    InitPtByGid(work_space);

    // init global superstep
    global_superstep_ = new std::atomic<size_t>(0);

    // init superstep of fragments as all 0;
    std::vector<GID_T> vec_gid;
    superstep_by_gid_ =
        new folly::AtomicHashMap<GID_T, std::atomic<size_t>*>(64);
    for (auto& iter : *pt_by_gid_) {
      superstep_by_gid_->insert(iter.first, new std::atomic<size_t>(0));
      vec_gid.push_back(iter.first);
    }

    // init read_trigger
    read_trigger_ = std::make_unique<folly::ProducerConsumerQueue<GID_T>>(
        vec_gid.size() + 1);
    for (auto& iter : vec_gid) {
      read_trigger_->write(iter);
    }

    // init thread pool
    // cpu_thread_pool has only one priority queue.
    cpu_thread_pool_ =
        std::make_unique<utility::CPUThreadPool>(num_threads_cpu, 1);

    // init state_machine
    state_machine_ = new utility::StateMachine<GID_T>(vec_gid);

    // init task queue
    task_queue_ = std::make_unique<folly::ProducerConsumerQueue<GID_T>>(
        vec_gid.size() + 1);

    // init partial result queue
    partial_result_queue_ =
        std::make_unique<folly::ProducerConsumerQueue<GID_T>>(vec_gid.size() +
                                                              1);

    // init auto_app.
    app_wrapper_ = std::make_unique<
        AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>>();
    app_wrapper_.reset(app_wrapper);
    if (data_mngr_->global_border_vertexes_ != nullptr) {
      app_wrapper_->InitBorderVertexes(
          data_mngr_->global_border_vertexes_.get(),
          data_mngr_->global_border_vertexes_info_.get());
    }
    // init components
    load_component_ = std::make_unique<
        components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T>>(
        cpu_thread_pool_.get(), superstep_by_gid_, global_superstep_,
        state_machine_, read_trigger_.get(), task_queue_.get(), pt_by_gid_,
        data_mngr_.get());
    computing_component_ = std::make_unique<components::ComputingComponent<
        GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T, AUTOAPP_T>>(
        cpu_thread_pool_.get(), superstep_by_gid_, global_superstep_,
        state_machine_, task_queue_.get(), partial_result_queue_.get(),
        data_mngr_.get(), app_wrapper_.get());
    discharge_component_ =
        std::make_unique<components::DischargeComponent<GID_T, VID_T, VDATA_T,
                                                        EDATA_T, GRAPH_T>>(
            cpu_thread_pool_.get(), superstep_by_gid_, global_superstep_,
            state_machine_, partial_result_queue_.get(), pt_by_gid_,
            read_trigger_.get(), data_mngr_.get());
    LOG_INFO("Init MiniGraphSys: Finish.");
  };

  ~MiniGraphSys() = default;

  void Stop() {
    load_component_->Stop();
    computing_component_->Stop();
    discharge_component_->Stop();
  }

  bool RunSys() {
    LOG_INFO("RunSys()");
    auto start_time = std::chrono::system_clock::now();
    auto task_lc = std::bind(&components::LoadComponent<GID_T, VID_T, VDATA_T,
                                                        EDATA_T, GRAPH_T>::Run,
                             load_component_.get());
    this->cpu_thread_pool_->Commit(task_lc);
    auto task_cc = std::bind(
        &components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T,
                                        AUTOAPP_T>::Run,
        computing_component_.get());
    this->cpu_thread_pool_->Commit(task_cc);

    auto task_dc =
        std::bind(&components::DischargeComponent<GID_T, VID_T, VDATA_T,
                                                  EDATA_T, GRAPH_T>::Run,
                  discharge_component_.get());
    this->cpu_thread_pool_->Commit(task_dc);
    while (!this->state_machine_->IsTerminated()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    this->Stop();
    data_mngr_->CleanUp();
    auto end_time = std::chrono::system_clock::now();
    std::cout << "         #### RUNSYS(): Finish"
              << " Elapse time: "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << " ####      " << std::endl;
    return true;
  }

  void ShowResult() {
    LOG_INFO("**************Show Result****************");
    for (auto& iter : *pt_by_gid_) {
      GID_T gid = iter.first;
      CSRPt csr_pt = iter.second;
      auto graph = new GRAPH_T;
      data_mngr_->csr_io_adapter_->Read((GRAPH_BASE_T*)graph, csr_bin, gid,
                                        csr_pt.meta_pt, csr_pt.data_pt);
      graph->ShowGraph();
    }
  }

 private:
  // configure.
  std::unique_ptr<folly::ProducerConsumerQueue<GID_T>> read_trigger_ = nullptr;

  // files list
  folly::AtomicHashMap<GID_T, CSRPt>* pt_by_gid_ = nullptr;

  // partitioner
  std::unique_ptr<minigraph::utility::partitioner::EdgeCutPartitioner<
      GID_T, VDATA_T, VDATA_T, EDATA_T>>
      edge_cut_partitioner_ = nullptr;

  // thread pool.
  std::unique_ptr<utility::IOThreadPool> io_thread_pool_ = nullptr;
  std::unique_ptr<utility::CPUThreadPool> cpu_thread_pool_ = nullptr;

  // superstep.
  folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid_ =
      nullptr;
  std::atomic<size_t>* global_superstep_ = nullptr;

  // state machine.
  utility::StateMachine<GID_T>* state_machine_ = nullptr;

  // task queue.
  std::unique_ptr<folly::ProducerConsumerQueue<GID_T>> task_queue_ = nullptr;

  // partial result queue.
  std::unique_ptr<folly::ProducerConsumerQueue<GID_T>> partial_result_queue_ =
      nullptr;

  // components.
  std::unique_ptr<
      components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T>>
      load_component_ = nullptr;
  std::unique_ptr<components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T,
                                                 GRAPH_T, AUTOAPP_T>>
      computing_component_ = nullptr;
  std::unique_ptr<
      components::DischargeComponent<GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T>>
      discharge_component_ = nullptr;

  // data manager
  std::unique_ptr<utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>>
      data_mngr_ = nullptr;

  // 2D-PIE
  std::unique_ptr<AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>>
      app_wrapper_ = nullptr;

  // std::unique_ptr<std::unordered_map<VID_T, std::vector<GID_T>*>>
  //     global_border_vertexes_ = nullptr;
  // std::unique_ptr<std::unordered_map<VID_T, VertexInfo*>>
  //     global_border_vertesxes_info_ = nullptr;

  void InitWorkList(const std::string& work_space) {
    std::string vertex_root = work_space + "/vertex/";
    std::string meta_out_root = work_space + "/meta/out/";
    std::string meta_in_root = work_space + "/meta/in/";
    std::string vdata_root = work_space + "/vdata/";
    std::string localid2globalid_root = work_space + "/localid2globalid/";
    std::string msg_root = work_space + "/msg/";
    std::string global_border_vertesxes_root = work_space + "/border_vertexes/";
    std::string meta_root = work_space + "/meta/";
    std::string data_root = work_space + "/data/";
    if (!data_mngr_->IsExist(global_border_vertesxes_root)) {
      data_mngr_->MakeDirectory(global_border_vertesxes_root);
    }
    if (!data_mngr_->IsExist(meta_root)) {
      data_mngr_->MakeDirectory(meta_root);
    }
    if (!data_mngr_->IsExist(data_root)) {
      data_mngr_->MakeDirectory(data_root);
    }
  }

  bool InitPtByGid(const std::string& work_space) {
    std::string vertex_root = work_space + "/vertex/";
    std::string meta_out_root = work_space + "/meta/out/";
    std::string meta_in_root = work_space + "/meta/in/";
    std::string vdata_root = work_space + "/vdata/";
    std::string localid2globalid_root = work_space + "/localid2globalid/";
    std::string msg_root = work_space + "/msg/";
    std::string meta_root = work_space + "/meta/";
    std::string data_root = work_space + "/data/";

    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(meta_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/meta/");
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/meta/").length();
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
      size_t pos = path.find("/data/");
      size_t pos2 = path.find(".data");
      int type_length = std::string("/data/").length();
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
    return true;
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_MINIGRAPH_SYS_H
