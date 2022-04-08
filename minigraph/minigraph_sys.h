#ifndef MINIGRAPH_MINIGRAPH_SYS_H
#define MINIGRAPH_MINIGRAPH_SYS_H
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
#include <dirent.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
namespace minigraph {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename GRAPH_T, typename AUTOAPP_T>
class MiniGraphSys {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  // using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  MiniGraphSys(
      const std::string& work_space, const size_t& num_workers_lc,
      const size_t& num_workers_cc, const size_t& num_workers_dc,
      const size_t& num_threads_cpu, const size_t& max_threads_io,
      const bool is_partition,
      AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>* app_wrapper) {
    // configure sys.
    num_workers_lc_ = num_workers_lc;
    num_workers_cc_ = num_workers_cc;
    num_workers_dc_ = num_workers_dc;

    // init partitioner
    if (is_partition) {
      edge_cut_partitioner_ =
          std::make_unique<minigraph::utility::partitioner::EdgeCutPartitioner<
              GID_T, VDATA_T, VDATA_T, EDATA_T>>(
              "/home/hsiaoko/Project/data/graph/a.csv", work_space);
      edge_cut_partitioner_->RunPartition(2);
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
        vec_gid.size() + 10);
    for (auto& iter : vec_gid) {
      read_trigger_->write(iter);
    }

    // init thread pool
    // cpu_thread_pool has only one priority queue.
    cpu_thread_pool_ =
        std::make_unique<utility::CPUThreadPool>(num_threads_cpu, 1);
    io_thread_pool_ =
        std::make_unique<utility::IOThreadPool>(1, max_threads_io);

    // init state_machine
    state_machine_ = new utility::StateMachine<GID_T>(vec_gid);

    // init task queue
    task_queue_ = std::make_unique<folly::ProducerConsumerQueue<GID_T>>(
        vec_gid.size() + 10);

    // init partial result queue
    partial_result_queue_ =
        std::make_unique<folly::ProducerConsumerQueue<GID_T>>(vec_gid.size() +
                                                              1);

    // init Data Manager.
    data_mngr_ = std::make_unique<
        utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T>>();

    // init auto_app.
    app_wrapper_ = std::make_unique<
        AppWrapper<AUTOAPP_T, GID_T, VID_T, VDATA_T, EDATA_T>>();
    app_wrapper_.reset(app_wrapper);
    if (edge_cut_partitioner_->global_border_vertexes_ != nullptr) {
      app_wrapper_->InitBorderVertexes(
          edge_cut_partitioner_->global_border_vertexes_);
    }

    // init components
    load_component_ = std::make_unique<
        components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T>>(
        cpu_thread_pool_.get(), io_thread_pool_.get(), superstep_by_gid_,
        global_superstep_, state_machine_, read_trigger_.get(),
        task_queue_.get(), pt_by_gid_, data_mngr_.get(), num_workers_lc);
    computing_component_ = std::make_unique<components::ComputingComponent<
        GID_T, VID_T, VDATA_T, EDATA_T, GRAPH_T, AUTOAPP_T>>(
        cpu_thread_pool_.get(), io_thread_pool_.get(), superstep_by_gid_,
        global_superstep_, state_machine_, task_queue_.get(),
        partial_result_queue_.get(), data_mngr_.get(), num_workers_cc,
        app_wrapper_.get());
    discharge_component_ =
        std::make_unique<components::DischargeComponent<GID_T, VID_T, VDATA_T,
                                                        EDATA_T, GRAPH_T>>(
            cpu_thread_pool_.get(), io_thread_pool_.get(), superstep_by_gid_,
            global_superstep_, state_machine_, partial_result_queue_.get(),
            pt_by_gid_, read_trigger_.get(), data_mngr_.get(), num_workers_cc);
  };

  ~MiniGraphSys() = default;

  void Stop() {
    load_component_->Stop();
    computing_component_->Stop();
    discharge_component_->Stop();
  }

  void RunSys() {
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
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    std::cout << ("         -***- RUNSYS(): finish -***-     ") << std::endl;
    GRAPH_T* graph = (GRAPH_T*)data_mngr_->GetGraph((GID_T)0);
    graph->Deserialized();
    graph->ShowGraph();

    graph = (GRAPH_T*)data_mngr_->GetGraph((GID_T)1);
    graph->Deserialized();
    graph->ShowGraph();
    this->Stop();
  }

 private:
  // configure.
  size_t num_workers_lc_ = 0;
  size_t num_workers_cc_ = 0;
  size_t num_workers_dc_ = 0;
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

  // global border vertexes
  std::unique_ptr<folly::AtomicHashMap<VID_T, std::pair<size_t, GID_T*>>>
      global_border_vertexes_ = nullptr;
  std::unique_ptr<folly::AtomicHashMap<VID_T, VertexInfo*>>
      global_border_vertesxes_info_ = nullptr;

  bool InitPtByGid(const std::string& work_space) {
    std::string vertex_root = work_space + "/vertex/";
    std::string meta_out_root = work_space + "/meta/out/";
    std::string meta_in_root = work_space + "/meta/in/";
    std::string vdata_root = work_space + "/vdata/";
    std::string localid2globalid_root = work_space + "/localid2globalid/";
    std::string msg_root = work_space + "/msg/";
    std::vector<std::string> files;
    for (const auto& entry :
         std::filesystem::directory_iterator(meta_out_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/out/");
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/out/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.meta_out_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.meta_out_pt = path;
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator(vertex_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/vertex/");
      size_t pos2 = path.find(".v");
      int type_length = std::string("/vertex/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.vertex_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.vertex_pt = path;
      }
    }
    for (const auto& entry :
         std::filesystem::directory_iterator(meta_in_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/in/");
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/in/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.meta_in_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.meta_in_pt = path;
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator(msg_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/msg/");
      size_t pos2 = path.find(".msg");
      int type_length = std::string("/msg/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.msg_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.msg_pt = path;
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator(vdata_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/vdata/");
      size_t pos2 = path.find(".vdata");
      int type_length = std::string("/vdata/").length();
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
    for (const auto& entry :
         std::filesystem::directory_iterator(localid2globalid_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/localid2globalid/");
      size_t pos2 = path.find(".map");
      int type_length = std::string("/localid2globalid/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.localid2globalid_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.localid2globalid_pt = path;
      }
    }
    return true;
  }
};

}  // namespace minigraph
#endif  // MINIGRAPH_MINIGRAPH_SYS_H
