#ifndef MINIGRAPH_MINIGRAPH_SYS_H
#define MINIGRAPH_MINIGRAPH_SYS_H
#include "components/computing_component.h"
#include "components/discharge_component.h"
#include "components/load_component.h"
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
using namespace std;
namespace minigraph {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class MiniGraphSys {
 public:
  MiniGraphSys(const std::string& work_space, const size_t& num_workers_lc,
               const size_t& num_workers_cc, const size_t& num_workers_dc,
               const size_t& num_threads_cpu = 3,
               const size_t& max_threads_io = 3,
               const bool is_partition = false) {
    XLOG(INFO, work_space);
    XLOG(INFO, "lc: ", num_workers_lc, "cc: ", num_workers_cc,
         "dc: ", num_workers_dc);
    // configure sys.
    num_workers_lc_ = num_workers_lc;
    num_workers_cc_ = num_workers_cc;
    num_workers_dc_ = num_workers_dc;

    // init partitioner
    if (is_partition) {
      edge_cut_partitioner_ =
          std::make_unique<minigraph::utility::partitioner::EdgeCutPartitioner<
              GID_T, VDATA_T, VDATA_T, EDATA_T>>(
              "/home/hsiaoko/Project/data/graph/soc-LiveJournal1.mini.csv",
              work_space);
      edge_cut_partitioner_->RunPartition(2);
    }

    // find all files.
    pt_by_gid_ =
        new folly::AtomicHashMap<GID_T, CSRPt, std::hash<int64_t>,
                                 std::equal_to<int64_t>, std::allocator<char>,
                                 folly::AtomicHashArrayQuadraticProbeFcn>(64);
    InitPtByGid(work_space);

    // init global superstep
    global_superstep_ = new std::atomic<size_t>(0);

    // init superstep of fragments as all 0;
    std::vector<GID_T> vec_gid;
    superstep_by_gid_ =
        new folly::AtomicHashMap<GID_T, std::shared_ptr<std::atomic<size_t>>,
                                 std::hash<int64_t>, std::equal_to<int64_t>,
                                 std::allocator<char>,
                                 folly::AtomicHashArrayQuadraticProbeFcn>(64);
    for (auto& iter : *pt_by_gid_) {
      auto step = std::atomic<size_t>(0);
      superstep_by_gid_->insert(iter.first,
                                std::make_shared<std::atomic<size_t>>());
      vec_gid.push_back(iter.first);
    }

    // init read_trigger
    read_trigger_ =
        std::make_unique<folly::ProducerConsumerQueue<GID_T>>(vec_gid.size());
    for (auto& iter : vec_gid) {
      read_trigger_->write(iter);
    }

    // init thread pool
    // cpu_thread_pool has only one priority queue.
    cpu_thread_pool_ = new utility::CPUThreadPool(num_threads_cpu, 1);
    io_thread_pool_ = new utility::IOThreadPool(1, max_threads_io);

    // init state_machine
    state_machine_ = new utility::StateMachine<GID_T>(vec_gid);

    // init task queue
    task_queue_ = new folly::ProducerConsumerQueue<std::pair<
        GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>(
        vec_gid.size());

    // init partial result queue
    partial_result_queue_ = new folly::ProducerConsumerQueue<std::pair<
        GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>(
        vec_gid.size());
    // init global message
    // global_msg_ = std::make_shared<graphs::Message<VID_T, VDATA_T, EDATA_T >>
    // ;

    // init components
    load_component_ = std::make_unique<
        components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T>>(
        cpu_thread_pool_, io_thread_pool_, superstep_by_gid_, global_superstep_,
        state_machine_, task_queue_, pt_by_gid_, read_trigger_.get());
    computing_component_ = std::make_unique<
        components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T>>(
        cpu_thread_pool_, io_thread_pool_, superstep_by_gid_, global_superstep_,
        state_machine_, task_queue_, partial_result_queue_);
    discharge_component_ = std::make_unique<
        components::DischargeComponent<GID_T, VID_T, VDATA_T, EDATA_T>>(
        cpu_thread_pool_, io_thread_pool_, superstep_by_gid_, global_superstep_,
        state_machine_, partial_result_queue_, pt_by_gid_);
  };

  ~MiniGraphSys() = default;

  void RunSys() {
    for (size_t i = 0; i < num_workers_lc_; i++) {
      auto task = std::bind(
          &components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T>::Run,
          load_component_.get());
      this->cpu_thread_pool_->Commit(task);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    XLOG(INFO, "RUNSYS");
  }

 private:
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

 protected:
  // configure.
  size_t num_workers_lc_ = 0;
  size_t num_workers_cc_ = 0;
  size_t num_workers_dc_ = 0;
  std::unique_ptr<folly::ProducerConsumerQueue<GID_T>> read_trigger_;

  // files list
  folly::AtomicHashMap<GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
                       std::allocator<char>,
                       folly::AtomicHashArrayQuadraticProbeFcn>* pt_by_gid_ =
      nullptr;

  // partitioner
  std::unique_ptr<minigraph::utility::partitioner::EdgeCutPartitioner<
      GID_T, VDATA_T, VDATA_T, EDATA_T>>
      edge_cut_partitioner_;

  // thread pool.
  utility::IOThreadPool* io_thread_pool_ = nullptr;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;

  // superstep.
  folly::AtomicHashMap<
      GID_T, std::shared_ptr<std::atomic<size_t>>, std::hash<int64_t>,
      std::equal_to<int64_t>, std::allocator<char>,
      folly::AtomicHashArrayQuadraticProbeFcn>* superstep_by_gid_ = nullptr;
  std::atomic<size_t>* global_superstep_ = nullptr;

  // state machine.
  utility::StateMachine<GID_T>* state_machine_ = nullptr;

  // task queue.
  folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
      task_queue_ = nullptr;

  // partial result queue.
  folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
      partial_result_queue_ = nullptr;

  // global message in shared memory.
  graphs::Message<VID_T, VDATA_T, EDATA_T>* global_msg_ = nullptr;

  // components.
  std::unique_ptr<components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      load_component_ = nullptr;
  std::unique_ptr<
      components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      computing_component_ = nullptr;
  std::unique_ptr<
      components::DischargeComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      discharge_component_ = nullptr;
};
}  // namespace minigraph
#endif  // MINIGRAPH_MINIGRAPH_SYS_H
