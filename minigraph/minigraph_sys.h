#ifndef MINIGRAPH_MINIGRAPH_SYS_H
#define MINIGRAPH_MINIGRAPH_SYS_H
#include "components/computing_component.h"
#include "components/discharge_component.h"
#include "components/load_component.h"
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
  MiniGraphSys(const std::string& work_space, const size_t& num_threads_cpu = 3,
               const size_t& max_threads_io = 3,
               const size_t& num_workers_lc = 2,
               const size_t& num_workers_cc = 3,
               const size_t& num_workers_dc = 2) {
    work_space_ = work_space;
    InitPtByGid(work_space);
    global_superstep_ = std::make_shared<std::atomic<size_t>>(0);

    // superstep_by_gid_ = std::make_shared<folly::AtomicHashMap<
    //     GID_T, std::atomic<size_t>, std::hash<int64_t>,
    //     std::equal_to<int64_t>, std::allocator<char>,
    //     folly::AtomicHashArrayQuadraticProbeFcn>(64)>();

    //    for (auto& iter : *pt_by_gid_) {
    //      superstep_by_gid_->insert(iter.first, std::atomic<size_t>(0));
    //    }
    // pt_by_gid_ = std::make_shared<folly::AtomicHashMap<
    //     GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
    //     std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>(64);
  };

  ~MiniGraphSys() = default;

 private:
  bool InitPtByGid(const std::string& work_space) {
    std::string meta_out_root = work_space + "/meta/out/";
    std::string meta_in_root = work_space + "/meta/in/";
    std::string vdata_root = work_space + "/vdata/";
    std::string localid2globalid_root = work_space + "/localid2globalid/";
    std::string msg_root = work_space + "/msg/";
    std::vector<std::string> files;
    for (const auto& entry : fs::directory_iterator(meta_out_root)) {
      //  std::string path = entry.path();
      //  size_t pos = path.find("/out/");
      //  size_t pos2 = path.find(".meta");
      //  int type_length = std::string("/out/").length();
      //  GID_T gid = (GID_T)std::stoi(
      //      path.substr(pos + type_length, pos2 - pos - type_length));
      //  auto iter = pt_by_gid_->find(gid);
      //  if (iter == pt_by_gid_->end()) {
      //    CSRPt csr_pt;
      //    csr_pt.meta_out_pt = path;
      //    pt_by_gid_->insert(gid, csr_pt);
      //  } else {
      //    iter->second.meta_out_pt = path;
      //  }
    }
    /*
    for (const auto& entry :
         std::filesystem::directory_iterator(meta_in_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/in/");
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/in/").length();
      GID_T gid = (GID_T)std::stoi(
          path.substr(pos + type_length, pos2 - pos - type_length));
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.meta_in_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.meta_in_pt = path;
      }
    }
    for (const auto& entry :
         std::filesystem::directory_iterator(meta_in_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/in/");
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/in/").length();
      GID_T gid = (GID_T)std::stoi(
          path.substr(pos + type_length, pos2 - pos - type_length));
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.meta_in_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.meta_in_pt = path;
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator(vdata_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/vdata/");
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/vdata/").length();
      GID_T gid = (GID_T)std::stoi(
          path.substr(pos + type_length, pos2 - pos - type_length));
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
      size_t pos2 = path.find(".meta");
      int type_length = std::string("/localid2globalid/").length();
      GID_T gid = (GID_T)std::stoi(
          path.substr(pos + type_length, pos2 - pos - type_length));
      auto iter = pt_by_gid_->find(gid);
      if (iter == pt_by_gid_->end()) {
        CSRPt csr_pt;
        csr_pt.localid2globalid_pt = path;
        pt_by_gid_->insert(gid, csr_pt);
      } else {
        iter->second.localid2globalid_pt = path;
      }
    }
    for (auto& iter : *pt_by_gid_) {
      cout << iter.first << ", " << iter.second.meta_out_pt << endl;
      XLOG(INFO, iter.first, iter.second.meta_out_pt, iter.second.meta_in_pt,
           iter.second.vdata_pt, iter.second.localid2globalid_pt);
    }
    */
    return true;
  }

 private:
  std::string work_space_;
  std::shared_ptr<std::atomic<size_t>> global_superstep_;

  std::shared_ptr<folly::AtomicHashMap<
      GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      pt_by_gid_;
  std::shared_ptr<folly::AtomicHashMap<
      GID_T, std::atomic<size_t>, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      superstep_by_gid_;

  std::unique_ptr<components::LoadComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      load_component_;
  std::unique_ptr<
      components::ComputingComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      computing_component_;
  std::unique_ptr<
      components::DischargeComponent<GID_T, VID_T, VDATA_T, EDATA_T>>
      discharge_component_;
};

}  // namespace minigraph

#endif  // MINIGRAPH_MINIGRAPH_SYS_H
