#ifndef MINIGRAPH_DATA_MNGR_H
#define MINIGRAPH_DATA_MNGR_H

#include <memory>

#include <folly/AtomicHashMap.h>

#include "utility/io/csr_io_adapter.h"

namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class DataMgnr {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using MSG_T = graphs::Message<VID_T, VDATA_T, EDATA_T>;
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  std::unique_ptr<utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_;

  DataMgnr() {
    pgraph_by_gid_ =
        std::make_unique<folly::AtomicHashMap<GID_T, GRAPH_BASE_T*>>(1024);

    csr_io_adapter_ = std::make_unique<
        utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>();
  };

  bool LoadGraph(const GID_T& gid, const CSRPt& csr_pt) {
    auto immutable_csr =
        new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
    if (csr_io_adapter_->Read((GRAPH_BASE_T*)immutable_csr, csr_bin, gid,
                              csr_pt.vertex_pt, csr_pt.meta_in_pt,
                              csr_pt.meta_out_pt, csr_pt.vdata_pt,
                              csr_pt.localid2globalid_pt)) {
      pgraph_by_gid_->insert(gid, (GRAPH_BASE_T*)immutable_csr);
      return true;
    } else {
      return false;
    }
  }

  bool WriteGraph(const GID_T& gid, const CSRPt& csr_pt) {
    auto graph = this->GetGraph(gid);
    LOG_INFO("write gid: ", gid);
    if (csr_io_adapter_->Write(*((GRAPH_BASE_T*)graph), csr_bin,
                               csr_pt.vertex_pt, csr_pt.meta_in_pt,
                               csr_pt.meta_out_pt, csr_pt.vdata_pt,
                               csr_pt.localid2globalid_pt)) {
      return true;
    } else {
      return false;
    }
  }

  bool WriteBorderVertexes(const std::unordered_map<VID_T, std::vector<GID_T>*>&
                               global_border_vertexes,
                           const std::string& border_vertexes_pt) {
    if (IsExist(border_vertexes_pt)) {
      remove(border_vertexes_pt.c_str());
    }
    size_t* buf_config = (size_t*)malloc(sizeof(size_t) * 2);
    buf_config[0] = global_border_vertexes.size();
    VID_T* buf_border_vertexes = (VID_T*)malloc(sizeof(VID_T) * buf_config[0]);
    std::ofstream border_vertexes_file(border_vertexes_pt,
                                       std::ios::binary | std::ios::app);
    size_t i = 0;
    for (auto& iter : global_border_vertexes) {
      buf_border_vertexes[i++] = iter.first;
    }
    border_vertexes_file.write((char*)buf_config, sizeof(size_t));
    border_vertexes_file.write((char*)buf_border_vertexes,
                               sizeof(VID_T) * buf_config[0]);
    free(buf_config);
    free(buf_border_vertexes);
    border_vertexes_file.close();
    return true;
  }

  std::unordered_map<VID_T, std::vector<GID_T>*>* ReadBorderVertexes(
      const std::string& border_vertexes_pt) {
    auto global_border_vertexes =
        new std::unordered_map<VID_T, std::vector<GID_T>*>();
    std::ifstream border_vertexes_file(border_vertexes_pt,
                                       std::ios::binary | std::ios::app);

    size_t* buf_config = (size_t*)malloc(sizeof(size_t));
    border_vertexes_file.read((char*)buf_config, sizeof(size_t));
    VID_T* buf_border_vertexes = (VID_T*)malloc(sizeof(VID_T) * buf_config[0]);

    border_vertexes_file.read((char*)buf_border_vertexes,
                              sizeof(VID_T) * buf_config[0]);
    for (size_t i = 0; i < buf_config[0]; i++) {
      global_border_vertexes->insert(
          std::make_pair(buf_border_vertexes[i], nullptr));
    }
    border_vertexes_file.close();
    return global_border_vertexes;
  }

  GRAPH_BASE_T* GetGraph(const GID_T& gid) {
    if (pgraph_by_gid_->count(gid)) {
      return pgraph_by_gid_->find(gid)->second;
    } else {
      return nullptr;
    }
  }

  void EraseGraph(const GID_T& gid) {
    if (pgraph_by_gid_->count(gid)) {
      auto&& graph = pgraph_by_gid_->find(gid)->second;
      graph->CleanUp();
      pgraph_by_gid_->erase(gid);
    }
  };

  void CleanUp() {
    for (auto& iter : *pgraph_by_gid_) {
      iter.second->CleanUp();
    }
  }

  void MakeDirectory(const std::string& pt) {
    std::string dir = pt;
    int len = dir.size();
    if (dir[len - 1] != '/') {
      dir[len] = '/';
      len++;
    }
    std::string temp;
    for (int i = 1; i < len; i++) {
      if (dir[i] == '/') {
        temp = dir.substr(0, i);
        if (access(temp.c_str(), 0) != 0) {
          if (mkdir(temp.c_str(), 0777) != 0) {
            VLOG(1) << "failed operaiton.";
          }
        }
      }
    }
  }

  bool IsExist(const std::string& pt) const {
    struct stat buffer;
    return (stat(pt.c_str(), &buffer) == 0);
  }

 private:
  std::unique_ptr<MSG_T> global_msg_ = nullptr;
  std::unique_ptr<folly::AtomicHashMap<GID_T, GRAPH_BASE_T*>> pgraph_by_gid_ =
      nullptr;
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph
#endif  // MINIGRAPH_DATA_MNGR_H
