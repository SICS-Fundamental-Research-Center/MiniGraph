//
// Created by hsiaoko on 2022/3/15.
//

#ifndef MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
#define MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H

#include "portability/sys_types.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/io/io_adapter_base.h"
#include <folly/AtomicHashMap.h>
#include <folly/FBVector.h>
#include <vector>

namespace minigraph {
namespace utility {
namespace partitioner {

// With an edgecut partition, each vertex is assigned to a fragment.
// In a fragment, inner vertices are those vertices assigned to it, and the
// outer vertices are the remaining vertices adjacent to some of the inner
// vertices. The load strategy defines how to store the adjacency between inner
// and outer vertices.
//
// For example, a graph
// G = {V, E}
// V = {v0, v1, v2, v3, v4}
// E = {(v0, v2), (v0, v3), (v1, v0), (v3, v1), (v3, v4), (v4, v1), (v4, v2)}
// might be splitted into F0 that consists of  V_F0: {v0, v1, v2}, E_F0: {(v0,
// v2), (v0, v3), (v1, v0)} and F1 that consists of V_F1: {v3, v4}, E_F1: {(v3,
// v1), (v3, v4), (v4, v1), (v4, v2)}
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class EdgeCutPartitioner {
 public:
  EdgeCutPartitioner(const std::string& graph_pt, const std::string& root_pt) {
    graph_pt_ = graph_pt;
    root_pt_ = root_pt;
    global_border_vertexes_ =
        new std::unordered_map<VID_T, std::vector<GID_T>*>();
  };

  bool RunPartition(const size_t& number_partitions) {
    XLOG(INFO, "RunPartition");
    if (!SplitImmutableCSR(number_partitions, graph_pt_)) {
      LOG_INFO("SplitFailure()");
      return false;
    };
    if (!data_mgnr_.IsExist(root_pt_ + "/meta/")) {
      data_mgnr_.MakeDirectory(root_pt_ + "/meta/");
    }
    if (!data_mgnr_.IsExist(root_pt_ + "/data/")) {
      data_mgnr_.MakeDirectory(root_pt_ + "/data/");
    }
    if (!data_mgnr_.IsExist(root_pt_ + "/border_vertexes/")) {
      data_mgnr_.MakeDirectory(root_pt_ + "/border_vertexes/");
    }
    auto count = 0;
    for (auto& iter_fragments : *fragments_) {
      auto fragment =
          (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*)iter_fragments;
      std::string meta_pt =
          root_pt_ + "/meta/" + std::to_string(count) + ".meta";
      std::string data_pt =
          root_pt_ + "/data/" + std::to_string(count) + ".data";
      fragment->Serialize();
      fragment->ShowGraphAbs();
      data_mgnr_.csr_io_adapter_->Write(*fragment, immutable_csr_bin, meta_pt,
                                        data_pt);
      auto border_vertexes = fragment->GetBorderVertexes();
      MergeBorderVertexes(border_vertexes);
      count++;
    }
    return true;
  }

  bool SplitImmutableCSR(const size_t& num_partitions,
                         const std::string& graph_pt) {
    csr_io_adapter_ =
        std::make_unique<io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>();
    auto immutable_csr =
        new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
    if (!csr_io_adapter_->Read(
            (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)immutable_csr,
            edge_list_csv, 0, graph_pt)) {
      return false;
    }
    fragments_ =
        new std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>();
    const size_t num_vertex_per_fragments =
        immutable_csr->get_num_vertexes() / num_partitions;
    VID_T localid = 0;
    GID_T gid = 0;
    size_t count = 0;
    graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>* csr_fragment =
        nullptr;
    auto iter_vertexes = immutable_csr->vertexes_info_->cbegin();
    while (iter_vertexes != immutable_csr->vertexes_info_->cend()) {
      if (csr_fragment == nullptr || count > num_vertex_per_fragments) {
        if (csr_fragment != nullptr) {
          csr_fragment->gid_ = gid++;
          csr_fragment->num_vertexes_ = csr_fragment->vertexes_info_->size();
          fragments_->push_back(csr_fragment);
          csr_fragment = nullptr;
          count = 0;
          localid = 0;
        }
        csr_fragment =
            new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>();
        csr_fragment->map_localid2globalid_->emplace(
            std::make_pair(localid, iter_vertexes->second->vid));
        csr_fragment->map_globalid2localid_->emplace(
            std::make_pair(iter_vertexes->second->vid, localid));
        iter_vertexes->second->vid = localid;
        csr_fragment->sum_in_edges_ += iter_vertexes->second->indegree;
        csr_fragment->sum_out_edges_ += iter_vertexes->second->outdegree;
        csr_fragment->vertexes_info_->emplace(
            std::make_pair(localid, iter_vertexes->second));
        iter_vertexes++;
        ++localid;
        ++count;
      } else {
        csr_fragment->map_localid2globalid_->emplace(
            std::make_pair(localid, iter_vertexes->second->vid));
        csr_fragment->map_globalid2localid_->emplace(
            std::make_pair(iter_vertexes->second->vid, localid));
        iter_vertexes->second->vid = localid;
        csr_fragment->sum_in_edges_ += iter_vertexes->second->indegree;
        csr_fragment->sum_out_edges_ += iter_vertexes->second->outdegree;
        csr_fragment->vertexes_info_->emplace(
            std::make_pair(localid, iter_vertexes->second));
        iter_vertexes++;
        ++localid;
        ++count;
      }
    }
    if (csr_fragment != nullptr) {
      csr_fragment->gid_ = gid++;
      csr_fragment->num_vertexes_ = csr_fragment->vertexes_info_->size();
      fragments_->push_back(csr_fragment);
    }
    if (fragments_->size() > 0) {
      return true;
    } else {
      return false;
    }
  }

  std::unordered_map<VID_T, std::vector<GID_T>*>* GetGlobalBorderVertexes()
      const {
    return global_border_vertexes_;
  }

 private:
  std::string graph_pt_;

  // to store fragments
  std::string root_pt_;

  std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>* fragments_ =
      nullptr;

  utility::io::DataMgnr<GID_T, VID_T, VDATA_T, EDATA_T> data_mgnr_;

  std::unique_ptr<io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_ = nullptr;

  std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes_ =
      nullptr;

  void MergeBorderVertexes(std::unordered_map<VID_T, GID_T>* border_vertexes) {
    if (global_border_vertexes_ == nullptr) {
      global_border_vertexes_ =
          new std::unordered_map<VID_T, std::vector<GID_T>*>();
    }
    for (auto iter = border_vertexes->begin(); iter != border_vertexes->end();
         iter++) {
      auto iter_global = global_border_vertexes_->find(iter->first);
      if (iter_global != global_border_vertexes_->end()) {
        iter_global->second->push_back(iter->second);
      } else {
        std::vector<GID_T>* vec_gid = new std::vector<GID_T>;
        vec_gid->push_back(iter->second);
        global_border_vertexes_->insert(std::make_pair(iter->first, vec_gid));
      }
    }
  }
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
