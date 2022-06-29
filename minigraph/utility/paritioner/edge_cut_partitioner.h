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
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  EdgeCutPartitioner() {
    globalid2gid_ = new std::unordered_map<VID_T, GID_T>;
    global_border_vertexes_by_gid_ =
        new std::unordered_map<GID_T, std::vector<VID_T>*>;
  }

  ~EdgeCutPartitioner() = default;

  bool RunPartition(CSR_T& graph, const size_t number_partitions,
                    const std::string init_model = "val",
                    const VDATA_T init_vdata = 0) {
    XLOG(INFO, "RunPartition");
    // auto graph = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
    // csr_io_adapter_->Read((GRAPH_BASE_T*)graph, edge_list_csv, 0, graph_pt_);
    communication_matrix_ =
        (bool*)malloc(sizeof(bool) * number_partitions * number_partitions);
    memset(communication_matrix_, 0,
           sizeof(bool) * number_partitions * number_partitions);
    if (!SplitImmutableCSR(number_partitions, graph)) {
      LOG_INFO("SplitFailure()");
      return false;
    };

    auto count = 0;
    for (auto& iter_fragments : *fragments_) {
      auto fragment = (CSR_T*)iter_fragments;
      fragment->Serialize();
      if (init_model == "val") {
        fragment->InitVdata2AllX(init_vdata);
      } else if (init_model == "max") {
        fragment->InitVdata2AllMax();
      } else if (init_model == "vid") {
        fragment->InitVdataByVid();
      }
      auto border_vertexes = fragment->GetVertexesThatRequiredByOtherGraphs();
      MergeBorderVertexes(border_vertexes);
      count++;
    }
    SetVertexesDependencies();
    SetCommunicationMatrix();
    return true;
  }

  std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>* GetFragments() {
    return fragments_;
  }

  std::unordered_map<VID_T, std::vector<GID_T>*>* GetGlobalBorderVertexes()
      const {
    return global_border_vertexes_;
  }

  std::pair<size_t, bool*> GetCommunicationMatrix() const {
    return std::make_pair(fragments_->size(), communication_matrix_);
  }

  std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>*
  GetBorderVertexesWithDependencies() const {
    return global_border_vertexes_with_dependencies_;
  }

  std::unordered_map<VID_T, GID_T>* GetGlobalid2Gid() const {
    return globalid2gid_;
  }

  std::unordered_map<GID_T, std::vector<VID_T>*>* GetGlobalBorderVertexesbyGid()
      const {
    return global_border_vertexes_by_gid_;
  }

 private:
  std::string graph_pt_;
  // to store fragments

  bool* communication_matrix_ = nullptr;
  std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>* fragments_ =
      nullptr;
  utility::io::DataMngr<GID_T, VID_T, VDATA_T, EDATA_T> data_mgnr_;
  std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes_ =
      nullptr;

  std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>*
      global_border_vertexes_with_dependencies_ = nullptr;

  std::unordered_map<VID_T, GID_T>* globalid2gid_ = nullptr;
  std::unordered_map<GID_T, std::vector<VID_T>*>*
      global_border_vertexes_by_gid_ = nullptr;

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

  bool SetVertexesDependencies(bool is_write = false,
                               std::string output_pt = "") {
    auto global_border_vertex_with_dependencies =
        new std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>;
    for (auto& iter : *global_border_vertexes_) {
      GID_T vid = iter.first;
      VertexDependencies<VID_T, GID_T>* vd =
          new VertexDependencies<VID_T, GID_T>(vid);
      for (auto& iter_who_need : *iter.second) {
        vd->who_need_->push_back(iter_who_need);
      }
      for (auto& iter_fragments : *fragments_) {
        auto fragment = (CSR_T*)iter_fragments;
        if (fragment->globalid2localid(vid) != VID_MAX) {
          vd->who_provide_->push_back(fragment->gid_);
        }
        global_border_vertex_with_dependencies->insert(
            std::make_pair(iter.first, vd));
      }
    }
    for (auto& iter : *global_border_vertex_with_dependencies) {
      iter.second->ShowVertexDependencies();
    }
    global_border_vertexes_with_dependencies_ =
        global_border_vertex_with_dependencies;
    return true;
  }

  bool SetCommunicationMatrix() {
    if (global_border_vertexes_with_dependencies_ == nullptr) {
      LOG_ERROR(
          "segmentation fault: global_border_vertexes_with_dependencies is "
          "nullptr");
      return false;
    }
    if (communication_matrix_ == nullptr) {
      LOG_ERROR("segmentation fault: communication_matrix is nullptr");
      return false;
    }
    size_t num_graphs = fragments_->size();
    for (auto& iter : *global_border_vertexes_with_dependencies_) {
      for (auto& iter_who_need : *iter.second->who_need_) {
        for (auto& iter_who_provide : *iter.second->who_provide_) {
          *(communication_matrix_ + num_graphs * iter_who_need +
            iter_who_provide) = 1;
        }
      }
    }
    return true;
  }

  bool SplitImmutableCSR(const size_t& num_partitions, CSR_T& graph) {
    fragments_ = new std::vector<GRAPH_BASE_T*>();
    const size_t num_vertex_per_fragments =
        graph.get_num_vertexes() / num_partitions;
    globalid2gid_->reserve(graph.get_num_vertexes());
    VID_T localid = 0;
    GID_T gid = 0;
    size_t count = 0;
    graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>* csr_fragment =
        nullptr;
    auto iter_vertexes = graph.vertexes_info_->cbegin();
    while (iter_vertexes != graph.vertexes_info_->cend()) {
      if (csr_fragment == nullptr || count > num_vertex_per_fragments) {
        if (csr_fragment != nullptr) {
          csr_fragment->gid_ = gid++;
          csr_fragment->num_vertexes_ = csr_fragment->vertexes_info_->size();
          fragments_->push_back(csr_fragment);
          csr_fragment = nullptr;
          count = 0;
          localid = 0;
        }
        csr_fragment = new CSR_T();
        globalid2gid_->insert(std::make_pair(iter_vertexes->second->vid, gid));
        //auto iter_global_border_vertexes_by_gid =
        //    global_border_vertexes_by_gid_->find(gid);
        //if (iter_global_border_vertexes_by_gid !=
        //    global_border_vertexes_by_gid_->end()) {
        //  iter_global_border_vertexes_by_gid->second->push_back(
        //      iter_vertexes->second->vid);
        //} else {
        //  auto tmp_vec = new std::vector<VID_T>;
        //  tmp_vec->push_back(iter_vertexes->second->vid);
        //  global_border_vertexes_by_gid_->insert(std::make_pair(gid, tmp_vec));
        //}

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
        globalid2gid_->insert(std::make_pair(iter_vertexes->second->vid, gid));
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
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
