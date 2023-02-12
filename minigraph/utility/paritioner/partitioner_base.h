#ifndef MINIGRAPH_PARTITIONER_BASE_H
#define MINIGRAPH_PARTITIONER_BASE_H

#include "graphs/graph.h"

namespace minigraph {
namespace utility {
namespace partitioner {

template <typename GRAPH_T>
class PartitionerBase {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  PartitionerBase(){
    if (this->fragments_ == nullptr)
      this->fragments_ =
          new std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>;
  }

  virtual bool ParallelPartitionFromBin(const std::string& pt,
                                        const size_t num_partitions,
                                        const size_t cores) = 0;
  virtual bool ParallelPartition(const std::string& pt,
                                 char separator_params = ',',
                                 const size_t num_partitions = 1,
                                 const size_t cores = 1) = 0;

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

  VID_T GetMaxVid() const { return max_vid_; }

  Bitmap* GetGlobalBorderVidMap() { return global_border_vid_map_; }

  VID_T* GetVidMap() { return vid_map_; }

 public:
  VID_T max_vid_ = 0;
  bool* communication_matrix_ = nullptr;
  Bitmap* global_border_vid_map_ = nullptr;
  std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>*
      global_border_vertexes_with_dependencies_ = nullptr;
  std::unordered_map<VID_T, GID_T>* globalid2gid_ = nullptr;
  std::unordered_map<GID_T, std::vector<VID_T>*>*
      global_border_vertexes_by_gid_ = nullptr;
  VID_T* vid_map_ = nullptr;
  std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>* fragments_ =
      nullptr;
  std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes_ =
      nullptr;
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph
#endif  // MINIGRAPH_PARTITIONER_BASE_H
