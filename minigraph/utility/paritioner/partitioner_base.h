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
  PartitionerBase() {
    if (this->fragments_ == nullptr)
      this->fragments_ =
          new std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>;
  }

  virtual bool ParallelPartition(EDGE_LIST_T* edgelist_graph = nullptr,
                                 const size_t num_partitions = 1,
                                 const size_t cores = 1, const std::string = "",
                                 bool delete_graph = false) = 0;

  StatisticInfo ParallelSetStatisticInfo(CSR_T& csr_graph, const size_t cores) {
    auto thread_pool = minigraph::utility::EDFThreadPool(cores);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);

    StatisticInfo si;
    si.num_active_vertexes = csr_graph.get_num_vertexes();
    si.num_vertexes = csr_graph.get_num_vertexes();
    si.num_edges = csr_graph.get_num_edges();

    size_t pending_package = cores;
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit(
          [tid, &cores, &csr_graph, &si, &pending_package, &finish_cv]() {
            size_t local_sum_border_vertexes = 0;
            size_t local_sum_out_degree = 0;
            size_t local_sum_dgv_times_dgv = 0;
            size_t local_sum_dlv_times_dlv = 0;
            size_t local_sum_dlv_times_dgv = 0;
            size_t local_sum_dlv = 0;
            size_t local_sum_dgv = 0;
            for (size_t i = tid; i < csr_graph.get_num_vertexes(); i += cores) {
              auto u = csr_graph.GetVertexByIndex(i);
              size_t dlv = 0;
              size_t dgv = u.outdegree;
              for (size_t out_nbr_i = 0; out_nbr_i < u.outdegree; ++out_nbr_i) {
                if (!csr_graph.IsInGraph(u.out_edges[out_nbr_i])) {
                  ++local_sum_border_vertexes;
                  continue;
                }
                ++dlv;
                ++local_sum_out_degree;
              }
              local_sum_dlv_times_dgv += dlv * dgv;
              local_sum_dlv_times_dlv += dlv * dlv;
              local_sum_dgv_times_dgv += dgv * dgv;
              local_sum_dgv += dgv;
              local_sum_dlv += dlv;
            }

            write_add(&si.sum_out_degree, local_sum_out_degree);
            write_add(&si.sum_dlv_times_dgv, local_sum_dlv_times_dgv);
            write_add(&si.sum_dlv_times_dlv, local_sum_dlv_times_dlv);
            write_add(&si.sum_dgv_times_dgv, local_sum_dgv_times_dgv);
            write_add(&si.sum_out_border_vertexes, local_sum_border_vertexes);
            write_add(&si.sum_dlv, local_sum_dlv);
            write_add(&si.sum_dgv, local_sum_dgv);
            if (__sync_fetch_and_sub(&pending_package, 1) == 1)
              finish_cv.notify_all();

            return;
          });
    }
    finish_cv.wait(lck, [&] { return pending_package == 0; });

    // si.ShowInfo();
    return si;
  }

  std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>* GetFragments() {
    return fragments_;
  }

  std::unordered_map<VID_T, std::vector<GID_T>*>* GetGlobalBorderVertexes()
      const {
    return global_border_vertexes_;
  }

  std::pair<size_t, bool*> GetCommunicationMatrix() const {
    return std::make_pair(num_partitions, communication_matrix_);
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
  // Basic parameters.
  VID_T max_vid_ = 0;
  VID_T aligned_max_vid_ = 0;
  size_t num_vertexes_ = 0;
  size_t num_edges_ = 0;
  size_t num_partitions = 0;

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
