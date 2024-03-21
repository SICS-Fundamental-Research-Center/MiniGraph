#ifndef MINIGRAPH_UTILITY_VERTEXCUT_PARTITIONER_H
#define MINIGRAPH_UTILITY_VERTEXCUT_PARTITIONER_H

#include <atomic>
#include <cstring>
#include <stdio.h>
#include <string.h>
#include <unordered_map>
#include <vector>

#include "graphs/graph.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/io/io_adapter_base.h"
#include "utility/paritioner/partitioner_base.h"
#include "utility/thread_pool.h"

#include <folly/AtomicHashMap.h>
#include <folly/FBVector.h>

namespace minigraph {
namespace utility {
namespace partitioner {

template <typename GRAPH_T>
class VertexCutPartitioner : public PartitionerBase<GRAPH_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  VertexCutPartitioner() = default;
  ~VertexCutPartitioner() = default;

  bool ParallelPartition(EDGE_LIST_T* edgelist_graph,
                         const size_t num_partitions = 1,
                         const size_t cores = 1, const std::string dst_pt = "",
                         bool delete_graph = false) override {
    LOG_INFO("ParallelPartition(): VertexCut");

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);

    minigraph::utility::io::DataMngr<CSR_T> data_mngr;
    VID_T aligned_max_vid =
        ceil(edgelist_graph->max_vid_ / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    this->vid_map_ = (VID_T*)malloc(sizeof(VID_T) * aligned_max_vid);
    memset(this->vid_map_, 0, sizeof(VID_T) * aligned_max_vid);
    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    memset(num_in_edges, 0, sizeof(size_t) * aligned_max_vid);
    memset(num_out_edges, 0, sizeof(size_t) * aligned_max_vid);
    size_t* size_per_bucket = new size_t[num_partitions];
    memset(size_per_bucket, 0, sizeof(size_t) * num_partitions);
    size_t num_edges = edgelist_graph->num_edges_;
    this->global_border_vid_map_ = new Bitmap(aligned_max_vid);
    this->global_border_vid_map_->clear();
    this->num_partitions = num_partitions;

    LOG_INFO("Compute the number of edges for each bucket.");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_edges, &edgelist_graph,
                          &num_partitions, &size_per_bucket, &pending_packages,
                          &finish_cv]() {
        if (tid > num_edges) return;
        for (size_t j = tid; j < num_edges; j += cores) {
          auto bucket_id =
              Hash(edgelist_graph->buf_graph_[j * 2]) % num_partitions;
          __sync_add_and_fetch(size_per_bucket + bucket_id, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    VID_T** edges_buckets = nullptr;
    edges_buckets = (VID_T**)malloc(sizeof(VID_T*) * num_partitions);
    for (GID_T i = 0; i < num_partitions; i++) {
      edges_buckets[i] = (VID_T*)malloc(sizeof(VID_T) * 2 * size_per_bucket[i]);
      memset(edges_buckets[i], 0, sizeof(VID_T) * 2 * size_per_bucket[i]);
    }
    Bitmap* is_in_bucketX[num_partitions];
    for (size_t i = 0; i < num_partitions; i++) {
      is_in_bucketX[i] = new Bitmap(aligned_max_vid);
      is_in_bucketX[i]->clear();
    }
    std::size_t* buckets_offset = new size_t[num_partitions];
    memset(buckets_offset, 0, sizeof(size_t) * num_partitions);
    std::size_t* num_vertexes_per_bucket = new size_t[num_partitions];
    memset(num_vertexes_per_bucket, 0, sizeof(size_t) * num_partitions);
    VID_T* max_vid_per_bucket = new VID_T[num_partitions];
    memset(max_vid_per_bucket, 0, sizeof(VID_T) * num_partitions);

    LOG_INFO("Second round of iterations to drop edges into buckets.");
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &num_edges, &edgelist_graph,
                          &num_partitions, &max_vid_per_bucket, &buckets_offset,
                          &is_in_bucketX, &num_vertexes_per_bucket,
                          &edges_buckets, &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = edgelist_graph->buf_graph_[j * 2];
          auto dst_vid = edgelist_graph->buf_graph_[j * 2 + 1];
          auto bucket_id = Hash(src_vid) % num_partitions;

          write_max(max_vid_per_bucket + bucket_id, src_vid);
          write_max(max_vid_per_bucket + bucket_id, dst_vid);

          auto offset = __sync_fetch_and_add(buckets_offset + bucket_id, 1);
          edges_buckets[bucket_id][offset * 2] = src_vid;
          edges_buckets[bucket_id][offset * 2 + 1] = dst_vid;

          is_in_bucketX[bucket_id]->set_bit(src_vid);
          is_in_bucketX[bucket_id]->set_bit(dst_vid);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    delete edgelist_graph;

    for (size_t i = 0; i < num_partitions; i++) {
      *(num_vertexes_per_bucket + i) = is_in_bucketX[i]->get_num_bit();
    }

    minigraph::utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>
        csr_io_adapter;

    LOG_INFO("Run: Construct sub-graphs");
    if (this->fragments_ != nullptr) this->fragments_->clear();

    for (size_t gid = 0; gid < num_partitions; gid++) {
      auto edgelist_graph = new EDGE_LIST_T(
          gid, size_per_bucket[gid], num_vertexes_per_bucket[gid],
          max_vid_per_bucket[gid], edges_buckets[gid]);
      auto csr_graph = csr_io_adapter.EdgeList2CSR(gid, edgelist_graph, cores);
      delete edgelist_graph;
      free(edges_buckets[gid]);
      csr_graph->InitVdata2AllX(0);
      csr_graph->Sort(cores);
      if (!delete_graph) {
        this->fragments_->push_back(csr_graph);
      } else {
        std::string meta_pt =
            dst_pt + "minigraph_meta/" + std::to_string(gid) + ".bin";
        std::string data_pt =
            dst_pt + "minigraph_data/" + std::to_string(gid) + ".bin";
        std::string vdata_pt =
            dst_pt + "minigraph_vdata/" + std::to_string(gid) + ".bin";
        data_mngr.csr_io_adapter_->Write(*csr_graph, csr_bin, false, meta_pt,
                                         data_pt, vdata_pt);
        StatisticInfo&& si = this->ParallelSetStatisticInfo(*csr_graph, cores);
        std::string si_pt =
            dst_pt + "minigraph_si/" + std::to_string(gid) + ".yaml";
        data_mngr.WriteStatisticInfo(si, si_pt);
        csr_graph->SetGlobalBorderVidMap(this->global_border_vid_map_,
                                         is_in_bucketX, num_partitions);
        delete csr_graph;
      }
    }

    LOG_INFO("Run: Set communication matrix");
    if (this->communication_matrix_ == nullptr)
      this->communication_matrix_ =
          (bool*)malloc(sizeof(bool) * num_partitions * num_partitions);
    else
      free(this->communication_matrix_);
    memset(this->communication_matrix_, 0,
           sizeof(bool) * num_partitions * num_partitions);
    for (size_t i = 0; i < num_partitions * num_partitions; i++)
      this->communication_matrix_[i] = 1;
    for (size_t i = 0; i < num_partitions; i++)
      *(this->communication_matrix_ + i * num_partitions + i) = 0;

    for (size_t i = 0; i < num_partitions; i++) {
      delete is_in_bucketX[i];
    }
    free(num_vertexes_per_bucket);
    free(max_vid_per_bucket);
    free(size_per_bucket);
    delete num_in_edges;
    delete num_out_edges;
    LOG_INFO("END");

    return true;
  }

 private:
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_VERTEXCUT_PARTITIONER_H