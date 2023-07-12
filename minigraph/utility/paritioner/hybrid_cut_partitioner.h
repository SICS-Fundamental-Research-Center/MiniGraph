#ifndef MINIGRAPH_UTILITY_HYBRID_CUT_PARTITIONER_H
#define MINIGRAPH_UTILITY_HYBRID_CUT_PARTITIONER_H

#include <atomic>
#include <cstring>
#include <stdio.h>
#include <unordered_map>
#include <vector>

#include <folly/AtomicHashMap.h>
#include <folly/FBVector.h>

#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/io/io_adapter_base.h"
#include "utility/paritioner/partitioner_base.h"
#include "utility/thread_pool.h"

namespace minigraph {
namespace utility {
namespace partitioner {

template <typename GRAPH_T>
class HybridCutPartitioner : public PartitionerBase<GRAPH_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  HybridCutPartitioner() = default;
  ~HybridCutPartitioner() = default;

  bool ParallelPartition(EDGE_LIST_T* edgelist_graph,
                         const size_t num_partitions = 1,
                         const size_t cores = 1, const std::string dst_pt = "",
                         bool delete_graph = false) override {
    LOG_INFO("ParallelPartition(): HybridCut");

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);

    this->vid_map_ = nullptr;
    LOG_INFO(ceil(edgelist_graph->get_aligned_max_vid() / ALIGNMENT_FACTOR) *
             ALIGNMENT_FACTOR);
    size_t num_new_buckets = NUM_NEW_BUCKETS;

    auto set_graphs = (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*) *
        (num_partitions + num_new_buckets));
    for (size_t i = 0; i < num_partitions + num_new_buckets; i++)
      set_graphs[i] = nullptr;
    VID_T aligned_max_vid = edgelist_graph->get_aligned_max_vid();
    this->aligned_max_vid_ = aligned_max_vid;
    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    memset(num_in_edges, 0, sizeof(size_t) * aligned_max_vid);
    memset(num_out_edges, 0, sizeof(size_t) * aligned_max_vid);
    size_t* size_per_bucket = new size_t[num_partitions];
    memset(size_per_bucket, 0, sizeof(size_t) * num_partitions);
    size_t num_edges = edgelist_graph->num_edges_;
    minigraph::utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>
        csr_io_adapter;

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

    Bitmap* is_in_bucketX[num_partitions + num_new_buckets];
    for (size_t i = 0; i < num_partitions + num_new_buckets; i++) {
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

          write_max(max_vid_per_bucket + bucket_id, (VID_T)src_vid);
          write_max(max_vid_per_bucket + bucket_id, (VID_T)dst_vid);

          auto offset = __sync_fetch_and_add(buckets_offset + bucket_id, 1);
          edges_buckets[bucket_id][offset * 2] = src_vid;
          edges_buckets[bucket_id][offset * 2 + 1] = dst_vid;

          if (is_in_bucketX[bucket_id]->get_bit(src_vid) == 0) {
            is_in_bucketX[bucket_id]->set_bit(src_vid);
            write_add(num_vertexes_per_bucket + bucket_id, (size_t)1);
          }
          if (is_in_bucketX[bucket_id]->get_bit(dst_vid) == 0) {
            is_in_bucketX[bucket_id]->set_bit(dst_vid);
            write_add(num_vertexes_per_bucket + bucket_id, (size_t)1);
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    delete edgelist_graph;

    LOG_INFO("Run: Construct sub-graphs");
    if (this->fragments_ != nullptr) this->fragments_->clear();

    for (size_t gid = 0; gid < num_partitions; gid++) {
      auto edgelist_graph = new EDGE_LIST_T(
          gid, size_per_bucket[gid], num_vertexes_per_bucket[gid],
          max_vid_per_bucket[gid], edges_buckets[gid]);
      auto csr_graph = csr_io_adapter.EdgeList2CSR(gid, edgelist_graph, cores,
                                                   this->vid_map_);
      csr_graph->Sort(cores);
      set_graphs[gid] = csr_graph;
      delete edgelist_graph;
    }

    auto bucket_id_to_be_splitted = 0;
    auto num_graph_max_vertexes = 0;
    for (GID_T gid = 0; gid < num_partitions; gid++) {
      LOG_INFO("GID: ", gid,
               " num_vertexes: ", set_graphs[gid]->get_num_vertexes());
      if (num_graph_max_vertexes < set_graphs[gid]->get_num_vertexes()) {
        bucket_id_to_be_splitted = gid;
        num_graph_max_vertexes = set_graphs[gid]->get_num_vertexes();
      }
    }

    LOG_INFO("Split gid: ", bucket_id_to_be_splitted);

    size_t* sum_in_edges_by_new_fragments =
        (size_t*)malloc(sizeof(size_t) * num_new_buckets);
    memset(sum_in_edges_by_new_fragments, 0, sizeof(size_t) * num_new_buckets);
    size_t* sum_out_edges_by_new_fragments =
        (size_t*)malloc(sizeof(size_t) * num_new_buckets);
    memset(sum_out_edges_by_new_fragments, 0, sizeof(size_t) * num_new_buckets);
    size_t num_vertexes_per_new_bucket[num_new_buckets] = {0};
    auto new_fragments = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>***)malloc(
        sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**) *
        num_new_buckets);

    for (size_t i = 0; i < num_new_buckets; i++) {
      new_fragments[i] = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
          sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) *
          (this->aligned_max_vid_));
      for (size_t j = 0; j < this->aligned_max_vid_; j++)
        new_fragments[i][j] = nullptr;
    }

    auto csr_graph_to_be_splitted =
        (CSR_T*)set_graphs[bucket_id_to_be_splitted];
    for (size_t i = 0; i < csr_graph_to_be_splitted->get_num_vertexes(); i++) {
      auto u = csr_graph_to_be_splitted->GetPVertexByIndex(i);
      VID_T global_id = csr_graph_to_be_splitted->localid2globalid(u->vid);
      GID_T gid = Hash(global_id) % num_new_buckets;
      new_fragments[gid][global_id] = u;
      __sync_fetch_and_add(sum_in_edges_by_new_fragments + gid, u->indegree);
      __sync_fetch_and_add(sum_out_edges_by_new_fragments + gid, u->outdegree);
      __sync_fetch_and_add(num_vertexes_per_new_bucket + gid, 1);
    }

    LOG_INFO("Run: Construct splitted sub-graphs");
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &num_partitions, &num_new_buckets,
                          &bucket_id_to_be_splitted,
                          &sum_in_edges_by_new_fragments, &max_vid_per_bucket,
                          &new_fragments, &sum_out_edges_by_new_fragments,
                          &set_graphs, &num_vertexes_per_new_bucket,
                          &pending_packages, &finish_cv]() {
        for (size_t gid = tid; gid < num_new_buckets; gid += cores) {
          auto graph = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>(
              gid + num_partitions, new_fragments[gid],
              num_vertexes_per_new_bucket[gid],
              sum_in_edges_by_new_fragments[gid],
              sum_out_edges_by_new_fragments[gid],
              max_vid_per_bucket[bucket_id_to_be_splitted]);
          set_graphs[num_partitions + gid] =
              (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    set_graphs[bucket_id_to_be_splitted] = nullptr;
    for (size_t gid = 0; gid < num_partitions + num_new_buckets; gid++) {
      if (set_graphs[gid] == nullptr) continue;
      this->fragments_->push_back(set_graphs[gid]);
    }

    LOG_INFO("Run: Set communication matrix");
    this->communication_matrix_ =
        (bool*)malloc(sizeof(bool) * (num_partitions + num_new_buckets - 1) *
                      (num_partitions + num_new_buckets - 1));
    memset(this->communication_matrix_, 0,
           sizeof(bool) * (num_partitions + num_new_buckets - 1) *
               (num_partitions + num_new_buckets - 1));
    for (size_t i = 0; i < (num_partitions + num_new_buckets - 1) *
                               (num_partitions + num_new_buckets - 1);
         i++)
      this->communication_matrix_[i] = 1;
    for (size_t i = 0; i < (num_partitions + num_new_buckets - 1); i++)
      *(this->communication_matrix_ +
        i * (num_partitions + num_new_buckets - 1) + i) = 0;

    is_in_bucketX[bucket_id_to_be_splitted] = nullptr;
    LOG_INFO("Run: Set global_border_vid_map, size: ", this->aligned_max_vid_);
    this->global_border_vid_map_ = new Bitmap(this->aligned_max_vid_);
    this->global_border_vid_map_->clear();
    for (auto& iter_fragments : *this->fragments_) {
      auto fragment = (CSR_T*)iter_fragments;
      fragment->InitVdata2AllX(0);
      fragment->SetGlobalBorderVidMap(this->global_border_vid_map_,
                                      is_in_bucketX,
                                      (num_partitions + num_new_buckets - 1));
    }

    GID_T local_gid = 0;
    for (auto& iter_fragments : *this->fragments_) {
      auto fragment = (CSR_T*)iter_fragments;
      LOG_INFO("gid: ", fragment->get_gid(), "->", local_gid);
      fragment->gid_ = local_gid++;
    }

    for (size_t i = 0; i < num_partitions; i++) {
      delete is_in_bucketX[i];
    }
    for (GID_T gid = 0; gid < num_partitions; gid++) {
      free(edges_buckets[gid]);
    }
    free(num_vertexes_per_bucket);
    free(max_vid_per_bucket);
    free(size_per_bucket);
    delete num_in_edges;
    delete num_out_edges;

    LOG_INFO("END");
    return true;
  }
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_HYBRID_CUT_PARTITIONER_H
