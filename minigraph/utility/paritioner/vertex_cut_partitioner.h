#ifndef MINIGRAPH_UTILITY_VERTEXCUT_PARTITIONER_H
#define MINIGRAPH_UTILITY_VERTEXCUT_PARTITIONER_H

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
#include <atomic>
#include <cstring>
#include <stdio.h>
#include <string.h>
#include <unordered_map>
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
  VertexCutPartitioner() {
    this->globalid2gid_ = new std::unordered_map<VID_T, GID_T>;
    this->global_border_vertexes_by_gid_ =
        new std::unordered_map<GID_T, std::vector<VID_T>*>;
  }

  ~VertexCutPartitioner() = default;

  bool ParallelPartitionFromBin(const std::string& pt,
                                const size_t num_partitions = 1,
                                const size_t cores = 1) {
    LOG_INFO("ParallelPartitionFromBin(): VertexCut");

    std::string meta_pt = pt + "minigraph_meta" + ".bin";
    std::string data_pt = pt + "minigraph_data" + ".bin";
    std::string vdata_pt = pt + "minigraph_vdata" + ".bin";
  }

  bool ParallelPartitionFromCSV(const std::string& pt,
                                char separator_params = ',',
                                const size_t num_partitions = 1,
                                const size_t cores = 1) {
    LOG_INFO("ParallelPartition(): VertexCut");
    rapidcsv::Document* doc =
        new rapidcsv::Document(pt, rapidcsv::LabelParams(),
                               rapidcsv::SeparatorParams(separator_params));
    std::vector<VID_T>* src = new std::vector<VID_T>();
    *src = doc->GetColumn<VID_T>(0);
    std::vector<VID_T>* dst = new std::vector<VID_T>();
    *dst = doc->GetColumn<VID_T>(1);
    size_t num_edges = src->size();

    VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
    VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
    memset(src_v, 0, sizeof(VID_T) * num_edges);
    memset(dst_v, 0, sizeof(VID_T) * num_edges);

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    std::atomic max_vid_atom(0);
    LOG_INFO("Run: Convert std::vector to array.");
    std::atomic<size_t> pending_packages(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &src_v, &dst_v, &src, &dst,
                          &pending_packages, &finish_cv, &max_vid_atom]() {
        for (size_t j = tid; j < src->size(); j += cores) {
          dst_v[j] = dst->at(j);
          src_v[j] = src->at(j);
          if (max_vid_atom.load() < dst_v[j]) max_vid_atom.store(dst_v[j]);
          if (max_vid_atom.load() < src_v[j]) max_vid_atom.store(src_v[j]);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    if (max_vid_atom.load() != this->max_vid_) {
      free(this->vid_map_);
      this->max_vid_ = ceil((float)max_vid_atom.load() / 64) * 64;
      this->vid_map_ = (VID_T*)malloc(sizeof(VID_T) * this->max_vid_);
    }

    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * this->max_vid_);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * this->max_vid_);
    memset(num_in_edges, 0, sizeof(size_t) * this->max_vid_);
    memset(num_out_edges, 0, sizeof(size_t) * this->max_vid_);
    size_t* size_per_bucket = new size_t[num_partitions];
    memset(size_per_bucket, 0, sizeof(size_t) * num_partitions);

    LOG_INFO(
        "First round of iterations to compute the number of edges for each "
        "bucket.");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_edges, src_v, dst_v,
                          &num_partitions, &size_per_bucket, &pending_packages,
                          &finish_cv]() {
        if (tid > num_edges) return;
        for (size_t j = tid; j < num_edges; j += cores) {
          auto bucket_id = Hash(src_v[j]) % num_partitions;
          __sync_add_and_fetch(size_per_bucket + bucket_id, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    LOG_INFO("Second round of iterations to drop edges into buckets.");
    VID_T** edges_buckets = nullptr;
    edges_buckets = (VID_T**)malloc(sizeof(VID_T*) * num_partitions);
    for (size_t i = 0; i < num_partitions; i++)
      edges_buckets[i] = (VID_T*)malloc(sizeof(VID_T) * 2 * size_per_bucket[i]);
    Bitmap* is_in_bucketX[num_partitions];
    for (size_t i = 0; i < num_partitions; i++) {
      is_in_bucketX[i] = new Bitmap(this->max_vid_);
      is_in_bucketX[i]->clear();
    }
    std::size_t* buckets_offset = new size_t[num_partitions];
    memset(buckets_offset, 0, sizeof(size_t) * num_partitions);
    std::size_t* num_vertexes_per_bucket = new size_t[num_partitions];
    memset(num_vertexes_per_bucket, 0, sizeof(size_t) * num_partitions);
    VID_T* max_vid_per_bucket = new VID_T[num_partitions];
    memset(max_vid_per_bucket, 0, sizeof(VID_T) * num_partitions);

    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_edges, src_v, dst_v, &edges_buckets,
                          &num_vertexes_per_bucket, &max_vid_per_bucket,
                          &is_in_bucketX, &buckets_offset, &num_partitions,
                          &size_per_bucket, &pending_packages, &finish_cv]() {
        if (tid > num_edges) return;
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = src_v[j];
          auto dst_vid = dst_v[j];
          auto bucket_id = Hash(src_vid) % num_partitions;

          if (max_vid_per_bucket[bucket_id] < src_vid) {
            __sync_bool_compare_and_swap(&max_vid_per_bucket[bucket_id],
                                         max_vid_per_bucket[bucket_id],
                                         src_vid);

            LOG_INFO(max_vid_per_bucket[bucket_id], " <-", src_vid);
          }

          edges_buckets[bucket_id][buckets_offset[bucket_id] * 2] = src_vid;
          edges_buckets[bucket_id][buckets_offset[bucket_id] * 2 + 1] = dst_vid;
          if (is_in_bucketX[bucket_id]->get_bit(src_vid) == 0) {
            is_in_bucketX[bucket_id]->set_bit(src_vid);
            __sync_add_and_fetch(num_vertexes_per_bucket + bucket_id, 1);
          }
          if (is_in_bucketX[bucket_id]->get_bit(dst_vid) == 0) {
            is_in_bucketX[bucket_id]->set_bit(dst_vid);
            __sync_add_and_fetch(num_vertexes_per_bucket + bucket_id, 1);
          }
          __sync_add_and_fetch(buckets_offset + bucket_id, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    minigraph::utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>
        csr_io_adapter;

    LOG_INFO("Run: Construct sub-graphs");
    if (this->fragments_ != nullptr) this->fragments_->clear();

    for (size_t gid = 0; gid < num_partitions; gid++) {
      auto edgelist_graph = new EDGE_LIST_T(
          gid, size_per_bucket[gid], num_vertexes_per_bucket[gid],
          max_vid_per_bucket[gid], edges_buckets[gid]);

      auto csr_graph = csr_io_adapter.EdgeList2CSR(gid, edgelist_graph, cores,
                                                   this->vid_map_);
      delete edgelist_graph;
      this->fragments_->push_back(csr_graph);
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

    LOG_INFO("Run: Set global_border_vid_map");
    if (this->global_border_vid_map_ == nullptr)
      this->global_border_vid_map_ = new Bitmap(this->max_vid_);
    else {
      delete this->global_border_vid_map_;
      this->global_border_vid_map_ = new Bitmap(this->max_vid_);
    }

    this->global_border_vid_map_->clear();
    for (GID_T i = 0; i < num_partitions; i++)
      ((CSR_T*)this->fragments_->at(i))
          ->SetGlobalBorderVidMap(this->global_border_vid_map_, is_in_bucketX,
                                  num_partitions);

    for (size_t i = 0; i < num_partitions; i++) {
      delete is_in_bucketX[i];
    }
    // delete is_in_bucketX;
    free(num_vertexes_per_bucket);
    free(max_vid_per_bucket);
    delete num_in_edges;
    delete num_out_edges;
    LOG_INFO("END");

    return true;
  }

  bool ParallelPartition(EDGE_LIST_T* edgelist_graph,
                         const size_t num_partitions = 1,
                         const size_t cores = 1) override {
    LOG_INFO("ParallelPartition(): VertexCut");
    size_t num_edges = edgelist_graph->num_edges_;

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);

    VID_T aligned_max_vid = ceil((float)edgelist_graph->max_vid_ / 64) * 64;
    this->vid_map_ = (VID_T*)malloc(sizeof(VID_T) * aligned_max_vid);
    memset(this->vid_map_, 0, sizeof(VID_T) * aligned_max_vid);
    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    memset(num_in_edges, 0, sizeof(size_t) * aligned_max_vid);
    memset(num_out_edges, 0, sizeof(size_t) * aligned_max_vid);
    size_t* size_per_bucket = new size_t[num_partitions];
    memset(size_per_bucket, 0, sizeof(size_t) * num_partitions);

    LOG_INFO(
        "First round of iterations to compute the number of edges for each "
        "bucket.");
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

    LOG_INFO("Second round of iterations to drop edges into buckets.");
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

    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_edges, &edgelist_graph,
                          &edges_buckets, &num_vertexes_per_bucket,
                          &max_vid_per_bucket, &is_in_bucketX, &buckets_offset,
                          &num_partitions, &size_per_bucket, &pending_packages,
                          &finish_cv]() {
        if (tid > num_edges) return;
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = edgelist_graph->buf_graph_[j * 2];
          auto dst_vid = edgelist_graph->buf_graph_[j * 2 + 1];
          auto bucket_id = Hash(src_vid) % num_partitions;

          if (max_vid_per_bucket[bucket_id] < src_vid)
            __sync_bool_compare_and_swap(&max_vid_per_bucket[bucket_id],
                                         max_vid_per_bucket[bucket_id],
                                         src_vid);

          edges_buckets[bucket_id][buckets_offset[bucket_id] * 2] = src_vid;
          edges_buckets[bucket_id][buckets_offset[bucket_id] * 2 + 1] = dst_vid;
          if (is_in_bucketX[bucket_id]->get_bit(src_vid) == 0) {
            is_in_bucketX[bucket_id]->set_bit(src_vid);
            __sync_add_and_fetch(num_vertexes_per_bucket + bucket_id, 1);
          }
          if (is_in_bucketX[bucket_id]->get_bit(dst_vid) == 0) {
            is_in_bucketX[bucket_id]->set_bit(dst_vid);
            __sync_add_and_fetch(num_vertexes_per_bucket + bucket_id, 1);
          }
          __sync_add_and_fetch(buckets_offset + bucket_id, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    minigraph::utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>
        csr_io_adapter;

    LOG_INFO("Run: Construct sub-graphs");
    if (this->fragments_ != nullptr) this->fragments_->clear();

    for (size_t gid = 0; gid < num_partitions; gid++) {
      auto edgelist_graph = new EDGE_LIST_T(
          gid, size_per_bucket[gid], num_vertexes_per_bucket[gid],
          max_vid_per_bucket[gid], edges_buckets[gid]);

      auto csr_graph = csr_io_adapter.EdgeList2CSR(gid, edgelist_graph, cores,
                                                   this->vid_map_);
      delete edgelist_graph;
      this->fragments_->push_back(csr_graph);
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

    LOG_INFO("Run: Set global_border_vid_map");
    if (this->global_border_vid_map_ == nullptr)
      this->global_border_vid_map_ = new Bitmap(aligned_max_vid);
    else {
      delete this->global_border_vid_map_;
      this->global_border_vid_map_ = new Bitmap(aligned_max_vid);
    }

    this->global_border_vid_map_->clear();
    for (GID_T i = 0; i < num_partitions; i++) {
      ((CSR_T*)this->fragments_->at(i))
          ->SetGlobalBorderVidMap(this->global_border_vid_map_, is_in_bucketX,
                                  num_partitions);
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

 private:
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_VERTEXCUT_PARTITIONER_H