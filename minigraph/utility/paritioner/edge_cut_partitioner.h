#ifndef MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
#define MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H

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
class EdgeCutPartitioner : public PartitionerBase<GRAPH_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  EdgeCutPartitioner() = default;
  ~EdgeCutPartitioner() = default;

  bool ParallelPartition(EDGE_LIST_T* edgelist_graph,
                         const size_t num_partitions = 1,
                         const size_t cores = 1, const std::string dst_pt = "",
                         bool delete_graph = false) override {
    LOG_INFO("ParallelPartition(): EdgeCut");
    edgelist_graph->ShowGraph(10);
    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);
    minigraph::utility::io::DataMngr<CSR_T> data_mngr;

    auto max_vid = edgelist_graph->get_max_vid();
    this->max_vid_ = edgelist_graph->get_max_vid();
    this->aligned_max_vid_ =
        ceil((float)edgelist_graph->get_aligned_max_vid() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR;
    auto aligned_max_vid = this->aligned_max_vid_;
    auto num_vertexes = edgelist_graph->get_num_vertexes();
    this->num_vertexes_ = edgelist_graph->get_num_vertexes();
    auto num_edges = edgelist_graph->get_num_edges();
    this->num_edges_ = edgelist_graph->get_num_edges();
    size_t num_new_buckets = NUM_NEW_BUCKETS;
    this->num_partitions = num_partitions + NUM_NEW_BUCKETS - 1;
    this->global_border_vid_map_ = new Bitmap(this->aligned_max_vid_);
    this->global_border_vid_map_->clear();

    auto vid_map =
        (VID_T*)malloc(sizeof(VID_T) * edgelist_graph->get_aligned_max_vid());
    this->vid_map_ = vid_map;

    memset(this->vid_map_, 0,
           sizeof(VID_T) * edgelist_graph->get_aligned_max_vid());

    Bitmap* vertex_indicator =
        new Bitmap(edgelist_graph->get_aligned_max_vid());
    vertex_indicator->clear();

    size_t* num_in_edges =
        (size_t*)malloc(sizeof(size_t) * edgelist_graph->get_aligned_max_vid());
    size_t* num_out_edges =
        (size_t*)malloc(sizeof(size_t) * edgelist_graph->get_aligned_max_vid());
    memset(num_in_edges, 0,
           sizeof(size_t) * edgelist_graph->get_aligned_max_vid());
    memset(num_out_edges, 0,
           sizeof(size_t) * edgelist_graph->get_aligned_max_vid());

    LOG_INFO("Run: Go through every edges to count the size of each vertex");
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &edgelist_graph, &vertex_indicator,
                          &num_out_edges, &num_in_edges, &pending_packages,
                          &finish_cv]() {
        for (size_t j = tid; j < edgelist_graph->get_num_edges(); j += cores) {
          auto src_vid = edgelist_graph->buf_graph_[j * 2];
          auto dst_vid = edgelist_graph->buf_graph_[j * 2 + 1];
          if (!vertex_indicator->get_bit(src_vid)) {
            vertex_indicator->set_bit(src_vid);
          }
          if (!vertex_indicator->get_bit(dst_vid)) {
            vertex_indicator->set_bit(dst_vid);
          }
          __sync_add_and_fetch(num_out_edges + src_vid, 1);
          __sync_add_and_fetch(num_in_edges + dst_vid, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>** vertexes = nullptr;
    vertexes = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) *
        edgelist_graph->get_aligned_max_vid());

    for (size_t i = 0; i < aligned_max_vid; i++) vertexes[i] = nullptr;

    LOG_INFO("Run: Pre join edges");
    pending_packages.store(cores);

    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, aligned_max_vid, &vertex_indicator,
                          &num_in_edges, &num_out_edges, &vertexes,
                          &pending_packages, &finish_cv]() {
        for (size_t global_id = tid; global_id < aligned_max_vid;
             global_id += cores) {
          if (!vertex_indicator->get_bit(global_id)) continue;
          auto u = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>();
          u->vid = global_id;
          u->indegree = num_in_edges[u->vid];
          u->outdegree = num_out_edges[u->vid];
          u->in_edges = (VID_T*)malloc(sizeof(VID_T) * (u->indegree));
          memset(u->in_edges, 0, sizeof(VID_T) * (u->indegree));
          u->out_edges = (VID_T*)malloc(sizeof(VID_T) * (u->outdegree));
          memset(u->out_edges, 0, sizeof(VID_T) * (u->outdegree));
          vertexes[global_id] = u;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    free(num_in_edges);
    free(num_out_edges);
    num_in_edges = nullptr;
    num_out_edges = nullptr;

    LOG_INFO("Run: Join edges");
    size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    size_t* offset_out_edges =
        (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    memset(offset_in_edges, 0, sizeof(size_t) * aligned_max_vid);
    memset(offset_out_edges, 0, sizeof(size_t) * aligned_max_vid);

    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &edgelist_graph, &vertexes, &num_edges,
                          &offset_in_edges, &offset_out_edges,
                          &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = edgelist_graph->buf_graph_[j * 2];
          auto dst_vid = edgelist_graph->buf_graph_[j * 2 + 1];
          assert(vertexes[src_vid] != nullptr);
          assert(vertexes[dst_vid] != nullptr);
          vertexes[src_vid]
              ->out_edges[__sync_fetch_and_add(offset_out_edges + src_vid, 1)] =
              dst_vid;
          vertexes[dst_vid]
              ->in_edges[__sync_fetch_and_add(offset_in_edges + dst_vid, 1)] =
              src_vid;
        }

        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    VID_T local_id_for_each_bucket[num_partitions] = {0};
    delete edgelist_graph;
    free(offset_in_edges);
    free(offset_out_edges);

    size_t* offset_fragments = (size_t*)malloc(sizeof(size_t) * num_partitions);
    memset(offset_fragments, 0, sizeof(size_t) * num_partitions);
    size_t* sum_in_edges_by_fragments =
        (size_t*)malloc(sizeof(size_t) * num_partitions);
    memset(sum_in_edges_by_fragments, 0, sizeof(size_t) * num_partitions);
    size_t* sum_out_edges_by_fragments =
        (size_t*)malloc(sizeof(size_t) * num_partitions);
    memset(sum_out_edges_by_fragments, 0, sizeof(size_t) * num_partitions);

    auto fragments = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>***)malloc(
        sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**) * num_partitions);

    for (size_t i = 0; i < num_partitions; i++) {
      fragments[i] = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
          sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) *
          (this->aligned_max_vid_));
      for (size_t j = 0; j < this->aligned_max_vid_; j++)
        fragments[i][j] = nullptr;
    }

    size_t num_vertexes_per_bucket[num_partitions] = {0};
    VID_T max_vid_per_bucket[num_partitions] = {0};
    Bitmap* is_in_bucketX[num_partitions + num_new_buckets];
    for (size_t i = 0; i < num_partitions + num_new_buckets; i++) {
      is_in_bucketX[i] = new Bitmap(aligned_max_vid);
      is_in_bucketX[i]->clear();
    }
    auto set_graphs = (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*) *
        (num_partitions + num_new_buckets));
    for (size_t i = 0; i < num_partitions + num_new_buckets; i++)
      set_graphs[i] = nullptr;

    LOG_INFO("Run: Fill buckets.");
    size_t count = 0;
    GID_T gid = 0;
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &is_in_bucketX, aligned_max_vid,
                          &vertexes, &fragments, &max_vid_per_bucket,
                          &num_partitions, &local_id_for_each_bucket,
                          &sum_in_edges_by_fragments,
                          &sum_out_edges_by_fragments, &vertex_indicator,
                          &num_vertexes_per_bucket, &vid_map, &num_vertexes,
                          &pending_packages, &finish_cv]() {
        for (VID_T global_vid = tid; global_vid < aligned_max_vid;
             global_vid += cores) {
          if (!vertex_indicator->get_bit(global_vid)) continue;
          auto u = vertexes[global_vid];

          GID_T gid = (Hash(global_vid) % num_partitions);
          // LOG_INFO(gid);
          fragments[gid][global_vid] = u;
          if (max_vid_per_bucket[gid] < global_vid)
            max_vid_per_bucket[gid] = global_vid;
          is_in_bucketX[gid]->set_bit(global_vid);
          u->vid = global_vid;
          __sync_fetch_and_add(sum_in_edges_by_fragments + gid, u->indegree);
          __sync_fetch_and_add(sum_out_edges_by_fragments + gid, u->outdegree);
          __sync_fetch_and_add(num_vertexes_per_bucket + gid, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    free(vertexes);
    auto bucket_id_to_be_splitted = GID_MAX;

    GID_T atom_gid = 0;
    if (num_new_buckets > 1) {
      LOG_INFO("Run: Split the biggest bucket.");
      auto max_degree = 0;
      for (size_t i = 0; i < num_partitions; i++) {
        LOG_INFO("GID: ", i, " num_vertexes: ", num_vertexes_per_bucket[i],
                 " sum_inedges: ", sum_in_edges_by_fragments[i],
                 " sum_out_edges: ", sum_out_edges_by_fragments[i]);
        if (max_degree <
            sum_in_edges_by_fragments[i] + sum_out_edges_by_fragments[i]) {
          bucket_id_to_be_splitted = i;
          max_degree =
              sum_in_edges_by_fragments[i] + sum_out_edges_by_fragments[i];
        }
      }

      LOG_INFO("Split gid: ", bucket_id_to_be_splitted);

      is_in_bucketX[bucket_id_to_be_splitted] = nullptr;
      size_t* sum_in_edges_by_new_fragments =
          (size_t*)malloc(sizeof(size_t) * num_new_buckets);
      memset(sum_in_edges_by_new_fragments, 0,
             sizeof(size_t) * num_new_buckets);
      size_t* sum_out_edges_by_new_fragments =
          (size_t*)malloc(sizeof(size_t) * num_new_buckets);
      memset(sum_out_edges_by_new_fragments, 0,
             sizeof(size_t) * num_new_buckets);
      size_t num_vertexes_per_new_bucket[num_new_buckets] = {0};

      auto new_fragments =
          (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>***)malloc(
              sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**) *
              num_new_buckets);

      for (size_t i = 0; i < num_new_buckets; i++) {
        new_fragments[i] =
            (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
                sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) *
                (this->aligned_max_vid_));
        for (size_t j = 0; j < this->aligned_max_vid_; j++)
          new_fragments[i][j] = nullptr;
      }

      pending_packages.store(cores);
      for (size_t tid = 0; tid < cores; tid++) {
        thread_pool.Commit(
            [tid, &cores, &is_in_bucketX, &aligned_max_vid, &num_new_buckets,
             &new_fragments, &fragments, &bucket_id_to_be_splitted,
             &num_partitions, &vertex_indicator, &local_id_for_each_bucket,
             &sum_in_edges_by_new_fragments, &sum_out_edges_by_new_fragments,
             &num_vertexes_per_new_bucket, &num_vertexes_per_bucket,
             &pending_packages, &finish_cv]() {
              for (VID_T global_vid = tid; global_vid < aligned_max_vid;
                   global_vid += cores) {
                if (fragments[bucket_id_to_be_splitted][global_vid] == nullptr)
                  continue;
                auto u = fragments[bucket_id_to_be_splitted][global_vid];
                GID_T gid = (Hash(global_vid) % num_new_buckets);
                is_in_bucketX[gid + num_partitions]->set_bit(global_vid);
                new_fragments[gid][u->vid] = u;
                __sync_fetch_and_add(sum_in_edges_by_new_fragments + gid,
                                     u->indegree);
                __sync_fetch_and_add(sum_out_edges_by_new_fragments + gid,
                                     u->outdegree);
                __sync_fetch_and_add(num_vertexes_per_new_bucket + gid, 1);
              }

              if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
              return;
            });
      }
      finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

      LOG_INFO("Run: Construct splitted sub-graphs");
      pending_packages.store(cores);
      for (size_t tid = 0; tid < cores; tid++) {
        thread_pool.Commit([&, tid, &cores, &atom_gid, &delete_graph,
                            &num_partitions, &num_new_buckets,
                            &bucket_id_to_be_splitted,
                            &sum_in_edges_by_new_fragments, &max_vid_per_bucket,
                            &new_fragments, &sum_out_edges_by_new_fragments,
                            &set_graphs, &num_vertexes_per_new_bucket, &vid_map,
                            &pending_packages, &finish_cv]() {
          for (size_t gid = tid; gid < num_new_buckets; gid += cores) {
            auto local_gid = __sync_fetch_and_add(&atom_gid, 1);
            auto graph =
                new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>(
                    local_gid, new_fragments[gid],
                    num_vertexes_per_new_bucket[gid],
                    sum_in_edges_by_new_fragments[gid],
                    sum_out_edges_by_new_fragments[gid],
                    max_vid_per_bucket[bucket_id_to_be_splitted], vid_map);
            for (size_t i = 0; i < max_vid_per_bucket[gid]; i++) {
              delete new_fragments[gid][i];
            }
            free(new_fragments[gid]);
            if (!delete_graph) {
              set_graphs[local_gid] =
                  (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
            } else {
              set_graphs[local_gid] = nullptr;
              std::string meta_pt = dst_pt + "minigraph_meta/" +
                                    std::to_string(local_gid) + ".bin";
              std::string data_pt = dst_pt + "minigraph_data/" +
                                    std::to_string(local_gid) + ".bin";
              std::string vdata_pt = dst_pt + "minigraph_vdata/" +
                                     std::to_string(local_gid) + ".bin";
              data_mngr.csr_io_adapter_->Write(*graph, csr_bin, false, meta_pt,
                                               data_pt, vdata_pt);
              StatisticInfo&& si =
                  this->ParallelSetStatisticInfo(*graph, cores);
              std::string si_pt = dst_pt + "minigraph_si/" +
                                  std::to_string(local_gid) + ".yaml";
              data_mngr.WriteStatisticInfo(si, si_pt);
              delete graph;
            }
          }
          if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
          return;
        });
      }
      finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    }

    LOG_INFO("Run: Construct sub-graphs");
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([&, tid, &cores, &atom_gid, &delete_graph,
                          &num_partitions, &dst_pt, &data_mngr,
                          &sum_in_edges_by_fragments, &max_vid_per_bucket,
                          &fragments, &sum_out_edges_by_fragments, &set_graphs,
                          &bucket_id_to_be_splitted, &num_vertexes_per_bucket,
                          &vid_map, &pending_packages, &finish_cv]() {
        for (size_t gid = tid; gid < num_partitions; gid += cores) {
          if (gid == bucket_id_to_be_splitted) continue;
          auto local_gid = __sync_fetch_and_add(&atom_gid, 1);
          auto graph = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>(
              local_gid, fragments[gid], num_vertexes_per_bucket[gid],
              sum_in_edges_by_fragments[gid], sum_out_edges_by_fragments[gid],
              max_vid_per_bucket[gid], vid_map);
          for (size_t i = 0; i < max_vid_per_bucket[gid]; i++) {
            delete fragments[gid][i];
          }
          free(fragments[gid]);
          graph->InitVdata2AllX(0);
          graph->SetGlobalBorderVidMap(this->global_border_vid_map_,
                                       is_in_bucketX,
                                       (num_partitions + num_new_buckets - 1));
          if (!delete_graph) {
            set_graphs[local_gid] =
                (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
          } else {
            set_graphs[local_gid] = nullptr;
            std::string meta_pt =
                dst_pt + "minigraph_meta/" + std::to_string(local_gid) + ".bin";
            std::string data_pt =
                dst_pt + "minigraph_data/" + std::to_string(local_gid) + ".bin";
            std::string vdata_pt = dst_pt + "minigraph_vdata/" +
                                   std::to_string(local_gid) + ".bin";
            data_mngr.csr_io_adapter_->Write(*graph, csr_bin, false, meta_pt,
                                             data_pt, vdata_pt);
            StatisticInfo&& si = this->ParallelSetStatisticInfo(*graph, cores);
            std::string si_pt =
                dst_pt + "minigraph_si/" + std::to_string(local_gid) + ".yaml";
            data_mngr.WriteStatisticInfo(si, si_pt);
            delete graph;
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

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

    LOG_INFO("Run: Set global_border_vid_map");

    if (!delete_graph) {
      for (auto& iter_fragments : *this->fragments_) {
        auto fragment = (CSR_T*)iter_fragments;
        fragment->InitVdata2AllX(0);
        fragment->SetGlobalBorderVidMap(this->global_border_vid_map_,
                                        is_in_bucketX,
                                        (num_partitions + num_new_buckets - 1));
      }
    }
    // GID_T local_gid = 0;
    // for (auto& iter_fragments : *this->fragments_) {
    //   auto fragment = (CSR_T*)iter_fragments;
    //   fragment->gid_ = local_gid++;
    // }

    return false;
  }
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
