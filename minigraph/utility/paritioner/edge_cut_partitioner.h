#ifndef MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
#define MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H

#include "graphs/graph.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/io/data_mngr.h"
#include "utility/io/io_adapter_base.h"
#include "utility/thread_pool.h"
#include <folly/AtomicHashMap.h>
#include <folly/FBVector.h>
#include <atomic>
#include <cstring>
#include <stdio.h>
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
class EdgeCutPartitioner {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T =
      minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

 public:
  EdgeCutPartitioner() {
    globalid2gid_ = new std::unordered_map<VID_T, GID_T>;
    global_border_vertexes_by_gid_ =
        new std::unordered_map<GID_T, std::vector<VID_T>*>;
  }

  EdgeCutPartitioner(const size_t max_vid) {
    globalid2gid_ = new std::unordered_map<VID_T, GID_T>;
    global_border_vertexes_by_gid_ =
        new std::unordered_map<GID_T, std::vector<VID_T>*>;
    max_vid_ = max_vid;
    vid_map_ = (VID_T*)malloc(sizeof(VID_T) * (max_vid_ + 64));
    memset(vid_map_, 0, sizeof(VID_T) * (max_vid + 64));
  }

  ~EdgeCutPartitioner() = default;

  bool ParallelPartitionFromBin(const std::string& pt,
                                const size_t num_partitions = 1,
                                std::size_t max_vid = 1, const size_t cores = 1,
                                const std::string init_model = "val",
                                const VDATA_T init_vdata = 0) {
    LOG_INFO("ParallelPartitionFromBin()");

    std::string meta_pt = pt + "minigraph_meta" + ".bin";
    std::string data_pt = pt + "minigraph_data" + ".bin";
    std::string vdata_pt = pt + "minigraph_vdata" + ".bin";

    minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
        edge_list_io_adapter;

    auto graph = new EDGE_LIST_T;
    edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edge_list_bin, ' ', 0,
                              meta_pt, data_pt, vdata_pt);

    graph->ShowGraph(10);
    size_t num_edges = graph->num_edges_;

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    std::atomic<VID_T> max_vid_atom(max_vid);
    LOG_INFO("Run: Convert std::vector to array.");
    std::atomic<size_t> pending_packages(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &graph, &pending_packages, &finish_cv,
                          &max_vid_atom]() {
        for (size_t j = tid; j < graph->num_edges_; j += cores) {
          if (max_vid_atom.load() < graph->buf_graph_[j * 2 + 1])
            max_vid_atom.store(graph->buf_graph_[j * 2 + 1]);
          if (max_vid_atom.load() < graph->buf_graph_[j * 2])
            max_vid_atom.store(graph->buf_graph_[j * 2]);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    if (max_vid_atom.load() != max_vid_) {
      free(vid_map_);
      max_vid_ = ((max_vid_atom.load() / 64) + 1) * 64;
      max_vid_ = max_vid_atom.load();
      vid_map_ = (VID_T*)malloc(sizeof(VID_T) * max_vid_);
    }

    LOG_INFO("MAX_VID: ", max_vid_);
    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    memset(num_in_edges, 0, sizeof(size_t) * max_vid_);
    memset(num_out_edges, 0, sizeof(size_t) * max_vid_);

    GID_T* gid_by_vid = (GID_T*)malloc(sizeof(GID_T) * max_vid_);
    memset(gid_by_vid, 0, sizeof(GID_T) * max_vid_);
    Bitmap* vertex_indicator = new Bitmap(max_vid_);
    vertex_indicator->clear();

    size_t num_vertexes = 0;

    // Go through every edges to count the size of each vertex.
    LOG_INFO("Run: Go through every edges to count the size of each vertex");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_in_edges, &num_out_edges,
                          &num_edges, &graph, &vertex_indicator,
                          &pending_packages, &finish_cv, &num_vertexes]() {
        if (tid > num_edges) return;
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = graph->buf_graph_[j * 2];
          auto dst_vid = graph->buf_graph_[j * 2 + 1];
          if (!vertex_indicator->get_bit(src_vid)) {
            vertex_indicator->set_bit(src_vid);
            __sync_add_and_fetch(&num_vertexes, 1);
          }
          if (!vertex_indicator->get_bit(dst_vid)) {
            vertex_indicator->set_bit(dst_vid);
            __sync_add_and_fetch(&num_vertexes, 1);
          }
          __sync_add_and_fetch(num_out_edges + src_vid, 1);
          __sync_add_and_fetch(num_in_edges + dst_vid, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    LOG_INFO("Real MAXIMUM ID: ", max_vid_);

    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>** vertexes = nullptr;
    vertexes = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) * max_vid_);

    for (size_t i = 0; i < max_vid_; i++) vertexes[i] = nullptr;

    LOG_INFO("Run: Merge edges");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([this, tid, &cores, &num_in_edges, &num_out_edges,
                          &vertex_indicator, &vertexes, &pending_packages,
                          &finish_cv]() {
        if (tid > max_vid_) return;
        for (size_t j = tid; j < max_vid_; j += cores) {
          if (!vertex_indicator->get_bit(j)) continue;
          auto u = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>();
          u->vid = j;
          u->indegree = num_in_edges[u->vid];
          u->outdegree = num_out_edges[u->vid];
          u->in_edges = (VID_T*)malloc(sizeof(VID_T) * (u->indegree));
          memset(u->in_edges, 0, sizeof(VID_T) * (u->indegree));
          u->out_edges = (VID_T*)malloc(sizeof(VID_T) * (u->outdegree));
          memset(u->out_edges, 0, sizeof(VID_T) * (u->outdegree));
          vertexes[u->vid] = u;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    LOG_INFO("Run: Edges fill");
    size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    size_t* offset_out_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    memset(offset_in_edges, 0, sizeof(size_t) * max_vid_);
    memset(offset_out_edges, 0, sizeof(size_t) * max_vid_);
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_in_edges, &num_out_edges,
                          &num_edges, &offset_in_edges, &offset_out_edges,
                          &graph, &vertex_indicator, &vertexes,
                          &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = graph->buf_graph_[j * 2];
          auto dst_vid = graph->buf_graph_[j * 2 + 1];
          if (vertexes[dst_vid] == nullptr) {
            LOG_INFO(dst_vid);
          }
          if (vertexes[src_vid] == nullptr) {
            LOG_INFO(src_vid);
          }
          assert(vertexes[src_vid] != nullptr);
          assert(vertexes[dst_vid] != nullptr);
          auto dst_in_offset =
              __sync_fetch_and_add(offset_in_edges + dst_vid, 1);
          auto src_out_offset =
              __sync_fetch_and_add(offset_out_edges + src_vid, 1);
          assert(dst_in_offset < num_in_edges[dst_vid]);
          assert(src_out_offset < num_out_edges[src_vid]);
          if (vertexes[src_vid] != nullptr) {
            vertexes[src_vid]->out_edges[src_out_offset] = dst_vid;
          }
          if (vertexes[dst_vid] != nullptr) {
            vertexes[dst_vid]->in_edges[dst_in_offset] = src_vid;
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
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
          (max_vid_atom.load() / num_partitions) * 2);
    }

    size_t* num_vertexes_per_bucket =
        (size_t*)malloc(sizeof(size_t) * num_partitions);
    memset(num_vertexes_per_bucket, 0, sizeof(size_t) * num_partitions);
    LOG_INFO("Run: Partition vertexes into buckets");

    size_t count = 0;
    GID_T gid = 0;

    for (size_t i = 0; i < max_vid_atom.load(); i++) {
      if (!vertex_indicator->get_bit(i)) continue;
      auto u = vertexes[i];
      auto offset_fragment = __sync_fetch_and_add(offset_fragments + gid, 1);
      fragments[gid][offset_fragment] = u;
      gid_by_vid[u->vid] = gid;
      __sync_fetch_and_add(sum_in_edges_by_fragments + gid, u->indegree);
      __sync_fetch_and_add(sum_out_edges_by_fragments + gid, u->outdegree);
      __sync_fetch_and_add(num_vertexes_per_bucket + gid, 1);
      if (count > num_vertexes / num_partitions) {
        gid++;
        count = 0;
      } else {
        count++;
      }
    }

    auto set_graphs = (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*) *
        num_partitions);

    LOG_INFO("Run: Construct sub-graphs");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([this, tid, &cores, &set_graphs,
                          &sum_in_edges_by_fragments,
                          &sum_out_edges_by_fragments, &fragments,
                          &num_vertexes_per_bucket, &num_partitions,
                          &pending_packages, &finish_cv]() {
        for (size_t i = tid; i < num_partitions; i += cores) {
          auto graph = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>(
              i, fragments[i], num_vertexes_per_bucket[i],
              sum_in_edges_by_fragments[i], sum_out_edges_by_fragments[i],
              max_vid_);
          set_graphs[i] = (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
          for (size_t j = 0; j < graph->get_num_vertexes(); j++) {
            auto u = graph->GetVertexByIndex(j);
            vid_map_[graph->localid2globalid(u.vid)] = u.vid;
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    if (fragments_ == nullptr)
      fragments_ =
          new std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>;
    for (size_t i = 0; i < num_partitions; i++)
      fragments_->push_back(set_graphs[i]);

    global_border_vid_map_ = new Bitmap(max_vid_);
    global_border_vid_map_->clear();
    for (auto& iter_fragments : *fragments_) {
      auto fragment = (CSR_T*)iter_fragments;
      if (init_model == "val") {
        fragment->InitVdata2AllX(init_vdata);
      } else if (init_model == "max") {
        fragment->InitVdata2AllMax();
      } else if (init_model == "vid") {
        fragment->InitVdataByVid();
      }
      fragment->SetGlobalBorderVidMap(global_border_vid_map_);
    }

    LOG_INFO("Run: Set communication matrix");
    if (communication_matrix_ == nullptr)
      communication_matrix_ =
          (bool*)malloc(sizeof(bool) * num_partitions * num_partitions);
    memset(communication_matrix_, 0,
           sizeof(bool) * num_partitions * num_partitions);

    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([this, tid, &cores, &graph, &gid_by_vid, &num_edges,
                          &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = graph->buf_graph_[j * 2];
          auto dst_vid = graph->buf_graph_[j * 2 + 1];
          auto src_gid = gid_by_vid[src_vid];
          auto dst_gid = gid_by_vid[dst_vid];
          if (src_gid == dst_gid) continue;
          *(communication_matrix_ + dst_gid * fragments_->size() + src_gid) = 1;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    delete num_vertexes_per_bucket;
    delete sum_in_edges_by_fragments;
    delete sum_out_edges_by_fragments;
    delete offset_out_edges;
    delete offset_in_edges;
    delete num_in_edges;
    delete num_out_edges;
    LOG_INFO("END");
    return true;
  }

  bool ParallelPartition(const std::string& pt, char separator_params = ',',
                         const size_t num_partitions = 1,
                         std::size_t max_vid = 1, const size_t cores = 1,
                         const std::string init_model = "val",
                         const VDATA_T init_vdata = 0) {
    LOG_INFO("ParallelPartition()");
    rapidcsv::Document* doc =
        new rapidcsv::Document(pt, rapidcsv::LabelParams(),
                               rapidcsv::SeparatorParams(separator_params));
    std::vector<VID_T>* src = new std::vector<VID_T>();
    *src = doc->GetColumn<VID_T>("src");
    std::vector<VID_T>* dst = new std::vector<VID_T>();
    *dst = doc->GetColumn<VID_T>("dst");
    size_t num_edges = src->size();

    VID_T* src_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
    VID_T* dst_v = (VID_T*)malloc(sizeof(VID_T) * num_edges);
    memset(src_v, 0, sizeof(VID_T) * num_edges);
    memset(dst_v, 0, sizeof(VID_T) * num_edges);

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    std::atomic max_vid_atom(max_vid);
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

    if (max_vid_atom.load() != max_vid_) {
      free(vid_map_);
      max_vid_ = ((max_vid_atom.load() / 64) + 1) * 64;
      vid_map_ = (VID_T*)malloc(sizeof(VID_T) * max_vid_);
    }

    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    memset(num_in_edges, 0, sizeof(size_t) * max_vid_);
    memset(num_out_edges, 0, sizeof(size_t) * max_vid_);

    GID_T* gid_by_vid = (GID_T*)malloc(sizeof(GID_T) * max_vid_);
    memset(gid_by_vid, 0, sizeof(GID_T) * max_vid_);
    Bitmap* vertex_indicator = new Bitmap(max_vid_);
    vertex_indicator->clear();

    src->clear();
    dst->clear();
    delete src;
    delete dst;
    doc->Clear();
    delete doc;
    // std::atomic num_vertexes(0);
    size_t num_vertexes = 0;
    // Go through every edges to count the size of each vertex.
    LOG_INFO("Run: Go through every edges to count the size of each vertex");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_in_edges, &num_out_edges,
                          &num_edges, &src_v, &dst_v, &vertex_indicator,
                          &pending_packages, &finish_cv, &num_vertexes]() {
        if (tid > num_edges) return;
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = src_v[j];
          auto dst_vid = dst_v[j];
          if (!vertex_indicator->get_bit(src_vid)) {
            vertex_indicator->set_bit(src_vid);
            __sync_add_and_fetch(&num_vertexes, 1);
          }
          if (!vertex_indicator->get_bit(dst_vid)) {
            vertex_indicator->set_bit(dst_vid);
            __sync_add_and_fetch(&num_vertexes, 1);
          }
          __sync_add_and_fetch(num_out_edges + src_vid, 1);
          __sync_add_and_fetch(num_in_edges + dst_vid, 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    LOG_INFO("Real MAXIMUM ID: ", max_vid_);

    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>** vertexes = nullptr;
    vertexes = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) * max_vid_);

    for (size_t i = 0; i < max_vid_; i++) vertexes[i] = nullptr;

    LOG_INFO("Run: Merge edges");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([this, tid, &cores, &num_in_edges, &num_out_edges,
                          &vertex_indicator, &vertexes, &pending_packages,
                          &finish_cv]() {
        if (tid > max_vid_) return;
        for (size_t j = tid; j < max_vid_; j += cores) {
          if (!vertex_indicator->get_bit(j)) continue;
          auto u = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>();
          u->vid = j;
          u->indegree = num_in_edges[u->vid];
          u->outdegree = num_out_edges[u->vid];
          u->in_edges = (VID_T*)malloc(sizeof(VID_T) * (u->indegree + 64));
          memset(u->in_edges, 0, sizeof(VID_T) * (u->indegree + 64));
          u->out_edges = (VID_T*)malloc(sizeof(VID_T) * (u->outdegree + 64));
          memset(u->out_edges, 0, sizeof(VID_T) * (u->outdegree + 64));
          vertexes[u->vid] = u;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    LOG_INFO("Run: Edges fill");
    size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    size_t* offset_out_edges = (size_t*)malloc(sizeof(size_t) * max_vid_);
    memset(offset_in_edges, 0, sizeof(size_t) * max_vid_);
    memset(offset_out_edges, 0, sizeof(size_t) * max_vid_);
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_in_edges, &num_out_edges,
                          &num_edges, &offset_in_edges, &offset_out_edges,
                          &src_v, &dst_v, &vertex_indicator, &vertexes,
                          &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = src_v[j];
          auto dst_vid = dst_v[j];
          assert(vertexes[src_vid] != nullptr);
          assert(vertexes[dst_vid] != nullptr);
          auto dst_in_offset =
              __sync_fetch_and_add(offset_in_edges + dst_vid, 1);
          auto src_out_offset =
              __sync_fetch_and_add(offset_out_edges + src_vid, 1);
          assert(dst_in_offset < num_in_edges[dst_vid]);
          assert(src_out_offset < num_out_edges[src_vid]);
          if (vertexes[src_vid] != nullptr) {
            vertexes[src_vid]->out_edges[src_out_offset] = dst_vid;
          }
          if (vertexes[dst_vid] != nullptr) {
            vertexes[dst_vid]->in_edges[dst_in_offset] = src_vid;
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
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
          (max_vid_atom.load() / num_partitions) * 2);
    }

    size_t* num_vertexes_per_bucket =
        (size_t*)malloc(sizeof(size_t) * num_partitions);
    memset(num_vertexes_per_bucket, 0, sizeof(size_t) * num_partitions);
    LOG_INFO("Run: Partition vertexes into buckets");
    // pending_packages.store(cores);
    // for (size_t i = 0; i < cores; i++) {
    //   size_t tid = i;
    //   thread_pool.Commit([this, tid, &cores, &max_vid_atom, &num_partitions,
    //                       &vertex_indicator, &vertexes, &offset_fragments,
    //                       &fragments, &num_vertexes,
    //                       &sum_in_edges_by_fragments,
    //                       &sum_out_edges_by_fragments, &pending_packages,
    //                       &finish_cv]() {
    //     if (tid > max_vid_atom.load()) return;
    //     for (size_t i = tid; i < max_vid_atom.load(); i += cores) {
    //       // if (vertex_indicator->get_bit(i) == 0) continue;
    //       if (!vertex_indicator[i]) continue;
    //       auto u = vertexes[i];
    //       GID_T gid = Hash(u->vid) % num_partitions;
    //       auto offset_fragment =
    //           __sync_fetch_and_add(offset_fragments + gid, 1);
    //       fragments[gid][offset_fragment] = u;
    //       __sync_fetch_and_add(sum_in_edges_by_fragments + gid, u->indegree);
    //       __sync_fetch_and_add(sum_out_edges_by_fragments + gid,
    //       u->outdegree);
    //       __sync_fetch_and_add(num_vertexes + gid, 1);
    //     }
    //     if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
    //     return;
    //   });
    // }
    // finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    size_t count = 0;
    GID_T gid = 0;

    for (size_t i = 0; i < max_vid_atom.load(); i++) {
      if (!vertex_indicator->get_bit(i)) continue;
      auto u = vertexes[i];
      auto offset_fragment = __sync_fetch_and_add(offset_fragments + gid, 1);
      fragments[gid][offset_fragment] = u;
      gid_by_vid[u->vid] = gid;
      __sync_fetch_and_add(sum_in_edges_by_fragments + gid, u->indegree);
      __sync_fetch_and_add(sum_out_edges_by_fragments + gid, u->outdegree);
      __sync_fetch_and_add(num_vertexes_per_bucket + gid, 1);
      if (count > num_vertexes / num_partitions) {
        gid++;
        count = 0;
      } else {
        count++;
      }
    }

    auto set_graphs = (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*) *
        num_partitions);

    LOG_INFO("Run: Construct sub-graphs");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([this, tid, &cores, &set_graphs,
                          &sum_in_edges_by_fragments,
                          &sum_out_edges_by_fragments, &fragments,
                          &num_vertexes_per_bucket, &num_partitions,
                          &pending_packages, &finish_cv]() {
        for (size_t i = tid; i < num_partitions; i += cores) {
          auto graph = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>(
              i, fragments[i], num_vertexes_per_bucket[i],
              sum_in_edges_by_fragments[i], sum_out_edges_by_fragments[i],
              max_vid_);
          set_graphs[i] = (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
          for (size_t j = 0; j < graph->get_num_vertexes(); j++) {
            auto u = graph->GetVertexByIndex(j);
            vid_map_[graph->localid2globalid(u.vid)] = u.vid;
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    if (fragments_ == nullptr)
      fragments_ =
          new std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>;
    for (size_t i = 0; i < num_partitions; i++)
      fragments_->push_back(set_graphs[i]);

    global_border_vid_map_ = new Bitmap(max_vid_);
    global_border_vid_map_->clear();
    for (auto& iter_fragments : *fragments_) {
      auto fragment = (CSR_T*)iter_fragments;
      if (init_model == "val") {
        fragment->InitVdata2AllX(init_vdata);
      } else if (init_model == "max") {
        fragment->InitVdata2AllMax();
      } else if (init_model == "vid") {
        fragment->InitVdataByVid();
      }
      fragment->SetGlobalBorderVidMap(global_border_vid_map_);
    }

    LOG_INFO("Run: Set communication matrix");
    if (communication_matrix_ == nullptr)
      communication_matrix_ =
          (bool*)malloc(sizeof(bool) * num_partitions * num_partitions);
    memset(communication_matrix_, 0,
           sizeof(bool) * num_partitions * num_partitions);

    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([this, tid, &cores, &dst_v, &src_v, &gid_by_vid,
                          &num_edges, &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = src_v[j];
          auto dst_vid = dst_v[j];
          auto src_gid = gid_by_vid[src_vid];
          auto dst_gid = gid_by_vid[dst_vid];
          if (src_gid == dst_gid) continue;
          *(communication_matrix_ + dst_gid * fragments_->size() + src_gid) = 1;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    delete num_vertexes_per_bucket;
    delete sum_in_edges_by_fragments;
    delete sum_out_edges_by_fragments;
    delete offset_out_edges;
    delete offset_in_edges;
    delete num_in_edges;
    delete num_out_edges;
    LOG_INFO("END");
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

  VID_T GetMaxVid() const { return max_vid_; }

  Bitmap* GetGlobalBorderVidMap() { return global_border_vid_map_; }

  VID_T* GetVidMap() { return vid_map_; }

 private:
  std::string graph_pt_;
  // to store fragments
  VID_T max_vid_ = 0;
  bool* communication_matrix_ = nullptr;
  VID_T* vid_map_ = nullptr;
  std::vector<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*>* fragments_ =
      nullptr;
  utility::io::DataMngr<GRAPH_T> data_mgnr_;
  std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes_ =
      nullptr;

  Bitmap* global_border_vid_map_ = nullptr;

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
    global_border_vertexes_with_dependencies_ =
        global_border_vertex_with_dependencies;
    return true;
  }

  bool SplitEdgeList(const size_t& num_partitions, EDGE_LIST_T& graph) {
    // size_t x = sqrt(num_partitions);
    // size_t y = num_partitions / x;
    LOG_INFO("SplitEdgeList: ", num_partitions);
    // size_t num_edges_for_each_fragment = graph.num_edges_ / num_partitions;
    fragments_ = new std::vector<GRAPH_BASE_T*>();
    EDGE_LIST_T* edge_list_fragment = nullptr;
    std::unordered_map<GID_T, std::vector<VID_T*>*> bucket;
    for (size_t gid = 0; gid < num_partitions; gid++) {
      bucket.insert(std::make_pair(gid, new std::vector<VID_T*>));
    }
    // std::hash<VID_T> vid_hash;
    for (size_t i = 0; i < graph.num_edges_; i++) {
      VID_T src = *(graph.buf_graph_ + i * 2);
      // VID_T dst = *(graph.buf_graph_ + i * 2 + 1);
      auto gid_x = Hash(src) % num_partitions;
      bucket.find((VID_T)gid_x)->second->push_back((graph.buf_graph_ + i * 2));
    }
    for (auto& iter : bucket) {
      edge_list_fragment = new EDGE_LIST_T(iter.first, iter.second->size(), 0);
      size_t localid = 0;
      for (size_t i = 0; i < iter.second->size(); i++) {
        memcpy((edge_list_fragment->buf_graph_ + 2 * i), iter.second->at(i),
               sizeof(VID_T) * 2);
        auto src = *(edge_list_fragment->buf_graph_ + 2 * i);
        auto dst = *(edge_list_fragment->buf_graph_ + 2 * i + 1);
        auto iter = edge_list_fragment->map_globalid2localid_->find(src);
        if (iter == edge_list_fragment->map_globalid2localid_->end()) {
          edge_list_fragment->map_globalid2localid_->insert(
              std::make_pair(src, localid++));
        }
        iter = edge_list_fragment->map_globalid2localid_->find(dst);
        if (iter == edge_list_fragment->map_globalid2localid_->end()) {
          edge_list_fragment->map_globalid2localid_->insert(
              std::make_pair(dst, localid++));
        }
      }
      edge_list_fragment->globalid_by_localid_ = (VID_T*)malloc(
          sizeof(VID_T) * edge_list_fragment->map_globalid2localid_->size());
      edge_list_fragment->vdata_ = (VDATA_T*)malloc(
          sizeof(VDATA_T) * edge_list_fragment->map_globalid2localid_->size());
      for (auto& iter : *edge_list_fragment->map_globalid2localid_) {
        edge_list_fragment->globalid_by_localid_[iter.second] = iter.first;
      }
      edge_list_fragment->num_vertexes_ =
          edge_list_fragment->map_globalid2localid_->size();
      fragments_->push_back(edge_list_fragment);
    }
    return true;
  }

  bool SplitImmutableCSR(const size_t& num_partitions, CSR_T& graph) {
    fragments_ = new std::vector<GRAPH_BASE_T*>();
    const size_t num_vertex_per_fragments =
        graph.get_num_vertexes() / num_partitions;
    LOG_INFO("Start graph partition: grouping ", graph.get_num_vertexes(),
             " vertexes into ", num_partitions, " fragments.");
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
          csr_fragment->max_vid_ = max_vid_;
          csr_fragment->num_vertexes_ = csr_fragment->vertexes_info_->size();
          fragments_->push_back(csr_fragment);
          csr_fragment = nullptr;
          count = 0;
          localid = 0;
        }
        csr_fragment = new CSR_T();
        globalid2gid_->insert(std::make_pair(iter_vertexes->second->vid, gid));
        // auto iter_global_border_vertexes_by_gid =
        //     global_border_vertexes_by_gid_->find(gid);
        // if (iter_global_border_vertexes_by_gid !=
        //     global_border_vertexes_by_gid_->end()) {
        //   iter_global_border_vertexes_by_gid->second->push_back(
        //       iter_vertexes->second->vid);
        // } else {
        //   auto tmp_vec = new std::vector<VID_T>;
        //   tmp_vec->push_back(iter_vertexes->second->vid);
        //   global_border_vertexes_by_gid_->insert(std::make_pair(gid,
        //   tmp_vec));
        // }

        csr_fragment->map_localid2globalid_->emplace(
            std::make_pair(localid, iter_vertexes->second->vid));
        csr_fragment->map_globalid2localid_->emplace(
            std::make_pair(iter_vertexes->second->vid, localid));
        vid_map_[iter_vertexes->second->vid] = localid;
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
        vid_map_[iter_vertexes->second->vid] = localid;
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
      csr_fragment->max_vid_ = max_vid_;
      csr_fragment->num_vertexes_ = csr_fragment->vertexes_info_->size();
      fragments_->push_back(csr_fragment);
    }
    if (fragments_->size() > 0) {
      return true;
    } else {
      return false;
    }
  }

  bool SplitImmutableCSRByHash(const size_t& num_partitions, CSR_T& graph) {
    fragments_ = new std::vector<GRAPH_BASE_T*>();
    const size_t num_vertex_per_fragments =
        graph.get_num_vertexes() / num_partitions;
    LOG_INFO("Start graph partition: grouping ", graph.get_num_vertexes(),
             " vertexes into ", num_partitions, " fragments.");
    globalid2gid_->reserve(graph.get_num_vertexes());

    std::unordered_map<GID_T, CSR_T*> fragments_map;
    //    std::unordered_map<GID_T, VID_T>
    VID_T* localid_by_gid = (VID_T*)malloc(sizeof(VID_T) * num_partitions);
    memset(localid_by_gid, 0, sizeof(VID_T) * num_partitions);

    for (size_t i = 0; i < num_partitions; i++)
      fragments_map.insert(std::make_pair((GID_T)i, new CSR_T()));

    for (auto& iter : *graph.vertexes_info_) {
      GID_T bucket_id = iter.second->vid % num_partitions;
      globalid2gid_->insert(std::make_pair(iter.second->vid, bucket_id));
      CSR_T* fragment = fragments_map.find(bucket_id)->second;
      fragment->map_localid2globalid_->insert(
          std::make_pair(localid_by_gid[bucket_id], iter.second->vid));
      fragment->map_globalid2localid_->insert(
          std::make_pair(iter.second->vid, localid_by_gid[bucket_id]));
      vid_map_[iter.second->vid] = localid_by_gid[bucket_id];
      fragment->sum_in_edges_ += iter.second->indegree;
      fragment->sum_out_edges_ += iter.second->outdegree;
      fragment->vertexes_info_->insert(
          std::make_pair(localid_by_gid[bucket_id], iter.second));
      iter.second->vid = localid_by_gid[bucket_id];
      fragment->gid_ = bucket_id;
      ++localid_by_gid[bucket_id];
    }

    for (GID_T gid = 0; gid < num_partitions; gid++) {
      CSR_T* fragment = fragments_map.find(gid)->second;
      fragment->num_vertexes_ = fragment->vertexes_info_->size();
      LOG_INFO("gid: ", gid, ", num_vertexes: ", fragment->num_vertexes_);
      fragment->max_vid_ = max_vid_;
      fragments_->push_back(fragment);
    }
    free(localid_by_gid);
    return true;
  }
};

}  // namespace partitioner
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_EDGE_CUT_PARTITIONER_H
