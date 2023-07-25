#ifndef MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H

#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "graphs/immutable_csr.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"

#include "rapidcsv.h"
#include "utility/atomic.h"
#include "utility/bitmap.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/FileUtil.h>


namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class CSRIOAdapter : public IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T = graphs::EdgeList<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  CSRIOAdapter(const std::string& pt)
      : IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T>(pt){};
  CSRIOAdapter() = default;
  ~CSRIOAdapter() = default;

  template <class... Args>
  bool Read(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
            const GraphFormat& graph_format, const GID_T& gid, Args&&... args) {
    std::string pt[] = {(args)...};
    switch (graph_format) {
      case edgelist_csv:
        return this->ReadCSRFromEdgeListCSV(graph, pt[0]);
      case csr_bin:
        return this->ReadCSRFromCSRBin(graph, gid, pt[0], pt[1], pt[2]);
      case weight_edgelist_csv:
        // not supported now.
        break;
      case immutable_csr_bin:
        // TO DO
        return false;
      default:
        break;
    }
    return false;
  }

  template <class... Args>
  bool Write(const graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>& graph,
             const GraphFormat& graph_format, bool vdata_only, Args&&... args) {
    std::string pt[] = {(args)...};

    bool tag = false;
    switch (graph_format) {
      case edgelist_csv:
        tag = false;
        break;
      case csr_bin:
        tag = this->WriteCSR2CSRBin(
            (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>&)graph,
            vdata_only, pt[0], pt[1], pt[2]);
        break;
      case weight_edgelist_csv:
        tag = false;
        break;
        // not supported now.
        // TO DO: load graph in weight edge csv format.
      case edgelist_bin:
        break;
      default:
        break;
    }
    return tag;
  }

  CSR_T* EdgeList2CSR(const GID_T gid = 0,
                      EDGE_LIST_T* edgelist_graph = nullptr,
                      const size_t cores = 1, VID_T* vid_map = nullptr) {
    assert(edgelist_graph != nullptr);
    LOG_INFO("EdgeList2CSR()");
    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    VID_T aligned_max_vid =
        ceil(edgelist_graph->get_aligned_max_vid() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR;

    std::atomic<size_t> pending_packages(cores);

    size_t* num_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    size_t* num_out_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    memset(num_in_edges, 0, sizeof(size_t) * aligned_max_vid);
    memset(num_out_edges, 0, sizeof(size_t) * aligned_max_vid);

    Bitmap* vertex_indicator = new Bitmap(aligned_max_vid);
    vertex_indicator->clear();

    size_t num_vertexes = edgelist_graph->get_num_vertexes();
    size_t num_edges = edgelist_graph->get_num_edges();

    LOG_INFO(
        "Run: Go through every edges to count the size of each vertex. "
        "NumEdges: ",
        num_edges);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &num_in_edges, &num_out_edges, &mtx,
                          &num_edges, &edgelist_graph, &vertex_indicator,
                          &pending_packages, &finish_cv, &num_vertexes]() {
        for (size_t j = tid; j < num_edges; j += cores) {
          auto src_vid = edgelist_graph->buf_graph_[j * 2];
          auto dst_vid = edgelist_graph->buf_graph_[j * 2 + 1];
          write_add(num_out_edges + src_vid, (size_t)1);
          write_add(num_in_edges + dst_vid, (size_t)1);
          vertex_indicator->set_bit(src_vid);
          vertex_indicator->set_bit(dst_vid);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>** vertexes = nullptr;

    vertexes = (graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>**)malloc(
        sizeof(graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*) * aligned_max_vid);

    for (size_t i = 0; i < aligned_max_vid; i++) vertexes[i] = nullptr;

    VID_T local_id = 0;

    LOG_INFO("Run: Merge edges");
    pending_packages.store(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([this, tid, &local_id, &cores, &num_in_edges, &vid_map,
                          &num_out_edges, &aligned_max_vid, &vertex_indicator,
                          &vertexes, &pending_packages, &finish_cv]() {
        for (VID_T global_id = tid; global_id < aligned_max_vid;
             global_id += cores) {
          if (!vertex_indicator->get_bit(global_id)) continue;
          auto u = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>();
          u->vid = __sync_fetch_and_add(&local_id, 1);
          if (vid_map != nullptr) vid_map[global_id] = u->vid;
          u->indegree = num_in_edges[global_id];
          u->outdegree = num_out_edges[global_id];
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

    LOG_INFO("Run: Edges fill");
    size_t sum_in_edges = 0, sum_out_edges = 0;
    size_t* offset_in_edges = (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    size_t* offset_out_edges =
        (size_t*)malloc(sizeof(size_t) * aligned_max_vid);
    memset(offset_in_edges, 0, sizeof(size_t) * aligned_max_vid);
    memset(offset_out_edges, 0, sizeof(size_t) * aligned_max_vid);
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &num_in_edges, &num_out_edges,
                          &num_edges, &offset_in_edges, &offset_out_edges,
                          sum_in_edges, sum_out_edges, &edgelist_graph,
                          &vertex_indicator, &vertexes, &pending_packages,
                          &finish_cv]() {
        for (VID_T global_eid = tid; global_eid < num_edges;
             global_eid += cores) {
          auto src_vid = edgelist_graph->buf_graph_[global_eid * 2];
          auto dst_vid = edgelist_graph->buf_graph_[global_eid * 2 + 1];
          assert(vertexes[src_vid] != nullptr);
          assert(vertexes[dst_vid] != nullptr);
          auto local_out_edges_offset =
              __sync_fetch_and_add(offset_out_edges + src_vid, 1);
          auto local_in_edges_offset =
              __sync_fetch_and_add(offset_in_edges + dst_vid, 1);
          vertexes[src_vid]->out_edges[local_out_edges_offset] = dst_vid;
          vertexes[dst_vid]->in_edges[local_in_edges_offset] = src_vid;
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    LOG_INFO("Run: Compute sum_in_edges & sum_out_edges");
    for (VID_T global_id = 0; global_id < aligned_max_vid; global_id++) {
      if (!vertex_indicator->get_bit(global_id)) continue;
      auto u = vertexes[global_id];
      __sync_fetch_and_add(&sum_in_edges, u->indegree);
      __sync_fetch_and_add(&sum_out_edges, u->outdegree);
    }

    LOG_INFO("Run: Construct graph");
    auto csr_graph = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>(
        gid, vertexes, edgelist_graph->num_vertexes_, sum_in_edges,
        sum_out_edges, edgelist_graph->max_vid_);
    csr_graph->ShowGraph();
    LOG_INFO("1");
    delete offset_out_edges;
    delete offset_in_edges;
    delete num_in_edges;
    delete num_out_edges;
    return csr_graph;
  }

 private:
  bool ReadCSRFromEdgeListCSV(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
      const std::string& pt) {
    if (!this->Exist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }
    LOG_INFO("Read edge list from csv.");
    auto immutable_csr = (CSR_T*)graph;

    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(','));

    // generate related edges for each vertex.
    std::unordered_map<VID_T, std::vector<VID_T>*> graph_out_edges;
    std::unordered_map<VID_T, std::vector<VID_T>*> graph_in_edges;

    std::vector<VID_T> src = doc.GetColumn<VID_T>(0);
    std::vector<VID_T> dst = doc.GetColumn<VID_T>(1);
    for (size_t i = 0; i < src.size(); i++) {
      auto iter = graph_out_edges.find(src.at(i));
      if (iter != graph_out_edges.end()) {
        iter->second->push_back(dst.at(i));
      } else {
        std::vector<VID_T>* out_edges = new std::vector<VID_T>;
        out_edges->push_back(dst.at(i));
        graph_out_edges.insert(std::make_pair(src.at(i), out_edges));
      }
      iter = graph_in_edges.find(dst.at(i));
      if (iter != graph_in_edges.end()) {
        iter->second->push_back(src.at(i));
      } else {
        std::vector<VID_T>* in_edges = new std::vector<VID_T>;
        in_edges->push_back(src.at(i));
        graph_in_edges.insert(std::make_pair(dst.at(i), in_edges));
      }
    }
    immutable_csr->sum_in_edges_ = src.size();
    immutable_csr->sum_out_edges_ = dst.size();
    for (auto& iter : graph_in_edges) {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* vertex_info =
          new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
      vertex_info->vid = iter.first;
      vertex_info->indegree = iter.second->size();
      vertex_info->in_edges =
          (VID_T*)malloc(sizeof(VID_T) * vertex_info->indegree);
      for (size_t i = 0; i < iter.second->size(); i++) {
        ((VID_T*)vertex_info->in_edges)[i] = iter.second->at(i);
      }
      immutable_csr->vertexes_info_->emplace(iter.first, vertex_info);
    }
    for (auto& iter : graph_out_edges) {
      auto iter_vertexes_info = immutable_csr->vertexes_info_->find(iter.first);
      if (iter_vertexes_info != immutable_csr->vertexes_info_->cend()) {
        iter_vertexes_info->second->outdegree = iter.second->size();
        iter_vertexes_info->second->out_edges =
            (VID_T*)malloc(sizeof(VID_T) * iter.second->size());
        for (size_t i = 0; i < iter.second->size(); i++) {
          iter_vertexes_info->second->out_edges[i] = iter.second->at(i);
        }
      } else {
        graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* vertex_info =
            new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
        vertex_info->vid = iter.first;
        vertex_info->outdegree = iter.second->size();
        vertex_info->out_edges =
            (VID_T*)malloc(sizeof(VID_T) * iter.second->size());
        for (size_t i = 0; i < iter.second->size(); i++) {
          ((VID_T*)vertex_info->out_edges)[i] = iter.second->at(i);
        }
        immutable_csr->vertexes_info_->emplace(iter.first, vertex_info);
      }
    }
    immutable_csr->num_vertexes_ = immutable_csr->vertexes_info_->size();
    return true;
  }

  bool ReadCSRFromCSRBin(GRAPH_BASE_T* graph_base, const GID_T& gid,
                         const std::string& meta_pt, const std::string& data_pt,
                         const std::string& vdata_pt) {
    if (!this->Exist(meta_pt)) {
      XLOG(ERR, "Read file fault: meta_pt, ", meta_pt, ", not exist");
      return false;
    }
    if (!this->Exist(data_pt)) {
      XLOG(ERR, "Read file fault: data_pt, ", data_pt, ", not exist");
      return false;
    }
    if (graph_base == nullptr) {
      XLOG(ERR,
           "Input fault: graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph "
           "is nullptr");
      return false;
    }
    auto graph = (CSR_T*)graph_base;
    size_t total_size = 0;
    size_t* buf_meta = (size_t*)malloc(sizeof(size_t) * 3);
    size_t buff_metta[3] = {0};
    {
      std::ifstream meta_file(meta_pt, std::ios::binary | std::ios::app);

      // read meta
      meta_file.read((char*)buf_meta, sizeof(size_t) * 3);
      graph->num_vertexes_ = buf_meta[0];
      graph->sum_in_edges_ = buf_meta[1];
      graph->sum_out_edges_ = buf_meta[2];
      graph->num_edges_ = buf_meta[1] + buf_meta[2];

      assert(graph->get_num_edges() > 0);
      assert(graph->get_num_vertexes() > 0);
      // read bitmap
      meta_file.read((char*)&graph->max_vid_, sizeof(VID_T));
      graph->aligned_max_vid_ =
          ceil(graph->get_max_vid() / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
      assert(graph->get_aligned_max_vid() > 0);
      graph->bitmap_ = new Bitmap(graph->get_aligned_max_vid());
      graph->bitmap_->clear();
      meta_file.close();
    }

    {
      // read data
      size_t size_globalid = sizeof(VID_T) * buf_meta[0];
      size_t size_localid_by_globalid =
          sizeof(VID_T) * graph->get_aligned_max_vid();
      size_t size_indegree = sizeof(size_t) * buf_meta[0];
      size_t size_outdegree = sizeof(size_t) * buf_meta[0];
      size_t size_in_offset = sizeof(size_t) * buf_meta[0];
      size_t size_out_offset = sizeof(size_t) * buf_meta[0];
      size_t size_in_edges = sizeof(VID_T) * buf_meta[1];
      size_t size_out_edges = sizeof(VID_T) * buf_meta[2];
      total_size = size_globalid + size_indegree + size_outdegree +
                   size_in_offset + size_out_offset + size_in_edges +
                   size_out_edges + size_localid_by_globalid;

      size_t start_globalid = 0;
      size_t start_indegree = start_globalid + size_globalid;
      size_t start_outdegree = start_indegree + size_indegree;
      size_t start_in_offset = start_outdegree + size_outdegree;
      size_t start_out_offset = start_in_offset + size_in_offset;
      size_t start_in_edges = start_out_offset + size_out_offset;
      size_t start_out_edges = start_in_edges + size_in_edges;
      size_t start_localid_by_globalid = start_out_edges + size_out_edges;

      std::ifstream data_file(data_pt, std::ios::binary | std::ios::app);
      graph->buf_graph_ = (VID_T*)malloc(total_size);
      data_file.read((char*)graph->buf_graph_, total_size);
      graph->globalid_by_index_ =
          (VID_T*)((char*)graph->buf_graph_ + start_globalid);
      graph->out_offset_ =
          (size_t*)((char*)graph->buf_graph_ + start_out_offset);
      graph->in_offset_ = (size_t*)((char*)graph->buf_graph_ + start_in_offset);
      graph->indegree_ = (size_t*)((char*)graph->buf_graph_ + start_indegree);
      graph->outdegree_ = (size_t*)((char*)graph->buf_graph_ + start_outdegree);
      graph->in_edges_ = (VID_T*)((char*)graph->buf_graph_ + start_in_edges);
      graph->out_edges_ = (VID_T*)((char*)graph->buf_graph_ + start_out_edges);
      graph->localid_by_globalid_ =
          (VID_T*)((char*)graph->buf_graph_ + start_localid_by_globalid);
      for (size_t i = 0; i < graph->num_vertexes_; i++)
        graph->bitmap_->set_bit(graph->globalid_by_index_[i]);
      data_file.close();
    }

    {
      // read vdata and edata
      std::ifstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);
      graph->vdata_ =
          (VDATA_T*)malloc(sizeof(VDATA_T) * graph->get_num_vertexes());
      memset(graph->vdata_, 0, sizeof(VDATA_T) * graph->get_num_vertexes());
      vdata_file.read((char*)graph->vdata_,
                      sizeof(VDATA_T) * graph->get_num_vertexes());
      // graph->vertexes_state_ =
      //     (char*)malloc(sizeof(char) * graph->get_num_vertexes());
      // memset(graph->vertexes_state_, VERTEXDISMATCH,
      //        sizeof(char) * graph->get_num_vertexes());
      // vdata_file.read((char*)graph->vertexes_state_,
      //                 sizeof(char*) * graph->get_num_vertexes());
      graph->edata_ = (EDATA_T*)malloc(
          sizeof(EDATA_T) * ceil(graph->get_num_in_edges() / ALIGNMENT_FACTOR) *
          ALIGNMENT_FACTOR);
      memset(graph->edata_, 0,
             sizeof(EDATA_T) *
                 ceil(graph->get_num_in_edges() / ALIGNMENT_FACTOR) *
                 ALIGNMENT_FACTOR);

      vdata_file.read((char*)graph->edata_,
                      sizeof(EDATA_T) * graph->get_num_in_edges());
      vdata_file.close();
    }

    graph->is_serialized_ = true;
    graph->gid_ = gid;
    graph->ShowGraph();
    while(1);
    return true;
  }

  bool WriteCSR2CSRBin(
      graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>& graph,
      bool vdata_only = false, const std::string& meta_pt = "",
      const std::string& data_pt = "", const std::string vdata_pt = "") {
    if (graph.is_serialized_ == false) {
      XLOG(ERR, "Graph has not been serialized.");
      return false;
    }
    if (graph.buf_graph_ == nullptr) {
      XLOG(ERR, "Segmentation fault: buf_graph is nullptr");
      return false;
    }
    if (!vdata_only) {
      // write meta
      if (this->Exist(meta_pt)) remove(meta_pt.c_str());
      if (this->Exist(data_pt)) remove(data_pt.c_str());
      std::ofstream meta_file(meta_pt, std::ios::binary | std::ios::app);
      size_t* buf_meta = (size_t*)malloc(sizeof(size_t) * 3);
      memset(buf_meta, 0, sizeof(size_t) * 3);
      buf_meta[0] = graph.num_vertexes_;
      buf_meta[1] = graph.sum_in_edges_;
      buf_meta[2] = graph.sum_out_edges_;
      meta_file.write((char*)buf_meta, sizeof(size_t) * 3);
      meta_file.write((char*)&graph.max_vid_, sizeof(VID_T));
      free(buf_meta);
      meta_file.close();

      // write data
      std::ofstream data_file(data_pt, std::ios::binary | std::ios::app);
      size_t size_localid = sizeof(VID_T) * graph.get_num_vertexes();
      size_t size_globalid = sizeof(VID_T) * graph.get_num_vertexes();
      size_t size_localid_by_globalid =
          sizeof(VID_T) * graph.get_aligned_max_vid();
      size_t size_indegree = sizeof(size_t) * graph.get_num_vertexes();
      size_t size_outdegree = sizeof(size_t) * graph.get_num_vertexes();
      size_t size_in_offset = sizeof(size_t) * graph.get_num_vertexes();
      size_t size_out_offset = sizeof(size_t) * graph.get_num_vertexes();
      size_t size_in_edges = sizeof(VID_T) * graph.sum_in_edges_;
      size_t size_out_edges = sizeof(VID_T) * graph.sum_out_edges_;

      size_t total_size = size_localid + size_globalid + size_in_offset +
                          size_indegree + size_outdegree + size_out_offset +
                          size_in_edges + size_out_edges +
                          size_localid_by_globalid;
      data_file.write((char*)graph.buf_graph_, total_size);
      data_file.close();
    }

    {
      // write vdata
      if (this->Exist(vdata_pt)) remove(vdata_pt.c_str());
      std::ofstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);
      vdata_file.write((char*)graph.vdata_,
                       sizeof(VDATA_T) * graph.get_num_vertexes());
      // vdata_file.write((char*)graph.vertexes_state_,
      //                  sizeof(char) * graph.num_vertexes_);

      // write edata
      vdata_file.write((char*)graph.edata_,
                       sizeof(EDATA_T) * graph.get_num_out_edges());
      vdata_file.close();
    }
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
