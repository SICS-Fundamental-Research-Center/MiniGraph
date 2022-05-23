//
// Created by hsiaoko on 2022/3/14.
//
#ifndef MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H

#include <sys/stat.h>

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "rapidcsv.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/FileUtil.h>

#include "graphs/immutable_csr.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"


namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class CSRIOAdapter : public IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

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
      case edge_list_csv:
        return this->ReadCSRFromEdgeListCSV(graph, pt[0]);
      case csr_bin:
        return this->ReadCSRFromCSRBin(graph, gid, pt[0], pt[1]);
      case weight_edge_list_csv:
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
             const GraphFormat& graph_format, Args&&... args) {
    std::string pt[] = {(args)...};

    bool tag = false;
    switch (graph_format) {
      case edge_list_csv:
        tag = false;
        break;
      case csr_bin:
        tag = this->WriteCSR2CSRBin(
            (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>&)graph, pt[0],
            pt[1]);
        break;
      case weight_edge_list_csv:
        tag = false;
        break;
        // not supported now.
        // TO DO: load graph in weight edge csv format.
      case immutable_csr_bin:
        tag = this->WriteCSR2CSRBin(
            (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>&)graph, pt[0],
            pt[1]);
        break;
      case edge_list_bin:
        break;
      default:
        break;
    }
    return tag;
  }

  bool ReadGlobalBorderVertexes(
      folly::AtomicHashMap<VID_T, std::pair<size_t, GID_T*>>*
          global_border_vertexes,
      const std::string& border_vertexes_pt) {
    if (global_border_vertexes == nullptr) {
      LOG_INFO("segmentation fault: ", "global_border_vertexes is nullptr");
      return false;
    }
    std::ifstream border_vertexes_file(border_vertexes_pt,
                                       std::ios::binary | std::ios::app);
    size_t* buf_config = (size_t*)malloc(sizeof(size_t) * 2);
    border_vertexes_file.read((char*)buf_config, sizeof(size_t) * 2);

    VID_T* buf_border_vertexes = (VID_T*)malloc(sizeof(VID_T) * buf_config[0]);
    size_t* buf_offset = (size_t*)malloc(sizeof(size_t) * buf_config[0]);
    GID_T* buf_gid = (GID_T*)malloc(sizeof(GID_T) * buf_config[1]);
    border_vertexes_file.read((char*)buf_border_vertexes,
                              sizeof(VID_T) * buf_config[0]);
    border_vertexes_file.read((char*)buf_offset,
                              sizeof(size_t) * buf_config[0]);
    border_vertexes_file.read((char*)buf_gid, sizeof(GID_T) * buf_config[1]);
    for (size_t i = 0; i < buf_config[0]; ++i) {
      if (i < buf_config[0] - 1) {
        size_t num_gid = buf_offset[i + 1] - buf_offset[i];
        global_border_vertexes->insert(
            buf_border_vertexes[i],
            std::make_pair(num_gid, (buf_gid + buf_offset[i])));
      } else {
        size_t num_gid = buf_config[1] - buf_offset[i];
        global_border_vertexes->insert(
            buf_border_vertexes[i],
            std::make_pair(num_gid, (buf_gid + buf_offset[i])));
      }
    }
    return true;
  }

 private:
  bool ReadCSRFromEdgeListCSV(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
      const std::string& pt) {
    if (!this->IsExist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }
    auto immutable_csr = (CSR_T*)graph;

    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(','));

    // generate related edges for each vertex.
    std::unordered_map<VID_T, std::vector<VID_T>*> graph_out_edges;
    std::unordered_map<VID_T, std::vector<VID_T>*> graph_in_edges;

    std::vector<VID_T> src = doc.GetColumn<VID_T>("src");
    std::vector<VID_T> dst = doc.GetColumn<VID_T>("dst");
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
    immutable_csr->sum_in_edges_ = graph_in_edges.size();
    immutable_csr->sum_out_edges_ = graph_out_edges.size();

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
                         const std::string& meta_pt,
                         const std::string& data_pt) {
    if (!this->IsExist(meta_pt)) {
      XLOG(ERR, "Read file fault: meta_pt, ", meta_pt, ", not exist");
      return false;
    }
    if (!this->IsExist(data_pt)) {
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

    std::ifstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ifstream data_file(data_pt, std::ios::binary | std::ios::app);

    // read meta
    size_t* buf_meta = (size_t*)malloc(sizeof(size_t) * 3);
    meta_file.read((char*)buf_meta, sizeof(size_t) * 3);
    size_t size_localid = sizeof(VID_T) * buf_meta[0];
    size_t size_globalid = sizeof(VID_T) * buf_meta[0];
    size_t size_index_by_vid = sizeof(size_t) * buf_meta[0];
    size_t size_indegree = sizeof(size_t) * buf_meta[0];
    size_t size_outdegree = sizeof(size_t) * buf_meta[0];
    size_t size_in_offset = sizeof(size_t) * buf_meta[0];
    size_t size_out_offset = sizeof(size_t) * buf_meta[0];
    size_t size_vdata = sizeof(VDATA_T) * buf_meta[0];
    size_t size_in_edges = sizeof(VID_T) * buf_meta[1];
    size_t size_out_edges = sizeof(VID_T) * buf_meta[2];
    size_t total_size = size_localid + size_globalid + size_index_by_vid +
                        size_indegree + size_outdegree + size_in_offset +
                        size_out_offset + size_in_edges + size_out_edges +
                        size_vdata;

    graph->num_vertexes_ = buf_meta[0];
    graph->sum_in_edges_ = buf_meta[1];
    graph->sum_out_edges_ = buf_meta[2];

    size_t start_localid = 0;
    size_t start_globalid = start_localid + size_localid;
    size_t start_index_by_vid = start_globalid + size_globalid;
    size_t start_indegree = start_index_by_vid + size_index_by_vid;
    size_t start_outdegree = start_indegree + size_indegree;
    size_t start_in_offset = start_outdegree + size_outdegree;
    size_t start_out_offset = start_in_offset + size_in_offset;
    size_t start_vdata = start_out_offset + size_out_offset;
    size_t start_in_edges = start_vdata + size_vdata;
    size_t start_out_edges = start_in_edges + size_in_edges;
    // read data

    graph->buf_graph_ = malloc(total_size);
    data_file.read((char*)graph->buf_graph_, total_size);
    graph->vid_by_index_ = ((VID_T*)((char*)graph->buf_graph_ + start_localid));
    graph->globalid_by_index_ =
        (VID_T*)((char*)graph->buf_graph_ + start_globalid);
    graph->vdata_ = (VDATA_T*)((char*)graph->buf_graph_ + start_vdata);
    graph->index_by_vid_ =
        ((size_t*)((char*)graph->buf_graph_ + start_index_by_vid));
    graph->out_offset_ = (size_t*)((char*)graph->buf_graph_ + start_out_offset);
    graph->in_offset_ = (size_t*)((char*)graph->buf_graph_ + start_in_offset);
    graph->indegree_ = (size_t*)((char*)graph->buf_graph_ + start_indegree);
    graph->outdegree_ = (size_t*)((char*)graph->buf_graph_ + start_outdegree);
    graph->in_edges_ = (VID_T*)((char*)graph->buf_graph_ + start_in_edges);
    graph->out_edges_ = (VID_T*)((char*)graph->buf_graph_ + start_out_edges);
    graph->map_globalid2localid_->reserve(graph->num_vertexes_);
    for (size_t i = 0; i < graph->num_vertexes_; i++) {
      graph->map_globalid2localid_->insert(std::make_pair(
          graph->globalid_by_index_[i], graph->vid_by_index_[i]));
    }
    free(buf_meta);
    buf_meta = nullptr;
    graph->is_serialized_ = true;
    graph->gid_ = gid;
    meta_file.close();
    data_file.close();
    return true;
  }

  bool WriteCSR2CSRBin(
      const graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>& graph,
      const std::string& meta_pt, const std::string& data_pt) {
    if (graph.is_serialized_ == false) {
      XLOG(ERR, "Graph has not been serialized.");
      return false;
    }
    if (graph.buf_graph_ == nullptr) {
      XLOG(ERR, "Segmentation fault: buf_graph is nullptr");
      return false;
    }
    if (this->IsExist(meta_pt)) {
      remove(meta_pt.c_str());
    }
    if (this->IsExist(data_pt)) {
      remove(data_pt.c_str());
    }
    std::ofstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ofstream data_file(data_pt, std::ios::binary | std::ios::app);

    // write meta
    size_t* buf_meta = (size_t*)malloc(sizeof(size_t) * 3);
    buf_meta[0] = graph.num_vertexes_;
    buf_meta[1] = graph.sum_in_edges_;
    buf_meta[2] = graph.sum_out_edges_;
    meta_file.write((char*)buf_meta, sizeof(size_t) * 3);

    // write data
    size_t size_localid = sizeof(VID_T) * graph.num_vertexes_;
    size_t size_globalid = sizeof(VID_T) * graph.num_vertexes_;
    size_t size_index_by_vid = sizeof(size_t) * graph.num_vertexes_;
    size_t size_indegree = sizeof(size_t) * graph.num_vertexes_;
    size_t size_outdegree = sizeof(size_t) * graph.num_vertexes_;
    size_t size_in_offset = sizeof(size_t) * graph.num_vertexes_;
    size_t size_out_offset = sizeof(size_t) * graph.num_vertexes_;
    size_t size_vdata = sizeof(VDATA_T) * graph.num_vertexes_;
    size_t size_in_edges = sizeof(VID_T) * graph.sum_in_edges_;
    size_t size_out_edges = sizeof(VID_T) * graph.sum_out_edges_;
    size_t total_size = size_localid + size_globalid + size_index_by_vid +
                        size_in_offset + size_indegree + size_outdegree +
                        size_out_offset + size_in_edges + size_out_edges +
                        size_vdata;
    meta_file.write((char*)buf_meta, sizeof(size_t) * 3);
    data_file.write((char*)graph.buf_graph_, total_size);
    free(buf_meta);
    buf_meta = nullptr;
    meta_file.close();
    data_file.close();
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
