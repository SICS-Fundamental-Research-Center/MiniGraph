#ifndef MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H

#include <sys/stat.h>

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "rapidcsv.h"

#include "graphs/edge_list.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"


namespace minigraph {
namespace utility {
namespace io {

// EdgeListIOAdapter support edge list graph with CSV format
//   i.e. <src, dst> or <src, dst, weight>.
// In addition, two types of binary formatted edge list files are also
// supported:
//   Unweighted. Edges are tuples of <4 byte source, 4 byte destination>.
//   Weighted. Edges are tuples of <4 byte source, 4 byte destination, 4 byte
//   float typed weight>.
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class EdgeListIOAdapter : public IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T = graphs::EdgeList<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  EdgeListIOAdapter() = default;
  ~EdgeListIOAdapter() = default;

  template <class... Args>
  bool Read(GRAPH_BASE_T* graph, const GraphFormat& graph_format,
            const GID_T& gid, const size_t num_edges, Args&&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edge_list_csv:
        tag = ReadEdgeListFromCSV(graph, pt[0], gid, false);
        break;
      case weight_edge_list_csv:
        break;
      case edge_list_bin:
        tag = ReadEdgeListFromBin(graph, gid, pt[0], pt[1], pt[2]);
      default:
        break;
    }
    return tag;
  }

  template <class... Args>
  bool Write(const EDGE_LIST_T& graph, const GraphFormat& graph_format,
             bool vdata_only, Args&&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edge_list_csv:
        break;
      case weight_edge_list_csv:
        tag = false;
        break;
      case edge_list_bin:
        tag = WriteEdgeList2EdgeListBin(graph, pt[0], pt[1], pt[2]);
        break;
      default:
        break;
    }
    return tag;
  }

 private:
  bool ReadEdgeListFromCSV(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
                           const std::string& pt, const GID_T gid = 0,
                           const bool serialize = false) {
    if (!this->IsExist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }
    // auto edge_list = (EDGE_LIST_T*)graph;
    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(','));
    std::vector<VID_T> src = doc.GetColumn<VID_T>("src");
    std::vector<VID_T> dst = doc.GetColumn<VID_T>("dst");
    ((EDGE_LIST_T*)graph)->buf_graph_ =
        (vid_t*)malloc(sizeof(vid_t) * (src.size() + dst.size()));

    VID_T* buff = (VID_T*)((EDGE_LIST_T*)graph)->buf_graph_;
    memset((char*)buff, 0, sizeof(vid_t) * (src.size() + dst.size()));
    for (size_t i = 0; i < src.size(); i++) {
      *(buff + i * 2) = src.at(i);
      *((VID_T*)((EDGE_LIST_T*)graph)->buf_graph_ + i * 2 + 1) = dst.at(i);
    }
    if (serialize) {
      std::unordered_map<VID_T, std::vector<VID_T>*> graph_out_edges;
      std::unordered_map<VID_T, std::vector<VID_T>*> graph_in_edges;

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
        ((EDGE_LIST_T*)graph)->vertexes_info_->emplace(iter.first, vertex_info);
      }
      for (auto& iter : graph_out_edges) {
        auto iter_vertexes_info =
            ((EDGE_LIST_T*)graph)->vertexes_info_->find(iter.first);
        if (iter_vertexes_info !=
            ((EDGE_LIST_T*)graph)->vertexes_info_->cend()) {
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
          ((EDGE_LIST_T*)graph)
              ->vertexes_info_->emplace(iter.first, vertex_info);
        }
      }
    }
    ((EDGE_LIST_T*)graph)->num_vertexes_ =
        ((EDGE_LIST_T*)graph)->vertexes_info_->size();

    ((EDGE_LIST_T*)graph)->is_serialized_ = true;
    ((EDGE_LIST_T*)graph)->vdata_ = (VDATA_T*)malloc(
        sizeof(VDATA_T) * ((EDGE_LIST_T*)graph)->num_vertexes_);
    memset(((EDGE_LIST_T*)graph)->vdata_, 0,
           sizeof(VDATA_T) * ((EDGE_LIST_T*)graph)->num_vertexes_);
    ((EDGE_LIST_T*)graph)->num_edges_ = src.size();
    ((EDGE_LIST_T*)graph)->gid_ = gid;
    LOG_INFO("ReadEdgeListFromCSV num_vertexes: ",
             ((EDGE_LIST_T*)graph)->num_vertexes_,
             " num_edges: ", ((EDGE_LIST_T*)graph)->num_edges_);
    return true;
  }

  bool ReadEdgeListFromBin(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
                           const GID_T gid = 0, const std::string& meta_pt = "",
                           const std::string& data_pt = "",
                           const std::string& vdata_pt = "") {
    auto edge_list_graph = (EDGE_LIST_T*)graph;
    std::ifstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ifstream data_file(data_pt, std::ios::binary | std::ios::app);
    std::ifstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);

    size_t* meta_buff = (size_t*)malloc(sizeof(size_t) * 2);
    memset((char*)meta_buff, 0, sizeof(size_t) * 2);
    meta_file.read((char*)meta_buff, sizeof(size_t) * 2);
    edge_list_graph->buf_graph_ =
        (VID_T*)malloc(sizeof(VID_T) * 2 * meta_buff[1]);
    edge_list_graph->vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * meta_buff[0]);
    data_file.read((char*)edge_list_graph->buf_graph_,
                   sizeof(VID_T) * 2 * meta_buff[1]);
    vdata_file.read((char*)edge_list_graph->vdata_,
                    sizeof(VDATA_T) * meta_buff[0]);
    edge_list_graph->globalid_by_localid_ =
        (VID_T*)malloc(sizeof(VID_T) * meta_buff[0]);
    vdata_file.read((char*)edge_list_graph->globalid_by_localid_,
                    sizeof(VID_T) * meta_buff[0]);
    edge_list_graph->map_globalid2localid_ =
        new std::unordered_map<VID_T, VID_T>;
    edge_list_graph->map_globalid2localid_->reserve(meta_buff[0]);
    for (size_t i = 0; i < meta_buff[0]; i++) {
      edge_list_graph->map_globalid2localid_->insert(
          std::make_pair(edge_list_graph->globalid_by_localid_[i], i));
      //      edge_list_graph->globalid_by_localid_[i];
    }
    edge_list_graph->num_vertexes_ = meta_buff[0];
    edge_list_graph->num_edges_ = meta_buff[1];
    free(meta_buff);
    data_file.close();
    meta_file.close();
    vdata_file.close();
    return true;
  }

  bool WriteEdgeList2EdgeListBin(const EDGE_LIST_T& graph,
                                 const std::string& meta_pt,
                                 const std::string& data_pt,
                                 const std::string& vdata_pt) {
    if (graph.is_serialized_ == false) {
      XLOG(ERR, "Graph has not been serialized.");
      return false;
    }
    if (graph.buf_graph_ == nullptr) {
      XLOG(ERR, "Segmentation fault: buf_graph is nullptr");
      return false;
    }
    if (this->IsExist(meta_pt)) remove(meta_pt.c_str());
    if (this->IsExist(data_pt)) remove(data_pt.c_str());
    if (this->IsExist(vdata_pt)) remove(vdata_pt.c_str());

    std::ofstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ofstream data_file(data_pt, std::ios::binary | std::ios::app);
    std::ofstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);

    size_t* meta_buff = (size_t*)malloc(sizeof(size_t) * 2);
    meta_buff[0] = ((EDGE_LIST_T*)&graph)->num_vertexes_;
    meta_buff[1] = ((EDGE_LIST_T*)&graph)->num_edges_;

    meta_file.write((char*)meta_buff, 2 * sizeof(size_t));
    data_file.write((char*)((EDGE_LIST_T*)&graph)->buf_graph_,
                    sizeof(vid_t) * 2 * ((EDGE_LIST_T*)&graph)->num_edges_);

    vdata_file.write((char*)((EDGE_LIST_T*)&graph)->vdata_,
                     sizeof(vdata_t) * ((EDGE_LIST_T*)&graph)->num_vertexes_);

    vdata_file.write((char*)((EDGE_LIST_T*)&graph)->globalid_by_localid_,
                     sizeof(vid_t) * ((EDGE_LIST_T*)&graph)->num_vertexes_);

    LOG_INFO("EDGE_UNIT: ", sizeof(vid_t) * 2,
             ", num_edges: ", ((EDGE_LIST_T*)&graph)->num_edges_,
             ", write size: ",
             sizeof(vid_t) * 2 * ((EDGE_LIST_T*)&graph)->num_edges_);
    free(meta_buff);
    data_file.close();
    meta_file.close();
    vdata_file.close();
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
