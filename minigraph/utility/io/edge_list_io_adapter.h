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
  bool Read(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
            const GraphFormat& graph_format, const GID_T& gid, Args&&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edge_list_csv:
        tag = ReadEdgeListFromCSV(graph, pt[0], gid);
        break;
      case weight_edge_list_csv:
        break;
      default:
        break;
    }
    return tag;
  }

  template <class... Args>
  bool Write(const EDGE_LIST_T& graph, const GraphFormat& graph_format,
             Args&&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edge_list_csv:
        break;
      case weight_edge_list_csv:
        tag = false;
        break;
      case edge_list_bin:
        tag = WriteEdgeList2EdgeListBin(graph, pt[0]);
        break;
      default:
        break;
    }
    return tag;
  }

 private:
  bool ReadEdgeListFromCSV(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
                           const std::string& pt, const GID_T gid = 0) {
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
    for (size_t i = 0; i < src.size(); i++) {
      ((VID_T*)((EDGE_LIST_T*)graph)->buf_graph_)[i] = src.at(i);
      ((VID_T*)((EDGE_LIST_T*)graph)->buf_graph_)[i + 1] = dst.at(i);
    }

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
      if (iter_vertexes_info != ((EDGE_LIST_T*)graph)->vertexes_info_->cend()) {
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
        ((EDGE_LIST_T*)graph)->vertexes_info_->emplace(iter.first, vertex_info);
      }
    }
    ((EDGE_LIST_T*)graph)->num_vertexes_ =
        ((EDGE_LIST_T*)graph)->vertexes_info_->size();

    ((EDGE_LIST_T*)graph)->is_serialized_ = true;
    ((EDGE_LIST_T*)graph)->num_edges_ = src.size() + dst.size();
    ((EDGE_LIST_T*)graph)->gid_ = gid;
    return true;
  }

  bool WriteEdgeList2EdgeListBin(const EDGE_LIST_T& graph,
                                 const std::string& dst_pt) {
    if (graph.is_serialized_ == false) {
      XLOG(ERR, "Graph has not been serialized.");
      return false;
    }
    if (graph.buf_graph_ == nullptr) {
      XLOG(ERR, "Segmentation fault: buf_graph is nullptr");
      return false;
    }
    if (this->IsExist(dst_pt)) {
      remove(dst_pt.c_str());
    }
    std::ofstream dst_file(dst_pt, std::ios::binary | std::ios::app);

    dst_file.write((char*)((EDGE_LIST_T*)&graph)->buf_graph_,
                   sizeof(vid_t) * ((EDGE_LIST_T*)&graph)->num_edges_);
    dst_file.close();
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
