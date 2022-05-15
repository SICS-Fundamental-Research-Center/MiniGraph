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
        tag = ReadEdgeListFromCSV(graph, pt[0]);
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
                           const std::string& pt) {
    if (!this->IsExist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }
    //auto edge_list = (EDGE_LIST_T*)graph;
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
    ((EDGE_LIST_T*)graph)->is_serialized_ = true;
    ((EDGE_LIST_T*)graph)->num_edges_ = src.size() + dst.size();
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
