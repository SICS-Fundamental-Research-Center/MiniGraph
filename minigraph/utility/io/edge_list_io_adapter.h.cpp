#ifndef MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H

#include "graphs/edge_list.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "rapidcsv.h"
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

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
  EdgeListIOAdapter(const std::string& pt)
      : IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T>(pt){};
  ~EdgeListIOAdapter() = default;

  template <class... Args>
  bool Read(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
            const GraphFormat& graph_format, const GID_T& gid, Args&&... args) {
    std::string pt[] = {(args)...};
    switch (graph_format) {
      case edge_list_csv:
        break;
      case weight_edge_list_csv:
        break;
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
      case weight_edge_list_csv:
        tag = false;
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
    auto edge_list = (EDGE_LIST_T*)graph;

    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(','));
    std::vector<VID_T> src = doc.GetColumn<VID_T>("src");
    std::vector<VID_T> dst = doc.GetColumn<VID_T>("dst");
    for (size_t i = 0; i < src.size(); i++) {
    }

    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
