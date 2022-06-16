#ifndef MINIGRAPH_GRAPHS_EDGE_LIST_H
#define MINIGRAPH_GRAPHS_EDGE_LIST_H

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/AtomicUnorderedMap.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Range.h>
#include <folly/portability/Asm.h>
#include <folly/portability/Atomic.h>
#include <folly/portability/SysTime.h>
#include <jemalloc/jemalloc.h>

#include "graphs/graph.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"


namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class EdgeList : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  EdgeList() : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    vertexes_info_ =
        new std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>();
  }

  EdgeList(const GID_T gid) : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid) {
    vertexes_info_ =
        new std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>();
  };

  ~EdgeList() = default;

  size_t get_num_vertexes() const override { return num_vertexes_; }

  void CleanUp() override {
    if (vertexes_info_ != nullptr) {
      std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*> tmp;
      vertexes_info_->swap(tmp);
      delete vertexes_info_;
      vertexes_info_ = nullptr;
    }
  };

  void ShowGraph(const size_t count = 2) {
    std::cout << "\n\n##### EdgeListGraph GID: " << gid_
              << ", num_verteses: " << num_vertexes_
              << ", num_edges: " << num_edges_ << " #####" << std::endl;
    size_t count_ = 0;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (count_++ > count) {
        std::cout << "############################" << std::endl;
        return;
      }
      VertexInfo* vertex_info = vertexes_info_->find(i)->second;
      vertex_info->ShowVertexInfo();
    }
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByVid(const VID_T vid) {
    auto iter = vertexes_info_->find(vid);
    if (iter != vertexes_info_->end()) {
      return *iter->second;
    } else {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
      return vertex_info;
    }
  }

 public:
  //  basic param
  GID_T gid_ = -1;
  size_t num_vertexes_ = 0;
  size_t num_edges_ = 0;

  GraphFormat graph_format;
  void* buf_graph_ = nullptr;
  bool is_serialized_ = false;
  std::map<VID_T, VertexInfo*>* vertexes_info_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif
