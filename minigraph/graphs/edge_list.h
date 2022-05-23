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
 public:
  EdgeList() : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {}

  EdgeList(const GID_T gid) : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid){};

  ~EdgeList() = default;

  size_t get_num_vertexes() const override { return num_vertexes_; }

  void CleanUp() override{};

  void ShowGraph(const size_t count = 2) {}

 public:
  //  basic param
  GID_T gid_ = -1;
  size_t num_vertexes_ = 0;
  size_t num_edges_ = 0;

  GraphFormat graph_format;
  void* buf_graph_ = nullptr;
  bool is_serialized_ = false;
};

}  // namespace graphs
}  // namespace minigraph
#endif
