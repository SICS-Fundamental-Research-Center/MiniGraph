#ifndef MINIGRAPH_GRAPHS_RELATION_H
#define MINIGRAPH_GRAPHS_RELATION_H

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

#include <jemalloc/jemalloc.h>

#include "graphs/graph.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include "table.h"

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class Relation : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
 public:
  Relation(const GID_T gid = 0, const size_t num_edges = 0,
           const size_t num_vertexes = 0, VID_T max_vid = 0,
           VID_T* buf_graph = nullptr)
      : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    this->gid_ = gid;
  }
  ~Relation() = default;

  Relation* GetClassType(void) override {
    return this;
  }

  size_t get_num_tables() { return num_table_; }

  void CleanUp() override {}
  void ShowGraph(const size_t count = 2) {}

 public:
  size_t num_table_ = 0;
  Table* tables_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif
