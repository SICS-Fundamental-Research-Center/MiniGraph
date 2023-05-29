#ifndef MINIGRAPH_GRAPHS_LINKEDLISTMUTABLECSR_H
#define MINIGRAPH_GRAPHS_LINKEDLISTMUTABLECSR_H

#include <malloc.h>

#include "graphs/graph.h"
#include "immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"

namespace minigraph {
namespace graphs {

// A mutable CSR implementation.
//
// It organized several graphs that in ImmutableCSR format into a LinkedList.
// It allows user to search a vertex with global_id in linked graphs.
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class LinkedListMutableCSR : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  struct LinkedList {
    ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>* p_graphs = nullptr;
    LinkedList* next = nullptr;
  }* linked_list_ = nullptr;

  LinkedListMutableCSR() : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    linked_list_ = new LinkedList();
  };

  ~LinkedListMutableCSR() {}
  void CleanUp() override { return; }

  size_t get_num_graphs() { return num_graphs_; }
  void set_num_graphs(size_t num_graphs) { num_graphs_ = num_graphs; }

  // Append a new graph im ImmutableCSR format to the trail of linked_list.
  void GraphAppend(
      ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>* immutable_csr) {
    if (num_graphs_ == 0) {
      num_graphs_ = 1;
      linked_list_->p_graphs = immutable_csr;
      linked_list_->next = new LinkedList;
    } else {
      auto next = linked_list_;
      while (next->next != nullptr) {
        next = next->next;
      }
      next->p_graphs = immutable_csr;
      next->next = new LinkedList;
      ++num_graphs_;
    }
    return;
  }

  VID_T get_localid(VID_T global_id) {
    auto next = linked_list_;
    auto local_id = global_id;
    while (next->next != nullptr) {
      if (next->p_graphs->get_num_vertexes() <= local_id) {
        local_id -= next->p_graphs->get_num_vertexes();
        next = next->next;
      } else {
        break;
      }
    }
    return local_id;
  }

  // Get a vertexinfo by globalid.
  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByVid(
      const VID_T globalid) {
    VID_T localid = get_localid(globalid);
    auto next = linked_list_;
    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* p = nullptr;
    while (next->next != nullptr) {
      if (next->p_graphs &&
          next->p_graphs->globalid_by_index_[localid] != globalid) {
        next = next->next;
      } else {
        break;
      }
    }
    return next->p_graphs->GetVertexByVid(localid);
  }

 private:
  size_t num_graphs_ = 0;
};

}  // namespace graphs
}  // namespace minigraph
#endif  // MINIGRAPH_GRAPHS_LINKEDLISTMUTABLECSR_H
