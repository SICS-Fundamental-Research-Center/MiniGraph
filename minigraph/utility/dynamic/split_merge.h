#ifndef ATOMIC_H_SPLIT_MERGE_H
#define ATOMIC_H_SPLIT_MERGE_H

#include "graphs/immutable_csr.h"
#include "graphs/linked_list_mutable_csr.h"

namespace minigraph {
namespace utility {
namespace dynamic {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class SplitMerge {
 public:
  SplitMerge() = default;
  ~SplitMerge() = default;

  // Split the ImmutableCSR graph into two fragments graph_a and graph_b, such
  // that they are approximately equal in num of edges.
  std::pair<graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*,
            graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*>
  Split(graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>& immutable_csr) {
    auto graph_a = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
    auto graph_b = new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

    // Binary search for splitted poit.
    size_t i = (immutable_csr.get_max_vid() - immutable_csr.get_vid_base()) / 2;
    size_t end = immutable_csr.get_max_vid() - immutable_csr.get_vid_base();
    size_t start = 0;
    size_t tmp_base =
        (immutable_csr.out_offset_[immutable_csr.get_num_vertexes() - 1] +
         immutable_csr.outdegree_[immutable_csr.get_num_vertexes() - 1] -
         immutable_csr.out_offset_[0]) /
        2;

    while (start <= end) {
      i = ceil(end - start) / 2;
      if (immutable_csr.out_offset_[i] - immutable_csr.out_offset_[0] +
              immutable_csr.outdegree_[i] <
          tmp_base)
        start = i + 1;
      else if (immutable_csr.out_offset_[i] - immutable_csr.out_offset_[0] >
               tmp_base)
        end = i - 1;
      else {
        break;
      }
    }

    // Construct graph_a
    graph_a->set_num_vertexes(i + 1);
    graph_a->vdata_ = (VDATA_T*)malloc(
        sizeof(VDATA_T) * ceil(graph_a->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_a->globalid_by_index_ = (VID_T*)malloc(
        sizeof(VID_T) * ceil(graph_a->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    memcpy(graph_a->globalid_by_index_, immutable_csr.globalid_by_index_,
           sizeof(VID_T) * graph_a->get_num_vertexes());

    graph_a->set_num_out_edges(immutable_csr.out_offset_[i] +
                               immutable_csr.outdegree_[i] -
                               immutable_csr.out_offset_[0]);
    graph_a->out_offset_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_a->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_a->outdegree_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_a->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_a->out_edges_ = (VID_T*)malloc(
        sizeof(VID_T) * ceil(graph_a->get_num_out_edges() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    memset(graph_a->out_offset_, 0,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memset(graph_a->outdegree_, 0,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memset(graph_a->out_edges_, 0, sizeof(VID_T) * graph_a->get_num_edges());
    memcpy(graph_a->out_offset_, immutable_csr.out_offset_,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memcpy(graph_a->outdegree_, immutable_csr.outdegree_,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memcpy(graph_a->out_edges_, immutable_csr.out_edges_,
           sizeof(VID_T) * graph_a->get_num_out_edges());

    graph_a->set_num_in_edges(immutable_csr.in_offset_[i] +
                              immutable_csr.indegree_[i] -
                              immutable_csr.in_offset_[0]);
    graph_a->in_offset_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_a->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_a->indegree_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_a->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_a->in_edges_ = (VID_T*)malloc(
        sizeof(VID_T) * ceil(graph_a->get_num_in_edges() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    memset(graph_a->in_offset_, 0,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memset(graph_a->indegree_, 0, sizeof(size_t) * graph_a->get_num_vertexes());
    memset(graph_a->in_edges_, 0, sizeof(VID_T) * graph_a->get_num_edges());

    memcpy(graph_a->in_edges_, immutable_csr.in_edges_,
           sizeof(VID_T) * graph_a->get_num_in_edges());
    memcpy(graph_a->in_offset_, immutable_csr.in_offset_,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memcpy(graph_a->indegree_, immutable_csr.indegree_,
           sizeof(size_t) * graph_a->get_num_vertexes());
    memcpy(graph_a->vdata_, immutable_csr.vdata_,
           sizeof(VID_T) * graph_a->get_num_vertexes());

    graph_a->set_in_offset_base(immutable_csr.in_offset_[0]);
    graph_a->set_out_offset_base(immutable_csr.out_offset_[0]);
    graph_a->set_vid_base(immutable_csr.localid2globalid(0));
    graph_a->set_max_vid(graph_a->localid2globalid(i));

    // Construct graph_b
    graph_b->set_num_vertexes(immutable_csr.get_num_vertexes() -
                              graph_a->get_num_vertexes());
    graph_b->vdata_ = (VDATA_T*)malloc(
        sizeof(VDATA_T) * ceil(graph_b->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_b->globalid_by_index_ = (VID_T*)malloc(
        sizeof(VID_T) * ceil(graph_b->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);

    memcpy(graph_b->globalid_by_index_,
           immutable_csr.globalid_by_index_ + graph_a->get_num_vertexes(),
           sizeof(VID_T) * graph_b->get_num_vertexes());

    graph_b->set_num_out_edges(immutable_csr.get_num_out_edges() -
                               graph_a->get_num_out_edges());
    graph_b->out_offset_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_b->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_b->outdegree_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_b->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_b->out_edges_ = (VID_T*)malloc(
        sizeof(VID_T) * ceil(graph_b->get_num_out_edges() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    memset(graph_b->out_offset_, 0,
           sizeof(size_t) * graph_b->get_num_vertexes());
    memset(graph_b->outdegree_, 0,
           sizeof(size_t) * graph_b->get_num_vertexes());
    memset(graph_b->out_edges_, 0, sizeof(VID_T) * graph_b->get_num_edges());

    memcpy(graph_b->out_edges_,
           immutable_csr.out_edges_ + graph_a->get_num_out_edges(),
           sizeof(VID_T) * graph_b->get_num_out_edges());
    memcpy(graph_b->out_offset_,
           immutable_csr.out_offset_ + graph_a->get_num_vertexes(),
           sizeof(size_t) * graph_b->get_num_vertexes());
    memcpy(graph_b->outdegree_,
           immutable_csr.outdegree_ + graph_a->get_num_vertexes(),
           sizeof(size_t) * graph_b->get_num_vertexes());

    graph_b->set_num_in_edges(immutable_csr.get_num_in_edges() -
                              graph_a->get_num_in_edges());
    graph_b->in_offset_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_b->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_b->indegree_ = (size_t*)malloc(
        sizeof(size_t) * ceil(graph_b->get_num_vertexes() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    graph_b->in_edges_ = (VID_T*)malloc(
        sizeof(VID_T) * ceil(graph_b->get_num_in_edges() / ALIGNMENT_FACTOR) *
        ALIGNMENT_FACTOR);
    memset(graph_b->in_offset_, 0,
           sizeof(size_t) * graph_b->get_num_vertexes());
    memset(graph_b->indegree_, 0, sizeof(size_t) * graph_b->get_num_vertexes());
    memset(graph_b->in_edges_, 0, sizeof(VID_T) * graph_b->get_num_edges());
    memcpy(graph_b->in_edges_,
           immutable_csr.in_edges_ + graph_a->get_num_in_edges(),
           sizeof(VID_T) * graph_b->get_num_in_edges());
    memcpy(graph_b->in_offset_,
           immutable_csr.in_offset_ + graph_a->get_num_vertexes(),
           sizeof(size_t) * graph_b->get_num_vertexes());
    memcpy(graph_b->indegree_,
           immutable_csr.indegree_ + graph_a->get_num_vertexes(),
           sizeof(size_t) * graph_b->get_num_vertexes());
    memcpy(graph_b->vdata_, immutable_csr.vdata_ + graph_a->get_num_vertexes(),
           sizeof(VID_T) * graph_b->get_num_vertexes());

    graph_b->set_in_offset_base(
        immutable_csr.in_offset_[graph_a->get_num_vertexes()]);
    graph_b->set_out_offset_base(
        immutable_csr.out_offset_[graph_a->get_num_vertexes()]);
    graph_b->set_vid_base(
        immutable_csr.localid2globalid(graph_a->get_num_vertexes()));
    graph_b->set_max_vid(immutable_csr.get_max_vid());

    return std::make_pair(graph_a, graph_b);
  }

  // Append a new graph im ImmutableCSR format to the trail of linked_list.
  void Merge(graphs::LinkedListMutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*
                 linkedlist_mutable_csr = nullptr,
             graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*
                 immutable_csr = nullptr) {
    assert(linkedlist_mutable_csr != nullptr && immutable_csr != nullptr);

    linkedlist_mutable_csr->GraphAppend(immutable_csr);
    return;
  }
};

}  // namespace dynamic
}  // namespace utility
}  // namespace minigraph
#endif  // ATOMIC_H_SPLIT_MERGE_H
