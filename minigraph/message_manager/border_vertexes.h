#ifndef MINIGRAPH_BORDER_VERTEXES_H
#define MINIGRAPH_BORDER_VERTEXES_H

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "utility/logging.h"

namespace minigraph {
namespace message {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class BorderVertexes {
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  std::unique_ptr<std::unordered_map<VID_T, VDATA_T>>
      global_border_vertexes_vdata_;
  std::unique_ptr<std::unordered_map<VID_T, std::vector<GID_T>*>>
      global_border_vertexes_ = nullptr;
  std::unique_ptr<std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>>
      global_border_vertexes_with_dependencies_ = nullptr;
  std::unique_ptr<std::unordered_map<GID_T, std::pair<size_t, VID_T*>>>
      global_border_vertexes_by_gid_ = nullptr;

  std::mutex* mtx_;

  BorderVertexes() = default;

  BorderVertexes(
      std::unordered_map<VID_T, std::vector<GID_T>*>* global_border_vertexes,
      std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>*
          global_border_vertexes_with_dependencies) {
    global_border_vertexes_.reset(global_border_vertexes);
    global_border_vertexes_with_dependencies_.reset(
        global_border_vertexes_with_dependencies);
    global_border_vertexes_vdata_.reset(
        new std::unordered_map<VID_T, VDATA_T>());
    mtx_ = new std::mutex;
  }

  ~BorderVertexes() = default;

  size_t get_num_border_vertexes() { return global_border_vertexes_->size(); }

  std::unordered_map<VID_T, VDATA_T>* GetBorderVertexVdata() {
    return global_border_vertexes_vdata_.get();
  }

  std::vector<VID_T>* GetBorderVertexOfX(GID_T gid) { ; }

  bool UpdateBorderVertexes(CSR_T& graph, bool* visited) {
    bool tag = false;
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      if (visited[i]) {
        VID_T global_id = graph.localid2globalid(i);
        tag ? 0 : tag = true;
        if (global_border_vertexes_->find(global_id) !=
            global_border_vertexes_->end()) {
          mtx_->lock();
          auto iter = global_border_vertexes_vdata_->find(global_id);
          if (iter != global_border_vertexes_vdata_->end()) {
            iter->second = *graph.GetVertexByVid(i).vdata;
          } else {
            global_border_vertexes_vdata_->insert(
                std::make_pair(global_id, *graph.GetVertexByVid(i).vdata));
          }
          mtx_->unlock();
        }
      }
    }
    return tag;
  }

  VID_T* GetBorderVertexesofX(GID_T gid) {}

 private:
};

}  // namespace message
}  // namespace minigraph
#endif  // MINIGRAPH_BORDER_VERTEXES_H
