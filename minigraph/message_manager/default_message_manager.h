#ifndef MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
#define MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H

#include <unordered_map>
#include <vector>

#include "graphs/graph.h"
#include "message_manager/border_vertexes.h"
#include "message_manager/message_manager_base.h"
#include "message_manager/partial_match.h"
#include "portability/sys_data_structure.h"
#include "utility/io/data_mngr.h"

namespace minigraph {
namespace message {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class DefaultMessageManager : public MessageManagerBase {
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T = graphs::EdgeList<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  size_t num_graphs_ = 0;
  size_t maximum_vid_ = 0;
  utility::io::DataMngr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;

  std::unique_ptr<std::unordered_map<VID_T, std::vector<GID_T>*>>
      global_border_vertexes_ = nullptr;
  std::unique_ptr<std::unordered_map<VID_T, VertexInfo*>>
      global_border_vertexes_info_ = nullptr;
  std::unique_ptr<std::unordered_map<VID_T, VertexDependencies<VID_T, GID_T>*>>
      global_border_vertexes_with_dependencies_ = nullptr;
  std::unique_ptr<GID_T> globalid2gid_ = nullptr;
  bool* communication_matrix_ = nullptr;

  std::unordered_map<VID_T, VDATA_T>* global_border_vertex_vdata_;

  std::mutex* mtx_;

  PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>* partial_match_ = nullptr;
  BorderVertexes<GID_T, VID_T, VDATA_T, EDATA_T>* border_vertexes_ = nullptr;

  DefaultMessageManager(
      utility::io::DataMngr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      const std::string& work_space)
      : MessageManagerBase() {
    data_mngr_ = data_mngr;
    border_vertexes_ = new BorderVertexes<GID_T, VID_T, VDATA_T, EDATA_T>(
        data_mngr_->ReadBorderVertexes(work_space +
                                       "border_vertexes/global.bv"),
        data_mngr_->ReadGraphDependencies(
            work_space + "border_vertexes/graph_dependencies.bin"));
    mtx_ = new std::mutex;
    global_border_vertex_vdata_ = new std::unordered_map<VID_T, VDATA_T>;
    auto out2 =
        data_mngr_->ReadGlobalid2Gid(work_space + "/message/globalid2gid.bin");
    maximum_vid_ = out2.first;
    globalid2gid_.reset(out2.second);
    partial_match_ =
        new PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>(out2.second);
  }

  void Init(std::string work_space) override {
    LOG_INFO("Init Message Manager: ", work_space);
    global_border_vertexes_.reset(data_mngr_->ReadBorderVertexes(
        work_space + "border_vertexes/global.bv"));

    auto out1 = data_mngr_->ReadCommunicationMatrix(
        work_space + "border_vertexes/communication_matrix.bin");
    num_graphs_ = out1.first;
    communication_matrix_ = out1.second;

    global_border_vertexes_with_dependencies_.reset(
        data_mngr_->ReadGraphDependencies(
            work_space + "border_vertexes/graph_dependencies.bin"));

    for (size_t i = 0; i < num_graphs_; i++) {
      for (size_t j = 0; j < num_graphs_; j++) {
        std::cout << *(communication_matrix_ + i * num_graphs_ + j) << ", ";
      }
      std::cout << std::endl;
    }
  };

  void BufferPartialResults(
      std::vector<std::vector<VID_T>*>& partial_matching_solutions) {
    this->partial_match_->BufferPartialResults(partial_matching_solutions);
  }

  void BufferResults(std::vector<std::vector<VID_T>*>& matching_solutions) {
    this->partial_match_->BufferResults(matching_solutions);
  }

  bool UpdateBorderVertexes(CSR_T& graph, bool* visited) {
    return this->border_vertexes_->UpdateBorderVertexes(graph, visited);
  }

  std::vector<std::vector<VID_T>*>* GetPartialMatchingSolutionsofX(GID_T gid) {
    return this->partial_match_->GetPartialMatchingSolutionsofX(gid);
  }

  void FlashMessageToSecondStorage() override{};

};

}  // namespace message
}  // namespace minigraph

#endif  // MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
