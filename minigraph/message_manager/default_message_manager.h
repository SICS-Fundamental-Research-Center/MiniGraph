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

 public:
  PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>* partial_match_ = nullptr;
  BorderVertexes<GID_T, VID_T, VDATA_T, EDATA_T>* border_vertexes_ = nullptr;

  DefaultMessageManager(
      utility::io::DataMngr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr,
      const std::string& work_space)
      : MessageManagerBase() {
    data_mngr_ = data_mngr;
    border_vertexes_ = new BorderVertexes<GID_T, VID_T, VDATA_T, EDATA_T>(
        data_mngr_->ReadBorderVertexes(work_space +
                                       "border_vertexes/global.bv"));
    auto out2 =
        data_mngr_->ReadGlobalid2Gid(work_space + "/message/globalid2gid.bin");
    size_t maximum_vid = out2.first;
    partial_match_ =
        new PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>(out2.second);
  }

  void Init(const std::string work_space,
            const bool load_dependencies = false) override {
    LOG_INFO("Init Message Manager: ", work_space);

    auto out1 = data_mngr_->ReadCommunicationMatrix(
        work_space + "border_vertexes/communication_matrix.bin");
    num_graphs_ = out1.first;
    communication_matrix_ = out1.second;

    for (size_t i = 0; i < num_graphs_; i++) {
      for (size_t j = 0; j < num_graphs_; j++)
        std::cout << *(communication_matrix_ + i * num_graphs_ + j) << ", ";
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

  PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>* GetPartialMatch() {
    return partial_match_;
  }

  BorderVertexes<GID_T, VID_T, VDATA_T, EDATA_T>* GetBorderVertexes() {
    return border_vertexes_;
  }

  bool* GetCommunicationMatrix() { return communication_matrix_; }

 private:
  size_t num_graphs_ = 0;
  utility::io::DataMngr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr_ = nullptr;
  bool* communication_matrix_ = nullptr;
};

}  // namespace message
}  // namespace minigraph

#endif  // MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
