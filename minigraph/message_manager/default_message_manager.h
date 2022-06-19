#ifndef MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
#define MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H

#include "graphs/graph.h"
#include "message_manager/message_manager_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/data_mngr.h"
#include <unordered_map>
#include <vector>

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
  // GID_T* globalid2gid_ = nullptr;
  bool* communication_matrix_ = nullptr;

  PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>* partial_match_ = nullptr;

  DefaultMessageManager(
      utility::io::DataMngr<GID_T, VID_T, VDATA_T, EDATA_T>* data_mngr)
      : MessageManagerBase() {
    data_mngr_ = data_mngr;
    partial_match_ = new PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>();
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

    auto out2 =
        data_mngr_->ReadGlobalid2Gid(work_space + "/message/globalid2gid.bin");
    maximum_vid_ = out2.first;
    //   globalid2gid_ = out2.second;
    globalid2gid_.reset(out2.second);

    for (size_t i = 0; i < num_graphs_; i++) {
      for (size_t j = 0; j < num_graphs_; j++) {
        std::cout << *(communication_matrix_ + i * num_graphs_ + j) << ", ";
      }
      std::cout << std::endl;
    }

    LOG_INFO(global_border_vertexes_->size());
  };

  void BufferPartialResult(std::vector<VID_T>& current_matching_solution,
                           CSR_T& graph, VID_T v_vid) {
    LOG_INFO("bufferPartialResult");
    auto partial_result = new std::vector<VertexInfo*>;
    LOG_INFO(global_border_vertexes_->size());
    for (auto& iter : current_matching_solution) {
      auto local_id = graph.globalid2localid(iter);
      if (local_id != VID_MAX) {
        auto v = graph.CopyVertexByVid(local_id);
        partial_result->push_back(v);
      }
    }
    LOG_INFO(v_vid, "Now: ", graph.gid_, ", from: ", Globalid2Gid(v_vid));
    GID_T&& gid = Globalid2Gid(v_vid);
    partial_match_->partial_result_->insert(
        std::make_pair(gid, partial_result));

  }

  void FlashMessageToSecondStorage() override{

  };

 private:
  GID_T Globalid2Gid(VID_T vid) {
    if (globalid2gid_ == nullptr)
      LOG_ERROR("segmentatino fault: globalid2gid_ is nullptr");
    return globalid2gid_.get()[vid];
  }
};

}  // namespace message
}  // namespace minigraph

#endif  // MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
