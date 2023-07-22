#ifndef MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
#define MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H

#include "graphs/graph.h"
#include "message_manager/message_manager_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/data_mngr.h"
#include <fstream>
#include <unordered_map>
#include <vector>

namespace minigraph {
namespace message {

template <typename GRAPH_T>
class DefaultMessageManager : public MessageManagerBase {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  DefaultMessageManager(utility::io::DataMngr<GRAPH_T>* data_mngr,
                        const std::string& work_space, bool is_mining = false)
      : MessageManagerBase() {}

  void Init(const std::string work_space,
            const bool load_dependencies = false) override {
    LOG_INFO("Init Message Manager: ", work_space);

    // Init Communication Matrix.
    auto out1 = data_mngr_->ReadCommunicationMatrix(
        work_space + "minigraph_border_vertexes/communication_matrix.bin");
    num_graphs_ = out1.first;
    communication_matrix_ = out1.second;

    // Init vid_map that map global vid to local vid.
    auto out2 =
        data_mngr_->ReadVidMap(work_space + "minigraph_message/vid_map.bin");
    if (out2.first == 0)
      vid_map_ = nullptr;
    else {
      vid_map_ = out2.second;
    }

    // Init global_border_vdata & border_vdata. The first one store v label of
    // all vertexes, while the second one can indcate which vertex is from
    // border.
    auto out3 = data_mngr_->ReadBitmap(
        work_space + "minigraph_message/global_border_vid_map.bin");
    max_vid_ = out3.first;
    global_border_vid_map_ = out3.second;
    aligned_max_vid_ =
        ceil((float)max_vid_ / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    global_border_vdata_ = (VDATA_T*)malloc(aligned_max_vid_ * sizeof(VDATA_T));

    for (VID_T vid = 0; vid < aligned_max_vid_; vid++)
      global_border_vdata_[vid] = VDATA_MAX;

    // Init StatisticInfo
    si_ = new StatisticInfo[num_graphs_];
    for (size_t i = 0; i < num_graphs_; i++) {
      std::string si_pt =
          work_space + "minigraph_si/" + std::to_string(i) + ".yaml";
      si_[i] = data_mngr_->ReadStatisticInfo(si_pt);
      si_[i].ShowInfo();
    }

    // Init others.
    historical_state_matrix_ = (char*)malloc(sizeof(char) * num_graphs_);
    memset(historical_state_matrix_, 0, sizeof(char) * num_graphs_);
    for (size_t i = 0; i < num_graphs_; i++) {
      *(historical_state_matrix_ + i) = IDLE;
    }

    active_vertexes_bit_map_ = new Bitmap(max_vid_);
    active_vertexes_bit_map_->clear();
    global_vertexes_state_ = (char*)malloc(sizeof(char) * max_vid_);
    memset(global_vertexes_state_, VERTEXUNLABELED, sizeof(char) * max_vid_);

    // init Message bucket
  };

  void ClearnUp() { active_vertexes_bit_map_->clear(); }

  bool* GetCommunicationMatrix() { return communication_matrix_; }

  Bitmap* GetGlobalBorderVidMap() { return global_border_vid_map_; }

  VDATA_T* GetGlobalVdata() { return global_border_vdata_; }

  Bitmap* GetGlobalActiveVidMap() { return active_vertexes_bit_map_; }

  char* GetGlobalState() { return global_vertexes_state_; }

  VID_T* GetVidMap() { return vid_map_; }

  VID_T globalid2localid(const VID_T globalid) { return vid_map_[globalid]; };

  void SetStateMatrix(const size_t gid, char state) {
    if (gid >= num_graphs_) {
      LOG_INFO(gid, "/ ", num_graphs_);
    }
    // assert(gid < num_graphs_);
    *(historical_state_matrix_ + gid) = state;
    return;
  }

  char GetStateMatrix(size_t gid) { return *(historical_state_matrix_ + gid); }

  StatisticInfo GetStatisticInfo(const GID_T gid) { return si_[gid]; }

  StatisticInfo* GetStatisticInfo() { return si_; }

  size_t get_max_vid() { return max_vid_; }

  bool WriteStatisticInfo(const std::string pt) {
    this->MakeDirectory(pt);
    for (size_t i = 0; i < num_graphs_; i++) {
      std::string out_pt = pt + std::to_string(i) + ".yaml";
      std::ofstream fout(out_pt);
    }
  }

  bool CheckDependenes(const GID_T x, const GID_T y) {
    return *(communication_matrix_ + x * num_graphs_ + y) == 1;
  }

 private:
  size_t num_graphs_ = 0;
  utility::io::DataMngr<GRAPH_T>* data_mngr_ = nullptr;
  VID_T* vid_map_ = nullptr;
  VID_T max_vid_ = 0;
  VID_T aligned_max_vid_ = 0;
  Bitmap* global_border_vid_map_ = nullptr;
  Bitmap* active_vertexes_bit_map_ = nullptr;
  VDATA_T* global_border_vdata_ = nullptr;
  char* global_vertexes_state_ = nullptr;
  bool* communication_matrix_ = nullptr;
  char* historical_state_matrix_ = nullptr;
  StatisticInfo* si_ = nullptr;
  std::atomic<size_t> offset_bucket = 0;
};

}  // namespace message
}  // namespace minigraph

#endif  // MINIGRAPH_DEFAULT_MESSAGE_MANAGER_H
