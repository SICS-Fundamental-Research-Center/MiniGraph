#ifndef MINIGRAPH_DATA_MNGR_H
#define MINIGRAPH_DATA_MNGR_H

#include <memory>

#include <folly/AtomicHashMap.h>
#include "yaml-cpp/yaml.h"

#include "utility/io/csr_io_adapter.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/io/relation_io_adapter.h"

namespace minigraph {
namespace utility {
namespace io {

template <typename GRAPH_T>
class DataMngr {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T = graphs::EdgeList<GID_T, VID_T, VDATA_T, EDATA_T>;
  using RELATION_T = graphs::Relation<GID_T, VID_T, VDATA_T, EDATA_T>;
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  std::unique_ptr<utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_;
  std::unique_ptr<
      utility::io::EdgeListIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      edge_list_io_adapter_;
  std::unique_ptr<
      utility::io::RelationIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      relation_io_adapter_;

  DataMngr() {
    pgraph_by_gid_ =
        std::make_unique<folly::AtomicHashMap<GID_T, GRAPH_BASE_T*>>(1024);

    csr_io_adapter_ = std::make_unique<
        utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>();

    edge_list_io_adapter_ = std::make_unique<
        utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>>();
    pgraph_mtx_ = new std::mutex;
  };

  bool ReadGraph(const GID_T& gid, const Path& path,
                 const GraphFormat& graph_format, char separator_params = ',') {
    bool out = false;
    GRAPH_BASE_T* graph = nullptr;
    if (graph_format == csr_bin) {
      graph = new CSR_T;
      out = csr_io_adapter_->Read((GRAPH_BASE_T*)graph, csr_bin, gid,
                                  path.meta_pt, path.data_pt, path.vdata_pt);
    } else if (graph_format == edgelist_bin) {
      graph = new EDGE_LIST_T;
      out = edge_list_io_adapter_->Read((GRAPH_BASE_T*)graph, edgelist_bin,
                                        separator_params, gid, path.meta_pt,
                                        path.data_pt, path.vdata_pt);
    } else if (graph_format == relation_bin) {
      graph = new RELATION_T;
      out = relation_io_adapter_->Read((GRAPH_BASE_T*)graph, relation_bin, 1,
                                       separator_params, path.meta_pt,
                                       path.data_pt);
    }

    if (out) {
      pgraph_mtx_->lock();
      auto iter = pgraph_by_gid_->find(gid);
      if (iter != pgraph_by_gid_->end()) {
        if (iter->second == nullptr) iter->second = (GRAPH_BASE_T*)graph;
      } else
        pgraph_by_gid_->insert(std::make_pair(gid, (GRAPH_BASE_T*)graph));
      pgraph_mtx_->unlock();
    }
    return out;
  }

  bool WriteGraph(const GID_T& gid, const Path& path,
                  const GraphFormat& graph_format, bool vdata_only = false) {
    if (graph_format == csr_bin) {
      auto graph = this->GetGraph(gid);
      return csr_io_adapter_->Write(*((GRAPH_BASE_T*)graph), csr_bin,
                                    vdata_only, path.meta_pt, path.data_pt,
                                    path.vdata_pt);
    } else if (graph_format == edgelist_bin) {
      return false;
    } else if (graph_format == relation_bin) {
    }
    return false;
  }

  bool WriteCommunicationMatrix(const std::string& output_pt,
                                bool* communication_matrix,
                                const size_t num_graphs) {
    std::ofstream communication_matrix_file(output_pt, std::ios::binary);

    communication_matrix_file.write((char*)&num_graphs, sizeof(size_t));
    communication_matrix_file.write((char*)communication_matrix,
                                    sizeof(bool) * num_graphs * num_graphs);
    // LOG_INFO("Write communication matrix, num_ghraphs: ", num_graphs);
    // for (size_t i = 0; i < num_graphs; i++) {
    //   for (size_t j = 0; j < num_graphs; j++) {
    //     std::cout << *(communication_matrix + i * num_graphs + j) << ", ";
    //   }
    //   std::cout << std::endl;
    // }
    communication_matrix_file.close();
    return true;
  }

  bool WriteGlobalBorderVertexesbyGid(
      std::unordered_map<GID_T, std::vector<VID_T>*

                         >& global_border_vertexes_by_gid,
      const std::string& output_pt) {
    size_t num_graph = global_border_vertexes_by_gid.size();
    size_t* num_vertexes_for_each_graph =
        (size_t*)malloc(sizeof(size_t) * num_graph);
    size_t* offset = (size_t*)malloc(sizeof(size_t) * num_graph);
    size_t i = 0;
    size_t count = 0;

    for (auto& iter : global_border_vertexes_by_gid) {
      num_vertexes_for_each_graph[i] = iter.second->size();
      offset[i] = count;
      count += iter.second->size();
      i++;
    }

    auto buf_vid = (VID_T*)malloc(sizeof(VID_T) * count);

    i = 0;
    for (auto& iter : global_border_vertexes_by_gid) {
      for (auto& iter_vid : *iter.second) buf_vid[i++] = iter_vid;
    }

    std::ofstream output_file(output_pt, std::ios::binary);
    output_file.write((char*)&num_graph, sizeof(size_t));
    output_file.write((char*)num_vertexes_for_each_graph,
                      sizeof(size_t) * num_graph);
    output_file.write((char*)offset, sizeof(size_t) * num_graph);
    output_file.write((char*)buf_vid, sizeof(VID_T) * count);

    free(buf_vid);
    free(num_vertexes_for_each_graph);
    free(offset);
    output_file.close();
    return true;
  }

  std::pair<size_t, bool*> ReadCommunicationMatrix(
      const std::string& input_pt) {
    std::ifstream communication_matrix_file(input_pt, std::ios::binary);
    size_t num_graphs = 0;
    communication_matrix_file.read((char*)&num_graphs, sizeof(size_t));
    bool* communication_matrix =
        (bool*)malloc(sizeof(bool) * num_graphs * num_graphs);
    memset(communication_matrix, 0, sizeof(bool) * num_graphs * num_graphs);
    communication_matrix_file.read((char*)communication_matrix,
                                   sizeof(bool) * num_graphs * num_graphs);
    communication_matrix_file.close();
    return std::make_pair(num_graphs, communication_matrix);
  }

  bool WriteBitmap(Bitmap* bitmap, const std::string& output_pt) {
    std::ofstream output_file(output_pt, std::ios::binary | std::ios::app);
    size_t meta_buff[2];
    meta_buff[0] = bitmap->size_;
    meta_buff[1] = bitmap->get_data_size(bitmap->size_);
    output_file.write((char*)meta_buff, sizeof(size_t) * 2);
    output_file.write((char*)bitmap->data_, meta_buff[1]);
    output_file.close();
    return true;
  }

  std::pair<size_t, Bitmap*> ReadBitmap(const std::string& input_pt) {
    std::ifstream input_file(input_pt, std::ios::binary | std::ios::app);
    size_t meta_buff[2];
    input_file.read((char*)meta_buff, sizeof(size_t) * 2);
    unsigned long* data = (unsigned long*)malloc(meta_buff[1]);
    memset(data, 0, meta_buff[1]);
    input_file.read((char*)data, meta_buff[1]);
    input_file.close();
    return std::make_pair(meta_buff[0], new Bitmap(meta_buff[0], data));
  }

  std::unordered_map<VID_T, std::vector<GID_T>*>*

  ReadBorderVertexes(const std::string& border_vertexes_pt) {
    auto global_border_vertexes =
        new std::unordered_map<VID_T, std::vector<GID_T>*>();
    std::ifstream border_vertexes_file(border_vertexes_pt,
                                       std::ios::binary | std::ios::app);
    size_t global_border_vertexes_size;

    border_vertexes_file.read((char*)&global_border_vertexes_size,
                              sizeof(size_t));
    global_border_vertexes->reserve(global_border_vertexes_size);
    VID_T* buf_global_border_vertexes =
        (VID_T*)malloc(sizeof(VID_T*) * global_border_vertexes_size);
    border_vertexes_file.read((char*)buf_global_border_vertexes,
                              sizeof(VID_T) * global_border_vertexes_size);

    for (size_t i = 0; i < global_border_vertexes_size; i++) {
      global_border_vertexes->insert(
          std::make_pair(buf_global_border_vertexes[i], nullptr));
    }
    LOG_INFO("LOAD global_border_vertexes: ", global_border_vertexes->size());
    return global_border_vertexes;
  }

  std::pair<VID_T, GID_T*> ReadGlobalid2Gid(const std::string& input_pt) {
    std::ifstream input_file(input_pt, std::ios::binary | std::ios::app);
    VID_T maximum_vid = 0;
    input_file.read((char*)&maximum_vid, sizeof(VID_T));
    GID_T* buf_globalid2gid = (GID_T*)malloc(sizeof(GID_T) * maximum_vid);
    memset(buf_globalid2gid, 0, sizeof(GID_T) * maximum_vid);
    input_file.read((char*)buf_globalid2gid, sizeof(GID_T) * maximum_vid);
    input_file.close();
    return std::make_pair(maximum_vid, buf_globalid2gid);
  }

  bool WriteGlobalid2Gid(std::unordered_map<VID_T, GID_T>& globalid2gid,
                         const std::string& output_pt) {
    if (Exist(output_pt)) {
      remove(output_pt.c_str());
    }
    VID_T maximum_vid = 0;
    for (auto& iter : globalid2gid)
      iter.first > maximum_vid ? maximum_vid = iter.first : 0;
    maximum_vid++;
    GID_T* buf_globalid2gid = (GID_T*)malloc(sizeof(GID_T) * maximum_vid);
    memset(buf_globalid2gid, 0, (maximum_vid) * sizeof(GID_T));
    for (auto& iter : globalid2gid) buf_globalid2gid[iter.first] = iter.second;

    std::ofstream output_file(output_pt, std::ios::binary | std::ios::app);
    output_file.write((char*)&maximum_vid, sizeof(VID_T));
    output_file.write((char*)buf_globalid2gid, sizeof(GID_T) * maximum_vid);
    free(buf_globalid2gid);
    output_file.close();
    return true;
  }

  bool WriteVidMap(const size_t max_vid, VID_T* vid_map,
                   const std::string& output_pt) {
    if (Exist(output_pt)) {
      remove(output_pt.c_str());
    }
    std::ofstream output_file(output_pt, std::ios::binary | std::ios::app);
    output_file.write((char*)&max_vid, sizeof(size_t));
    output_file.write((char*)vid_map, sizeof(VID_T) * max_vid);
    return true;
  }

  std::pair<size_t, VID_T*> ReadVidMap(const std::string& input_pt) {
    std::ifstream vid_map_file(input_pt, std::ios::binary);
    size_t max_vid = 0;
    vid_map_file.read((char*)&max_vid, sizeof(size_t));
    VID_T* vid_map = (VID_T*)malloc(sizeof(VID_T) * max_vid);
    memset(vid_map, 0, sizeof(VID_T) * max_vid);
    vid_map_file.read((char*)vid_map, sizeof(VID_T) * max_vid);
    vid_map_file.close();
    return std::make_pair(max_vid, vid_map);
  }

  bool WriteStatisticInfo(StatisticInfo& si, const std::string& out_pt) {
    std::ofstream fout(out_pt);
    YAML::Node si_node;
    assert(si_node.IsNull());

    si_node["num_vertexes"] = si.num_vertexes;
    si_node["num_edges"] = si.num_edges;
    si_node["num_active_vertexes"] = si.num_active_vertexes;
    si_node["sum_out_border_vertexes"] = si.sum_out_border_vertexes;
    si_node["sum_dlv"] = si.sum_dlv;
    si_node["sum_dgv"] = si.sum_dgv;
    si_node["sum_dlv_times_dlv"] = si.sum_dlv_times_dlv;
    si_node["sum_dlv_times_dgv"] = si.sum_dlv_times_dgv;
    si_node["sum_dgv_times_dgv"] = si.sum_dgv_times_dgv;
    fout << si_node << std::endl;

    return true;
  }

  StatisticInfo ReadStatisticInfo(const std::string& in_pt) {
    YAML::Node si_node;
    try {
      si_node = YAML::LoadFile(in_pt);
    } catch (YAML::BadFile& e) {
      LOG_INFO("Read", in_pt, " error.");
      return -1;
    }

    StatisticInfo si;
    si.num_vertexes = si_node["num_vertexes"].as<size_t>();
    si.num_edges = si_node["num_edges"].as<size_t>();
    si.num_active_vertexes = si_node["num_active_vertexes"].as<size_t>();
    si.sum_out_border_vertexes =
        si_node["sum_out_border_vertexes"].as<size_t>();
    si.sum_dlv = si_node["sum_dlv"].as<size_t>();
    si.sum_dgv = si_node["sum_dgv"].as<size_t>();
    si.sum_dlv_times_dlv = si_node["sum_dlv_times_dlv"].as<size_t>();
    si.sum_dlv_times_dgv = si_node["sum_dlv_times_dgv"].as<size_t>();
    si.sum_dgv_times_dgv = si_node["sum_dgv_times_dgv"].as<size_t>();
    return si;
  }

  GRAPH_BASE_T* GetGraph(const GID_T& gid) {
    if (pgraph_by_gid_->count(gid)) {
      return pgraph_by_gid_->find(gid)->second;
    } else {
      return nullptr;
    }
  }

  void EraseGraph(const GID_T& gid) {
    LOG_INFO("Erase", gid);
    pgraph_mtx_->lock();
    if (pgraph_by_gid_->count(gid)) {
      auto iter = pgraph_by_gid_->find(gid);
      delete (GRAPH_T*)iter->second;
      iter->second = nullptr;
      pgraph_by_gid_->erase(gid);
    }
    pgraph_mtx_->unlock();
  };

  void CleanUp() {
    for (auto& iter : *pgraph_by_gid_) {
      iter.second->CleanUp();
      pgraph_by_gid_->erase(iter.first);
    }
  }

  void MakeDirectory(const std::string& pt) {
    std::string dir = pt;
    int len = dir.size();
    if (dir[len - 1] != '/') {
      dir[len] = '/';
      len++;
    }
    std::string temp;
    for (int i = 1; i < len; i++) {
      if (dir[i] == '/') {
        temp = dir.substr(0, i);
        if (access(temp.c_str(), 0) != 0)
          if (mkdir(temp.c_str(), 0777) != 0) VLOG(1) << "failed operaiton.";
      }
    }
  }

  bool Exist(const std::string& pt) const {
    struct stat buffer;
    return (stat(pt.c_str(), &buffer) == 0);
  }

  void InitWorkList(const std::string& work_space) {
    std::string meta_root = work_space + "minigraph_meta/";
    std::string data_root = work_space + "minigraph_data/";
    std::string vdata_root = work_space + "minigraph_vdata/";
    if (!Exist(meta_root)) MakeDirectory(meta_root);
    if (!Exist(data_root)) MakeDirectory(data_root);
    if (!Exist(vdata_root)) MakeDirectory(vdata_root);
  }

  std::unordered_map<GID_T, Path> InitPtByGid(const std::string& work_space) {
    std::string meta_root = work_space + "minigraph_meta/";
    std::string data_root = work_space + "minigraph_data/";
    std::string vdata_root = work_space + "minigraph_vdata/";
    std::unordered_map<GID_T, Path> pt_by_gid;

    std::vector<std::string> files;
    for (const auto& entry : std::filesystem::directory_iterator(meta_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/minigraph_meta/");
      size_t pos2 = path.find(".bin");
      int type_length = std::string("/minigraph_meta/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid.find(gid);
      if (iter == pt_by_gid.end()) {
        Path _path;
        _path.meta_pt = path;
        pt_by_gid.insert(std::make_pair(gid, _path));
      } else {
        iter->second.meta_pt = path;
      }
    }

    for (const auto& entry : std::filesystem::directory_iterator(data_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/minigraph_data/");
      size_t pos2 = path.find(".bin");
      int type_length = std::string("/minigraph_data/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid.find(gid);
      if (iter == pt_by_gid.end()) {
        Path _path;
        _path.data_pt = path;
        pt_by_gid.insert(std::make_pair(gid, _path));
      } else {
        iter->second.data_pt = path;
      }
    }
    for (const auto& entry : std::filesystem::directory_iterator(vdata_root)) {
      std::string path = entry.path();
      size_t pos = path.find("/minigraph_vdata/");
      size_t pos2 = path.find(".bin");
      int type_length = std::string("/minigraph_vdata/").length();
      std::string gid_str =
          path.substr(pos + type_length, pos2 - pos - type_length);
      GID_T gid = (GID_T)std::stoi(gid_str);
      auto iter = pt_by_gid.find(gid);
      if (iter == pt_by_gid.end()) {
        Path _path;
        _path.vdata_pt = path;
        pt_by_gid.insert(std::make_pair(gid, _path));
      } else {
        iter->second.vdata_pt = path;
      }
    }
    for (auto iter = pt_by_gid.begin(); iter != pt_by_gid.end(); iter++) {
      LOG_INFO(iter->first, " ");
    }
    return pt_by_gid;
  }

 private:
  std::unique_ptr<folly::AtomicHashMap<GID_T, GRAPH_BASE_T*>> pgraph_by_gid_ =
      nullptr;
  std::mutex* pgraph_mtx_ = nullptr;
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph
#endif  // MINIGRAPH_DATA_MNGR_H
