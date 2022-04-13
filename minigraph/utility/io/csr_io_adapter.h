//
// Created by hsiaoko on 2022/3/14.
//
#ifndef MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H

#include "graphs/immutable_csr.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "rapidcsv.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

using std::cout;
using std::endl;

namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class CSRIOAdapter : public IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T> {
 public:
  CSRIOAdapter(const std::string& pt)
      : IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T>(pt){};
  CSRIOAdapter(){};
  ~CSRIOAdapter(){};

  template <class... Args>
  bool Read(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
            const GraphFormat& graph_format, const GID_T& gid, Args&&... args) {
    std::string pt[] = {(args)...};
    switch (graph_format) {
      case edge_graph_csv:
        return this->ReadCSV2ImmutableCSR(graph, pt[0]);
      case csr_bin:
        return this->ReadBIN2ImmutableCSR(graph, gid, pt[0], pt[1], pt[2],
                                          pt[3], pt[4]);
      case weight_edge_graph_csv:
        // not supported now.
        return false;
    }
    return false;
  }

  template <class... Args>
  bool Write(const graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>& graph,
             const GraphFormat& graph_format, Args&&... args) {
    std::string pt[] = {(args)...};

    switch (graph_format) {
      case edge_graph_csv:
        return false;
      case csr_bin:
        return this->WriteCSR2Bin(
            (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>&)graph, pt[0],
            pt[1], pt[2], pt[3], pt[4]);
      case weight_edge_graph_csv:
        // not supported now.
        // TO DO: load graph in weight edge csv format.
        return false;
    }
    return true;
  }

  bool IsExist(const std::string& pt) const override {
    struct stat buffer;
    return (stat(pt.c_str(), &buffer) == 0);
  }

  void MakeDirectory(const std::string& pt) override {
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
        if (access(temp.c_str(), 0) != 0) {
          if (mkdir(temp.c_str(), 0777) != 0) {
            VLOG(1) << "failed operaiton.";
          }
        }
      }
    }
  }

  void Touch(const std::string& pt) override {
    std::ofstream file(pt, std::ios::binary);
    file.close();
  };

  bool WriteGlobalBorderVertexes(
      const folly::AtomicHashMap<VID_T, std::vector<GID_T>*>&
          global_border_vertexes,
      const std::string& border_vertexes_pt) {
    if (IsExist(border_vertexes_pt)) {
      remove(border_vertexes_pt.c_str());
    }
    std::ofstream border_vertexes_file(border_vertexes_pt,
                                       std::ios::binary | std::ios::app);
    VID_T* buf_border_vertexes =
        (VID_T*)malloc(sizeof(VID_T) * global_border_vertexes.size());
    size_t* buf_offset =
        (size_t*)malloc(sizeof(size_t) * global_border_vertexes.size());
    size_t buf_size[global_border_vertexes.size()];
    size_t sum_gid = 0;
    size_t i = 0;
    for (auto iter = global_border_vertexes.begin();
         iter != global_border_vertexes.end(); iter++) {
      sum_gid += iter->second->size();
      buf_border_vertexes[i] = iter->first;
      buf_size[i] = iter->second->size();
      i++;
    }

    GID_T* buf_gid = (GID_T*)malloc(sizeof(GID_T) * sum_gid);
    i = 0;
    for (auto iter = global_border_vertexes.begin();
         iter != global_border_vertexes.end(); iter++) {
      LOG_INFO(iter->first);
      if (i == 0) {
        buf_offset[i] = 0;
      } else {
        buf_offset[i] = buf_offset[i - 1] + buf_size[i];
      }
      size_t j = 0;
      for (auto& iter_vec_gid : *iter->second) {
        LOG_INFO(iter_vec_gid);
        *(buf_gid + buf_offset[i] + j++) = iter_vec_gid;
      }
      i++;
    }
    for (int i = 0; i < global_border_vertexes.size(); i++) {
      std::cout << buf_border_vertexes[i] << ", ";
    }
    cout << endl;
    for (int i = 0; i < global_border_vertexes.size(); i++) {
      std::cout << buf_offset[i] << ", ";
    }
    cout << endl;
    for (int i = 0; i < sum_gid; i++) {
      std::cout << buf_gid[i] << ", ";
    }
    cout << endl;

    size_t buf_config[2];
    buf_config[0] = global_border_vertexes.size();
    buf_config[1] = sum_gid;
    border_vertexes_file.write((char(*)) & buf_config, sizeof(size_t) * 2);
    border_vertexes_file.write((char(*))buf_border_vertexes,
                               sizeof(VID_T) * buf_config[0]);
    border_vertexes_file.write((char(*))buf_offset,
                               sizeof(size_t) * buf_config[0]);
    border_vertexes_file.write((char(*))buf_gid, sizeof(GID_T) * buf_config[1]);
    free(buf_border_vertexes);
    free(buf_offset);
    free(buf_gid);
    return true;
  }

  bool ReadGlobalBorderVertexes(
      folly::AtomicHashMap<VID_T, std::pair<size_t, GID_T*>>*
          global_border_vertexes,
      const std::string& border_vertexes_pt) {
    if (global_border_vertexes == nullptr) {
      LOG_INFO("segmentation fault: ", "global_border_vertexes is nullptr");
      return false;
    }
    std::ifstream border_vertexes_file(border_vertexes_pt,
                                       std::ios::binary | std::ios::app);
    size_t* buf_config = (size_t*)malloc(sizeof(size_t) * 2);
    border_vertexes_file.read((char*)buf_config, sizeof(size_t) * 2);

    VID_T* buf_border_vertexes = (VID_T*)malloc(sizeof(VID_T) * buf_config[0]);
    size_t* buf_offset = (size_t*)malloc(sizeof(size_t) * buf_config[0]);
    GID_T* buf_gid = (GID_T*)malloc(sizeof(GID_T) * buf_config[1]);
    border_vertexes_file.read((char*)buf_border_vertexes,
                              sizeof(VID_T) * buf_config[0]);
    border_vertexes_file.read((char*)buf_offset,
                              sizeof(size_t) * buf_config[0]);
    border_vertexes_file.read((char*)buf_gid, sizeof(GID_T) * buf_config[1]);
    for (size_t i = 0; i < buf_config[0]; ++i) {
      if (i < buf_config[0] - 1) {
        size_t num_gid = buf_offset[i + 1] - buf_offset[i];
        global_border_vertexes->insert(
            buf_border_vertexes[i],
            std::make_pair(num_gid, (buf_gid + buf_offset[i])));
      } else {
        size_t num_gid = buf_config[1] - buf_offset[i];
        global_border_vertexes->insert(
            buf_border_vertexes[i],
            std::make_pair(num_gid, (buf_gid + buf_offset[i])));
      }
    }
    return true;
  }

 private:
  bool ReadCSV2ImmutableCSR(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
      const std::string& pt) {
    if (!IsExist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "Input fault: nullptr");
      return false;
    }
    graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>* immutable_csr =
        (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;

    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(','));

    // generate related edges for each vertex.
    folly::AtomicHashMap<VID_T, std::vector<VID_T>*> graph_in_edges(1024);
    folly::AtomicHashMap<VID_T, std::vector<VID_T>*> graph_out_edges(1024);
    std::vector<VID_T> src = doc.GetColumn<VID_T>("src");
    std::vector<VID_T> dst = doc.GetColumn<VID_T>("dst");
    for (size_t i = 0; i < src.size(); i++) {
      auto iter = graph_out_edges.find(src.at(i));
      if (iter != graph_out_edges.end()) {
        iter->second->push_back(dst.at(i));
      } else {
        std::vector<VID_T>* out_edges = new std::vector<VID_T>;
        out_edges->push_back(dst.at(i));
        graph_out_edges.insert(std::make_pair(src.at(i), out_edges));
      }
      iter = graph_in_edges.find(dst.at(i));
      if (iter != graph_in_edges.end()) {
        iter->second->push_back(src.at(i));
      } else {
        std::vector<VID_T>* in_edges = new std::vector<VID_T>;
        in_edges->push_back(src.at(i));
        graph_in_edges.insert(std::make_pair(dst.at(i), in_edges));
      }
    }
    immutable_csr->sum_in_edges_ = graph_in_edges.size();
    immutable_csr->sum_out_edges_ = graph_out_edges.size();

    for (auto& iter : graph_in_edges) {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* vertex_info =
          new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
      vertex_info->vid = iter.first;
      vertex_info->indegree = iter.second->size();
      vertex_info->in_edges =
          (VID_T*)malloc(sizeof(VID_T) * vertex_info->indegree);
      for (size_t i = 0; i < iter.second->size(); i++) {
        ((VID_T*)vertex_info->in_edges)[i] = iter.second->at(i);
      }
      immutable_csr->vertexes_info_->insert(
          std::make_pair(iter.first, vertex_info));
    }
    for (auto& iter : graph_out_edges) {
      auto iter_vertexes_info = immutable_csr->vertexes_info_->find(iter.first);
      if (iter_vertexes_info != immutable_csr->vertexes_info_->end()) {
        iter_vertexes_info->second->outdegree = iter.second->size();
        iter_vertexes_info->second->out_edges =
            (VID_T*)malloc(sizeof(VID_T) * iter.second->size());
        for (size_t i = 0; i < iter.second->size(); i++) {
          iter_vertexes_info->second->out_edges[i] = iter.second->at(i);
        }
      } else {
        graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* vertex_info =
            new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
        vertex_info->vid = iter.first;
        vertex_info->outdegree = iter.second->size();
        vertex_info->out_edges =
            (VID_T*)malloc(sizeof(VID_T) * iter.second->size());
        for (size_t i = 0; i < iter.second->size(); i++) {
          ((VID_T*)vertex_info->out_edges)[i] = iter.second->at(i);
        }
        immutable_csr->unorder_map_vertexes_info_->emplace(iter.first,
                                                           vertex_info);
        LOG_INFO(iter.first);
        // immutable_csr->vertexes_info_->insert(
        //     std::make_pair(iter.first, vertex_info));
      }
    }
    immutable_csr->num_vertexes_ = immutable_csr->vertexes_info_->size();
    // immutable_csr->num_vertexes_ =
    //     immutable_csr->unorder_map_vertexes_info_->size();
    immutable_csr->vdata_ =
        (VDATA_T*)malloc(sizeof(VDATA_T) * immutable_csr->num_vertexes_);

    return true;
  }

  bool ReadBIN2ImmutableCSR(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph, const GID_T& gid,
      const std::string& vertex_pt, const std::string& meta_in_pt,
      const std::string& meta_out_pt, const std::string& vdata_pt,
      const std::string& localid2globalid_pt) {
    if (!IsExist(vertex_pt)) {
      XLOG(ERR, "Read file fault: vertex_pt, ", vertex_pt, ", not exist");
      return false;
    }
    if (!IsExist(meta_in_pt)) {
      XLOG(ERR, "Read file fault: vertex_pt, ", meta_in_pt, ", not exist");
      return false;
    }
    if (!IsExist(meta_out_pt)) {
      XLOG(ERR, "Read file fault: vertex_pt, ", meta_out_pt, ", not exist");
      return false;
    }
    if (!IsExist(localid2globalid_pt)) {
      XLOG(ERR, "Read file fault: vertex_pt, ", localid2globalid_pt,
           ", not exist");
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR,
           "Input fault: graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph "
           "is nullptr");
      return false;
    }

    XLOG(INFO, "Read Graph: ", gid);
    auto immutable_csr =
        (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
    immutable_csr->CleanUp();

    std::ifstream vertex_file(vertex_pt, std::ios::binary | std::ios::app);
    std::ifstream meta_in_file(meta_in_pt, std::ios::binary | std::ios::app);
    std::ifstream meta_out_file(meta_out_pt, std::ios::binary | std::ios::app);
    std::ifstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);
    std::ifstream localid2globalid_file(localid2globalid_pt,
                                        std::ios::binary | std::ios::app);

    // read vertexes_.
    {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      buf = (size_t*)malloc(sizeof(size_t));
      vertex_file.read((char*)buf, sizeof(size_t));
      immutable_csr->num_vertexes_ = buf[0];
      immutable_csr->vertex_ =
          (VID_T*)malloc(sizeof(VID_T) * immutable_csr->num_vertexes_);
      vertex_file.read((char*)immutable_csr->vertex_, sizeof(VID_T) * buf[0]);
      free(buf);
    }

    // read in_offset and in edges;
    {
      size_t* buf = (size_t*)malloc(sizeof(size_t) * 2);
      meta_in_file.read((char*)buf, sizeof(size_t) * 2);
      if (buf[0] != immutable_csr->num_vertexes_) {
        XLOG(ERR, "files don't match");
        return false;
      }
      immutable_csr->sum_in_edges_ = buf[1];
      immutable_csr->in_offset_ = (size_t*)malloc(sizeof(size_t) * buf[0]);
      meta_in_file.read((char*)immutable_csr->in_offset_,
                        sizeof(size_t) * buf[0]);
      immutable_csr->in_edges_ = (VID_T*)malloc(sizeof(VID_T) * buf[1]);
      meta_in_file.read((char*)immutable_csr->in_edges_,
                        sizeof(VID_T) * buf[1]);
      free(buf);
    }

    // read out_offset and in edges;
    {
      size_t* buf = (size_t*)malloc(sizeof(size_t) * 2);
      meta_out_file.read((char*)buf, sizeof(size_t) * 2);
      if (buf[0] != immutable_csr->num_vertexes_) {
        XLOG(ERR, "files don't match");
        return false;
      }
      immutable_csr->sum_out_edges_ = buf[1];
      immutable_csr->out_offset_ = (size_t*)malloc(sizeof(size_t) * buf[0]);
      meta_out_file.read((char*)immutable_csr->out_offset_,
                         sizeof(size_t) * buf[0]);
      immutable_csr->out_edges_ = (VID_T*)malloc(sizeof(VID_T) * buf[1]);
      meta_out_file.read((char*)immutable_csr->out_edges_,
                         sizeof(VID_T) * buf[1]);
      free(buf);
    }

    // read vdata.
    {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      vdata_file.read((char*)buf, sizeof(size_t));
      immutable_csr->vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * buf[0]);
      vdata_file.read((char*)immutable_csr->vdata_, sizeof(VDATA_T) * buf[0]);
      free(buf);
    }

    // load map from local id to global id.
    {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      localid2globalid_file.read((char*)buf, sizeof(size_t));
      immutable_csr->buf_localid2globalid_ =
          (VID_T*)malloc(sizeof(VID_T) * buf[0] * 2);
      localid2globalid_file.read((char*)immutable_csr->buf_localid2globalid_,
                                 sizeof(VID_T) * buf[0] * 2);
      for (size_t i = 0; i < buf[0]; ++i) {
        immutable_csr->map_localid2globalid_->insert(
            immutable_csr->buf_localid2globalid_[i],
            immutable_csr->buf_localid2globalid_[i + buf[0]]);
        immutable_csr->map_globalid2localid_->insert(
            immutable_csr->buf_localid2globalid_[i + buf[0]],
            immutable_csr->buf_localid2globalid_[i]);
      }
      free(buf);
    }

    immutable_csr->is_serialized_ = true;
    immutable_csr->gid_ = gid;

    meta_in_file.close();
    meta_out_file.close();
    localid2globalid_file.close();
    vdata_file.close();
    return true;
  }

  bool WriteCSR2Bin(
      const graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>& graph,
      const std::string& vertex_pt, const std::string& meta_in_pt,
      const std::string& meta_out_pt, const std::string& vdata_pt,
      const std::string& localid2globalid_pt) {
    if (graph.is_serialized_ == false) {
      XLOG(ERR, "Graph has not been serialized.");
      return false;
    }
    if (IsExist(vertex_pt)) {
      remove(vertex_pt.c_str());
    }
    if (IsExist(meta_in_pt)) {
      remove(meta_in_pt.c_str());
    }
    if (IsExist(meta_out_pt)) {
      remove(meta_out_pt.c_str());
    }
    if (IsExist(vdata_pt)) {
      remove(vdata_pt.c_str());
    }
    if (IsExist(localid2globalid_pt)) {
      remove(localid2globalid_pt.c_str());
    }

    std::ofstream vertex_file(vertex_pt, std::ios::binary | std::ios::app);
    std::ofstream meta_in_file(meta_in_pt, std::ios::binary | std::ios::app);
    std::ofstream meta_out_file(meta_out_pt, std::ios::binary | std::ios::app);
    std::ofstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);
    std::ofstream localid2globalid_file(localid2globalid_pt,
                                        std::ios::binary | std::ios::app);

    // write vertexes.
    if (graph.vertex_ != nullptr) {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      buf[0] = graph.num_vertexes_;
      vertex_file.write((char*)buf, sizeof(size_t));
      vertex_file.write((char*)graph.vertex_,
                        graph.num_vertexes_ * sizeof(VID_T));
      free(buf);
    }

    // write in edges.
    if (graph.in_offset_ != nullptr && graph.in_edges_ != nullptr) {
      size_t* buf = (size_t*)malloc(sizeof(size_t) * 2);
      buf[0] = graph.num_vertexes_;
      buf[1] = graph.sum_in_edges_;
      meta_in_file.write((char*)buf, sizeof(size_t) * 2);
      meta_in_file.write((char*)graph.in_offset_, sizeof(size_t) * buf[0]);
      meta_in_file.write((char*)graph.in_edges_, sizeof(VID_T) * buf[1]);
      free(buf);
    }

    // write out edges.

    if (graph.in_offset_ != nullptr && graph.in_edges_ != nullptr) {
      size_t* buf = (size_t*)malloc(sizeof(size_t) * 2);
      buf[0] = graph.num_vertexes_;
      buf[1] = graph.sum_out_edges_;
      meta_out_file.write((char*)buf, sizeof(size_t) * 2);
      meta_out_file.write((char*)graph.out_offset_, sizeof(size_t) * buf[0]);
      meta_out_file.write((char*)graph.out_edges_, sizeof(VID_T) * buf[1]);
      free(buf);
    }

    // write label of vertexes
    if (graph.vdata_ != nullptr) {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      buf[0] = graph.num_vertexes_;
      vdata_file.write((char*)buf, sizeof(size_t));
      vdata_file.write((char*)graph.vdata_, sizeof(VDATA_T) * buf[0]);
      free(buf);
    } else {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      buf[0] = graph.num_vertexes_;
      auto buf_vdata = (VID_T*)malloc(sizeof(VDATA_T) * buf[0]);
      memset(buf_vdata, 0, sizeof(VDATA_T) * buf[0]);
      vdata_file.write((char*)buf, sizeof(size_t));
      vdata_file.write((char*)buf_vdata, sizeof(VDATA_T) * buf[0]);
      free(buf);
    }

    // write buf localid 2 globalid (only for subgraph)
    if (graph.buf_localid2globalid_ != nullptr) {
      size_t* buf = (size_t*)malloc(sizeof(size_t));
      buf[0] = graph.num_vertexes_;
      localid2globalid_file.write((char*)buf, sizeof(size_t));
      localid2globalid_file.write((char*)graph.buf_localid2globalid_,
                                  buf[0] * 2 * sizeof(VID_T));
      free(buf);
    }

    vertex_file.close();
    meta_in_file.close();
    meta_out_file.close();
    localid2globalid_file.close();
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
