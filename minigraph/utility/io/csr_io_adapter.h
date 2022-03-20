//
// Created by hsiaoko on 2022/3/14.
//
#ifndef MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H

#include "graphs/immutable_csr.h"
#include "io_adapter_base.h"
#include "portability/sys_types.h"
#include "rapidcsv.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

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
            const mode_t& mode = 1, Args&&... args) {
    std::string pt[] = {(args)...};
    if (mode == 1) {
      return this->ReadCSV2ImmutableCSR(graph, pt[0]);
    } else {
      return this->ReadBIN2ImmutableCSR(graph, pt[0], pt[1], pt[2], "a", pt[3]);
    }
  }

  template <class... Args>
  bool Write(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>& graph,
             Args&&... args) {
    std::string pt[] = {(args)...};
    return this->WriteCSR2Bin(
        (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>&)graph, pt[0],
        pt[1], pt[2], pt[3], pt[4]);
  }

  bool IsExist(const std::string& pt) const override {
    struct stat buffer;
    return (stat(pt.c_str(), &buffer) == 0);
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
    for (int i = 0; i < src.size(); i++) {
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
      for (int i = 0; i < iter.second->size(); i++) {
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
        for (int i = 0; i < iter.second->size(); i++) {
          iter_vertexes_info->second->out_edges[i] = iter.second->at(i);
        }
      } else {
        graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* vertex_info =
            new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
        vertex_info->vid = iter.first;
        vertex_info->outdegree = iter.second->size();
        vertex_info->out_edges =
            (VID_T*)malloc(sizeof(VID_T) * iter.second->size());
        for (int i = 0; i < iter.second->size(); i++) {
          ((VID_T*)vertex_info->out_edges)[i] = iter.second->at(i);
        }
        immutable_csr->vertexes_info_->insert(
            std::make_pair(iter.first, vertex_info));
      }
    }
    immutable_csr->num_vertexes_ = immutable_csr->vertexes_info_->size();
    return true;
  }

  bool ReadBIN2ImmutableCSR(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph,
      const std::string& vertex_pt, const std::string& meta_in_pt,
      const std::string& meta_out_pt, const std::string& vdata_pt,
      const std::string& localid2globalid_pt) {
    if (!IsExist(vertex_pt) || !IsExist(meta_in_pt) || !IsExist(meta_out_pt) ||
        !IsExist(localid2globalid_pt)) {
      XLOG(ERR, "Read file fault: a path does not exist");
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR,
           "Input fault: graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>* graph "
           "is nullptr");
      return false;
    }

    auto immutable_csr =
        (graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>*)graph;
    immutable_csr->CleanUp();

    folly::File vertex_file(vertex_pt, O_RDONLY);
    folly::File meta_in_file(meta_in_pt, O_RDONLY);
    folly::File meta_out_file(meta_out_pt, O_RDONLY);
    folly::File localid2globalid_file(localid2globalid_pt, O_RDONLY);

    // read vertexes_.
    auto vertex_fd = vertex_file.fd();
    size_t* buf = (size_t*)malloc(sizeof(size_t) * 2);
    buf = (size_t*)malloc(sizeof(size_t));
    folly::readNoInt(vertex_fd, buf, sizeof(size_t));
    immutable_csr->num_vertexes_ = buf[0];
    immutable_csr->vertex_ =
        (VID_T*)malloc(sizeof(VID_T) * immutable_csr->num_vertexes_);
    folly::readNoInt(vertex_fd, immutable_csr->vertex_,
                     sizeof(VID_T) * immutable_csr->num_vertexes_);

    // read in_offset and in edges;
    auto meta_in_fd = meta_in_file.fd();
    folly::readNoInt(meta_in_fd, buf, sizeof(size_t) * 2);
    if (buf[0] != immutable_csr->num_vertexes_) {
      XLOG(ERR, "files don't match");
      return false;
    }
    immutable_csr->sum_in_edges_ = buf[1];
    immutable_csr->in_offset_ = (size_t*)malloc(sizeof(size_t) * buf[0]);
    folly::readNoInt(meta_in_fd, immutable_csr->in_offset_,
                     sizeof(size_t) * buf[0]);
    immutable_csr->in_edges_ = (VID_T*)malloc(sizeof(VID_T) * buf[1]);
    folly::readNoInt(meta_in_fd, immutable_csr->in_edges_,
                     sizeof(VID_T) * buf[1]);

    // read out_offset and out edges;
    auto meta_out_fd = meta_out_file.fd();
    folly::readNoInt(meta_out_fd, buf, sizeof(size_t) * 2);
    if (buf[0] != immutable_csr->num_vertexes_) {
      XLOG(ERR, "files don't match");
      return false;
    }
    immutable_csr->sum_out_edges_ = buf[1];
    immutable_csr->out_offset_ = (size_t*)malloc(sizeof(size_t) * buf[0]);
    folly::readNoInt(meta_out_fd, immutable_csr->out_offset_,
                     sizeof(size_t) * buf[0]);
    immutable_csr->out_edges_ = (VID_T*)malloc(sizeof(VID_T) * buf[1]);
    folly::readNoInt(meta_out_fd, immutable_csr->out_edges_,
                     sizeof(VID_T) * buf[1]);

    free(buf);
    immutable_csr->is_serialized_ = true;
    immutable_csr->Deserialized();
    immutable_csr->ShowGraph();
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
    std::ofstream vertex_file_obj(vertex_pt, std::ios::binary);
    std::ofstream meta_in_file_obj(meta_in_pt, std::ios::binary);
    std::ofstream meta_out_file_obj(meta_out_pt, std::ios::binary);
    std::ofstream localid2globalid_file_obj(localid2globalid_pt,
                                            std::ios::binary);
    vertex_file_obj.close();
    meta_in_file_obj.close();
    meta_out_file_obj.close();
    localid2globalid_file_obj.close();

    folly::File vertex_file(vertex_pt, O_WRONLY);
    folly::File meta_in_file(meta_in_pt, O_WRONLY);
    folly::File meta_out_file(meta_out_pt, O_WRONLY);
    folly::File localid2globalid_file(localid2globalid_pt, O_WRONLY);

    XLOG(INFO, "Write file: ", vertex_pt);
    XLOG(INFO, "Write file: ", meta_in_pt);
    XLOG(INFO, "Write file: ", meta_out_pt);
    XLOG(INFO, "Write file: ", localid2globalid_pt);

    // write Vertexes.
    if (graph.vertex_ != nullptr) {
      auto vertex_fd = vertex_file.fd();
      size_t* buff = (size_t*)malloc(sizeof(size_t));
      buff[0] = graph.num_vertexes_;
      folly::writeNoInt(vertex_fd, buff, sizeof(size_t));
      folly::writeNoInt(vertex_fd, graph.vertex_,
                        graph.num_vertexes_ * sizeof(VID_T));
      free(buff);
    }

    // write in edges.
    if (graph.in_offset_ != nullptr && graph.in_edges_ != nullptr) {
      auto meta_in_fd = meta_in_file.fd();
      size_t* buff = (size_t*)malloc(sizeof(size_t) * 2);
      buff[0] = graph.num_vertexes_;
      buff[1] = graph.sum_in_edges_;
      folly::writeNoInt(meta_in_fd, buff, sizeof(size_t) * 2);
      folly::writeNoInt(meta_in_fd, graph.in_offset_,
                        sizeof(size_t) * graph.num_vertexes_);
      folly::writeNoInt(meta_in_fd, graph.in_edges_,
                        sizeof(VID_T) * graph.sum_in_edges_);
      free(buff);
    }

    // write out edges.
    if (graph.out_offset_ != nullptr && graph.out_edges_ != nullptr) {
      auto meta_out_fd = meta_out_file.fd();
      size_t* buff = (size_t*)malloc(sizeof(size_t) * 2);
      buff[0] = graph.num_vertexes_;
      buff[1] = graph.sum_out_edges_;
      folly::writeNoInt(meta_out_fd, buff, sizeof(size_t) * 2);
      folly::writeNoInt(meta_out_fd, graph.out_offset_,
                        sizeof(size_t) * graph.num_vertexes_);
      folly::writeNoInt(meta_out_fd, graph.out_edges_,
                        sizeof(VID_T) * graph.sum_out_edges_);
      free(buff);
    }

    // write buf (only for subgraph)
    if (graph.buf_localid2globalid_ != nullptr) {
      auto localid2globalid_fd = localid2globalid_file.fd();
      size_t* buff = (size_t*)malloc(sizeof(size_t));
      buff[0] = graph.num_vertexes_;
      folly::writeNoInt(localid2globalid_fd, buff, sizeof(size_t));
      folly::writeNoInt(localid2globalid_fd, graph.buf_localid2globalid_,
                        graph.num_vertexes_ * 2 * sizeof(VID_T));
      free(buff);
    }

    meta_in_file.close();
    meta_out_file.close();
    localid2globalid_file.close();
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_CSR_IO_ADAPTER_H
