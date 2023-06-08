#ifndef MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
#define MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H

#include <sys/stat.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "rapidcsv.h"

#include "graphs/edgelist.h"
#include "io_adapter_base.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/atomic.h"
#include "utility/thread_pool.h"

namespace minigraph {
namespace utility {
namespace io {

// EdgeListIOAdapter support edge list graph with CSV format
//   i.e. <src, dst> or <src, dst, weight>.
// In addition, two types of binary formatted edge list files are also
// supported:
//   Unweighted. Edges are tuples of <4 byte source, 4 byte destination>.
//   Weighted. Edges are tuples of <4 byte source, 4 byte destination, 4 byte
//   float typed weight>.
template<typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class EdgeListIOAdapter : public IOAdapterBase<GID_T, VID_T, VDATA_T, EDATA_T> {
  using GRAPH_BASE_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using EDGE_LIST_T = graphs::EdgeList<GID_T, VID_T, VDATA_T, EDATA_T>;

 public:
  EdgeListIOAdapter() = default;
  ~EdgeListIOAdapter() = default;

  template<class... Args>
  bool Read(GRAPH_BASE_T *graph, const GraphFormat &graph_format,
            char separator_params, const GID_T &gid, Args &&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edgelist_csv:tag = ReadEdgeListFromCSV(graph, pt[0], gid, true, separator_params);
        break;
      case weight_edgelist_csv:break;
      case edgelist_bin:tag = ReadEdgeListFromBin(graph, gid, pt[0], pt[1], pt[2]);
      default:break;
    }
    return tag;
  }

  template<class... Args>
  bool ParallelRead(GRAPH_BASE_T *graph, const GraphFormat &graph_format,
                    char separator_params, const GID_T &gid, const size_t cores,
                    Args &&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edgelist_csv:
        tag = ParallelReadEdgeListFromCSV(graph, pt[0], gid, separator_params,
                                          cores);
        break;
      default:break;
    }
    return tag;
  }

  template<class... Args>
  bool BatchParallelRead(GRAPH_BASE_T *graph, const GraphFormat &graph_format,
                         char separator_params, const GID_T &gid,
                         const size_t cores, Args &&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edgelist_csv:
        tag = BatchParallelReadEdgeListFromCSV(graph, pt[0], gid, true,
                                               separator_params, cores);
        break;
      default:break;
    }
    return tag;
  }

  template<class... Args>
  bool Write(const EDGE_LIST_T &graph, const GraphFormat &graph_format,
             Args &&... args) {
    std::string pt[] = {(args)...};
    bool tag = false;
    switch (graph_format) {
      case edgelist_csv:break;
      case weight_edgelist_csv:tag = false;
        break;
      case edgelist_bin:tag = WriteEdgeList2EdgeListBin(graph, pt[0], pt[1], pt[2]);
        break;
      default:break;
    }
    return tag;
  }

  bool ReadEdgeListFromCSV(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T> *graph,
                           const std::string &pt, const GID_T gid = 0,
                           const bool assemble = false,
                           char separator_params = ',') {
    if (!this->Exist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }
    // auto edge_list = (EDGE_LIST_T*)graph;
    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(separator_params));
    std::vector<VID_T> src = doc.GetColumn<VID_T>(0);
    std::vector<VID_T> dst = doc.GetColumn<VID_T>(1);
    ((EDGE_LIST_T *) graph)->buf_graph_ =
        (vid_t *) malloc(sizeof(vid_t) * (src.size() + dst.size()));

    VID_T *buff = (VID_T *) ((EDGE_LIST_T *) graph)->buf_graph_;
    memset((char *) buff, 0, sizeof(VID_T) * (src.size() + dst.size()));
    LOG_INFO("num edges: ", src.size(), " sizeof(VID_T)", sizeof(VID_T));
    for (size_t i = 0; i < src.size(); i++) {
      *((VID_T *) ((EDGE_LIST_T *) graph)->buf_graph_ + i * 2) = src.at(i);
      *((VID_T *) ((EDGE_LIST_T *) graph)->buf_graph_ + i * 2 + 1) = dst.at(i);
    }
    if (assemble) {
      std::unordered_map < VID_T, std::vector < VID_T > * > graph_out_edges;
      std::unordered_map < VID_T, std::vector < VID_T > * > graph_in_edges;

      for (size_t i = 0; i < src.size(); i++) {
        auto iter = graph_out_edges.find(src.at(i));
        if (iter != graph_out_edges.end()) {
          iter->second->push_back(dst.at(i));
        } else {
          std::vector<VID_T> *out_edges = new std::vector<VID_T>;
          out_edges->push_back(dst.at(i));
          graph_out_edges.insert(std::make_pair(src.at(i), out_edges));
        }
        iter = graph_in_edges.find(dst.at(i));
        if (iter != graph_in_edges.end()) {
          iter->second->push_back(src.at(i));
        } else {
          std::vector<VID_T> *in_edges = new std::vector<VID_T>;
          in_edges->push_back(src.at(i));
          graph_in_edges.insert(std::make_pair(dst.at(i), in_edges));
        }
      }

      ((EDGE_LIST_T *) graph)->max_vid_ = 0;
      for (auto &iter : graph_in_edges) {
        graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> *vertex_info =
            new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
        ((EDGE_LIST_T *) graph)->max_vid_ < iter.first
        ? ((EDGE_LIST_T *) graph)->max_vid_ = iter.first
        : 0;
        vertex_info->vid = iter.first;
        vertex_info->indegree = iter.second->size();
        vertex_info->in_edges =
            (VID_T *) malloc(sizeof(VID_T) * vertex_info->indegree);
        for (size_t i = 0; i < iter.second->size(); i++) {
          ((VID_T *) vertex_info->in_edges)[i] = iter.second->at(i);
        }
        ((EDGE_LIST_T *) graph)->vertexes_info_->emplace(iter.first, vertex_info);
      }

      for (auto &iter : graph_out_edges) {
        auto iter_vertexes_info =
            ((EDGE_LIST_T *) graph)->vertexes_info_->find(iter.first);
        if (iter_vertexes_info !=
            ((EDGE_LIST_T *) graph)->vertexes_info_->cend()) {
          iter_vertexes_info->second->outdegree = iter.second->size();
          iter_vertexes_info->second->out_edges =
              (VID_T *) malloc(sizeof(VID_T) * iter.second->size());
          for (size_t i = 0; i < iter.second->size(); i++) {
            iter_vertexes_info->second->out_edges[i] = iter.second->at(i);
          }
        } else {
          graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> *vertex_info =
              new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
          vertex_info->vid = iter.first;
          ((EDGE_LIST_T *) graph)->max_vid_ < iter.first
          ? ((EDGE_LIST_T *) graph)->max_vid_ = iter.first
          : 0;
          vertex_info->outdegree = iter.second->size();
          vertex_info->out_edges =
              (VID_T *) malloc(sizeof(VID_T) * iter.second->size());
          for (size_t i = 0; i < iter.second->size(); i++) {
            ((VID_T *) vertex_info->out_edges)[i] = iter.second->at(i);
          }
          ((EDGE_LIST_T *) graph)
              ->vertexes_info_->emplace(iter.first, vertex_info);
        }
      }
    }

    ((EDGE_LIST_T *) graph)->num_vertexes_ =
        ((EDGE_LIST_T *) graph)->vertexes_info_->size();

    ((EDGE_LIST_T *) graph)->index_by_vid_ =
        (size_t *) malloc(sizeof(size_t) * ((EDGE_LIST_T *) graph)->max_vid_);
    memset(((EDGE_LIST_T *) graph)->index_by_vid_, 0,
           ((EDGE_LIST_T *) graph)->max_vid_ * sizeof(size_t));

    ((EDGE_LIST_T *) graph)->vid_by_index_ =
        (VID_T *) malloc(sizeof(VID_T) * ((EDGE_LIST_T *) graph)->num_vertexes_);
    memset(((EDGE_LIST_T *) graph)->vid_by_index_, 0,
           ((EDGE_LIST_T *) graph)->num_vertexes_ * sizeof(VID_T));

    size_t index = 0;
    for (auto &iter : *((EDGE_LIST_T *) graph)->vertexes_info_) {
      auto vid = iter.first;
      ((EDGE_LIST_T *) graph)->index_by_vid_[vid] = index;
      ((EDGE_LIST_T *) graph)->vid_by_index_[index++] = vid;
    }

    ((EDGE_LIST_T *) graph)->is_serialized_ = true;
    ((EDGE_LIST_T *) graph)->vdata_ = (VDATA_T *) malloc(
        sizeof(VDATA_T) * ((EDGE_LIST_T *) graph)->num_vertexes_);
    memset(((EDGE_LIST_T *) graph)->vdata_, 0,
           sizeof(VDATA_T) * ((EDGE_LIST_T *) graph)->num_vertexes_);

    ((EDGE_LIST_T *) graph)->num_edges_ = src.size();
    ((EDGE_LIST_T *) graph)->gid_ = gid;
    ((EDGE_LIST_T *) graph)->vertexes_state_ =
        (char *) malloc(sizeof(char) * ((EDGE_LIST_T *) graph)->get_num_vertexes());

    memset(((EDGE_LIST_T *) graph)->vertexes_state_, VERTEXUNLABELED,
           sizeof(char) * ((EDGE_LIST_T *) graph)->get_num_vertexes());
    LOG_INFO("ReadEdgeListFromCSV num_vertexes: ",
             ((EDGE_LIST_T *) graph)->num_vertexes_,
             " num_edges: ", ((EDGE_LIST_T *) graph)->num_edges_);
    return true;
  }

  bool ReadEdgeListFromBin(graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T> *graph,
                           const GID_T gid = 0, const std::string &meta_pt = "",
                           const std::string &data_pt = "",
                           const std::string &vdata_pt = "") {
    auto edge_list_graph = (EDGE_LIST_T *) graph;
    std::ifstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ifstream data_file(data_pt, std::ios::binary | std::ios::app);
    std::ifstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);

    LOG_INFO("Read workspace: ", meta_pt);

    size_t *meta_buff = (size_t *) malloc(sizeof(size_t) * 2);
    memset((char *) meta_buff, 0, sizeof(size_t) * 2);
    meta_file.read((char *) meta_buff, sizeof(size_t) * 2);
    meta_file.read((char *) &edge_list_graph->max_vid_, sizeof(VID_T));
    edge_list_graph->aligned_max_vid_ =
        ceil((float) edge_list_graph->max_vid_ / ALIGNMENT_FACTOR) *
            ALIGNMENT_FACTOR;

    edge_list_graph->buf_graph_ =
        (VID_T *) malloc(sizeof(VID_T) * 2 * meta_buff[1]);
    edge_list_graph->vdata_ = (VDATA_T *) malloc(sizeof(VDATA_T) * meta_buff[0]);
    data_file.read((char *) edge_list_graph->buf_graph_,
                   sizeof(VID_T) * 2 * meta_buff[1]);
    vdata_file.read((char *) edge_list_graph->vdata_,
                    sizeof(VDATA_T) * meta_buff[0]);
    edge_list_graph->globalid_by_localid_ =
        (VID_T *) malloc(sizeof(VID_T) * meta_buff[0]);
    vdata_file.read((char *) edge_list_graph->globalid_by_localid_,
                    sizeof(VID_T) * meta_buff[0]);

    edge_list_graph->num_vertexes_ = meta_buff[0];
    edge_list_graph->num_edges_ = meta_buff[1];

    if (edge_list_graph->max_vid_ == 0) {
      VID_T max_vid = 0;
      for (size_t j = 0; j < edge_list_graph->num_edges_; j++) {
        auto src_vid = edge_list_graph->buf_graph_[j * 2];
        auto dst_vid = edge_list_graph->buf_graph_[j * 2 + 1];
        write_max(&max_vid, src_vid);
        write_max(&max_vid, dst_vid);
      }
      edge_list_graph->max_vid_ = max_vid;
      edge_list_graph->aligned_max_vid_ =
          ceil((float) max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    }

    LOG_INFO("Read ", data_pt, " successful", ", num vertexes: ", meta_buff[0],
             " num edges: ", meta_buff[1],
             " max_vid: ", edge_list_graph->get_max_vid(),
             " aligned max vid: ", edge_list_graph->get_aligned_max_vid());

    free(meta_buff);
    data_file.close();
    meta_file.close();
    vdata_file.close();
    return true;
  }

  bool ParallelReadEdgeListFromCSV(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T> *graph,
      const std::string &pt, const GID_T gid, char separator_params,
      const size_t cores = 1) {
    if (!this->Exist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    rapidcsv::Document doc(pt, rapidcsv::LabelParams(),
                           rapidcsv::SeparatorParams(separator_params));
    LOG_INFO("Open ", pt);

    std::vector<VID_T> src = doc.GetColumn<VID_T>(0);
    std::vector<VID_T> dst = doc.GetColumn<VID_T>(1);
    graph->buf_graph_ =
        (VID_T *) malloc(sizeof(VID_T) * (src.size() + dst.size()));

    memset((char *) graph->buf_graph_, 0,
           sizeof(VID_T) * (src.size() + dst.size()));
    std::atomic<VID_T> max_vid_atom(0);

    VID_T max_vid = 0;
    LOG_INFO("Traverse the entire graph to get the maximum vid.");
    std::atomic<size_t> pending_packages(cores);
    for (size_t tid = 0; tid < cores; tid++) {
      thread_pool.Commit([tid, &cores, &src, &dst, &graph, &pending_packages,
                             &finish_cv, &max_vid]() {
        for (size_t j = tid; j < src.size(); j += cores) {
          *(graph->buf_graph_ + j * 2) = src.at(j);
          *(graph->buf_graph_ + j * 2 + 1) = dst.at(j);
          write_max(&max_vid, src.at(j));
          write_max(&max_vid, dst.at(j));
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    ((EDGE_LIST_T *) graph)->num_edges_ = src.size();
    ((EDGE_LIST_T *) graph)->max_vid_ = max_vid;
    ((EDGE_LIST_T *) graph)->aligned_max_vid_ =
        ceil((float) max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    ((EDGE_LIST_T *) graph)->gid_ = gid;

    Bitmap *vertex_indicator = new Bitmap(graph->get_aligned_max_vid());
    vertex_indicator->clear();

    LOG_INFO("Traverse the entire graph again to fill the vertex_indicator.");
    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &graph, &vertex_indicator,
                             &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < graph->get_num_edges(); j += cores) {
          auto src_vid = ((EDGE_LIST_T *) graph)->buf_graph_[j * 2];
          auto dst_vid = ((EDGE_LIST_T *) graph)->buf_graph_[j * 2 + 1];
          if (!vertex_indicator->get_bit(src_vid)) {
            vertex_indicator->set_bit(src_vid);
            __sync_add_and_fetch(&((EDGE_LIST_T *) graph)->num_vertexes_, 1);
          }
          if (!vertex_indicator->get_bit(dst_vid)) {
            vertex_indicator->set_bit(dst_vid);
            __sync_add_and_fetch(&((EDGE_LIST_T *) graph)->num_vertexes_, 1);
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    ((EDGE_LIST_T *) graph)->vdata_ =
        (VDATA_T *) malloc(sizeof(VDATA_T) * graph->get_num_vertexes());
    memset(((EDGE_LIST_T *) graph)->vdata_, 0,
           sizeof(VDATA_T) * graph->get_num_vertexes());

    return true;
  }

  bool WriteEdgeList2EdgeListBin(const EDGE_LIST_T &graph,
                                 const std::string &meta_pt,
                                 const std::string &data_pt,
                                 const std::string &vdata_pt) {
    if (graph.is_serialized_ == false) {
      XLOG(ERR, "Graph has not been serialized.");
      return false;
    }
    if (graph.buf_graph_ == nullptr) {
      XLOG(ERR, "Segmentation fault: buf_graph is nullptr");
      return false;
    }
    if (this->Exist(meta_pt)) remove(meta_pt.c_str());
    if (this->Exist(data_pt)) remove(data_pt.c_str());
    if (this->Exist(vdata_pt)) remove(vdata_pt.c_str());

    std::ofstream meta_file(meta_pt, std::ios::binary | std::ios::app);
    std::ofstream data_file(data_pt, std::ios::binary | std::ios::app);
    std::ofstream vdata_file(vdata_pt, std::ios::binary | std::ios::app);

    size_t *meta_buff = (size_t *) malloc(sizeof(size_t) * 2);
    meta_buff[0] = ((EDGE_LIST_T *) &graph)->num_vertexes_;
    meta_buff[1] = ((EDGE_LIST_T *) &graph)->num_edges_;

    meta_file.write((char *) meta_buff, 2 * sizeof(size_t));
    meta_file.write((char *) &graph.max_vid_, sizeof(VID_T));
    data_file.write((char *) ((EDGE_LIST_T *) &graph)->buf_graph_,
                    sizeof(VID_T) * 2 * ((EDGE_LIST_T *) &graph)->num_edges_);

    LOG_INFO("VID_T size: ", sizeof(VID_T));
    if ((char *) ((EDGE_LIST_T *) &graph)->vdata_ != nullptr)
      vdata_file.write((char *) ((EDGE_LIST_T *) &graph)->vdata_,
                       sizeof(VID_T) * ((EDGE_LIST_T *) &graph)->num_vertexes_);

    LOG_INFO("EDGE_UNIT: ", sizeof(VID_T) * 2,
             ", num_edges: ", ((EDGE_LIST_T *) &graph)->num_edges_,
             ", write size: ",
             sizeof(VID_T) * 2 * ((EDGE_LIST_T *) &graph)->num_edges_);
    free(meta_buff);
    data_file.close();
    meta_file.close();
    vdata_file.close();
    return true;
  }

  bool BatchParallelReadEdgeListFromCSV(
      graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T> *graph,
      const std::string &pt, const GID_T gid = 0, const bool assemble = false,
      char separator_params = ',', const size_t cores = 1) {
    if (!this->Exist(pt)) {
      XLOG(ERR, "Read file fault: ", pt);
      return false;
    }
    if (graph == nullptr) {
      XLOG(ERR, "segmentation fault: graph is nullptr");
      return false;
    }

    auto thread_pool = CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);

    std::vector<std::string> files;
    for (const auto &entry : std::filesystem::directory_iterator(pt)) {
      std::string path = entry.path();
      files.push_back(path);
    }

    VID_T **buf_graph_bucket_ = (VID_T **) malloc(sizeof(VID_T *) * files.size());
    auto num_edges_bucket = (size_t *) malloc(sizeof(size_t) * files.size());
    auto offset_edges_bucket =
        (size_t *) malloc(sizeof(size_t) * (files.size() + 1));

    memset((char *) num_edges_bucket, 0, sizeof(size_t) * (files.size()));
    memset((char *) offset_edges_bucket, 0, sizeof(size_t) * (1 + files.size()));

    VID_T max_vid = 0;
    std::atomic<VID_T> max_vid_atom(0);
    for (auto pi = 0; pi < files.size(); pi++) {
      LOG_INFO("Process ", files.at(pi));
      rapidcsv::Document doc(files.at(pi), rapidcsv::LabelParams(),
                             rapidcsv::SeparatorParams(separator_params));
      std::vector<VID_T> src = doc.GetColumn<VID_T>(0);
      std::vector<VID_T> dst = doc.GetColumn<VID_T>(1);
      VID_T *buff = (vid_t *) malloc(sizeof(vid_t) * (src.size() + dst.size()));
      memset((char *) buff, 0, sizeof(VID_T) * (src.size() + dst.size()));
      buf_graph_bucket_[pi] = buff;

      std::atomic<size_t> pending_packages(cores);

      for (size_t i = 0; i < cores; i++) {
        size_t tid = i;
        thread_pool.Commit([tid, &cores, &src, &dst, &graph, &pending_packages,
                               &finish_cv, &max_vid, &buff]() {
          for (size_t j = tid; j < src.size(); j += cores) {
            *(buff + j * 2) = src.at(j);
            *(buff + j * 2 + 1) = dst.at(j);
            write_max(&max_vid, src.at(j));
            write_max(&max_vid, dst.at(j));
          }
          if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
          return;
        });
      }
      finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
      ((EDGE_LIST_T *) graph)->num_edges_ += src.size();
      num_edges_bucket[pi] = src.size();
      offset_edges_bucket[pi + 1] = src.size();
    }

    ((EDGE_LIST_T *) graph)->buf_graph_ =
        (VID_T *) malloc(sizeof(VID_T) * ((EDGE_LIST_T *) graph)->num_edges_ * 2);
    memset((char *) ((EDGE_LIST_T *) graph)->buf_graph_, 0,
           sizeof(VID_T) * ((EDGE_LIST_T *) graph)->num_edges_ * 2);

    // Copy edges from buffs to graph->buf_graph_;
    for (size_t pi = 0; pi < files.size(); pi++) {
      memcpy(((EDGE_LIST_T *) graph)->buf_graph_ + offset_edges_bucket[pi] * 2,
             buf_graph_bucket_[pi], num_edges_bucket[pi] * 2 * sizeof(VID_T));
    }

    ((EDGE_LIST_T *) graph)->max_vid_ = max_vid;
    ((EDGE_LIST_T *) graph)->aligned_max_vid_ =
        ceil((float) max_vid / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    ((EDGE_LIST_T *) graph)->gid_ = gid;

    LOG_INFO("Traverse the entire graph again to fill the vertex_indicator.");
    std::atomic<size_t> pending_packages(cores);
    Bitmap vertex_indicator(graph->get_aligned_max_vid());
    vertex_indicator.clear();
    for (size_t i = 0; i < cores; i++) {
      size_t tid = i;
      thread_pool.Commit([tid, &cores, &graph, &vertex_indicator,
                             &pending_packages, &finish_cv]() {
        for (size_t j = tid; j < graph->get_num_edges(); j += cores) {
          auto src_vid = ((EDGE_LIST_T *) graph)->buf_graph_[j * 2];
          auto dst_vid = ((EDGE_LIST_T *) graph)->buf_graph_[j * 2 + 1];
          if (!vertex_indicator.get_bit(src_vid)) {
            vertex_indicator.set_bit(src_vid);
            __sync_add_and_fetch(&((EDGE_LIST_T *) graph)->num_vertexes_, 1);
          }
          if (!vertex_indicator.get_bit(dst_vid)) {
            vertex_indicator.set_bit(dst_vid);
            __sync_add_and_fetch(&((EDGE_LIST_T *) graph)->num_vertexes_, 1);
          }
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

    ((EDGE_LIST_T *) graph)->vdata_ =
        (VDATA_T *) malloc(sizeof(VDATA_T) * graph->get_num_vertexes());
    memset(((EDGE_LIST_T *) graph)->vdata_, 0,
           sizeof(VDATA_T) * graph->get_num_vertexes());

    LOG_INFO("Gid: ", ((EDGE_LIST_T *) graph)->gid_,
             " num_vertexes: ", graph->num_vertexes_,
             " num_edges: ", graph->num_edges_,
             " max_vid: ", graph->get_max_vid(),
             " aligned_mavid: ", graph->get_aligned_max_vid());
    return true;
  }
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_EDGE_LIST_IO_ADAPTER_H
