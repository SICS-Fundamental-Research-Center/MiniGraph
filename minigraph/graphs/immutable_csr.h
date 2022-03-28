#ifndef MINIGRAPH_GRAPHS_IMMUTABLECSR_H
#define MINIGRAPH_GRAPHS_IMMUTABLECSR_H

#include "graphs/graph.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Range.h>
#include <folly/portability/Asm.h>
#include <folly/portability/Atomic.h>
#include <folly/portability/GTest.h>
#include <folly/portability/SysTime.h>
#include <jemalloc/jemalloc.h>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>

using std::cout;
using std::endl;

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ImmutableCSR : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
 public:
  ImmutableCSR() : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    this->vertexes_info_ = new folly::AtomicHashMap<
        VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*, std::hash<int64_t>,
        std::equal_to<int64_t>, std::allocator<char>,
        folly::AtomicHashArrayQuadraticProbeFcn>(65536 * 8);
    this->map_globalid2localid_ =
        new folly::AtomicHashMap<VID_T, VID_T, std::hash<int64_t>,
                                 std::equal_to<int64_t>, std::allocator<char>,
                                 folly::AtomicHashArrayQuadraticProbeFcn>(
            65536 * 8);
    this->map_localid2globalid_ =
        new folly::AtomicHashMap<VID_T, VID_T, std::hash<int64_t>,
                                 std::equal_to<int64_t>, std::allocator<char>,
                                 folly::AtomicHashArrayQuadraticProbeFcn>(
            65536 * 8);
  };

  ImmutableCSR(GID_T gid) : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid){};

  ~ImmutableCSR() {
    if (vdata_ != nullptr) {
      free(vdata_);
      vdata_ = nullptr;
    }
    if (in_edges_ != nullptr) {
      free(in_edges_);
      in_edges_ = nullptr;
    }
    if (out_edges_ != nullptr) {
      free(out_edges_);
      out_edges_ = nullptr;
    }
    if (in_offset_ != nullptr) {
      free(in_offset_);
      in_offset_ = nullptr;
    }
    if (out_offset_ != nullptr) {
      free(out_offset_);
      out_offset_ = nullptr;
    }
    if (this->vertexes_info_ != nullptr) {
      delete this->vertexes_info_;
    }
    if (map_localid2globalid_ != nullptr) {
      delete map_localid2globalid_;
    }
    if (map_globalid2localid_ != nullptr) {
      delete map_globalid2localid_;
    }
    if (buf_localid2globalid_ != nullptr) {
      free(buf_localid2globalid_);
      buf_localid2globalid_ = nullptr;
    }
    if (buf_globalid2localid_ != nullptr) {
      free(buf_globalid2localid_);
      buf_globalid2localid_ = nullptr;
    }
  };

  size_t get_num_vertexes() const override { return num_vertexes_; }

  bool CleanUp() {
    if (vdata_ != nullptr) {
      free(vdata_);
      vdata_ = nullptr;
    }
    if (in_edges_ != nullptr) {
      free(in_edges_);
      in_edges_ = nullptr;
    }
    if (out_edges_ != nullptr) {
      free(out_edges_);
      out_edges_ = nullptr;
    }
    if (in_offset_ != nullptr) {
      free(in_offset_);
      in_offset_ = nullptr;
    }
    if (out_offset_ != nullptr) {
      free(out_offset_);
      out_offset_ = nullptr;
    }
    if (this->vertexes_info_ != nullptr) {
      vertexes_info_->clear();
    }
    if (map_localid2globalid_ != nullptr) {
      map_localid2globalid_->clear();
    }
    if (map_globalid2localid_ != nullptr) {
      map_globalid2localid_->clear();
    }
    if (buf_localid2globalid_ != nullptr) {
      free(buf_localid2globalid_);
      buf_localid2globalid_ = nullptr;
    }
    if (buf_globalid2localid_ != nullptr) {
      free(buf_globalid2localid_);
      buf_globalid2localid_ = nullptr;
    }
  };

  void ShowGraph(const size_t& count = 2) {
    if (vertexes_info_ == nullptr) {
      XLOG(ERR, "ShowGraph fault: ", "vertexes_info_ is nullptr");
      return;
    }
    if (vertexes_info_->size() == 0) {
      XLOG(ERR, "ShowGraph fault: ", "vertexes_info_ empty");
      return;
    }

    cout << "##### ImmutableCSRGraph GID: " << gid_
         << ", num_verteses: " << num_vertexes_
         << ", sum_in_degree:" << sum_in_edges_ << ", sum_out_degree"
         << sum_out_edges_ << " #####" << endl;

    int i = 0;
    for (auto& iter_vertex : *vertexes_info_) {
      if (++i > count) {
        break;
      }
      cout << "   vid: " << iter_vertex.second->vid
           << " outdegree: " << iter_vertex.second->outdegree
           << " indegree:" << iter_vertex.second->indegree << endl;
      if (iter_vertex.second->indegree > 0) {
        cout << "     "
             << "------in edge------" << endl
             << "   ";
        for (int i = 0; i < iter_vertex.second->indegree; i++) {
          cout << iter_vertex.second->in_edges[i] << ", ";
        }
        cout << endl;
      }
      if (iter_vertex.second->outdegree > 0) {
        cout << "     "
             << "------out edge------" << endl
             << "   ";
        for (int i = 0; i < iter_vertex.second->outdegree; i++) {
          cout << iter_vertex.second->out_edges[i] << ", ";
        }
        cout << endl;
      }
    }
  }

  bool Serialize() {
    XLOG(INFO, "Serialize()");
    if (vertexes_info_ == nullptr) {
      return false;
    }
    in_edges_ = (VID_T*)malloc(sizeof(VID_T) * sum_in_edges_);
    memset(in_edges_, 0, sizeof(VID_T) * sum_in_edges_);
    out_edges_ = (VID_T*)malloc(sizeof(VID_T) * sum_out_edges_);
    memset(out_edges_, 0, sizeof(VID_T) * sum_out_edges_);
    in_offset_ = (size_t*)malloc(sizeof(size_t) * num_vertexes_);
    out_offset_ = (size_t*)malloc(sizeof(size_t) * num_vertexes_);
    vertex_ = (VID_T*)malloc(sizeof(VID_T) * num_vertexes_);
    VID_T outdegree[num_vertexes_];
    VID_T indegree[num_vertexes_];
    int i = 0;
    for (auto& iter_vertex : *vertexes_info_) {
      vertex_[i] = iter_vertex.second->vid;
      outdegree[i] = iter_vertex.second->outdegree;
      indegree[i] = iter_vertex.second->indegree;
      if (i == 0) {
        in_offset_[i] = 0;
        if (iter_vertex.second->indegree > 0) {
          int start = 0, end = iter_vertex.second->indegree;
          memcpy((in_edges_ + start), iter_vertex.second->in_edges,
                 sizeof(VID_T) * iter_vertex.second->indegree);
        }
        out_offset_[i] = 0;
        if (iter_vertex.second->outdegree > 0) {
          int start = 0, end = iter_vertex.second->outdegree;
          memcpy((out_edges_), iter_vertex.second->out_edges,
                 sizeof(VID_T) * iter_vertex.second->outdegree);
        }
      } else {
        out_offset_[i] = outdegree[i - 1] + out_offset_[i - 1];
        if (iter_vertex.second->outdegree > 0) {
          int start = out_offset_[i],
              end = out_offset_[i] + iter_vertex.second->outdegree;
          memcpy((out_edges_ + start), iter_vertex.second->out_edges,
                 iter_vertex.second->outdegree * sizeof(VID_T));
        }
        in_offset_[i] = indegree[i - 1] + in_offset_[i - 1];
        if (iter_vertex.second->indegree > 0) {
          int start = in_offset_[i],
              end = in_offset_[i] + iter_vertex.second->indegree;
          memcpy((in_edges_ + start), iter_vertex.second->in_edges,
                 iter_vertex.second->indegree * sizeof(VID_T));
        }
      }
      ++i;
    }

    // serialized localid2globalid;
    if (map_localid2globalid_ != nullptr) {
      buf_localid2globalid_ = (VID_T*)malloc(sizeof(VID_T) * 2 * num_vertexes_);
      int count = 0;
      for (auto& iter : *map_localid2globalid_) {
        buf_localid2globalid_[count] = iter.first;
        buf_localid2globalid_[count + num_vertexes_] = iter.second;
        count++;
      }
    }

    is_serialized_ = true;
    return true;
  }

  bool Deserialized() {
    if (vertexes_info_ == nullptr) {
      XLOG(INFO, "Deserialized fault: ", "vertex_info_ is nullptr.");
      return false;
    }
    if (vertexes_info_->size() > 0) {
      XLOG(INFO, "Deserialized fault: ", "vertex_info_ already exist..");
      return false;
    }
    vertexes_info_ = new folly::AtomicHashMap<
        VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*, std::hash<int64_t>,
        std::equal_to<int64_t>, std::allocator<char>,
        folly::AtomicHashArrayQuadraticProbeFcn>(1024);

    for (int i = 0; i < num_vertexes_; i++) {
      auto vertex_info = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
      vertex_info->vid = vertex_[i];
      if (i != num_vertexes_ - 1) {
        vertex_info->outdegree = out_offset_[i + 1] - out_offset_[i];
        vertex_info->indegree = in_offset_[i + 1] - in_offset_[i];
        vertex_info->in_edges = (in_edges_ + in_offset_[i]);
        vertex_info->out_edges = (in_edges_ + out_offset_[i]);
      } else {
        vertex_info->outdegree = sum_out_edges_ - out_offset_[i];
        vertex_info->indegree = sum_in_edges_ - in_offset_[i];
        vertex_info->in_edges = (in_edges_ + in_offset_[i]);
        vertex_info->out_edges = (in_edges_ + out_offset_[i]);
      }
      vertexes_info_->insert(std::make_pair(vertex_info->vid, vertex_info));
    }
    return true;
  }

 public:
  // basic param
  GID_T gid_ = -1;
  size_t num_vertexes_ = 0;
  size_t sum_in_edges_ = 0;
  size_t sum_out_edges_ = 0;

  bool is_serialized_ = false;

  // serialized data in CSR format.
  VID_T* vertex_ = nullptr;
  VDATA_T* vdata_ = nullptr;
  VID_T* in_edges_ = nullptr;
  VID_T* out_edges_ = nullptr;
  size_t* in_offset_ = nullptr;
  size_t* out_offset_ = nullptr;

  // serialized
  VID_T* buf_localid2globalid_ = nullptr;
  VID_T* buf_globalid2localid_ = nullptr;

  // vertexes_info_ is required to have data if the "is_serialized" is true.
  folly::AtomicHashMap<
      VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*, std::hash<int64_t>,
      std::equal_to<int64_t>, std::allocator<char>,
      folly::AtomicHashArrayQuadraticProbeFcn>* vertexes_info_ = nullptr;

  // only for subgraphs.
  folly::AtomicHashMap<VID_T, VID_T, std::hash<int64_t>, std::equal_to<int64_t>,
                       std::allocator<char>,
                       folly::AtomicHashArrayQuadraticProbeFcn>*
      map_localid2globalid_ = nullptr;
  folly::AtomicHashMap<VID_T, VID_T, std::hash<int64_t>, std::equal_to<int64_t>,
                       std::allocator<char>,
                       folly::AtomicHashArrayQuadraticProbeFcn>*
      map_globalid2localid_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif
