#ifndef MINIGRAPH_GRAPHS_IMMUTABLECSR_H
#define MINIGRAPH_GRAPHS_IMMUTABLECSR_H

#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/AtomicUnorderedMap.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Range.h>
#include <folly/portability/Asm.h>
#include <folly/portability/Atomic.h>
#include <folly/portability/SysTime.h>
#include <jemalloc/jemalloc.h>

#include "graphs/graph.h"
#include "portability/sys_types.h"
#include "utility/logging.h"

using std::cout;
using std::endl;

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ImmutableCSR : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  ImmutableCSR() : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    vertexes_info_ =
        new std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>();
    map_globalid2localid_ = new std::unordered_map<VID_T, VID_T>();
    map_localid2globalid_ = new std::unordered_map<VID_T, VID_T>();
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

  void CleanUp() override {
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
    if (vertexes_info_ != nullptr) {
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

  void ShowGraph(const size_t& count = 10) {
    cout << "##### ImmutableCSRGraph GID: " << gid_
         << ", num_verteses: " << num_vertexes_
         << ", sum_in_degree:" << sum_in_edges_
         << ", sum_out_degree: " << sum_out_edges_ << " #####" << endl;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (i > count) {
        return;
      }
      VertexInfo&& vertex_info = GetVertex(vertex_[i]);
      vertex_info.ShowVertexInfo();
    }
  }

  bool Serialize() {
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
    VID_T* outdegree = (VID_T*)malloc(sizeof(VID_T) * num_vertexes_);
    VID_T* indegree = (VID_T*)malloc(sizeof(VID_T) * num_vertexes_);
    int i = 0;
    for (auto& iter_vertex : *vertexes_info_) {
      vertex_[i] = iter_vertex.second->vid;
      outdegree[i] = iter_vertex.second->outdegree;
      indegree[i] = iter_vertex.second->indegree;
      if (i == 0) {
        in_offset_[i] = 0;
        if (iter_vertex.second->indegree > 0) {
          size_t start = 0;
          memcpy((in_edges_ + start), iter_vertex.second->in_edges,
                 sizeof(VID_T) * iter_vertex.second->indegree);
        }
        out_offset_[i] = 0;
        if (iter_vertex.second->outdegree > 0) {
          size_t start = 0;
          memcpy((out_edges_ + start), iter_vertex.second->out_edges,
                 sizeof(VID_T) * iter_vertex.second->outdegree);
        }
      } else {
        out_offset_[i] = outdegree[i - 1] + out_offset_[i - 1];
        if (iter_vertex.second->outdegree > 0) {
          size_t start = out_offset_[i];
          memcpy((out_edges_ + start), iter_vertex.second->out_edges,
                 iter_vertex.second->outdegree * sizeof(VID_T));
        }
        in_offset_[i] = indegree[i - 1] + in_offset_[i - 1];
        if (iter_vertex.second->indegree > 0) {
          size_t start = in_offset_[i];
          memcpy((in_edges_ + start), iter_vertex.second->in_edges,
                 iter_vertex.second->indegree * sizeof(VID_T));
        }
      }
      ++i;
    }

    // serialized vdata.
    if (vdata_ == nullptr) {
      vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * num_vertexes_);
      memset(vdata_, 0, sizeof(VDATA_T) * num_vertexes_);
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
    free(outdegree);
    free(indegree);
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
    LOG_INFO("Deserialized()");
    for (size_t i = 0; i < num_vertexes_; i++) {
      auto vertex_info = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
      vertex_info->vid = vertex_[i];
      if (i != num_vertexes_ - 1) {
        vertex_info->outdegree = out_offset_[i + 1] - out_offset_[i];
        vertex_info->indegree = in_offset_[i + 1] - in_offset_[i];
        vertex_info->in_edges = (in_edges_ + in_offset_[i]);
        vertex_info->out_edges = (out_edges_ + out_offset_[i]);
        vertex_info->vdata = (vdata_ + i);
      } else {
        vertex_info->outdegree = sum_out_edges_ - out_offset_[i];
        vertex_info->indegree = sum_in_edges_ - in_offset_[i];
        vertex_info->in_edges = (in_edges_ + in_offset_[i]);
        vertex_info->out_edges = (out_edges_ + out_offset_[i]);
        vertex_info->vdata = (vdata_ + i);
      }
      vertexes_info_->insert(std::make_pair(vertex_info->vid, vertex_info));
    }
    return true;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertex(VID_T vid) {
    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
    vertex_info.vid = vid;
    if (vid != num_vertexes_ - 1) {
      vertex_info.outdegree = out_offset_[vid + 1] - out_offset_[vid];
      vertex_info.indegree = in_offset_[vid + 1] - in_offset_[vid];
      vertex_info.in_edges = (in_edges_ + in_offset_[vid]);
      vertex_info.out_edges = (out_edges_ + out_offset_[vid]);
      vertex_info.vdata = (vdata_ + vid);
    } else {
      vertex_info.outdegree = sum_out_edges_ - out_offset_[vid];
      vertex_info.indegree = sum_in_edges_ - in_offset_[vid];
      vertex_info.in_edges = (in_edges_ + in_offset_[vid]);
      vertex_info.out_edges = (out_edges_ + out_offset_[vid]);
      vertex_info.vdata = (vdata_ + vid);
    }
    // vertex_info.ShowVertexInfo();
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* CopyVertex(VID_T vid) {
    VertexInfo* vertex_info = new VertexInfo;
    vertex_info->vid = vertex_[vid];
    if (vid != num_vertexes_ - 1) {
      vertex_info->outdegree = out_offset_[vid + 1] - out_offset_[vid];
      vertex_info->indegree = in_offset_[vid + 1] - in_offset_[vid];
    } else {
      vertex_info->outdegree = sum_out_edges_ - out_offset_[vid];
      vertex_info->indegree = sum_in_edges_ - in_offset_[vid];
    }
    vertex_info->in_edges =
        (VID_T*)malloc(sizeof(VID_T) * vertex_info->indegree);
    memcpy(vertex_info->in_edges, in_edges_ + in_offset_[vid],
           vertex_info->indegree * sizeof(VID_T));
    vertex_info->out_edges =
        (VID_T*)malloc(sizeof(VID_T) * vertex_info->outdegree);
    memcpy(vertex_info->out_edges, out_edges_ + out_offset_[vid],
           vertex_info->outdegree * sizeof(VID_T));
    vertex_info->vdata = (VDATA_T*)malloc(sizeof(VDATA_T));
    *vertex_info->vdata = *(vdata_ + vid);
    return vertex_info;
  }

  VID_T globalid2localid(const VID_T& vid) const {
    auto local_id_iter = map_globalid2localid_->find(vid);
    if (local_id_iter != map_globalid2localid_->end()) {
      return local_id_iter->second;
    } else {
      return VID_MAX;
    }
  }

  VID_T localid2globalid(const VID_T& vid) const {
    auto local_id_iter = map_localid2globalid_->find(vid);
    if (local_id_iter != map_localid2globalid_->end()) {
      return local_id_iter->second;
    } else {
      return VID_MAX;
    }
  }

  std::unordered_map<VID_T, GID_T>* GetBorderVertexes() {
    std::unordered_map<VID_T, GID_T>* border_vertexes =
        new std::unordered_map<VID_T, GID_T>();
    for (size_t vid = 0; vid < num_vertexes_; vid++) {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>&& vertex_info =
          GetVertex(vertex_[vid]);
      for (size_t i_ngh = 0; i_ngh < vertex_info.indegree; ++i_ngh) {
        if (globalid2localid(vertex_info.in_edges[i_ngh]) == VID_MAX) {
          border_vertexes->insert(
              std::make_pair(vertex_info.in_edges[i_ngh], gid_));
        }
      }
    }
    return border_vertexes;
  }

 public:
  //  basic param
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

  std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*
      vertexes_info_ = nullptr;

  std::unordered_map<VID_T, VID_T>* map_localid2globalid_ = nullptr;
  std::unordered_map<VID_T, VID_T>* map_globalid2localid_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif
