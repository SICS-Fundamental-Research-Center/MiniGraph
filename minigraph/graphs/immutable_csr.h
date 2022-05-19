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

  ImmutableCSR(const GID_T gid) : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid){};

  ~ImmutableCSR() {
    if (vertexes_info_ != nullptr) {
      std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*> tmp;
      vertexes_info_->swap(tmp);
      delete vertexes_info_;
      vertexes_info_ = nullptr;
    }
    if (map_localid2globalid_ != nullptr) {
      std::unordered_map<VID_T, VID_T> tmp;
      map_localid2globalid_->swap(tmp);
      delete map_localid2globalid_;
      map_localid2globalid_ = nullptr;
    }
    if (map_globalid2localid_ != nullptr) {
      std::unordered_map<VID_T, VID_T> tmp;
      map_globalid2localid_->swap(tmp);
      delete map_globalid2localid_;
      map_localid2globalid_ = nullptr;
    }
    if (buf_graph_ != nullptr) {
      free(buf_graph_);
      buf_graph_ = nullptr;
    }
    vid_by_index_ = nullptr;
    index_by_vid_ = nullptr;
    in_edges_ = nullptr;
    out_edges_ = nullptr;
    indegree_ = nullptr;
    outdegree_ = nullptr;
    vdata_ = nullptr;
    in_offset_ = nullptr;
    out_offset_ = nullptr;
    globalid_by_index_ = nullptr;
  };

  size_t get_num_vertexes() const override { return num_vertexes_; }

  void CleanUp() override {
    if (map_localid2globalid_ != nullptr) {
      std::unordered_map<VID_T, VID_T> tmp;
      map_localid2globalid_->swap(tmp);
      delete map_localid2globalid_;
      map_localid2globalid_ = nullptr;
    }
    if (map_globalid2localid_ != nullptr) {
      std::unordered_map<VID_T, VID_T> tmp;
      map_globalid2localid_->swap(tmp);
      delete map_globalid2localid_;
      map_localid2globalid_ = nullptr;
    }
    if (buf_graph_ != nullptr) {
      free(buf_graph_);
      buf_graph_ = nullptr;
    }
    if (vertexes_info_ != nullptr) {
      std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*> tmp;
      vertexes_info_->swap(tmp);
      delete vertexes_info_;
      vertexes_info_ = nullptr;
    }
    vid_by_index_ = nullptr;
    index_by_vid_ = nullptr;
    in_edges_ = nullptr;
    out_edges_ = nullptr;
    indegree_ = nullptr;
    outdegree_ = nullptr;
    vdata_ = nullptr;
    in_offset_ = nullptr;
    out_offset_ = nullptr;
    globalid_by_index_ = nullptr;
  };

  void ShowGraph(const size_t count = 2) {
    std::cout << "\n\n##### ImmutableCSRGraph GID: " << gid_
         << ", num_verteses: " << num_vertexes_
         << ", sum_in_degree:" << sum_in_edges_
         << ", sum_out_degree: " << sum_out_edges_ << " #####" << std::endl;
    size_t count_ = 0;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (count_++ > count) {
        std::cout << "############################" << std::endl;
        return;
      }
      VertexInfo&& vertex_info = GetVertexByIndex(i);
      VID_T global_id = globalid_by_index_[i];
      vertex_info.ShowVertexAbs(global_id);
    }
  }

  void ShowGraphAbs(const size_t count = 2) {
    std::cout << "\n\n##### ImmutableCSRGraph GID: " << gid_
         << ", num_verteses: " << num_vertexes_
         << ", sum_in_degree:" << sum_in_edges_
         << ", sum_out_degree: " << sum_out_edges_ << " #####" << std::endl;
    size_t count_ = 0;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (count_++ > count) {
        std::cout << "############################" << std::endl;
        return;
      }
      VertexInfo&& vertex_info = GetVertexByIndex(i);
      VID_T global_id = globalid_by_index_[i];
      vertex_info.ShowVertexAbs(global_id);
    }
  }

  bool InitVdata2AllX(const VDATA_T init_vdata) {
    assert(vdata_ != nullptr);
    assert(is_serialized_);
    if (init_vdata == 0) {
      memset(vdata_, 0, sizeof(VDATA_T) * num_vertexes_);
    } else {
      for (size_t i = 0; i < num_vertexes_; i++) {
        vdata_[i] = init_vdata;
      }
    }
    return true;
  }

  void InitVdataByVid() {
    assert(vdata_ != nullptr);
    assert(is_serialized_);
    for (size_t i = 0; i < num_vertexes_; i++) {
      auto u = GetVertexByIndex(i);
      u.vdata[0] = localid2globalid(u.vid);
    }
  }

  void InitVdata2AllMax() {
    assert(vdata_ != nullptr);
    assert(is_serialized_);
    for (size_t i = 0; i < num_vertexes_; i++) {
      auto u = GetVertexByIndex(i);
      u.vdata[0] = num_vertexes_;
    }
  }

  bool Serialize() {
    if (vertexes_info_ == nullptr) {
      return false;
    }
    size_t size_localid = sizeof(VID_T) * num_vertexes_;
    size_t size_globalid = sizeof(VID_T) * num_vertexes_;
    size_t size_index_by_vid = sizeof(size_t) * num_vertexes_;
    size_t size_indegree = sizeof(size_t) * num_vertexes_;
    size_t size_outdegree = sizeof(size_t) * num_vertexes_;
    size_t size_in_offset = sizeof(size_t) * num_vertexes_;
    size_t size_out_offset = sizeof(size_t) * num_vertexes_;
    size_t size_vdata = sizeof(VDATA_T) * num_vertexes_;
    size_t size_in_edges = sizeof(VID_T) * sum_in_edges_;
    size_t size_out_edges = sizeof(VID_T) * sum_out_edges_;
    size_t total_size = size_localid + size_globalid + size_index_by_vid +
                        size_indegree + size_outdegree + size_in_offset +
                        size_out_offset + size_in_edges + size_out_edges +
                        size_vdata;
    size_t start_localid = 0;
    size_t start_globalid = start_localid + size_localid;
    size_t start_index_by_vid = start_globalid + size_globalid;
    size_t start_indegree = start_index_by_vid + size_index_by_vid;
    size_t start_outdegree = start_indegree + size_indegree;
    size_t start_in_offset = start_outdegree + size_outdegree;
    size_t start_out_offset = start_in_offset + size_in_offset;
    size_t start_vdata = start_out_offset + size_out_offset;
    size_t start_in_edges = start_vdata + size_vdata;
    size_t start_out_edges = start_in_edges + size_in_edges;

    buf_graph_ = malloc(total_size);
    memset(buf_graph_, 0, total_size);
    size_t i = 0;
    for (auto& iter_vertex : *vertexes_info_) {
      ((VID_T*)((char*)buf_graph_ + start_localid))[i] =
          iter_vertex.second->vid;
      ((VID_T*)((char*)buf_graph_ + start_globalid))[i] =
          this->localid2globalid(iter_vertex.second->vid);
      ((size_t*)((char*)buf_graph_ +
                 start_index_by_vid))[iter_vertex.second->vid] = i;
      ((size_t*)((char*)buf_graph_ + start_indegree))[i] =
          iter_vertex.second->indegree;
      ((size_t*)((char*)buf_graph_ + start_outdegree))[i] =
          iter_vertex.second->outdegree;
      if (i == 0) {
        ((size_t*)((char*)buf_graph_ + start_in_offset))[i] = 0;
        if (iter_vertex.second->indegree > 0) {
          memcpy((VID_T*)((char*)buf_graph_ + start_in_edges),
                 iter_vertex.second->in_edges,
                 sizeof(VID_T) * iter_vertex.second->indegree);
        }
        ((size_t*)((char*)buf_graph_ + start_out_offset))[i] = 0;
        if (iter_vertex.second->outdegree > 0) {
          memcpy((VID_T*)((char*)buf_graph_ + start_out_edges),
                 iter_vertex.second->out_edges,
                 sizeof(VID_T) * iter_vertex.second->outdegree);
        }
      } else {
        ((size_t*)((char*)buf_graph_ + start_in_offset))[i] =
            ((size_t*)((char*)buf_graph_ + start_indegree))[i - 1] +
            ((size_t*)((char*)buf_graph_ + start_in_offset))[i - 1];
        if (iter_vertex.second->indegree > 0) {
          size_t start = ((size_t*)((char*)buf_graph_ + start_in_offset))[i];
          memcpy(((char*)buf_graph_ + start_in_edges + start * sizeof(VID_T)),
                 iter_vertex.second->in_edges,
                 sizeof(VID_T) * iter_vertex.second->indegree);
        }
        ((size_t*)((char*)buf_graph_ + start_out_offset))[i] =
            ((size_t*)((char*)buf_graph_ + start_outdegree))[i - 1] +
            ((size_t*)((char*)buf_graph_ + start_out_offset))[i - 1];

        if (iter_vertex.second->outdegree > 0) {
          size_t start = ((size_t*)((char*)buf_graph_ + start_out_offset))[i];
          memcpy(((char*)buf_graph_ + start_out_edges + start * sizeof(VID_T)),
                 iter_vertex.second->out_edges,
                 sizeof(VID_T) * iter_vertex.second->outdegree);
        }
      }
      ++i;
    }
    vid_by_index_ = ((VID_T*)((char*)buf_graph_ + start_localid));
    index_by_vid_ = ((size_t*)((char*)buf_graph_ + start_index_by_vid));
    globalid_by_index_ = (VID_T*)((char*)buf_graph_ + start_globalid);
    vdata_ = (VDATA_T*)((char*)buf_graph_ + start_vdata);
    out_offset_ = (size_t*)((char*)buf_graph_ + start_out_offset);
    in_offset_ = (size_t*)((char*)buf_graph_ + start_in_offset);
    indegree_ = (size_t*)((char*)buf_graph_ + start_indegree);
    outdegree_ = (size_t*)((char*)buf_graph_ + start_outdegree);
    in_edges_ = (VID_T*)((char*)buf_graph_ + start_in_edges);
    out_edges_ = (VID_T*)((char*)buf_graph_ + start_out_edges);

    is_serialized_ = true;
    return true;
  }

  bool Deserialized() {
    if (vertexes_info_ == nullptr) {
      XLOG(INFO, "segmentation fault: ", "vertex_info_ is nullptr.");
      return false;
    }
    if (vertexes_info_->size() > 0) {
      XLOG(INFO, "Deserialized fault: ", "vertex_info_ already exist..");
      return false;
    }
    LOG_INFO("Deserialized()");
    for (size_t i = 0; i < num_vertexes_; i++) {
      auto vertex_info = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
      vertex_info->vid = vid_by_index_[i];
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

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByIndex(
      const size_t index) {
    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
    vertex_info.vid = vid_by_index_[index];
    if (index != num_vertexes_ - 1) {
      vertex_info.outdegree = outdegree_[index];
      vertex_info.indegree = indegree_[index];
      vertex_info.in_edges = (in_edges_ + in_offset_[index]);
      vertex_info.out_edges = (out_edges_ + out_offset_[index]);
      vertex_info.vdata = (vdata_ + index);
    } else {
      vertex_info.outdegree = outdegree_[index];
      vertex_info.indegree = indegree_[index];
      vertex_info.in_edges = (in_edges_ + in_offset_[index]);
      vertex_info.out_edges = (out_edges_ + out_offset_[index]);
      vertex_info.vdata = (vdata_ + index);
    }
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* GetPVertexByIndex(
      const size_t index) {
    auto vertex_info = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
    vertex_info->vid = vid_by_index_[index];
    if (index != num_vertexes_ - 1) {
      vertex_info->outdegree = outdegree_[index];
      vertex_info->indegree = indegree_[index];
      vertex_info->in_edges = (in_edges_ + in_offset_[index]);
      vertex_info->out_edges = (out_edges_ + out_offset_[index]);
      vertex_info->vdata = (vdata_ + index);
    } else {
      vertex_info->outdegree = outdegree_[index];
      vertex_info->indegree = indegree_[index];
      vertex_info->in_edges = (in_edges_ + in_offset_[index]);
      vertex_info->out_edges = (out_edges_ + out_offset_[index]);
      vertex_info->vdata = (vdata_ + index);
    }
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByVid(const VID_T vid) {
    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
    vertex_info.vid = vid;
    size_t index = index_by_vid_[vid];
    if (index != num_vertexes_ - 1) {
      vertex_info.outdegree = outdegree_[index];
      vertex_info.indegree = indegree_[index];
      vertex_info.in_edges = (in_edges_ + in_offset_[index]);
      vertex_info.out_edges = (out_edges_ + out_offset_[index]);
      vertex_info.vdata = (vdata_ + index);
    } else {
      vertex_info.outdegree = outdegree_[index];
      vertex_info.indegree = indegree_[index];
      vertex_info.in_edges = (in_edges_ + in_offset_[index]);
      vertex_info.out_edges = (out_edges_ + out_offset_[index]);
      vertex_info.vdata = (vdata_ + index);
    }
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* CopyVertexByIndex(
      const size_t index) {
    VertexInfo* vertex_info = new VertexInfo;
    vertex_info->vid = vid_by_index_[index];
    vertex_info->outdegree = outdegree_[index];
    vertex_info->indegree = indegree_[index];
    vertex_info->in_edges =
        (VID_T*)malloc(sizeof(VID_T) * vertex_info->indegree);
    memcpy(vertex_info->in_edges, in_edges_ + in_offset_[index],
           vertex_info->indegree * sizeof(VID_T));
    vertex_info->out_edges =
        (VID_T*)malloc(sizeof(VID_T) * vertex_info->outdegree);
    memcpy(vertex_info->out_edges, out_edges_ + out_offset_[index],
           vertex_info->outdegree * sizeof(VID_T));
    vertex_info->vdata = (VDATA_T*)malloc(sizeof(VDATA_T));
    *vertex_info->vdata = *(vdata_ + index);
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* CopyVertexByVid(
      const VID_T vid) {
    VertexInfo* vertex_info = new VertexInfo;
    vertex_info->vid = vid;
    size_t index = index_by_vid_[vid];
    vertex_info->outdegree = outdegree_[index];
    vertex_info->indegree = indegree_[index];
    vertex_info->in_edges =
        (VID_T*)malloc(sizeof(VID_T) * vertex_info->indegree);
    memcpy(vertex_info->in_edges, in_edges_ + in_offset_[index],
           vertex_info->indegree * sizeof(VID_T));
    vertex_info->out_edges =
        (VID_T*)malloc(sizeof(VID_T) * vertex_info->outdegree);
    memcpy(vertex_info->out_edges, out_edges_ + out_offset_[index],
           vertex_info->outdegree * sizeof(VID_T));
    vertex_info->vdata = (VDATA_T*)malloc(sizeof(VDATA_T));
    *vertex_info->vdata = *(vdata_ + index);
    return vertex_info;
  }

  VID_T globalid2localid(const VID_T vid) const {
    auto local_id_iter = map_globalid2localid_->find(vid);
    if (local_id_iter != map_globalid2localid_->end()) {
      return local_id_iter->second;
    } else {
      return VID_MAX;
    }
  }

  VID_T localid2globalid(const VID_T vid) const {
    if (globalid_by_index_ != nullptr && index_by_vid_ != nullptr) {
      return globalid_by_index_[index_by_vid_[vid]];
    } else {
      auto local_id_iter = map_localid2globalid_->find(vid);
      if (local_id_iter != map_localid2globalid_->end()) {
        return local_id_iter->second;
      } else {
        return VID_MAX;
      }
    }
  }

  VID_T Index2Globalid(const size_t index) const {
    return globalid_by_index_[index];
  }

  std::unordered_map<VID_T, GID_T>* GetBorderVertexes() {
    std::unordered_map<VID_T, GID_T>* border_vertexes =
        new std::unordered_map<VID_T, GID_T>();
    for (size_t index = 0; index < num_vertexes_; index++) {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>&& vertex_info =
          GetVertexByIndex(vid_by_index_[index]);
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
  void* buf_graph_ = nullptr;
  VID_T* vid_by_index_ = nullptr;
  size_t* index_by_vid_ = nullptr;
  VID_T* in_edges_ = nullptr;
  VID_T* out_edges_ = nullptr;
  size_t* indegree_ = nullptr;
  size_t* outdegree_ = nullptr;
  VDATA_T* vdata_ = nullptr;
  size_t* in_offset_ = nullptr;
  size_t* out_offset_ = nullptr;
  VID_T* globalid_by_index_ = nullptr;

  std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*
      vertexes_info_ = nullptr;
  std::unordered_map<VID_T, VID_T>* map_localid2globalid_ = nullptr;
  std::unordered_map<VID_T, VID_T>* map_globalid2localid_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif // MINIGRAPH_GRAPHS_IMMUTABLECSR_H
