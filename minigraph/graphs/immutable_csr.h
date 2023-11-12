#ifndef MINIGRAPH_GRAPHS_IMMUTABLECSR_H
#define MINIGRAPH_GRAPHS_IMMUTABLECSR_H

#include <fstream>
#include <iostream>
#include <malloc.h>
#include <map>
#include <memory>
#include <unordered_map>

#include "graphs/edgelist.h"
#include "graphs/graph.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"
#include "utility/sort.h"
#include "utility/thread_pool.h"

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

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ImmutableCSR : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
  using VertexInfo = graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;

 public:
  ImmutableCSR() : Graph<GID_T, VID_T, VDATA_T, EDATA_T>() {
    vertexes_info_ =
        new std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>();
  };

  ImmutableCSR(const GID_T gid) : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid){};

  ImmutableCSR(
      const GID_T gid,
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>** set_vertexes = nullptr,
      size_t num_vertexes = 0, const size_t sum_in_edges = 0,
      const size_t sum_out_edges = 0, const VID_T max_vid = 0,
      VID_T* vid_map = nullptr)
      : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid) {
    if (set_vertexes == nullptr) return;
    this->num_vertexes_ = num_vertexes;
    sum_in_edges_ = sum_in_edges;
    sum_out_edges_ = sum_out_edges;
    this->max_vid_ = max_vid;
    LOG_INFO("max_vid: ", this->get_max_vid());
    this->aligned_max_vid_ =
        ceil((float)this->get_max_vid() / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;
    assert(this->get_max_vid() > 0);
    assert(this->get_aligned_max_vid() > 0);
    this->bitmap_ = new Bitmap(this->get_aligned_max_vid());
    this->bitmap_->clear();

    // size_t size_localid = sizeof(VID_T) * num_vertexes;
    size_t size_globalid = sizeof(VID_T) * num_vertexes;
    size_t size_indegree = sizeof(size_t) * num_vertexes;
    size_t size_outdegree = sizeof(size_t) * num_vertexes;
    size_t size_in_offset = sizeof(size_t) * num_vertexes;
    size_t size_out_offset = sizeof(size_t) * num_vertexes;
    size_t size_in_edges = sizeof(VID_T) * sum_in_edges;
    size_t size_out_edges = sizeof(VID_T) * sum_out_edges;
    size_t size_localid_by_globalid =
        sizeof(VID_T) * this->get_aligned_max_vid();

    size_t total_size = size_globalid + size_indegree + size_outdegree +
                        size_in_offset + size_out_offset + size_in_edges +
                        size_out_edges + size_localid_by_globalid;
    size_t start_globalid = 0;
    size_t start_indegree = start_globalid + size_globalid;
    size_t start_outdegree = start_indegree + size_indegree;
    size_t start_in_offset = start_outdegree + size_outdegree;
    size_t start_out_offset = start_in_offset + size_in_offset;
    size_t start_in_edges = start_out_offset + size_out_offset;
    size_t start_out_edges = start_in_edges + size_in_edges;
    size_t start_localid_by_globalid = start_out_edges + size_out_edges;

    this->vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * num_vertexes);
    memset(this->vdata_, 0, sizeof(VDATA_T) * num_vertexes);
    this->buf_graph_ = (VID_T*)malloc(total_size);
    memset(this->buf_graph_, 0, total_size);

    size_t count = 0;
    auto local_id = 0;
    for (VID_T global_id = 0; global_id < this->get_aligned_max_vid();
         global_id++) {
      if (set_vertexes[global_id] == nullptr) continue;
      if (vid_map != nullptr) vid_map[global_id] = local_id;
      this->bitmap_->set_bit(global_id);
      ((VID_T*)((char*)this->buf_graph_ +
                start_localid_by_globalid))[global_id] = local_id;
      ((VID_T*)((char*)this->buf_graph_ + start_globalid))[local_id] =
          global_id;
      ((size_t*)((char*)this->buf_graph_ + start_indegree))[local_id] =
          set_vertexes[global_id]->indegree;
      ((size_t*)((char*)this->buf_graph_ + start_outdegree))[local_id] =
          set_vertexes[global_id]->outdegree;
      if (local_id == 0) {
        ((size_t*)((char*)this->buf_graph_ + start_in_offset))[local_id] = 0;
        if (set_vertexes[global_id]->indegree > 0) {
          memcpy((VID_T*)((char*)this->buf_graph_ + start_in_edges),
                 set_vertexes[global_id]->in_edges,
                 sizeof(VID_T) * set_vertexes[global_id]->indegree);
        }
        ((size_t*)((char*)this->buf_graph_ + start_out_offset))[local_id] = 0;
        if (set_vertexes[global_id]->outdegree > 0) {
          memcpy((VID_T*)((char*)this->buf_graph_ + start_out_edges),
                 set_vertexes[global_id]->out_edges,
                 sizeof(VID_T) * set_vertexes[global_id]->outdegree);
        }
      } else {
        ((size_t*)((char*)this->buf_graph_ + start_in_offset))[local_id] =
            ((size_t*)((char*)this->buf_graph_ +
                       start_indegree))[local_id - 1] +
            ((size_t*)((char*)this->buf_graph_ +
                       start_in_offset))[local_id - 1];
        if (set_vertexes[global_id]->indegree > 0) {
          size_t start =
              ((size_t*)((char*)this->buf_graph_ + start_in_offset))[local_id];
          memcpy(((char*)this->buf_graph_ + start_in_edges +
                  start * sizeof(VID_T)),
                 set_vertexes[global_id]->in_edges,
                 sizeof(VID_T) * set_vertexes[global_id]->indegree);
        }
        ((size_t*)((char*)this->buf_graph_ + start_out_offset))[local_id] =
            ((size_t*)((char*)this->buf_graph_ +
                       start_outdegree))[local_id - 1] +
            ((size_t*)((char*)this->buf_graph_ +
                       start_out_offset))[local_id - 1];

        if (set_vertexes[global_id]->outdegree > 0) {
          size_t start =
              ((size_t*)((char*)this->buf_graph_ + start_out_offset))[local_id];
          memcpy(((char*)this->buf_graph_ + start_out_edges +
                  start * sizeof(VID_T)),
                 set_vertexes[global_id]->out_edges,
                 sizeof(VID_T) * set_vertexes[global_id]->outdegree);
        }
      }
      local_id++;
    }

    globalid_by_index_ = (VID_T*)((char*)this->buf_graph_ + start_globalid);
    out_offset_ = (size_t*)((char*)this->buf_graph_ + start_out_offset);
    in_offset_ = (size_t*)((char*)this->buf_graph_ + start_in_offset);
    indegree_ = (size_t*)((char*)this->buf_graph_ + start_indegree);
    outdegree_ = (size_t*)((char*)this->buf_graph_ + start_outdegree);
    in_edges_ = (VID_T*)((char*)this->buf_graph_ + start_in_edges);
    out_edges_ = (VID_T*)((char*)this->buf_graph_ + start_out_edges);
    localid_by_globalid_ =
        (VID_T*)((char*)this->buf_graph_ + start_localid_by_globalid);

    this->num_edges_ = sum_in_edges_ + sum_out_edges_;
    this->gid_ = gid;
    this->vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * this->get_num_vertexes());
    memset(this->vdata_, 0, sizeof(VDATA_T) * this->get_num_vertexes());
    this->edata_ =
        (EDATA_T*)malloc(sizeof(EDATA_T) * this->get_num_out_edges());
    memset(this->edata_, 0, sizeof(EDATA_T) * this->get_num_out_edges());

    is_serialized_ = true;
    return;
  };

  ~ImmutableCSR() {
    if (vertexes_info_ != nullptr) {
      std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*> tmp;
      vertexes_info_->swap(tmp);
      delete vertexes_info_;
      vertexes_info_ = nullptr;
    }
    if (this->buf_graph_ != nullptr) {
      free(this->buf_graph_);
      this->buf_graph_ = nullptr;
    }
    if (this->vdata_ != nullptr) {
      free(this->vdata_);
      this->vdata_ = nullptr;
    }
    if (this->edata_ != nullptr) {
      free(this->edata_);
      this->edata_ = nullptr;
    }
    in_edges_ = nullptr;
    out_edges_ = nullptr;
    indegree_ = nullptr;
    outdegree_ = nullptr;
    in_offset_ = nullptr;
    out_offset_ = nullptr;
    globalid_by_index_ = nullptr;

    if (vertexes_state_ != nullptr) {
      free(vertexes_state_);
      vertexes_state_ = nullptr;
    }
    if (this->bitmap_ != nullptr) {
      delete this->bitmap_;
      this->bitmap_ = nullptr;
    }
    malloc_trim(0);
    return;
  };

  void CleanUp() override {
    if (this->buf_graph_ != nullptr) {
      LOG_INFO("Free:  buf_graph", this->gid_);
      free(this->buf_graph_);
      this->buf_graph_ = nullptr;
    }
    if (vertexes_info_ != nullptr) {
      LOG_INFO("Free vertexes_info: ", this->gid_);
      std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*> tmp;
      vertexes_info_->swap(tmp);
      delete vertexes_info_;
      vertexes_info_ = nullptr;
    }
    if (this->vdata_ != nullptr) {
      free(this->vdata_);
      this->vdata_ = nullptr;
    }
    localid_by_globalid_ = nullptr;
    in_edges_ = nullptr;
    out_edges_ = nullptr;
    indegree_ = nullptr;
    outdegree_ = nullptr;
    in_offset_ = nullptr;
    out_offset_ = nullptr;
    globalid_by_index_ = nullptr;
    malloc_trim(0);
  };

  void ShowGraph(const size_t count = 2) {
    std::cout << "\n\n##### ImmutableCSRGraph GID: " << this->get_gid()
              << ", num_verteses: " << this->get_num_vertexes()
              << ", sum_in_degree:" << sum_in_edges_
              << ", sum_out_degree: " << sum_out_edges_
              << ", max_vid: " << this->get_max_vid() << " #####" << std::endl;
    size_t count_ = 0;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (count_++ > count) return;
      VertexInfo&& vertex_info = GetVertexByIndex(i);
      VID_T global_id = globalid_by_index_[i];
      vertex_info.ShowVertexInfo(global_id);
    }
    std::cout << std::endl;
  }

  void ShowGraphAbs(const size_t count = 2) {
    std::cout << "\n\n##### ImmutableCSRGraph GID: " << this->get_gid()
              << ", num_verteses: " << this->get_num_vertexes()
              << ", sum_in_degree:" << sum_in_edges_
              << ", sum_out_degree: " << sum_out_edges_ << " #####"
              << std::endl;
    size_t count_ = 0;
    for (size_t i = 0; i < this->get_num_vertexes(); i++) {
      if (count_++ > count) return;
      VertexInfo&& vertex_info = GetVertexByIndex(i);
      VID_T global_id = globalid_by_index_[i];
      vertex_info.ShowVertexAbs(global_id);
    }
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByIndex(
      const size_t index) {
    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
    vertex_info.vid = index;
    vertex_info.outdegree = outdegree_[index];
    vertex_info.indegree = indegree_[index];
    vertex_info.in_edges = (in_edges_ + get_in_offset_by_index(index));
    vertex_info.out_edges = (out_edges_ + get_out_offset_by_index(index));
    vertex_info.vdata = (this->vdata_ + index);
    vertex_info.edata = (this->edata_ + get_in_offset_by_index(index));

    vertex_info.state = (vertexes_state_ + index);
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>* GetPVertexByIndex(
      const size_t index) {
    auto vertex_info = new graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>;
    vertex_info->vid = index;
    vertex_info->outdegree = outdegree_[index];
    vertex_info->indegree = indegree_[index];
    vertex_info->in_edges = (in_edges_ + in_offset_[index]);
    vertex_info->out_edges = (out_edges_ + out_offset_[index]);
    vertex_info->vdata = (this->vdata_ + index);
    vertex_info->edata = (this->edata_ + in_offset_[index]);
    vertex_info->state = (vertexes_state_ + index);
    return vertex_info;
  }

  graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> GetVertexByVid(const VID_T vid) {
    graphs::VertexInfo<VID_T, VDATA_T, EDATA_T> vertex_info;
    vertex_info.vid = vid;
    size_t index = vid;
    vertex_info.outdegree = outdegree_[index];
    vertex_info.indegree = indegree_[index];
    vertex_info.in_edges = (in_edges_ + in_offset_[index]);
    vertex_info.out_edges = (out_edges_ + out_offset_[index]);
    vertex_info.edata = (this->edata_ + in_offset_[index]);
    vertex_info.vdata = (this->vdata_ + index);
    vertex_info.state = (vertexes_state_ + index);
    return vertex_info;
  }

  inline VID_T localid2globalid(const VID_T vid) const {
    assert(globalid_by_index_ != nullptr && vid < this->get_num_vertexes());
    return globalid_by_index_[vid];
  }

  inline VID_T globalid2localid(const VID_T vid) const {
    assert(localid_by_globalid_ != nullptr);
    return localid_by_globalid_[vid];
  }

  // @brief: set Global Border vertexes in the format of Bitmap.
  // @param: global_border_vid_map is a bitmap that indicate whether a vertex
  // belong to border vertexes. It is shared by all the fragments.
  // is_in_bucketX store those vertexes that belong to border vertexes for each
  // of fragment.
  // num_partitions is the number of total fragments
  void SetGlobalBorderVidMap(Bitmap* global_border_vid_map = nullptr,
                             Bitmap** is_in_bucketX = nullptr,
                             const size_t num_partitions = 1) {
    assert(global_border_vid_map != nullptr);
    assert(is_in_bucketX != nullptr);
    assert(num_partitions > 0);

    LOG_INFO("SetGlobalBorderVidMap, GID: ", this->get_gid());
    for (VID_T local_id = 0; local_id < this->get_num_vertexes(); local_id++) {
      graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>&& vertex_info =
          GetVertexByIndex(local_id);
      for (size_t i = 0; i < vertex_info.indegree; i++) {
        for (GID_T gid = 0; gid < num_partitions; gid++) {
          if (gid == this->get_gid()) continue;
          if (is_in_bucketX[gid] == nullptr) continue;
          if (is_in_bucketX[gid]->get_bit(vertex_info.in_edges[i]) &&
              global_border_vid_map->get_bit((vertex_info.in_edges[i])) == 0) {
            global_border_vid_map->set_bit((vertex_info.in_edges[i]));
          }
        }
      }
      for (size_t i = 0; i < vertex_info.outdegree; i++) {
        for (GID_T gid = 0; gid < num_partitions; gid++) {
          if (gid == this->get_gid()) continue;
          if (is_in_bucketX[gid] == nullptr) continue;
          if (is_in_bucketX[gid]->get_bit(vertex_info.out_edges[i]) &&
              global_border_vid_map->get_bit((vertex_info.out_edges[i])) == 0) {
            global_border_vid_map->set_bit((vertex_info.out_edges[i]));
          }
        }
      }
    }
    return;
  }

  void Sort(size_t cores = 1) {
    auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);
    std::mutex mtx;
    std::condition_variable finish_cv;
    std::unique_lock<std::mutex> lck(mtx);
    std::atomic<size_t> pending_packages(cores);

    pending_packages.store(cores);
    for (size_t i = 0; i < cores; i++) {
      thread_pool.Commit([&, i, &cores, &pending_packages, &finish_cv]() {
        for (size_t j = i; j < this->get_num_vertexes(); j += cores) {
          auto u = GetVertexByIndex(j);
          QuickSort(u.out_edges, 0, u.outdegree - 1);
          QuickSort(u.in_edges, 0, u.indegree - 1);
        }
        if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        return;
      });
    }
    finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
    return;
  }

  size_t get_num_in_edges() { return sum_in_edges_; }
  size_t get_num_out_edges() { return sum_out_edges_; }

  void set_num_in_edges(const size_t n) { sum_in_edges_ = n; }
  void set_num_out_edges(const size_t n) { sum_out_edges_ = n; }

  size_t get_out_offset_by_index(size_t i) {
    return out_offset_[i] - out_offset_base_;
  };
  size_t get_in_offset_by_index(size_t i) {
    return in_offset_[i] - in_offset_base_;
  };
  void set_out_offset_base(size_t base) { out_offset_base_ = base; };
  void set_in_offset_base(size_t base) { in_offset_base_ = base; };

  ImmutableCSR* GetClassType(void) override { return this; }

 public:
  size_t sum_in_edges_ = 0;
  size_t sum_out_edges_ = 0;

  bool is_serialized_ = false;

  // serialized data in CSR format.
  VID_T* localid_by_globalid_ = nullptr;
  VID_T* globalid_by_index_ = nullptr;
  VID_T* in_edges_ = nullptr;
  VID_T* out_edges_ = nullptr;
  size_t* indegree_ = nullptr;
  size_t* outdegree_ = nullptr;
  size_t* in_offset_ = nullptr;
  size_t* out_offset_ = nullptr;

  size_t in_offset_base_ = 0;
  size_t out_offset_base_ = 0;

  char* vertexes_state_ = nullptr;
  std::map<VID_T, graphs::VertexInfo<VID_T, VDATA_T, EDATA_T>*>*
      vertexes_info_ = nullptr;
};

}  // namespace graphs
}  // namespace minigraph
#endif  // MINIGRAPH_GRAPHS_IMMUTABLECSR_H
