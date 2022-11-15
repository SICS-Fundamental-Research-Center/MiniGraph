#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"
#include <folly/concurrency/DynamicBoundedQueue.h>

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  SSSPAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByVid(i);
      graph->vdata_[i] = VDATA_MAX;
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;

 public:
  SSSPPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
          minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
          const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  SSSPPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
          const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

  enum DijikstraState { LABELED, UNLABELED, SCANNED };

  class MinHeap {
    size_t capacity_ = 0;

   public:
    size_t heap_size_ = 0;
    VertexInfo** harr_ = nullptr;

    MinHeap(size_t capacity) {
      heap_size_ = 0;
      capacity_ = capacity;
      harr_ = new VertexInfo*[capacity_];
    }

    int parent(int i) { return (i - 1) / 2; }

    int left(int i) { return (2 * i + 1); }

    int right(int i) { return (2 * i + 2); }

    int getMin() { return harr_[0]->vdata[0]; }

    void insertKey(VertexInfo* k) {
      if (heap_size_ == capacity_) {
        LOG_ERROR("\nOverflow: Could not insertKey\n");
        return;
      }
      heap_size_++;
      int i = heap_size_ - 1;
      harr_[i] = k;
      while (i != 0 && harr_[parent(i)]->vdata[0] > harr_[i]->vdata[0]) {
        swap(&harr_[i], &harr_[parent(i)]);
        i = parent(i);
      }
    }

    VertexInfo* extractMin() {
      if (heap_size_ <= 0) return nullptr;
      if (heap_size_ == 1) {
        heap_size_--;
        return harr_[0];
      }
      VertexInfo* root = harr_[0];
      harr_[0] = harr_[heap_size_ - 1];
      heap_size_--;
      MinHeapify(0);
      return root;
    }

    void swap(VertexInfo** x, VertexInfo** y) {
      VertexInfo* temp = *x;
      *x = *y;
      *y = temp;
    }

    void MinHeapify(int i) {
      int l = left(i);
      int r = right(i);
      int smallest = i;
      if (l < heap_size_ && harr_[l]->vdata[0] < harr_[i]->vdata[0])
        smallest = l;
      if (r < heap_size_ && harr_[r]->vdata[0] < harr_[smallest]->vdata[0])
        smallest = r;
      if (smallest != i) {
        swap(&harr_[i], &harr_[smallest]);
        MinHeapify(smallest);
      }
    }

    void decreaseKey(int new_val, VertexInfo* v) {
      int i;
      v->vdata[0] = new_val;

      for (i = 0; i < capacity_; i++) {
        if (harr_[i] == v) break;
      }
      if (i = capacity_) {
        // insertKey(v);
        return;
      } else {
        while (i != 0 && harr_[parent(i)]->vdata[0] > harr_[i]->vdata[0]) {
          swap(&harr_[i], &harr_[parent(i)]);
          i = parent(i);
        }
      }
    }
  };

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    // LOG_INFO("Init() - Processing gid: ", graph.gid_);
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(graph, task_runner, visited,
                               SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);
    memset(graph.vertexes_state_, VERTEXUNLABELED, graph.get_num_vertexes());
    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    if (!graph.IsInGraph(this->context_.root_id)) {
      LOG_INFO("PEval() - Skip gid: ", graph.gid_);
      return false;
    };
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    VID_T* vid_map = this->msg_mngr_->GetVidMap();
    Bitmap* global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    VDATA_T* global_vdata = this->msg_mngr_->GetGlobalVdata();
    MinHeap h(graph.get_num_vertexes());

    auto u = graph.GetPVertexByVid(vid_map[this->context_.root_id]);
    u->vdata[0] = 0;
    h.insertKey(u);

    do {
      auto v = h.extractMin();
      if (global_border_vid_map->get_bit(graph.localid2globalid(v->vid)))
        write_min(global_vdata + graph.localid2globalid(v->vid), v->vdata[0]);
      if (v == nullptr) break;
      *v->state = VERTEXSCANNED;
      for (size_t i = 0; i < v->outdegree; i++) {
        if (!graph.IsInGraph(v->out_edges[i])) continue;
        auto nbr_local_id = vid_map[v->out_edges[i]];
        auto nbr = graph.GetPVertexByVid(nbr_local_id);
        if (*nbr->state != VERTEXSCANNED) {
          if (*nbr->state == VERTEXUNLABELED) {
            *nbr->state = VERTEXLABELED;
            nbr->vdata[0] = v->vdata[0] + 1;
            h.insertKey(nbr);
            if (global_border_vid_map->get_bit(v->out_edges[i])) {
              write_min(global_vdata + v->out_edges[i], nbr->vdata[0]);
            }
          } else if (v->vdata[0] + 1 < nbr->vdata[0]) {
            nbr->vdata[0] = v->vdata[0] + 1;
            h.decreaseKey(v->vdata[0] + 1, nbr);
            if (global_border_vid_map->get_bit(v->out_edges[i]))
              write_min(global_vdata + v->out_edges[i], nbr->vdata[0]);
          }
        }
      }
      delete v;
    } while (h.heap_size_);

    // graph.ShowGraph(100);
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    memset(graph.vertexes_state_, VERTEXUNLABELED, graph.get_num_vertexes());
    VID_T* vid_map = this->msg_mngr_->GetVidMap();
    Bitmap* global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    VDATA_T* global_border_vdata = this->msg_mngr_->GetGlobalVdata();
    MinHeap h(graph.get_num_vertexes());
    Bitmap visited(graph.max_vid_);
    visited.clear();
    auto global_vdata = this->msg_mngr_->GetGlobalVdata();
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      auto u = graph.GetPVertexByIndex(i);
      bool tag = false;
      if (global_vdata[graph.localid2globalid(u->vid)] < u->vdata[0]) {
        write_min(u->vdata,
                  global_border_vdata[graph.localid2globalid(u->vid)]);
        tag = true;
      }
      for (size_t nbr_i = 0; nbr_i < u->indegree; nbr_i++) {
        if (global_border_vid_map->get_bit(u->in_edges[nbr_i]) == 0) continue;
        if (global_border_vdata[u->in_edges[nbr_i]] + 1 < u->vdata[0]) {
          u->vdata[0] = global_border_vdata[u->in_edges[nbr_i]] + 1;
          visited.set_bit(u->vid);
          tag == true ? 0 : tag = true;
          if (global_border_vid_map->get_bit(graph.localid2globalid(u->vid)))
            write_min(global_border_vdata + graph.localid2globalid(u->vid),
                      u->vdata[0]);
        }
      }
      if (!tag) {
        delete u;
      } else {
        h.insertKey(u);
        do {
          auto v = h.extractMin();
          if (v == nullptr) break;
          *v->state = VERTEXSCANNED;
          for (size_t i = 0; i < v->outdegree; i++) {
            if (!graph.IsInGraph(v->out_edges[i])) {
              if ((global_vdata[v->out_edges[i]] > v->vdata[0] + 1)) {
                write_min(global_vdata + v->out_edges[i], v->vdata[0] + 1);
                visited.set_bit(v->out_edges[i]);
              }
              continue;
            }
            auto nbr_local_id = vid_map[v->out_edges[i]];
            auto nbr = graph.GetPVertexByVid(nbr_local_id);
            if (*nbr->state != VERTEXSCANNED) {
              if (*nbr->state == VERTEXUNLABELED) {
                *nbr->state = VERTEXLABELED;
                nbr->vdata[0] = v->vdata[0] + 1;
                visited.set_bit(nbr->vid);
                h.insertKey(nbr);
                if (global_border_vid_map->get_bit(v->out_edges[i]))
                  write_min(global_border_vdata + v->out_edges[i],
                            nbr->vdata[0]);
              } else if (v->vdata[0] + 1 < nbr->vdata[0]) {
                nbr->vdata[0] = v->vdata[0] + 1;
                h.decreaseKey(v->vdata[0] + 1, nbr);
                if (global_border_vid_map->get_bit(v->out_edges[i]))
                  write_min(global_border_vdata + v->out_edges[i],
                            nbr->vdata[0]);
              }
            }
          }
          delete v;
        } while (h.heap_size_);
      };
    }
    LOG_INFO("#");
    return !visited.empty();
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  size_t root_id = 12;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using SSSPPIE_T = SSSPPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;

  Context context;
  context.root_id = FLAGS_root;
  auto wcc_auto_map = new SSSPAutoMap<CSR_T, Context>();
  auto bfs_pie = new SSSPPIE<CSR_T, Context>(wcc_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<SSSPPIE<CSR_T, Context>, CSR_T>(bfs_pie);

  minigraph::MiniGraphSys<CSR_T, SSSPPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(100);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}