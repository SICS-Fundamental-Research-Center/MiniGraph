#include <folly/concurrency/DynamicBoundedQueue.h>
#include "jemalloc/jemalloc.h"

#include "2d_pie/auto_app_base.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"

class MST {
  using GID_T = gid_t;
  using VID_T = vid_t;
  using VDATA_T = vdata_t;
  using EDATA_T = edata_t;

 private:
  size_t* offset_ = nullptr;
  VID_T* buffer_ = nullptr;
  size_t curr_offset_ = 0;

 public:
  MST(size_t num_vertexes) {
    buffer_ = (VID_T*)malloc(sizeof(VID_T) * 2 *
                             ceil(num_vertexes / ALIGNMENT_FACTOR) *
                             ALIGNMENT_FACTOR);
    offset_ =
        (size_t*)malloc(sizeof(size_t) * ceil(num_vertexes / ALIGNMENT_FACTOR) *
                        ALIGNMENT_FACTOR);
    memset(offset_, 0,
           sizeof(size_t) * ceil(num_vertexes / ALIGNMENT_FACTOR) *
               ALIGNMENT_FACTOR);
    return;
  }

  ~MST() {
    free(offset_);
    free(buffer_);
    return;
  }

  // Append an edge with minimum-weight outgoing edge.
  void Append(const VID_T& src, const VID_T& dst) {
    if (offset_[dst] != 0 &&
        *(buffer_ + 2 * sizeof(VID_T) * offset_[dst]) != src) {
      *(buffer_ + 2 * sizeof(VID_T) * offset_[dst]) = src;
    } else if (*(buffer_ + 2 * sizeof(VID_T) * offset_[dst]) == src &&
               *(buffer_ + 2 * sizeof(VID_T) * offset_[dst] + 1) == dst) {
    } else {
      auto local_offset = __sync_fetch_and_add(&curr_offset_, 1);
      offset_[dst] = local_offset;
      *(buffer_ + 2 * sizeof(VID_T) * local_offset) = src;
      *(buffer_ + 2 * sizeof(VID_T) * local_offset + 1) = dst;
    }
    return;
  }

  void ShowMST() {
    for (size_t i = 0; i < curr_offset_; i++) {
      LOG_INFO(*(buffer_ + 2 * sizeof(VID_T) * i), "->",
               *(buffer_ + 2 * sizeof(VID_T) * i + 1));
    }
    return;
  }

  const inline size_t get_num_edges() { return curr_offset_; }
};

template <typename GRAPH_T, typename CONTEXT_T>
class MSTAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<std::pair<VID_T, VID_T>, false>;

 public:
  MSTAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static void kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_in_edges(); i += step)
      graph->edata_[i] = Hash(i) % 10;
    return;
  }

  static void kernel_global_init(const size_t tid, const size_t step,
                                 const size_t niters, VDATA_T* vdata) {
    for (size_t i = tid; i < niters; i += step) vdata[i] = i;
    return;
  }

  // Find and update moe for each component (fragment);
  static void kernel_choose_minimum(GRAPH_T* graph, const size_t tid,
                                    Bitmap* visited, const size_t step,
                                    Bitmap* in_visited, size_t* active_vertex,
                                    VDATA_T* vdata, VID_T* moe,
                                    EDATA_T* fragment_moe_val) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      for (size_t j = 0; j < u.indegree; j++) {
        if (vdata[u.in_edges[j]] == vdata[graph->localid2globalid(i)]) continue;
        if (write_min(fragment_moe_val + vdata[graph->localid2globalid(i)],
                      u.edata[j])) {
          moe[graph->localid2globalid(i)] = u.in_edges[j];
          in_visited->set_bit(graph->localid2globalid(i));
        }
      }
    }
    return;
  }

  // Merge two components (fragments) if there is a moe connected between them.
  static void kernel_label_prop(const size_t tid, const size_t step,
                                const size_t niters, Bitmap* in_visited,
                                Bitmap* out_visited, size_t* active_vertexes,
                                VDATA_T* vdata, VID_T* moe, MST* mst,
                                size_t* num_new_edges) {
    for (size_t i = tid; i < niters; i += step) {
      if (!in_visited->get_bit(i)) continue;
      if (moe[i] == VID_MAX) continue;
      if (write_min(vdata + i, vdata[moe[i]])) {
        out_visited->set_bit(moe[i]);
        mst->Append(moe[i], i);
        write_add(num_new_edges, (size_t)1);
        write_add(active_vertexes, (size_t)1);
      }
    }
    return;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class MSTPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using Frontier = folly::DMPMCQueue<std::pair<VID_T, VID_T>, false>;

  // an array to store minimum-weight outgoing edge~(MOE).
  VID_T* moe_ = nullptr;

  // an array to streo minimum-weight of a fragement.
  EDATA_T* fragment_moe_val_ = nullptr;

  // a container to store MST.
  MST* mst_ = nullptr;

 public:
  MSTPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {
    moe_ = (VID_T*)malloc(sizeof(VID_T) * this->context_.num_vertexes);
    memset(moe_, 1, sizeof(VID_T) * this->context_.num_vertexes);

    fragment_moe_val_ =
        (EDATA_T*)malloc(sizeof(EDATA_T) * this->context_.num_vertexes);
    memset(fragment_moe_val_, 1, sizeof(EDATA_T) * this->context_.num_vertexes);

    mst_ = new MST(this->context_.num_vertexes);

    return;
  }

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("Init:", graph.get_gid());
    auto local_t = this->context_.t;
    write_add(&this->context_.t, 1);
    if (local_t == 0) {
      LOG_INFO("Init vdata");
      auto vdata = this->msg_mngr_->GetGlobalVdata();
      memset(vdata, 0, sizeof(VID_T) * this->context_.num_vertexes);
      this->auto_map_->template ParallelDo(
          task_runner, MSTAutoMap<GRAPH_T, CONTEXT_T>::kernel_global_init,
          this->context_.num_vertexes, this->msg_mngr_->GetGlobalVdata());
    }

    LOG_INFO(graph.get_num_in_edges());
    this->auto_map_->ActiveMap(graph, task_runner, nullptr,
                               MSTAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);

    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());

    Bitmap visited(graph.get_num_vertexes());
    Bitmap* in_visited = new Bitmap(this->context_.num_vertexes);
    Bitmap* out_visited = new Bitmap(this->context_.num_vertexes);
    size_t active_vertexes = 1;
    in_visited->fill();
    out_visited->clear();
    visited.clear();
    size_t num_new_edges = 0;
    while (active_vertexes != 0) {
      in_visited->clear();
      LOG_INFO(active_vertexes);
      active_vertexes = 0;
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          MSTAutoMap<GRAPH_T, CONTEXT_T>::kernel_choose_minimum, in_visited,
          &active_vertexes, this->msg_mngr_->GetGlobalVdata(), moe_,
          fragment_moe_val_);
      while (!in_visited->empty()) {
        this->auto_map_->ParallelDo(
            task_runner, MSTAutoMap<GRAPH_T, CONTEXT_T>::kernel_label_prop,
            this->context_.num_vertexes, in_visited, out_visited,
            &active_vertexes, this->msg_mngr_->GetGlobalVdata(), moe_, mst_,
            &num_new_edges);

        std::swap(in_visited, out_visited);
        out_visited->clear();
      }
      LOG_INFO("#");
    }

    LOG_INFO("Size MST: ", mst_->get_num_edges());
    delete in_visited;
    delete out_visited;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());
    Bitmap visited(graph.get_num_vertexes());
    Bitmap* in_visited = new Bitmap(this->context_.num_vertexes);
    Bitmap* out_visited = new Bitmap(this->context_.num_vertexes);
    size_t active_vertexes = 1;
    in_visited->fill();
    out_visited->clear();
    visited.clear();
    size_t num_new_edges = 0;
    while (active_vertexes != 0) {
      in_visited->clear();
      active_vertexes = 0;
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          MSTAutoMap<GRAPH_T, CONTEXT_T>::kernel_choose_minimum, in_visited,
          &active_vertexes, this->msg_mngr_->GetGlobalVdata(), moe_,
          fragment_moe_val_);

      while (!in_visited->empty()) {
        this->auto_map_->ParallelDo(
            task_runner, MSTAutoMap<GRAPH_T, CONTEXT_T>::kernel_label_prop,
            this->context_.num_vertexes, in_visited, out_visited,
            &active_vertexes, this->msg_mngr_->GetGlobalVdata(), moe_, mst_,
            &num_new_edges);

        std::swap(in_visited, out_visited);
        out_visited->clear();
      }
    }
    // this->mst_->ShowMST();
    delete in_visited;
    delete out_visited;
    return num_new_edges > 0;
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  size_t num_vertexes;
  int t = 0;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using MSTPIE_T = MSTPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;

  assert(FLAGS_vertexes > 0);
  Context context;
  context.num_vertexes = FLAGS_vertexes;

  auto mst_auto_map = new MSTAutoMap<CSR_T, Context>();
  auto mst_pie = new MSTPIE<CSR_T, Context>(mst_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<MSTPIE<CSR_T, Context>, CSR_T>(mst_pie);

  minigraph::MiniGraphSys<CSR_T, MSTPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode, FLAGS_niters, FLAGS_scheduler);
  minigraph_sys.RunSys();
  gflags::ShutDownCommandLineFlags();
  exit(0);
}