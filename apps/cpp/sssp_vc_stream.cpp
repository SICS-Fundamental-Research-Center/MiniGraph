#include "2d_pie/auto_app_base.h"
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
    return write_min(v.vdata, u.vdata[0] + 1);
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step, VDATA_T* vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      graph->vdata_[i] = VDATA_MAX;
      // vdata[graph->localid2globalid(i)] = VDATA_MAX;
    }
    return true;
  }

  static bool kernel_init_vdata(const size_t tid, const size_t step,
                                const size_t scope, VDATA_T* vdata) {
    for (size_t i = tid; i < scope; i += step) vdata[i] = VDATA_MAX;
    return true;
  }

  static void kernel_update(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                            const size_t step, Bitmap* out_visited,
                            VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      for (size_t j = 0; j < u.outdegree; ++j) {
        if (write_min(&global_border_vdata[u.out_edges[j]],
                      global_border_vdata[graph->localid2globalid(i)] + 1)) {
          out_visited->set_bit(u.out_edges[j]);
          visited->set_bit(i);
        }
      }
    }
    return;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SSSPPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
          const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("Init() - Processing gid: ", graph.gid_);
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(graph, task_runner, visited,
                               SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_init,
                               this->msg_mngr_->GetGlobalVdata());

    auto local_t = __sync_fetch_and_add(&this->context_.t, (unsigned)1);
    if (local_t == 0) {
      auto vdata = this->msg_mngr_->GetGlobalVdata();
      memset(
          vdata, 1,
          sizeof(typename GRAPH_T::vdata_t) * this->msg_mngr_->get_max_vid());
      this->auto_map_->template ParallelDo(
          task_runner, SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_init_vdata,
          this->msg_mngr_->get_max_vid(), this->msg_mngr_->GetGlobalVdata());
    }
    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    if (!graph.IsInGraph(this->context_.root_id)) return false;
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    auto vid_map = this->msg_mngr_->GetVidMap();

    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->clear();
    out_visited->clear();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();

    auto u = graph.GetVertexByVid(vid_map[this->context_.root_id]);
    u.vdata[0] = 0;
    in_visited->set_bit(vid_map[this->context_.root_id]);
    visited.set_bit(vid_map[this->context_.root_id]);
    while (!in_visited->empty()) {
      this->auto_map_->ActiveMap(graph, task_runner, &visited,
                                 SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
                                 out_visited,
                                 this->msg_mngr_->GetGlobalVdata());
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    delete in_visited;
    delete out_visited;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);

    auto vid_map = this->msg_mngr_->GetVidMap();

    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();

    while (!in_visited->empty()) {
      this->auto_map_->ActiveMap(graph, task_runner, &visited,
                                 SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
                                 out_visited,
                                 this->msg_mngr_->GetGlobalVdata());
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    delete in_visited;
    delete out_visited;
    return !visited.empty();
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  size_t root_id = 12;
  unsigned t = 0;
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
  auto sssp_auto_map = new SSSPAutoMap<CSR_T, Context>();
  auto sssp_pie = new SSSPPIE<CSR_T, Context>(sssp_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<SSSPPIE<CSR_T, Context>, CSR_T>(sssp_pie);

  minigraph::MiniGraphSys<CSR_T, SSSPPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(3);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}