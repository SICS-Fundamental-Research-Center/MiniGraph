#include "2d_pie/auto_app_base.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"

template <typename GRAPH_T, typename CONTEXT_T>
class WCCAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  WCCAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static void kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step, VDATA_T* vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      graph->vdata_[i] = graph->localid2globalid(u.vid);
      write_min(&vdata[graph->localid2globalid(i)], graph->localid2globalid(i));
    }
    return;
  }

  static void kernel_update(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                            const size_t step, Bitmap* in_visited,
                            Bitmap* out_visited, VID_T* vid_map,
                            VDATA_T* global_border_vdata,
                            size_t* num_active_vertices) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (!in_visited->get_bit(i)) continue;
      auto u = graph->GetVertexByIndex(i);

      // for (size_t j = 0; j < u.indegree; ++j) {
      //   if (write_min(&global_border_vdata[graph->localid2globalid(i)],
      //                 global_border_vdata[u.in_edges[j]])) {
      //     out_visited->set_bit(i);
      //     write_add(num_active_vertices, (size_t)1);
      //   }
      // }

      for (size_t j = 0; j < u.outdegree; ++j) {
        if (write_min(&global_border_vdata[u.out_edges[j]],
                      global_border_vdata[graph->localid2globalid(i)])) {
          if (graph->IsInGraph(u.out_edges[j])) {
            out_visited->set_bit(vid_map[u.out_edges[j]]);
          }
          write_add(num_active_vertices, (size_t)1);
        }
      }
    }
    return;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(graph, task_runner, visited,
                               WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_init,
                               this->msg_mngr_->GetGlobalVdata());
    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());
    if (!graph.IsInGraph(0)) return true;
    auto start_time = std::chrono::system_clock::now();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();

    size_t num_active_vertices = 0;

    size_t count = 0;
    while (!in_visited->empty()) {
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_update, in_visited,
          out_visited, this->msg_mngr_->GetVidMap(),
          this->msg_mngr_->GetGlobalVdata(), &num_active_vertices);
      LOG_INFO("#", count++);
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    auto end_time = std::chrono::system_clock::now();

    LOG_INFO("End PEval: elapsed ",
             std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                   start_time)
                     .count() /
                 (double)CLOCKS_PER_SEC);

    delete in_visited;
    delete out_visited;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    auto start_time = std::chrono::system_clock::now();
    Bitmap visited(graph.get_num_vertexes());
    Bitmap output_visited(graph.get_num_vertexes());
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    output_visited.clear();
    visited.clear();
    in_visited->fill();

    size_t num_active_vertices = 0;
    while (!in_visited->empty()) {
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_update, in_visited,
          out_visited, this->msg_mngr_->GetVidMap(),
          this->msg_mngr_->GetGlobalVdata(), &num_active_vertices);
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    delete in_visited;
    delete out_visited;
    return num_active_vertices != 0;
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using WCCPIE_T = WCCPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;

  Context context;
  auto wcc_auto_map = new WCCAutoMap<CSR_T, Context>();
  auto wcc_pie = new WCCPIE<CSR_T, Context>(wcc_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<WCCPIE<CSR_T, Context>, CSR_T>(wcc_pie);

  minigraph::MiniGraphSys<CSR_T, WCCPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode, FLAGS_niters, FLAGS_scheduler);
  minigraph_sys.RunSys();
  gflags::ShutDownCommandLineFlags();
  exit(0);
}
