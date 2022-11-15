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
    return write_min(v.vdata, u.vdata[0] + 1);
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step)
      graph->vdata_[i] = VDATA_MAX;

    return true;
  }

  static bool kernel_push_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      if (global_border_vid_map->get_bit(graph->localid2globalid(u.vid)) == 0)
        continue;
      auto global_id = graph->localid2globalid(u.vid);
      if (global_border_vdata[global_id] > u.vdata[0]) {
        write_min((global_border_vdata + global_id), u.vdata[0]);
        visited->set_bit(u.vid);
      }
    }
    return true;
  }

  static bool kernel_pull_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* in_visited,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      if (global_border_vdata[graph->localid2globalid(u.vid)] + 1 <
          u.vdata[0]) {
        write_min(u.vdata,
                  global_border_vdata[graph->localid2globalid(u.vid)] + 1);
        in_visited->set_bit(u.vid);
        visited->set_bit(u.vid);
      }
      for (size_t nbr_i = 0; nbr_i < u.indegree; nbr_i++) {
        if (global_border_vid_map->get_bit(u.in_edges[nbr_i]) == 0) continue;
        if (global_border_vdata[u.in_edges[nbr_i]] + 1 < u.vdata[0]) {
          u.vdata[0] = global_border_vdata[u.in_edges[nbr_i]] + 1;
          in_visited->set_bit(u.vid);
        }
      }
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SSSPPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
          minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
          const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

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
                               SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);
    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    if (!graph.IsInGraph(this->context_.root_id)) return false;
    auto vid_map = this->msg_mngr_->GetVidMap();
    auto start_time = std::chrono::system_clock::now();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    // in_visited->fill();

    auto u = graph.GetVertexByVid(vid_map[this->context_.root_id]);
    u.vdata[0] = 0;
    in_visited->set_bit(vid_map[this->context_.root_id]);

    out_visited->clear();
    // bool active = false;
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    while (in_visited->get_num_bit()) {
      this->auto_map_->ActiveEMap(in_visited, out_visited, graph, task_runner,
                                  vid_map, &visited);
      std::swap(in_visited, out_visited);
    }
    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    auto end_time = std::chrono::system_clock::now();
    std::cout << "Gid " << graph.gid_ << ":  PEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;
    delete in_visited;
    delete out_visited;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);

    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    auto vid_map = this->msg_mngr_->GetVidMap();

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes,
        in_visited, this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());
    auto border_bit_map = this->msg_mngr_->GetGlobalBorderVidMap();

    bool run = true;
    while (run) {
      run = this->auto_map_->ActiveEMap(in_visited, out_visited, graph,
                                        task_runner, vid_map, &visited);
      std::swap(in_visited, out_visited);
    }

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SSSPAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    delete in_visited;
    delete out_visited;

    LOG_INFO("Visited: ", visited.get_num_bit());
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
  auto sssp_auto_map = new SSSPAutoMap<CSR_T, Context>();
  auto sssp_pie = new SSSPPIE<CSR_T, Context>(sssp_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<SSSPPIE<CSR_T, Context>, CSR_T>(sssp_pie);

  minigraph::MiniGraphSys<CSR_T, SSSPPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode);
  minigraph_sys.RunSys();
  //minigraph_sys.ShowResult(3);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}