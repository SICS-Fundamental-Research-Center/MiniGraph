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
    return write_min(v.vdata, u.vdata[0]);
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      graph->vdata_[i] = graph->localid2globalid(u.vid);
    }
    return true;
  }

  static bool kernel_push_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    if (global_border_vid_map->size_ == 0) return true;
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      if (global_border_vid_map->get_bit(graph->localid2globalid(u.vid)) == 0)
        continue;
      auto global_id = graph->localid2globalid(u.vid);
      if (write_min((global_border_vdata + global_id), u.vdata[0]))
        visited->set_bit(u.vid);
    }
    return true;
  }

  static bool kernel_pull_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* in_visited,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    if (global_border_vid_map->size_ == 0) return true;
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      if (global_border_vdata[graph->localid2globalid(u.vid)] < u.vdata[0]) {
        u.vdata[0] = global_border_vdata[graph->localid2globalid(u.vid)];
        in_visited->set_bit(u.vid);
      }
      for (size_t nbr_i = 0; nbr_i < u.indegree; nbr_i++) {
        if (global_border_vid_map->get_bit(u.in_edges[nbr_i]) == 0) continue;
        if (write_min(u.vdata, global_border_vdata[u.in_edges[nbr_i]]))
          in_visited->set_bit(u.vid);
      }
    }
    return true;
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
    LOG_INFO("Init() - Processing gid: ", graph.gid_);
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(graph, task_runner, visited,
                               WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);
    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());
    if(!graph.IsInGraph(0)) return true;
    auto vid_map = this->msg_mngr_->GetVidMap();
    auto start_time = std::chrono::system_clock::now();

    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();

    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    bool run = true;
    while (run) {
      run = this->auto_map_->ActiveEMap(in_visited, out_visited, graph,
                                        task_runner, vid_map, &visited);
      std::swap(in_visited, out_visited);
    }

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
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
    auto start_time = std::chrono::system_clock::now();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    Bitmap output_visited(graph.get_num_vertexes());
    output_visited.clear();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());

    auto vid_map = this->msg_mngr_->GetVidMap();

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes, in_visited,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    bool run = true;
    while (run) {
      run = this->auto_map_->ActiveEMap(in_visited, out_visited, graph,
                                        task_runner, vid_map, &visited);
      std::swap(in_visited, out_visited);
    }

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    delete in_visited;
    delete out_visited;
    auto visited_num = visited.get_num_bit();
    auto end_time = std::chrono::system_clock::now();
    LOG_INFO("Visited: ", visited_num, " / ", graph.get_num_vertexes(), " | ",
             graph.get_num_vertexes() / 10000);
    std::cout << "Gid " << graph.gid_ << ":  IncEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;
    if (visited_num < graph.get_num_vertexes() / 10000) {
      LOG_INFO("Jump out");
      // return false;
    }
    return !(visited_num == 0);
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
      buffer_size, app_wrapper, FLAGS_mode);
  minigraph_sys.RunSys();
  gflags::ShutDownCommandLineFlags();
  exit(0);
}