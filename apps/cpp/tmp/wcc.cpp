#include "2d_pie/auto_app_base.h"
#include "2d_pie/auto_map.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include <folly/concurrency/DynamicBoundedQueue.h>

template <typename GRAPH_T, typename CONTEXT_T>
class WCCAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  WCCAutoMap(const CONTEXT_T& context)
      : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>(context) {}

  bool C(const VertexInfo& u, const VertexInfo& v) override {
    // if (u.vdata[0] < v.vdata[0])
    //   return true;
    // else
    //   return false;
    return false;
  }

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    // v.vdata[0] = u.vdata[0];
    // return true;
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override {
    // bool tag = false;
    // for (size_t i = 0; i < u.indegree; i++) {
    //   auto nbr_local = graph->globalid2localid(u.in_edges[i]);
    //   if (nbr_local == VID_MAX) continue;
    //   auto nbr = graph->GetVertexByVid(nbr_local);
    //   if (nbr.vdata[0] < u.vdata[0]) {
    //     u.vdata[0] = nbr.vdata[0];
    //     tag == true ? 0 : tag = true;
    //   };
    // }
    // return tag;
    return false;
  }

  bool C(const VertexInfo& u) override { return true; }

  static bool kernel_pull_all_vertexes(GRAPH_T* graph, size_t tid,
                                       Frontier* frontier_out) {
    // auto u = graph->GetVertexByIndex(tid);
    // frontier_out->enqueue(u);
    return false;
  }

  static bool kernel_relax_all(GRAPH_T* graph, const size_t tid,
                               const size_t step,
                               folly::DMPMCQueue<size_t, false>* frontier_out,
                               bool* visited, bool* global_visited) {
    for (size_t i = tid; i < graph->get_num_edges(); i += step) {
      auto src = graph->globalid2localid(*(graph->buf_graph_ + i * 2));
      auto dst = graph->globalid2localid(*(graph->buf_graph_ + i * 2 + 1));
      if (src == VID_MAX || dst == VID_MAX) continue;
      if (graph->vdata_[src] < graph->vdata_[dst]) {
        graph->vdata_[dst] = graph->vdata_[src];
        visited[dst] == true ? 0 : visited[dst] = true;
        *global_visited == true ? 0 : *global_visited = true;
      }
    }
  }

  static bool kernel_edges_relax(GRAPH_T* graph, const size_t tid,
                                 const size_t step,
                                 folly::DMPMCQueue<size_t, false>* frontier_out,
                                 bool* visited, bool* global_visited) {
    for (size_t i = tid; i < graph->get_num_edges(); i += step) {
      auto src = graph->globalid2localid(*(graph->buf_graph_ + i * 2));
      auto dst = graph->globalid2localid(*(graph->buf_graph_ + i * 2 + 1));
      if (src == VID_MAX || dst == VID_MAX) continue;
      if (visited[src] != true) continue;
      if (graph->vdata_[src] < graph->vdata_[dst]) {
        graph->vdata_[dst] = graph->vdata_[src];
        visited[dst] == true ? 0 : visited[dst] = true;
        *global_visited == true ? 0 : *global_visited = true;
      }
    }
  }

  static bool kernel_pull_border_vertexes(
      GRAPH_T* graph, size_t tid,
      folly::DMPMCQueue<size_t, false>* frontier_out, bool* visited,
      bool* global_visited,
      std::unordered_map<typename GRAPH_T::vid_t, typename GRAPH_T::vdata_t>*
          border_vertexes_vdata) {
    auto src = graph->globalid2localid(*(graph->buf_graph_ + tid * 2));
    auto dst = graph->globalid2localid(*(graph->buf_graph_ + tid * 2 + 1));
    auto iter = border_vertexes_vdata->find(src);
    if (iter == border_vertexes_vdata->end()) return false;
    if (graph->vdata_[iter->first] < graph->vdata_[dst]) {
      graph->vdata_[dst] = iter->second;
      *visited = true;
      return true;
    } else
      return false;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  WCCPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
         minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
         minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, auto_map,
                                                   context) {}
  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

  bool Init(GRAPH_T& graph) override { return true; }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    auto frontier_out =
        new folly::DMPMCQueue<size_t, false>(graph.get_num_edges() + 1024);
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    bool* global_visited = new bool(false);
    this->auto_map_->MapAllGraph(
        graph, task_runner, WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_relax_all,
        frontier_out, visited, global_visited);
    auto tag = false;
    while (*global_visited) {
      tag = true;
      *global_visited = false;
      this->auto_map_->MapAllGraph(
          graph, task_runner,
          WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_edges_relax, frontier_out,
          visited, global_visited);
    }
    graph.ShowGraph(30);
    return tag;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    auto frontier_out =
        new folly::DMPMCQueue<size_t, false>(graph.get_num_edges() + 1024);
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    bool* global_visited = new bool(false);

    //this->auto_map_->MapAllGraph(
    //    graph, task_runner,
    //    WCCAutoMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes,
    //    frontier_out, visited, global_visited,
    //    this->msg_mngr_->border_vertexes_->GetBorderVertexVdata());

    // memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
    // Frontier* frontier_in = new Frontier(graph.get_num_vertexes() + 1);
    // for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
    //   frontier_in->enqueue(graph.GetVertexByIndex(i));
    // }
    // frontier_in = this->vmap_->Map(
    //     frontier_in, visited, graph, task_runner,
    //     WCCEMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes, &graph,
    //     this->msg_mngr_->border_vertexes_->GetBorderVertexVdata(), visited);
    // bool tag = false;

    // for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
    //   frontier_in->enqueue(graph.GetVertexByIndex(i));
    // }
    // LOG_INFO("IncEval() - Processing gid: ", graph.gid_, " frontier_size",
    // frontier_in->size()); while (!frontier_in->empty()) {
    //   frontier_in =
    //       this->auto_map_->EMap(frontier_in, visited, graph, task_runner);
    // }
    // tag = this->msg_mngr_->UpdateBorderVertexes(graph, visited);
    // free(visited);
    // return tag;
    return false;
  }
};

struct Context {
  size_t root_id = 0;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using WCCPIE_T = WCCPIE<EDGE_LIST_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;
  Context context;
  // auto wcc_emap = new WCCEMap<EDGE_LIST_T, Context>(context);
  // auto wcc_vmap = new WCCVMap<EDGE_LIST_T, Context>(context);
  auto wcc_auto_map = new WCCAutoMap<EDGE_LIST_T, Context>(context);
  auto bfs_pie = new WCCPIE<EDGE_LIST_T, Context>(wcc_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<WCCPIE<EDGE_LIST_T, Context>, EDGE_LIST_T>(
          bfs_pie);

  minigraph::MiniGraphSys<EDGE_LIST_T, WCCPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper);
  minigraph_sys.RunSys();
  minigraph_sys.ShowResult(10);
  // minigraph_sys.ShowNumComponents();
  gflags::ShutDownCommandLineFlags();
}