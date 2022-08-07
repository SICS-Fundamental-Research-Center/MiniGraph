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
class WCCAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  WCCAutoMap(const CONTEXT_T& context)
      : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>(context) {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return write_min(v.vdata, u.vdata[0]);
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override { return false; }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByVid(i);
      graph->vdata_[i] = graph->localid2globalid(u.vid);
    }
    return true;
  }

  static bool kernel_push_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (visited->get_bit(i) == 0) continue;
      if (global_border_vid_map->get_bit(tid) == 0) continue;
      auto u = graph->GetVertexByVid(tid);
      auto global_id = graph->localid2globalid(u.vid);
      write_min((global_border_vdata + global_id), u.vdata[0]);
    }
    return true;
  }

  static bool kernel_pull_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* in_visited,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata,
                                          bool* active) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      for (size_t nbr_i = 0; nbr_i < u.indegree; nbr_i++) {
        if (global_border_vid_map->get_bit(u.in_edges[nbr_i]) == 0) continue;
        if (global_border_vdata[u.in_edges[nbr_i]] < u.vdata[0]) {
          u.vdata[0] = global_border_vdata[u.in_edges[nbr_i]];
          in_visited->set_bit(u.vid);
          visited->set_bit(u.vid);
          *active ? 0 : *active = true;
        }
      }
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class WCCPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  WCCPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
         minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  WCCPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

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
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    if (!graph.IsInGraph(this->context_.root_id)) return false;
    Bitmap* global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    VID_T* vid_map = this->msg_mngr_->GetVidMap();
    VDATA_T* global_vdata = this->msg_mngr_->GetGlobalVdata();
    Bitmap visited(graph.max_vid_);
    visited.clear();

    // process root_vertex
    auto local_root_id = vid_map[this->context_.root_id];
    Frontier frontier(graph.get_num_vertexes() + 64);
    frontier.enqueue(graph.GetVertexByVid(local_root_id));
    VertexInfo u;
    visited.set_bit(local_root_id);
    while (!frontier.empty()) {
      frontier.dequeue(u);
      if (global_border_vid_map->get_bit(graph.localid2globalid(u.vid)))
        write_min(global_vdata + graph.localid2globalid(u.vid), u.vdata[0]);
      for (size_t i = 0; i < u.outdegree; i++) {
        if (!graph.IsInGraph(u.out_edges[i])) continue;
        if (visited.get_bit(vid_map[u.out_edges[i]]) != 0) continue;
        auto local_nbr_id = vid_map[u.out_edges[i]];
        auto v = graph.GetVertexByVid(vid_map[u.out_edges[i]]);
        if (v.vdata[0] > u.vdata[0]) {
          v.vdata[0] = u.vdata[0];
          frontier.enqueue(v);
          visited.set_bit(v.vid);
        }
      }
    }

    // process the rest of vertexes.
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      auto v = graph.GetVertexByIndex(i);
      if (visited.get_bit(v.vid) != 0) continue;
      frontier.enqueue(v);
      while (!frontier.empty()) {
        frontier.dequeue(u);
        if (global_border_vid_map->get_bit(graph.localid2globalid(u.vid)))
          write_min(global_vdata + graph.localid2globalid(u.vid), u.vdata[0]);

        for (size_t j = 0; j < u.outdegree; j++) {
          if (!graph.IsInGraph(u.out_edges[j])) continue;
          if (visited.get_bit(vid_map[u.out_edges[j]]) != 0) continue;
          auto v = graph.GetVertexByVid(vid_map[u.out_edges[j]]);
          if (v.vdata[0] > u.vdata[0]) {
            v.vdata[0] = u.vdata[0];
            frontier.enqueue(v);
            visited.set_bit(v.vid);
          }
        }
      }
    }
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    Bitmap* global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    VID_T* vid_map = this->msg_mngr_->GetVidMap();
    VDATA_T* global_vdata = this->msg_mngr_->GetGlobalVdata();
    Frontier frontier(graph.get_num_vertexes() + 64);
    Bitmap visited(graph.max_vid_);
    visited.clear();
    VertexInfo u;
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      auto u = graph.GetVertexByIndex(i);
      if (visited.get_bit(u.vid) != 0) continue;
      VDATA_T tmp_vdata = u.vdata[0];
      for (size_t j = 0; j < u.indegree; j++) {
        if (global_border_vid_map->get_bit(u.in_edges[j]) == 0) continue;
        global_vdata[u.in_edges[j]] < tmp_vdata
            ? tmp_vdata = global_vdata[u.in_edges[j]]
            : 0;
      }
      if (tmp_vdata < u.vdata[0]) {
        u.vdata[0] = tmp_vdata;
        frontier.enqueue(u);
        while (!frontier.empty()) {
          frontier.dequeue(u);
          if (global_border_vid_map->get_bit(graph.localid2globalid(u.vid)))
            write_min(global_vdata + graph.localid2globalid(u.vid), u.vdata[0]);
          for (size_t j = 0; j < u.outdegree; j++) {
            if (!graph.IsInGraph(u.out_edges[j])) continue;
            if (visited.get_bit(vid_map[u.out_edges[j]]) != 0) continue;
            auto v = graph.GetVertexByVid(vid_map[u.out_edges[j]]);

            if (v.vdata[0] > u.vdata[0]) {
              v.vdata[0] = u.vdata[0];
              frontier.enqueue(v);
              visited.set_bit(v.vid);
            }
          }
        }
      }
    }

    // process the rest of vertexes.
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      auto v = graph.GetVertexByIndex(i);
      if (visited.get_bit(v.vid) != 0) continue;
      frontier.enqueue(v);
      while (!frontier.empty()) {
        frontier.dequeue(u);
        if (global_border_vid_map->get_bit(graph.localid2globalid(u.vid)))
          write_min(global_vdata + graph.localid2globalid(u.vid), u.vdata[0]);
        for (size_t j = 0; j < u.outdegree; j++) {
          if (!graph.IsInGraph(u.out_edges[j])) continue;
          if (visited.get_bit(vid_map[u.out_edges[j]]) != 0) continue;
          auto v = graph.GetVertexByVid(vid_map[u.out_edges[j]]);
          if (v.vdata[0] > u.vdata[0]) {
            v.vdata[0] = u.vdata[0];
            frontier.enqueue(v);
            visited.set_bit(v.vid);
          }
        }
      }
    }
    return !visited.empty();
  }
};

struct Context {
  size_t root_id = 0;
};

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
  auto wcc_auto_map = new WCCAutoMap<CSR_T, Context>(context);
  auto bfs_pie = new WCCPIE<CSR_T, Context>(wcc_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<WCCPIE<CSR_T, Context>, CSR_T>(bfs_pie);

  minigraph::MiniGraphSys<CSR_T, WCCPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(2);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}