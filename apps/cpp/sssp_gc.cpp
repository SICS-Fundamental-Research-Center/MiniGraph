#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <condition_variable>

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPVMap : public minigraph::VMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SSSPVMap(const CONTEXT_T& context)
      : minigraph::VMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool C(const VertexInfo& u) override { return false; }
  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override { return false; }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPEMap : public minigraph::EMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SSSPEMap(const CONTEXT_T& context)
      : minigraph::EMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool F(const VertexInfo& u, VertexInfo& v) override {
    v.vdata[0] = u.vdata[0] + 1;
    return true;
  }

  bool C(const VertexInfo& u, const VertexInfo& v) override {
    if (u.vdata[0] + 1 < v.vdata[0]) {
      return true;
    } else {
      return false;
    }
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

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
             minigraph::executors::TaskRunner* task_runner) override {
    auto local_id = graph.globalid2localid(this->context_.root_id);
    if (local_id == VID_MAX) {
      LOG_INFO("PEval() - Discarding gid: ", graph.gid_);
      return false;
    }
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
    Frontier* frontier_in = new Frontier(graph.get_num_vertexes() + 1024);
    VertexInfo&& vertex_info = graph.GetVertexByVid(local_id);
    vertex_info.vdata[0] = 1;
    visited[local_id] = true;
    frontier_in->enqueue(vertex_info);
    VertexInfo u;
    while (!frontier_in->empty()) {
      frontier_in->dequeue(u);
      for (size_t i = 0; i < u.outdegree; i++) {
        auto local_id = graph.globalid2localid(u.out_edges[i]);
        if (local_id == VID_MAX || visited[local_id]) {
          continue;
        } else {
          VertexInfo&& v = graph.GetVertexByVid(local_id);
          if (v.vdata[0] > u.vdata[0] + 1) {
            v.vdata[0] = u.vdata[0] + 1;
            visited[local_id] = true;
            frontier_in->enqueue(v);
          }
        }
      }
      frontier_in = this->emap_->Map(frontier_in, visited, graph, task_runner);
    }
    bool tag = this->GetPartialBorderResult(graph, visited, partial_result);
    MsgAggr(partial_result);
    free(visited);
    return tag;
  }

  bool IncEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
               minigraph::executors::TaskRunner* task_runner) override {
    if (this->global_border_vertexes_info_->size() == 0) {
      LOG_INFO("IncEval() - Discarding gid: ", graph.gid_);
      return false;
    }
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    Frontier* frontier_in =
        new Frontier(this->global_border_vertexes_info_->size() +
                     graph.get_num_vertexes() + 1024);

    for (auto& iter : *this->global_border_vertexes_info_) {
      frontier_in->enqueue(*iter.second);
    }
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
    VertexInfo u;
    while (!frontier_in->empty()) {
      frontier_in = this->emap_->Map(frontier_in, visited, graph, task_runner);
    }
    auto tag = this->GetPartialBorderResult(graph, visited, partial_result);
    MsgAggr(partial_result);
    free(visited);
    return tag;
  }

  bool MsgAggr(PARTIAL_RESULT_T* partial_result) override {
    if (partial_result->size() == 0) {
      return false;
    }
    for (auto iter = partial_result->begin(); iter != partial_result->end();
         iter++) {
      auto iter_global = this->global_border_vertexes_info_->find(iter->first);
      if (iter_global != this->global_border_vertexes_info_->end()) {
        if (iter_global->second->vdata[0] != 1) {
          iter_global->second->UpdateVdata(1);
        }
      } else {
        VertexInfo* vertex_info = new VertexInfo(iter->second);
        this->global_border_vertexes_info_->insert(
            std::make_pair(iter->first, vertex_info));
      }
    }
    return true;
  }
};

struct Context {
  size_t root_id = 0;
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
  Context context;
  auto sssp_edge_map = new SSSPEMap<CSR_T, Context>(context);
  auto sssp_vertex_map = new SSSPVMap<CSR_T, Context>(context);
  auto sssp_pie =
      new SSSPPIE<CSR_T, Context>(sssp_vertex_map, sssp_edge_map, context);
  auto app_wrapper =
      new AppWrapper<SSSPPIE<CSR_T, Context>, gid_t, vid_t, vdata_t, edata_t>(
          sssp_pie);

  minigraph::MiniGraphSys<CSR_T, SSSPPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores, app_wrapper);
  minigraph_sys.RunSys();
  minigraph_sys.ShowResult();
  gflags::ShutDownCommandLineFlags();
}