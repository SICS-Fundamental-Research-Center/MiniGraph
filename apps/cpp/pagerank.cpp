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
class PRVMap : public minigraph::VMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  PRVMap(const CONTEXT_T& context)
      : minigraph::VMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool C(const VertexInfo& u) override { return true; }
  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override {
    float next = 0;
    for (size_t i = 0; i < u.indegree; i++) {
      auto nbr_id = u.in_edges[i];
      VertexInfo&& v = graph->GetVertexByVid(nbr_id);
      next += v.vdata[0];
    }
    next = this->context_.gamma * (next / (float)u.indegree);
    if ((u.vdata[0] - next) * (u.vdata[0] - next) > this->context_.epsilon) {
      u.vdata[0] = next;
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class PREMap : public minigraph::EMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  PREMap(const CONTEXT_T& context)
      : minigraph::EMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool F(const VertexInfo& u, VertexInfo& v) override { return false; }
  bool C(const VertexInfo& u, const VertexInfo& v) override { return false; }
};

template <typename GRAPH_T, typename CONTEXT_T>
class PRPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  PRPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
        minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap, const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
    Frontier* frontier_in = new Frontier(graph.get_num_vertexes() + 10);
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      frontier_in->enqueue(graph.GetVertexByIndex(i));
    }
    size_t iter = 0;
    while (!frontier_in->empty()) {
      if (iter++ > this->context_.num_iter) {
        break;
      }
      frontier_in = this->vmap_->Map(frontier_in, visited, graph, task_runner);
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
        new Frontier(this->global_border_vertexes_info_->size() + 1);

    for (auto& iter : *this->global_border_vertexes_info_) {
      frontier_in->enqueue(*iter.second);
    }
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
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
  size_t num_iter = 0;
  float epsilon = 0.01;
  float gamma = 0.5;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using PRPIE_T = PRPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  Context context;
  context.num_iter = FLAGS_iter;
  auto pr_emap = new PREMap<CSR_T, Context>(context);
  auto pr_vmap = new PRVMap<CSR_T, Context>(context);
  auto pr_pie = new PRPIE<CSR_T, Context>(pr_vmap, pr_emap, context);
  auto app_wrapper =
      new AppWrapper<PRPIE<CSR_T, Context>, gid_t, vid_t, vdata_t, edata_t>(
          pr_pie);

  minigraph::MiniGraphSys<CSR_T, PRPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores, app_wrapper);
  minigraph_sys.RunSys();
  minigraph_sys.ShowResult();
  gflags::ShutDownCommandLineFlags();
}