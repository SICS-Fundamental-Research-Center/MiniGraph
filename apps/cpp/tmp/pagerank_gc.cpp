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
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  PREMap(const CONTEXT_T& context)
      : minigraph::EMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool F(const VertexInfo& u, VertexInfo& v) override { return false; }
  bool C(const VertexInfo& u, const VertexInfo& v) override { return false; }

  static bool kernel_pull_border_vertexes(
      size_t tid, Frontier* frontier_out, VertexInfo& u, GRAPH_T* graph,
      std::unordered_map<VID_T, VDATA_T>* global_border_vertexes_vdata,
      bool* visited, float gamma, float epsilon) {
    bool tag = false;
    float next = 0;
    size_t count = 0;
    for (size_t i = 0; i < u.indegree; i++) {
      auto u_local_id = graph->globalid2localid(u.in_edges[i]);
      if (u_local_id != VID_MAX) {
        next += *(graph->GetVertexByVid(u_local_id).vdata);
        count++;
      } else {
        auto iter = global_border_vertexes_vdata->find(u.in_edges[i]);
        if (iter != global_border_vertexes_vdata->end()) {
          next += iter->second;
        }
      }
    }
    next = gamma * (next / (float)count);
    if ((u.vdata[0] - next) * (u.vdata[0] - next) > epsilon) {
      u.vdata[0] = next;
      frontier_out->enqueue(u);
      tag = true;
      visited[u.vid] = 1;
    }
    return tag;
  }
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

  bool Init(GRAPH_T& graph) override { return true; }

  bool PEval(GRAPH_T& graph,
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
    auto tag = this->msg_mngr_->UpdateBorderVertexes(graph, visited);
    free(visited);
    return tag;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    Frontier* frontier_in = new Frontier(graph.get_num_vertexes() + 1);
    auto global_border_vertexes_vdata =
        *this->msg_mngr_->border_vertexes_->GetBorderVertexVdata();

    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      VertexInfo&& u = graph.GetVertexByIndex(i);
      float next = 0;
      size_t count = 0;
      for (size_t j = 0; j < u.indegree; j++) {
        auto u_local_id = graph.globalid2localid(u.in_edges[i]);
        if (u_local_id != VID_MAX) {
          next += *(graph.GetVertexByVid(u_local_id).vdata);
          count++;
        } else {
          auto iter = global_border_vertexes_vdata.find(u.in_edges[i]);
          if (iter != global_border_vertexes_vdata.end()) {
            next += iter->second;
            count++;
          }
        }
      }
      next = this->context_.gamma * (next / (float)count);
      auto diff = (u.vdata[0] - next) * (u.vdata[0] - next);
      if (diff == std::numeric_limits<float>::infinity()) continue;
      if (diff > this->context_.epsilon) {
        u.vdata[0] = next;
        visited[u.vid] = 1;
      }
    }

    auto tag = false;
    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      frontier_in->enqueue(graph.GetVertexByIndex(i));
    }
    VertexInfo u;
    size_t n = this->context_.num_iter * graph.get_num_vertexes();
    while (!frontier_in->empty()) {
      if (--n == 0) {
        free(frontier_in);
        break;
      }
      frontier_in->dequeue(u);
      float next = 0;
      size_t count = 0;
      for (size_t i = 0; i < u.indegree; i++) {
        auto u_local_id = graph.globalid2localid(u.in_edges[i]);
        if (u_local_id != VID_MAX) {
          next += *(graph.GetVertexByVid(u_local_id).vdata);
          count++;
        } else {
          auto iter = global_border_vertexes_vdata.find(u.in_edges[i]);
          if (iter != global_border_vertexes_vdata.end()) {
            next += iter->second;
          }
        }
      }
      next = this->context_.gamma * (next / (float)count);
      auto diff = (u.vdata[0] - next) * (u.vdata[0] - next);
      if (diff == std::numeric_limits<float>::infinity()) continue;
      if (diff > this->context_.epsilon) {
        u.vdata[0] = next;
        frontier_in->enqueue(u);
        visited[u.vid] = 1;
      }
    }
    tag = this->msg_mngr_->UpdateBorderVertexes(graph, visited);
    LOG_INFO(tag);
    free(visited);
    return tag;
  }
};

struct Context {
  size_t num_iter = 0;
  float epsilon = 0.01;
  float gamma = 0.03;
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
  size_t buffer_size = FLAGS_buffer_size;
  Context context;
  context.num_iter = FLAGS_iter;
  auto pr_emap = new PREMap<CSR_T, Context>(context);
  auto pr_vmap = new PRVMap<CSR_T, Context>(context);
  auto pr_pie = new PRPIE<CSR_T, Context>(pr_vmap, pr_emap, context);
  auto app_wrapper = new minigraph::AppWrapper<PRPIE<CSR_T, Context>, gid_t,
                                               vid_t, vdata_t, edata_t>(pr_pie);

  minigraph::MiniGraphSys<CSR_T, PRPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper);
  minigraph_sys.RunSys();
  minigraph_sys.ShowResult();
  gflags::ShutDownCommandLineFlags();
}