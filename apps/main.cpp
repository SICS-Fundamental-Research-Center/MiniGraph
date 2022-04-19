#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <condition_variable>

template <typename GRAPH_T, typename CONTEXT_T>
class BFSVertexMap : public minigraph::VertexMapBase<GRAPH_T, CONTEXT_T> {
 public:
  BFSVertexMap(const CONTEXT_T& context)
      : minigraph::VertexMapBase<GRAPH_T, CONTEXT_T>(context) {}
  void VertexReduce(const CONTEXT_T& context) {
    XLOG(INFO, "In VertexReduce()");
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class BFSEdgeMap : public minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  BFSEdgeMap(const CONTEXT_T& context)
      : minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T>(context) {}

  bool C(const VertexInfo& vertex_info) override {
    if (*vertex_info.vdata == 1) {
      return false;
    } else {
      return true;
    }
  }

  bool F(VertexInfo& vertex_info) override {
    *vertex_info.vdata = 1;
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class BFSPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  BFSPIE(minigraph::VertexMapBase<GRAPH_T, CONTEXT_T>* vertex_map,
         minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T>* edge_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vertex_map, edge_map,
                                                   context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T& partial_result) override {
    auto local_id = graph.globalid2localid(this->context_.root_id);
    if (local_id == VID_MAX) {
      LOG_INFO("PEval() - skip");
      return false;
    }
    LOG_INFO("PEval() - local_id: ", local_id, " processing");
    bool* visited = (bool*)malloc(sizeof(bool) * graph.get_num_vertexes());
    Frontier* frontier_in = new Frontier(graph.get_num_vertexes() + 1);
    VertexInfo&& vertex_info = graph.GetVertex(local_id);
    frontier_in->enqueue(vertex_info);
    while (!frontier_in->empty()) {
      frontier_in = this->edge_map_->EdgeMap(frontier_in, visited, graph,
                                             this->task_runner_);
    }
    return this->GetPartialBorderResult(graph, visited, partial_result);
  }

  bool IncEval(GRAPH_T& graph, PARTIAL_RESULT_T& partial_result) override {
    XLOG(INFO, "IncEval() - gid: ", graph.gid_);

    Frontier* frontier_in = new Frontier(this->global_border_vertexes_info_->size() + 1);

    for (auto& iter : *this->global_border_vertexes_info_) {
      frontier_in->enqueue(*iter.second);
    }
    bool* visited = (bool*)malloc(sizeof(bool) * graph.get_num_vertexes());
    while (!frontier_in->empty()) {
      frontier_in = this->edge_map_->EdgeMap(frontier_in, visited, graph,
                                             this->task_runner_);
    }
    return this->GetPartialBorderResult(graph, visited, partial_result);
  }

  void MsgAggr(PARTIAL_RESULT_T& partial_result) override {
    if (partial_result.size() == 0) {
      return;
    }
    for (auto iter = partial_result.begin(); iter != partial_result.end();
         iter++) {
      auto iter_global = this->global_border_vertexes_info_->find(iter->first);
      if (iter_global != this->global_border_vertexes_info_->end()) {
        if (iter->second->vdata[0] == 1 && iter_global->second->vdata[0] != 1) {
          iter_global->second->RecycleVertex();
          iter_global->second = iter->second;
        }
      } else {
        this->global_border_vertexes_info_->insert(
            std::make_pair(iter->first, iter->second));
      }
    }
  }
};

struct Context {
  size_t root_id = 0;
};

int main(int argc, char* argv[]) {
  using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
  using BFSPIE_T = BFSPIE<CSR_T, Context>;
  if (argc < 6) {
    XLOG(ERR, "input Error");
  }
  std::string row_data = argv[1];
  std::string work_space = argv[2];
  size_t num_workers_lc = atoi(argv[3]);
  size_t num_workers_cc = atoi(argv[4]);
  size_t num_workers_dc = atoi(argv[5]);
  size_t num_thread_cpu = atoi(argv[6]);
  bool is_partition = false;
  if (atoi(argv[7]) == 0) {
    is_partition = false;
  } else {
    is_partition = true;
  }
  Context context;
  auto bfs_edge_map = new BFSEdgeMap<CSR_T, Context>(context);
  auto bfs_vertex_map = new BFSVertexMap<CSR_T, Context>(context);
  auto bfs_pie =
      new BFSPIE<CSR_T, Context>(bfs_vertex_map, bfs_edge_map, context);
  auto app_wrapper =
      new AppWrapper<BFSPIE<CSR_T, Context>, gid_t, vid_t, vdata_t, edata_t>(
          bfs_pie);

  minigraph::MiniGraphSys<CSR_T, BFSPIE_T> minigraph_sys(
      row_data, work_space, num_workers_lc, num_workers_cc, num_workers_dc,
      num_thread_cpu, is_partition, app_wrapper);
  if (!is_partition) {
    minigraph_sys.RunSys();
    minigraph_sys.ShowResult();
  }
}