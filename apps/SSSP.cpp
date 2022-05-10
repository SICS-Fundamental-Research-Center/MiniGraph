#include <folly/concurrency/DynamicBoundedQueue.h>
#include <condition_variable>

#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPVertexMap : public minigraph::VertexMapBase<GRAPH_T, CONTEXT_T> {
 public:
  SSSPVertexMap(const CONTEXT_T& context)
      : minigraph::VertexMapBase<GRAPH_T, CONTEXT_T>(context) {}
  void VertexReduce(const CONTEXT_T& context) {
    XLOG(INFO, "In VertexReduce()");
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPEdgeMap : public minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SSSPEdgeMap(const CONTEXT_T& context)
      : minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T>(context) {}

  bool C(const VertexInfo& vertex_info, const VDATA_T val) override {
    if (vertex_info.vdata[0] < val) {
      return true;
    } else {
      return false;
    }
  }

  bool F(VertexInfo& vertex_info, const VDATA_T val) override {
    *vertex_info.vdata = val + 1;
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SSSPPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SSSPPIE(minigraph::VertexMapBase<GRAPH_T, CONTEXT_T>* vertex_map,
         minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T>* edge_map,
         const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vertex_map, edge_map,
                                                   context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result) override {
    auto local_id = graph.globalid2localid(this->context_.root_id);
    if (local_id == VID_MAX) {
      LOG_INFO("PEval() - Discarding gid: ", graph.gid_);
      return false;
    }
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    memset(visited, 0, sizeof(bool) * graph.get_num_vertexes());
    Frontier* frontier_in = new Frontier(graph.get_num_vertexes() + 1000);
    VertexInfo&& vertex_info = graph.GetVertexByVid(local_id);
    vertex_info.vdata[0] = 0;
    visited[local_id] = true;
    frontier_in->enqueue(vertex_info);
    while (!frontier_in->empty()) {
      frontier_in = this->edge_map_->EdgeMap(frontier_in, visited, graph,
                                             this->task_runner_);
    }
    bool tag = this->GetPartialBorderResult(graph, visited, partial_result);
    MsgAggr(partial_result);
    free(visited);
    return tag;
  }

  bool IncEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result) override {
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
      frontier_in = this->edge_map_->EdgeMap(frontier_in, visited, graph,
                                             this->task_runner_);
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

int main(int argc, char* argv[]) {
  using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
  using BFSPIE_T = BFSPIE<CSR_T, Context>;
  std::string row_data = "../inputs/edge_graph_csv/test.csv";
  std::string work_space = "../inputs/tmp";
  size_t num_workers_lc = 1;
  size_t num_workers_cc = 3;
  size_t num_workers_dc = 1;
  size_t num_thread_cpu = 4;
  if (argc > 8) {
    // XLOG(ERR, "input Error");
    row_data = std::string(argv[1]);
    work_space = std::string(argv[2]);
    num_workers_lc = atoi(argv[3]);
    num_workers_cc = atoi(argv[4]);
    num_workers_dc = atoi(argv[5]);
    num_thread_cpu = atoi(argv[6]);
  }
  bool is_partition = false;
  if (atoi(argv[7]) == 0) {
    is_partition = false;
  } else {
    is_partition = true;
  }
  size_t num_partitions = atoi(argv[8]);
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
      num_thread_cpu, is_partition, num_partitions, app_wrapper);
  if (!is_partition) {
    minigraph_sys.RunSys();
    minigraph_sys.ShowResult();
  }
}