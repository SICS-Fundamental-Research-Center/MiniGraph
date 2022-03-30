#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include "utility/thread_pool.h"
#include <folly/AtomicHashMap.h>

template <typename GRAPH_T, typename CONTEXT_T>
class BFSVertexMap : public minigraph::VertexMapBase<GRAPH_T, CONTEXT_T> {
 public:
  void VertexReduce(const CONTEXT_T& context) override {
    XLOG(INFO, "In VertexReduce()");
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class BFSEdgeMap : public minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T> {
 public:
  void EdgeReduce(const CONTEXT_T& context) override {
    XLOG(INFO, "In EdgeReduce()");
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class BFSPIE : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
 public:
  BFSPIE(minigraph::VertexMapBase<GRAPH_T, CONTEXT_T>* vertex_map,
         minigraph::EdgeMapBase<GRAPH_T, CONTEXT_T>* edge_map)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vertex_map, edge_map) {}

  void PEval(
      GRAPH_T* graph,
      minigraph::utility::CPUThreadPool* cpu_thread_pool = nullptr) override {
    XLOG(INFO, "In PEval()");
    auto VID_T typeof()
        // folly::MQMCQueue<> a = ;
        auto vertex_info = graph->GetVertex(this->context.root_id);

    LOG_INFO(vertex_info.outdegree);
    LOG_INFO(vertex_info.indegree);
    LOG_INFO(vertex_info.in_edges[0]);
    for (size_t i = 0; i < vertex_info.indegree; ++i) {
      cout << vertex_info.in_edges[i] << ", ";
    }
    cout << endl;
    // this->vertex_map_->VertexMap(graph, this->context);
    this->edge_map_->EdgeMap(graph, this->context);
  }
  void IncEval(
      GRAPH_T* graph,
      minigraph::utility::CPUThreadPool* cpu_thread_pool = nullptr) override {
    XLOG(INFO, "In IncEval()");
  }
  void MsgAggr() override { XLOG(INFO, "In MsgAggr()"); }
};

struct Context {
  size_t root_id = 1;
  size_t num_threads = 3;
};

int main(int argc, char* argv[]) {
  using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
  using BFSPIE_T = BFSPIE<CSR_T, Context>;
  if (argc < 6) {
    XLOG(ERR, "input Error");
  }
  std::string work_space = argv[1];
  size_t num_workers_lc = atoi(argv[2]);
  size_t num_workers_cc = atoi(argv[3]);
  size_t num_workers_dc = atoi(argv[4]);
  size_t num_thread_cpu = atoi(argv[5]);
  size_t max_thread_io = atoi(argv[6]);

  auto bfs_edge_map = new BFSEdgeMap<CSR_T, Context>;
  auto bfs_vertex_map = new BFSVertexMap<CSR_T, Context>;
  auto bfs_pie = new BFSPIE<CSR_T, Context>(bfs_vertex_map, bfs_edge_map);
  auto app_wrapper = new AppWrapper<BFSPIE<CSR_T, Context>>(bfs_pie);

  minigraph::MiniGraphSys<gid_t, vid_t, vdata_t, edata_t, CSR_T, BFSPIE_T>
      minigraph_sys(work_space, num_workers_lc, num_workers_cc, num_workers_dc,
                    num_thread_cpu, max_thread_io, false, app_wrapper);
  minigraph_sys.RunSys();
}