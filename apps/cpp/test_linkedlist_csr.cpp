#include "2d_pie/auto_app_base.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "graphs/linked_list_mutable_csr.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/dynamic/split_merge.h"
#include "utility/logging.h"

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

  // Push vdata from u to v.
  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    auto tag = false;
    if (write_min(v.vdata, u.vdata[0])) tag = true;
    return tag;
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
    // graph.ShowGraph(99);
    if (!graph.IsInGraph(0)) return true;
    auto vid_map = this->msg_mngr_->GetVidMap();
    auto start_time = std::chrono::system_clock::now();

    auto linkedlist_mutable_csr = new minigraph::graphs::LinkedListMutableCSR<
        typename GRAPH_T::gid_t, typename GRAPH_T::vid_t,
        typename GRAPH_T::vdata_t, typename GRAPH_T::edata_t>();

    minigraph::utility::dynamic::SplitMerge<
        typename GRAPH_T::gid_t, typename GRAPH_T::vid_t,
        typename GRAPH_T::vdata_t, typename GRAPH_T::edata_t>
        split_merge;

    auto pair_graphs = split_merge.Split(graph);

    auto graph_a = pair_graphs.first;
    auto graph_b = pair_graphs.second;

    // graph_a->ShowGraph(99);
    // graph_b->ShowGraph(99);

    // split_merge.Merge(linkedlist_mutable_csr, graph_a);
    // split_merge.Merge(linkedlist_mutable_csr, graph_b);
    // auto u = linkedlist_mutable_csr->GetVertexByVid(4);
    // u.ShowVertexInfo();

    // auto pair_graphs_a = split_merge.Split(*graph_a);
    // auto graph_aa = pair_graphs_a.first;
    // auto graph_ab = pair_graphs_a.second;

    auto pair_graphs_b = split_merge.Split(*graph_b);
    auto graph_ba = pair_graphs_b.first;
    auto graph_bb = pair_graphs_b.second;
    graph_ba->ShowGraph(99);
    graph_bb->ShowGraph(99);

    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    return false;
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
      buffer_size, app_wrapper, FLAGS_mode, FLAGS_niters, FLAGS_scheduler);
  minigraph_sys.RunSys();
  gflags::ShutDownCommandLineFlags();
  exit(0);
}
