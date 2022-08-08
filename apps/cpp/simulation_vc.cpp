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

using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;

struct MatchSets {
  Bitmap* indicator_ = nullptr;
  vid_t** match_sets_ = nullptr;
  size_t* offset_sets_ = nullptr;
};

template <typename GRAPH_T, typename CONTEXT_T>
class SimulationAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  SimulationAutoMap(const CONTEXT_T& context)
      : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>(context) {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override { return false; }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByVid(i);
      graph->vdata_[i] = rand() % 3;
    }
    return true;
  }

  // Initially, it is assumed that all vertexes of graph is not a match. The
  // func label VERTEXMATCH flag if the vertex its label matches the label of a
  // vertex in pattern.
  //
  // Then each vertex Init its match_set based on their label.
  static bool kernel_match_vertex(GRAPH_T* graph, const size_t tid,
                                  Bitmap* visited, const size_t step,
                                  CSR_T* pattern, MatchSets* match_sets,
                                  Bitmap* in_visited) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      for (size_t j = 0; j < pattern->get_num_vertexes(); j++) {
        auto v = pattern->GetVertexByIndex(j);
        if (u.vdata[0] != v.vdata[0]) continue;
        (*u.state) == VERTEXMATCH ? 0 : (*u.state) = VERTEXMATCH;
        visited->set_bit(i);
        in_visited->set_bit(i);
        if (match_sets->indicator_->get_bit(i)) {
          match_sets->match_sets_[i][match_sets->offset_sets_[i]++] = v.vid;
        } else {
          match_sets->indicator_->set_bit(i);
          match_sets->match_sets_[i] = new VID_T[pattern->get_num_vertexes()];
          match_sets->match_sets_[i][match_sets->offset_sets_[i]++] = v.vid;
        }
      }
    }
  }

  // Filter vertexes by using information from its childrens.
  static bool kernel_ask_childs(GRAPH_T* graph, const size_t tid,
                                Bitmap* visited, const size_t step,
                                CSR_T* pattern, MatchSets* match_sets,
                                Bitmap* in_visited, Bitmap* out_visited,
                                VID_T* vid_map) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (in_visited->get_bit(i) == 0) continue;
      LOG_INFO("visit: ", i);
      auto u = graph->GetVertexByIndex(i);
      if (*u.state == VERTEXDISMATCH) continue;
      if (match_sets->indicator_->get_bit(i) == 0) continue;

      // evaluate match_sets by ask children
      auto child_all_in_dismatch = true;
      for (size_t j = 0; j < u.outdegree; j++) {
        if (!graph->IsInGraph(u.out_edges[j])) continue;
        auto v = graph->GetVertexByVid(vid_map[u.out_edges[j]]);
        if (match_sets->indicator_->get_bit(graph->index_by_vid_[v.vid]) &&
            *v.state == VERTEXMATCH)
          child_all_in_dismatch = false;
      }

      if (child_all_in_dismatch) {
        bool keep_u = false;
        for (size_t j = 0; j < match_sets->offset_sets_[i]; j++) {
          auto p_vid = match_sets->match_sets_[i][j];
          auto p_u = pattern->GetVertexByVid(p_vid);
          if (p_u.outdegree == 0)
            keep_u == true ? 0 : keep_u = true;
          else
            match_sets->match_sets_[i][j] = VID_MAX;
        }

        // We avoid to remove u, when there is a leaf vertex in match_sets.
        if (keep_u) {
          // else
          size_t count = 0;
          size_t ai = 0, bi = match_sets->offset_sets_[ai] - 1;
          for (; ai < match_sets->offset_sets_[i]; ai++) {
            if (match_sets->match_sets_[i][ai] != VID_MAX) {
              count++;
            } else {
              while (match_sets->match_sets_[i][bi] == VID_MAX) bi--;
              swap(match_sets->match_sets_[i][bi],
                   match_sets->match_sets_[i][ai]);
            }
          }
          if (count == 0) {
            // notive all parents
            for (size_t k = 0; k < u.indegree; k++) {
              auto parent_index = graph->index_by_vid_[u.in_edges[k]];
              if (match_sets->indicator_->get_bit(parent_index)) {
                out_visited->set_bit(parent_index);
              }
            }
            LOG_INFO("remove", i);
            match_sets->indicator_->rm_bit(i);
            match_sets->offset_sets_[i] = 0;
            *u.state = VERTEXDISMATCH;
            free(match_sets->match_sets_[i]);
            *(match_sets->match_sets_ + i) = nullptr;
          } else
            match_sets->offset_sets_[i] = count;
        } else {
          // notive all parents
          for (size_t k = 0; k < u.indegree; k++) {
            auto parent_index = graph->index_by_vid_[u.in_edges[k]];
            if (match_sets->indicator_->get_bit(parent_index)) {
              out_visited->set_bit(parent_index);
            }
          }
          LOG_INFO("remove", i);
          match_sets->indicator_->rm_bit(i);
          match_sets->offset_sets_[i] = 0;
          *u.state = VERTEXDISMATCH;
          free(match_sets->match_sets_[i]);
          *(match_sets->match_sets_ + i) = nullptr;
        }
      }
    }
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SimulationPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;

 public:
  SimulationPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
                minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
                const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  SimulationPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
                const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("Init() - Processing gid: ", graph.gid_);
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(
        graph, task_runner, visited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);
    auto vid_map = this->msg_mngr_->GetVidMap();
    if (graph.IsInGraph(0)) graph.vdata_[graph.index_by_vid_[vid_map[0]]] = 0;
    if (graph.IsInGraph(1)) graph.vdata_[graph.index_by_vid_[vid_map[1]]] = 0;
    if (graph.IsInGraph(4)) graph.vdata_[graph.index_by_vid_[vid_map[4]]] = 1;
    if (graph.IsInGraph(6)) graph.vdata_[graph.index_by_vid_[vid_map[6]]] = 1;
    if (graph.IsInGraph(25)) graph.vdata_[graph.index_by_vid_[vid_map[25]]] = 1;

    this->context_.p->vdata_[0] = 0;
    this->context_.p->vdata_[1] = 0;
    this->context_.p->vdata_[2] = 1;
    this->context_.p->vdata_[3] = 1;

    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    if (!graph.IsInGraph(this->context_.root_id)) return false;
    auto vid_map = this->msg_mngr_->GetVidMap();
    auto start_time = std::chrono::system_clock::now();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    in_visited->clear();
    out_visited->clear();

    // in_visited->fill();
    auto pattern = this->context_.p;
    pattern->ShowGraph();
    auto u = pattern->GetVertexByVid(0);
    MatchSets match_sets;
    match_sets.indicator_ = new Bitmap(graph.get_num_vertexes());
    match_sets.indicator_->clear();
    match_sets.match_sets_ =
        (VID_T**)malloc(sizeof(VID_T*) * graph.get_num_vertexes());

    memset(match_sets.match_sets_, 0,
           sizeof(VID_T*) * graph.get_num_vertexes());
    match_sets.offset_sets_ =
        (size_t*)malloc(sizeof(size_t) * graph.get_num_vertexes());
    memset(match_sets.offset_sets_, 0,
           sizeof(size_t) * graph.get_num_vertexes());

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_match_vertex,
        this->context_.p, &match_sets, in_visited);

    while (in_visited->empty()) {
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_ask_childs,
          this->context_.p, &match_sets, in_visited, out_visited,
          this->msg_mngr_->GetVidMap());
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }
    // for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
    //   if (in_visited->get_bit(i)) LOG_INFO(i);
    // }

    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      if (match_sets.indicator_->get_bit(i) == 0) continue;
      LOG_INFO(graph.vid_by_index_[i], " matches : ");
      for (size_t j = 0; j < match_sets.offset_sets_[i]; j++) {
        LOG_INFO(match_sets.match_sets_[i][j]);
      }
    }

    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());

    return !visited.empty();
  }
};

struct Context {
  size_t root_id = 0;
  CSR_T* p = nullptr;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using SimulationPIE_T = SimulationPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;
  std::string pattern_pt = FLAGS_pattern;

  auto csr_io_adapter =
      minigraph::utility::io::CSRIOAdapter<CSR_T::gid_t, CSR_T::vid_t,
                                           CSR_T::vdata_t, CSR_T::edata_t>();

  auto pattern = new CSR_T;
  csr_io_adapter.Read(pattern, edge_list_csv, 0, pattern_pt);
  Context context;
  context.p = pattern;
  pattern->Serialize();
  context.p->ShowGraph();

  auto simulation_auto_map = new SimulationAutoMap<CSR_T, Context>(context);
  auto simulation_pie =
      new SimulationPIE<CSR_T, Context>(simulation_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<SimulationPIE<CSR_T, Context>, CSR_T>(
          simulation_pie);

  minigraph::MiniGraphSys<CSR_T, SimulationPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper);

  auto sys_data_mngr = minigraph_sys.GetDataMngr();
  minigraph_sys.RunSys();
  minigraph_sys.ShowResult(20);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}