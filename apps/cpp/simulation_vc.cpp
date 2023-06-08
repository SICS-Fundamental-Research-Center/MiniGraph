#include "2d_pie/auto_app_base.h"
//#include "2d_pie/edge_map_reduce.h"
//#include "2d_pie/vertex_map_reduce.h"
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
  Bitmap** sim_sets_ = nullptr;
  size_t x_ = 0;
  size_t y_ = 0;
  MatchSets(const size_t x, const size_t y, const bool init = false) {
    x_ = x;
    y_ = y;
    indicator_ = new Bitmap(x);
    indicator_->clear();
    sim_sets_ = (Bitmap**)malloc(sizeof(Bitmap*) * x);

    if (!init)
      for (size_t i = 0; i < x; i++) sim_sets_[i] = nullptr;
    else {
      for (size_t i = 0; i < x; i++) {
        sim_sets_[i] = new Bitmap(y);
        sim_sets_[i]->clear();
      }
    }
  }
  ~MatchSets() {
    if (indicator_ != nullptr) delete indicator_;
    if (sim_sets_ != nullptr) {
      for (size_t i = 0; i < x_; i++)
        if (sim_sets_[i] == nullptr) delete sim_sets_[i];
      free(sim_sets_);
    }
  }
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
  SimulationAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static void kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step)
      graph->vdata_[i] = rand() % 5;
    return;
  }

  static void kernel_init_global_vdata(GRAPH_T* graph, const size_t tid,
                                       Bitmap* visited, const size_t step,
                                       Bitmap* global_border_vid_map,
                                       VDATA_T* global_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      // if (global_border_vid_map->get_bit(graph->localid2globalid(u.vid)))
      global_vdata[graph->localid2globalid(u.vid)] = u.vdata[0];
    }
    return;
  }

  // Initially, it is assumed that all vertexes of graph is not a match. The
  // func label VERTEXMATCH flag if the vertex its label matches the label of a
  // vertex in pattern.
  //
  // Then each vertex Init its match_set based on their label.
  static void kernel_match_vertex(GRAPH_T* graph, const size_t tid,
                                  Bitmap* visited, const size_t step,
                                  CSR_T* pattern, MatchSets* match_sets,
                                  Bitmap* in_visited) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      for (size_t j = 0; j < pattern->get_num_vertexes(); j++) {
        auto v = pattern->GetVertexByIndex(j);
        if (u.vdata[0] != v.vdata[0]) continue;

        if (u.outdegree == 0) {
          if (v.outdegree == 0) {
            if (match_sets->indicator_->get_bit(i))
              ;
            else {
              match_sets->indicator_->set_bit(i);
              match_sets->sim_sets_[i] =
                  new Bitmap(pattern->get_num_vertexes());
            }
            match_sets->sim_sets_[i]->set_bit(v.vid);
            (*u.state) == VERTEXMATCH ? 0 : (*u.state) = VERTEXMATCH;
            in_visited->try_set_bit(i);
          };
        } else {
          if (match_sets->indicator_->get_bit(i))
            ;
          else {
            match_sets->indicator_->set_bit(i);
            match_sets->sim_sets_[i] = new Bitmap(pattern->get_num_vertexes());
          }

          match_sets->sim_sets_[i]->set_bit(v.vid);
          (*u.state) == VERTEXMATCH ? 0 : (*u.state) = VERTEXMATCH;
          in_visited->try_set_bit(i);
        }
      }
    }
    return;
  }

  static bool check_childs(VertexInfo& a, VertexInfo& b, GRAPH_T& graph,
                           CSR_T& pattern, VID_T* vid_map,
                           MatchSets* match_sets) {
    // @para: vertex a from pattern graph, vertex b from data graph.
    if (a.vdata[0] != b.vdata[0]) return false;
    size_t count = 0;
    for (size_t j = 0; j < b.outdegree; j++) {
      auto nbr_b = pattern.GetVertexByVid(b.out_edges[j]);
      for (size_t i = 0; i < a.outdegree; i++) {
        auto nbr_a = graph.GetVertexByVid(vid_map[a.out_edges[i]]);
        if (nbr_a.vdata[0] == nbr_b.vdata[0]) {
          if (!graph.IsInGraph(nbr_b.vid)) continue;
          if (match_sets->indicator_->get_bit(nbr_a.vid) == 0) continue;
          if (match_sets->sim_sets_[nbr_a.vid] == nullptr) continue;

          auto local_id = vid_map[nbr_b.vid];
          if (match_sets->sim_sets_[nbr_a.vid]->get_bit(local_id)) {
            count++;
            break;
          }
        }
      }
    }
    return count == b.outdegree;
  }

  // Filter vertexes by using information from its childrens.
  static void kernel_ask_childs(GRAPH_T* graph, const size_t tid,
                                Bitmap* visited, const size_t step,
                                CSR_T* pattern, MatchSets* match_sets,
                                Bitmap* in_visited, Bitmap* out_visited,
                                Bitmap* active_bit_map, VID_T* vid_map,
                                VDATA_T* global_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (in_visited->get_bit(i) == 0) continue;
      if (match_sets->indicator_->get_bit(i) == 0) continue;
      auto u = graph->GetVertexByIndex(i);
      for (size_t j = 0; j < pattern->get_num_vertexes(); j++) {
        if (match_sets->sim_sets_[i]->get_bit(j)) {
          //  remove v from match_sets->sim_sets[i] when there is dis-match(es)
          //  between childs of v and childs of u
          auto v = pattern->GetVertexByIndex(j);
          if (!check_childs(u, v, *graph, *pattern, vid_map, match_sets)) {
            match_sets->sim_sets_[u.vid]->rm_bit(v.vid);
            for (size_t nbr_u_i = 0; nbr_u_i < u.indegree; nbr_u_i++) {
              if (!graph->IsInGraph(u.in_edges[nbr_u_i])) continue;
              out_visited->try_set_bit(vid_map[u.in_edges[nbr_u_i]]);
            }
          }
        }
      }
    }
    return;
  }

  static void kernel_convert_match_sets(GRAPH_T* graph, const size_t tid,
                                        Bitmap* visited, const size_t step,
                                        CSR_T* pattern, MatchSets* a,
                                        MatchSets* b) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (a->indicator_->get_bit(i) == 0) continue;
      for (size_t j = 0; j < pattern->get_num_vertexes(); j++) {
        if (a->sim_sets_[i]->get_bit(j) == 0) continue;
        if (b->indicator_->get_bit(j))
          ;
        else {
          b->indicator_->set_bit(j);
          b->sim_sets_[j] = new Bitmap(graph->get_num_vertexes());
        }
        b->sim_sets_[j]->set_bit(i);
      }
    }
    return;
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
  SimulationPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
                const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("Init() - Processing gid: ", graph.gid_);
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(
        graph, task_runner, visited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);

    auto vid_map = this->msg_mngr_->GetVidMap();

    if (graph.IsInGraph(19))
      graph.vdata_[graph.localid_by_globalid_[vid_map[19]]] = 1;
    if (graph.IsInGraph(20))
      graph.vdata_[graph.localid_by_globalid_[vid_map[20]]] = 2;
    if (graph.IsInGraph(21))
      graph.vdata_[graph.localid_by_globalid_[vid_map[21]]] = 3;
    if (graph.IsInGraph(22))
      graph.vdata_[graph.localid_by_globalid_[vid_map[22]]] = 4;
    if (graph.IsInGraph(1))
      graph.vdata_[graph.localid_by_globalid_[vid_map[0]]] = 1;
    if (graph.IsInGraph(1))
      graph.vdata_[graph.localid_by_globalid_[vid_map[2]]] = 2;
    if (graph.IsInGraph(5))
      graph.vdata_[graph.localid_by_globalid_[vid_map[5]]] = 3;
    if (graph.IsInGraph(6))
      graph.vdata_[graph.localid_by_globalid_[vid_map[6]]] = 4;
    if (graph.IsInGraph(7))
      graph.vdata_[graph.localid_by_globalid_[vid_map[7]]] = 4;
    if (graph.IsInGraph(8))
      graph.vdata_[graph.localid_by_globalid_[vid_map[8]]] = 4;
    if (graph.IsInGraph(9))
      graph.vdata_[graph.localid_by_globalid_[vid_map[9]]] = 4;
    if (graph.IsInGraph(10))
      graph.vdata_[graph.localid_by_globalid_[vid_map[10]]] = 4;
    if (graph.IsInGraph(11))
      graph.vdata_[graph.localid_by_globalid_[vid_map[11]]] = 4;
    if (graph.IsInGraph(11))
      graph.vdata_[graph.localid_by_globalid_[vid_map[15]]] = 1;
    if (graph.IsInGraph(11))
      graph.vdata_[graph.localid_by_globalid_[vid_map[16]]] = 2;
    if (graph.IsInGraph(11))
      graph.vdata_[graph.localid_by_globalid_[vid_map[17]]] = 5;
    if (graph.IsInGraph(11))
      graph.vdata_[graph.localid_by_globalid_[vid_map[33]]] = 4;

    this->context_.p->vdata_[0] = 1;
    this->context_.p->vdata_[1] = 2;
    this->context_.p->vdata_[2] = 3;
    // this->context_.p->vdata_[3] = 4;
    // this->context_.p->vdata_[4] = 5;
    // this->context_.p->vdata_[5] = 6;
    // this->context_.p->vdata_[6] = 7;
    // this->context_.p->vdata_[7] = 8;
    // this->context_.p->vdata_[8] = 9;
    // this->context_.p->vdata_[9] = 10;

    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " max_vid: ", graph.max_vid_, "num_vertexes",
             graph.get_num_vertexes());
    Bitmap* pvisited = new Bitmap(graph.max_vid_);
    pvisited->fill();

    this->auto_map_->ActiveMap(
        graph, task_runner, pvisited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_init_global_vdata,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());
    delete pvisited;

    // TMP
    Bitmap visited(graph.get_num_vertexes());
    // Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    // Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    visited.clear();
    in_visited->clear();
    out_visited->clear();

    auto pattern = this->context_.p;
    MatchSets match_sets(graph.get_num_vertexes(),
                         this->context_.p->get_num_vertexes());

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_match_vertex,
        this->context_.p, &match_sets, in_visited);

    // for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
    //   if (match_sets.indicator_->get_bit(i) == 0) continue;
    //   LOG_INFO(graph.localid2globalid(i), "match: ");
    //   for (size_t j = 0; j < this->context_.p->get_num_vertexes(); j++) {
    //     if (match_sets.sim_sets_[i]->get_bit(j)) LOG_INFO(j);
    //   }
    // }

    size_t term = 0;
    while (!in_visited->empty()) {
      LOG_INFO("while: ", term);
      term++;
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_ask_childs,
          this->context_.p, &match_sets, in_visited, out_visited,
          this->msg_mngr_->GetGlobalActiveVidMap(),
          this->msg_mngr_->GetVidMap(), this->msg_mngr_->GetGlobalVdata());
      std::swap(in_visited, out_visited);
      LOG_INFO("Active vertexes: ", in_visited->get_num_bit());
      out_visited->clear();
    }

    MatchSets match_sets_result(this->context_.p->get_num_vertexes(),
                                graph.get_num_vertexes(), true);

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_convert_match_sets,
        this->context_.p, &match_sets, &match_sets_result);

    for (size_t i = 0; i < graph.get_num_vertexes(); i++)
      if (match_sets.indicator_->get_bit(i)) delete match_sets.sim_sets_[i];
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_,
             " num vertexes: ", graph.get_num_vertexes());
    if (graph.gid_ == 126) return false;
    Bitmap visited(graph.get_num_vertexes());
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    visited.clear();
    in_visited->clear();
    out_visited->clear();

    auto pattern = this->context_.p;
    MatchSets match_sets(graph.get_num_vertexes(),
                         this->context_.p->get_num_vertexes());

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_match_vertex,
        this->context_.p, &match_sets, in_visited);

    size_t term = 0;
    while (!in_visited->empty()) {
      term++;
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          SimulationAutoMap<GRAPH_T, CONTEXT_T>::kernel_ask_childs,
          this->context_.p, &match_sets, in_visited, out_visited,
          this->msg_mngr_->GetGlobalActiveVidMap(),
          this->msg_mngr_->GetVidMap(), this->msg_mngr_->GetGlobalVdata());
      std::swap(in_visited, out_visited);
      LOG_INFO("Active vertexes: ", in_visited->get_num_bit());
      out_visited->clear();
    }

    for (size_t i = 0; i < graph.get_num_vertexes(); i++)
      if (match_sets.indicator_->get_bit(i)) delete match_sets.sim_sets_[i];

    return term == 1;
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
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
  size_t niters = FLAGS_niters;

  auto csr_io_adapter =
      minigraph::utility::io::CSRIOAdapter<CSR_T::gid_t, CSR_T::vid_t,
                                           CSR_T::vdata_t, CSR_T::edata_t>();

  auto pattern = new CSR_T;
  csr_io_adapter.Read(pattern, edgelist_csv, 0, pattern_pt);
  Context context;
  context.p = pattern;
  pattern->Serialize();

  auto simulation_auto_map = new SimulationAutoMap<CSR_T, Context>();
  auto simulation_pie =
      new SimulationPIE<CSR_T, Context>(simulation_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<SimulationPIE<CSR_T, Context>, CSR_T>(
          simulation_pie);

  minigraph::MiniGraphSys<CSR_T, SimulationPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode, niters);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(1);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}
