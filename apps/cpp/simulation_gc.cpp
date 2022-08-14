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

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override { return false; }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step)
      graph->vdata_[i] = rand() % 5;
    return true;
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
    if (graph.IsInGraph(19)) graph.vdata_[graph.index_by_vid_[vid_map[19]]] = 1;
    if (graph.IsInGraph(20)) graph.vdata_[graph.index_by_vid_[vid_map[20]]] = 0;
    if (graph.IsInGraph(21)) graph.vdata_[graph.index_by_vid_[vid_map[21]]] = 0;
    if (graph.IsInGraph(22)) graph.vdata_[graph.index_by_vid_[vid_map[22]]] = 1;

    if (graph.IsInGraph(1)) graph.vdata_[graph.index_by_vid_[vid_map[1]]] = 1;
    if (graph.IsInGraph(4)) graph.vdata_[graph.index_by_vid_[vid_map[4]]] = 0;
    if (graph.IsInGraph(5)) graph.vdata_[graph.index_by_vid_[vid_map[5]]] = 0;

    if (graph.IsInGraph(6)) graph.vdata_[graph.index_by_vid_[vid_map[6]]] = 1;
    if (graph.IsInGraph(7)) graph.vdata_[graph.index_by_vid_[vid_map[7]]] = 1;
    if (graph.IsInGraph(8)) graph.vdata_[graph.index_by_vid_[vid_map[8]]] = 1;
    if (graph.IsInGraph(9)) graph.vdata_[graph.index_by_vid_[vid_map[9]]] = 1;
    if (graph.IsInGraph(10)) graph.vdata_[graph.index_by_vid_[vid_map[10]]] = 1;
    if (graph.IsInGraph(11)) graph.vdata_[graph.index_by_vid_[vid_map[11]]] = 1;
    if (graph.IsInGraph(11)) graph.vdata_[graph.index_by_vid_[vid_map[33]]] = 1;

    this->context_.p->vdata_[0] = 1;
    this->context_.p->vdata_[1] = 0;
    this->context_.p->vdata_[2] = 0;
    this->context_.p->vdata_[3] = 1;

    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    auto global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    auto global_vdata = this->msg_mngr_->GetGlobalVdata();

    for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
      auto u = graph.GetVertexByIndex(i);
      if (global_border_vid_map->get_bit(u.vid))
        global_vdata[graph.localid2globalid(u.vid)] = u.vdata[0];
    }
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);

    // graph.ShowGraph();
    MatchSets match_sets(this->context_.p->get_num_vertexes(),
                         graph.get_num_vertexes(), true);

    RefinedSimilarity(graph, *this->context_.p, match_sets,
                      this->msg_mngr_->GetGlobalBorderVidMap(),
                      this->msg_mngr_->GetVidMap(),
                      this->msg_mngr_->GetGlobalVdata());

    for (size_t i = 0; i < this->context_.p->get_num_vertexes(); i++) {
      if (match_sets.indicator_->get_bit(i) == 0) continue;
      LOG_INFO(i, "match: ");
      for (size_t j = 0; j < graph.get_num_vertexes(); j++) {
        if (match_sets.sim_sets_[i]->get_bit(j) && j < 10)
          LOG_INFO(graph.localid2globalid(j));
      }
    }

    return false;
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }

  void RefinedSimilarity(GRAPH_T& graph, CSR_T& pattern, MatchSets& match_sets,
                         Bitmap* global_border_map, VID_T* vid_map,
                         VDATA_T* global_vdata) {
    LOG_INFO("RefindSimilarity");
    Bitmap** prevsim =
        (Bitmap**)malloc(sizeof(Bitmap*) * pattern.get_num_vertexes());

    for (size_t i = 0; i < pattern.get_num_vertexes(); i++) {
      prevsim[i] = new Bitmap(graph.get_num_vertexes());
      prevsim[i]->fill();
      auto u = pattern.GetVertexByIndex(i);
      for (size_t j = 0; j < graph.get_num_vertexes(); j++) {
        auto v = graph.GetVertexByIndex(j);
        if (u.vdata[0] != v.vdata[0]) continue;
        match_sets.indicator_->set_bit(i);
        if (v.outdegree == 0) {
          if (u.outdegree == 0) {
            match_sets.sim_sets_[i]->set_bit(graph.index_by_vid_[v.vid]);
          };
        } else {
          match_sets.sim_sets_[i]->set_bit(graph.index_by_vid_[v.vid]);
        }
      }
    }

    size_t count = 0;
    Bitmap** keep_sim =
        (Bitmap**)malloc(sizeof(Bitmap*) * pattern.get_num_vertexes());

    for (size_t i = 0; i < pattern.get_num_vertexes(); i++)
      keep_sim[i] = new Bitmap(graph.get_num_vertexes());

    while (count < pattern.get_num_vertexes()) {
      count = 0;
      for (size_t i = 0; i < pattern.get_num_vertexes(); i++) {
        keep_sim[i]->clear();
        if (prevsim[i]->is_equal_to(*match_sets.sim_sets_[i])) {
          count++;
          continue;
        }

        Bitmap pre_prevsim_u(graph.get_num_vertexes());
        Bitmap pre_sim_u(graph.get_num_vertexes());
        Bitmap post_sim_u_in_border(graph.get_num_vertexes());
        pre_prevsim_u.clear();
        pre_sim_u.clear();
        pre_sim_u.clear();
        post_sim_u_in_border.clear();

        auto u = pattern.GetVertexByIndex(i);
        for (size_t j = 0; j < graph.get_num_vertexes(); j++) {
          if (prevsim[i]->get_bit(j)) {
            auto v = graph.GetVertexByIndex(j);
            for (size_t in_i = 0; in_i < v.indegree; in_i++)
              if (graph.IsInGraph(v.in_edges[in_i]))
                pre_prevsim_u.set_bit(vid_map[v.in_edges[in_i]]);
          }
          if (match_sets.sim_sets_[i]->get_bit(j)) {
            auto v = graph.GetVertexByIndex(j);
            for (size_t in_i = 0; in_i < v.indegree; in_i++)
              if (graph.IsInGraph(v.in_edges[in_i]))
                pre_sim_u.set_bit(vid_map[v.in_edges[in_i]]);
            for (size_t out_i = 0; out_i < v.outdegree; out_i++) {
              if (!graph.IsInGraph(v.out_edges[out_i])) {
                for (size_t nbr_u_i = 0; nbr_u_i < u.outdegree; nbr_u_i++) {
                  auto nbr_u = pattern.GetVertexByIndex(nbr_u_i);
                  if (nbr_u.vdata[0] == global_vdata[v.out_edges[out_i]]) {
                    keep_sim[i]->set_bit(v.vid);
                    break;
                  }
                }
              }
            }
          }
        }
        pre_prevsim_u.batch_rm_bit(pre_sim_u);
        for (size_t in_u = 0; in_u < u.indegree; in_u++) {
          match_sets.sim_sets_[pattern.index_by_vid_[u.in_edges[in_u]]]
              ->batch_rm_bit(pre_prevsim_u);
        }
      }
      for (size_t i = 0; i < pattern.get_num_vertexes(); i++) {
        match_sets.sim_sets_[i]->batch_or_bit(*keep_sim[i]);
        prevsim[i]->copy_bit(*match_sets.sim_sets_[i]);
      }
    }

    for (size_t i = 0; i < pattern.get_num_vertexes(); i++) {
      delete keep_sim[i];
      delete prevsim[i];
    }
    free(keep_sim);
    free(prevsim);
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

  auto csr_io_adapter =
      minigraph::utility::io::CSRIOAdapter<CSR_T::gid_t, CSR_T::vid_t,
                                           CSR_T::vdata_t, CSR_T::edata_t>();

  auto pattern = new CSR_T;
  csr_io_adapter.Read(pattern, edge_list_csv, 0, pattern_pt);
  Context context;
  context.p = pattern;
  pattern->Serialize();
  context.p->ShowGraph();

  auto simulation_auto_map = new SimulationAutoMap<CSR_T, Context>();
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
  // minigraph_sys.ShowResult(20);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}