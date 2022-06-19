#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "message_manager/default_message_manager.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/logging.h"
#include <folly/concurrency/DynamicBoundedQueue.h>
#include <queue>

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

template <typename GRAPH_T, typename CONTEXT_T>
class SubIsoVMap : public minigraph::VMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

 public:
  SubIsoVMap(const CONTEXT_T& context)
      : minigraph::VMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool C(const VertexInfo& u) override { return false; }
  bool F(VertexInfo& u, GRAPH_T* graph = nullptr) override { return false; }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SubIsoEMap : public minigraph::EMapBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;
  using Frontier = folly::DMPMCQueue<VertexInfo, false>;
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;

 public:
  SubIsoEMap(const CONTEXT_T& context)
      : minigraph::EMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool C(const VertexInfo& u, const VertexInfo& v) override { return false; }
  bool F(const VertexInfo& u, VertexInfo& v) override { return false; }
};

template <typename GRAPH_T, typename CONTEXT_T>
class SubIsoPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  SubIsoPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
            minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap,
            const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;
  using PARTIAL_RESULT_T =
      std::unordered_map<typename GRAPH_T::vid_t, VertexInfo*>;

 public:
  class VF2 {
   public:
    bool CoversAll(bool* visited, size_t num_vertexes) {
      for (size_t i = 0; i < num_vertexes; i++) {
        if (visited[i] == 0) return false;
      }
      return true;
    }

    bool IsInCurrentMatchingSolution(
        VID_T vid, std::vector<VID_T>& current_matching_solution) {
      for (size_t i = 0; i < current_matching_solution.size(); i++) {
        if (current_matching_solution.at(i) == vid) return true;
      }
      return false;
    }

    std::queue<VID_T> GetCandidates(
        EDGE_LIST_T& pattern, GRAPH_T& graph,
        std::vector<VID_T>& current_matching_solution) {
      std::queue<VID_T> candidate;
      if (current_matching_solution.empty()) {
        for (size_t i = 0; i < graph.get_num_vertexes(); i++)
          candidate.emplace(
              graph.localid2globalid(graph.GetVertexByIndex(i).vid));
      } else {
        size_t num_candidates = 0;
        for (size_t i = current_matching_solution.size() - 1; i > 0; i--) {
          auto v = graph.GetVertexByVid(
              graph.globalid2localid(current_matching_solution.at(i)));
          for (size_t j = 0; j < v.outdegree; j++) {
            if (!IsInCurrentMatchingSolution(v.out_edges[j],
                                             current_matching_solution)) {
              num_candidates++;
              candidate.emplace(v.out_edges[j]);
            }
          }
          for (size_t j = 0; j < v.indegree; j++) {
            if (!IsInCurrentMatchingSolution(v.in_edges[j],
                                             current_matching_solution)) {
              num_candidates++;
              candidate.emplace(v.in_edges[j]);
            }
          }
        }
        if (num_candidates == 0) {
          for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
            if (!IsInCurrentMatchingSolution(
                    graph.localid2globalid(graph.GetVertexByIndex(i).vid),
                    current_matching_solution)) {
              candidate.emplace(
                  graph.localid2globalid(graph.GetVertexByIndex(i).vid));
            }
          }
        }
      }
      return candidate;
    }
    bool IsDead(EDGE_LIST_T& pattern, GRAPH_T& graph, std::vector<VID_T>& meta,
                std::vector<VID_T>& current_matching_solution) {
      return false;
    }

    bool CompareVertexes(VertexInfo& u, VertexInfo& v) {
      if (u.outdegree > v.outdegree) return false;
      if (u.indegree > v.indegree) return false;
      return true;
    }

    bool CompareNbrVertexes(VertexInfo& u, VertexInfo& v, EDGE_LIST_T& pattern,
                            GRAPH_T& graph) {
      for (size_t i = 0; i < u.outdegree; i++) {
        auto nbr_u = pattern.GetVertexByVid(u.out_edges[i]);
        bool tag = false;
        for (size_t j = 0; j < v.outdegree; j++) {
          auto nbr_v_vid = graph.globalid2localid(v.out_edges[i]);
          if (nbr_v_vid == VID_MAX) {
            continue;
          }
          auto nbr_v = graph.GetVertexByVid(nbr_v_vid);
          if (CompareVertexes(nbr_u, nbr_v)) {
            tag = true;
            break;
          }
        }
        if (tag) {
          continue;
        } else {
          return false;
        }
      }
      for (size_t i = 0; i < u.indegree; i++) {
        auto nbr_u = pattern.GetVertexByVid(u.in_edges[i]);
        bool tag = false;
        for (size_t j = 0; j < v.indegree; j++) {
          auto nbr_v_vid = graph.globalid2localid(v.in_edges[i]);
          if (nbr_v_vid == VID_MAX) {
            continue;
          }
          auto nbr_v = graph.GetVertexByVid(graph.globalid2localid(nbr_v_vid));

          if (CompareVertexes(nbr_u, nbr_v)) {
            tag = true;
            break;
          }
        }
        if (tag) {
          continue;
        } else {
          return false;
        }
      }
      return true;
    }

    bool IsGoal(EDGE_LIST_T& pattern, GRAPH_T& graph, std::vector<VID_T>& meta,
                std::vector<VID_T>& current_matching_solution) {
      if (meta.size() < pattern.get_num_vertexes() ||
          meta.size() != current_matching_solution.size()) {
        return false;
      }

      for (size_t i = 0; i < current_matching_solution.size(); i++) {
        auto u = pattern.GetVertexByVid(meta.at(i));
        auto v = graph.GetVertexByVid(
            graph.globalid2localid(current_matching_solution.at(i)));
        if (!CompareVertexes(u, v)) return false;
        // if (!CompareNbrVertexes(u, v, pattern, graph)) return false;
      }
      return true;
    }

    bool CheckFeasibilityRules(EDGE_LIST_T& pattern, GRAPH_T& graph,
                               std::vector<VID_T>& meta,
                               std::vector<VID_T>& current_matching_solution) {
      VertexInfo&& u = pattern.GetVertexByVid(meta.back());
      VertexInfo&& v = graph.GetVertexByVid(
          graph.globalid2localid(current_matching_solution.back()));
      if (!CompareVertexes(u, v)) return false;
      if (!CompareNbrVertexes(u, v, pattern, graph)) return false;
      return true;
    }

    VID_T GetNewPredicates(EDGE_LIST_T& pattern, std::vector<VID_T>& meta) {
      if (meta.size() < pattern.get_num_vertexes()) {
        if (meta.empty()) {
          return pattern.vertexes_info_->at(0)->vid;
        } else {
          // auto back_u = pattern.GetVertexByVid(meta.back());
          for (auto& iter : *pattern.vertexes_info_) {
            bool tag = true;
            VID_T out = iter.first;
            for (auto& iter_meta : meta) {
              if (out == iter_meta) {
                tag = false;
                break;
              }
            }
            if (tag) {
              return out;
            }
          }
        }
      } else {
        return VID_MAX;
      }
    }

    void Match(EDGE_LIST_T& pattern, GRAPH_T& graph,
               PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>& partial_match,
               std::vector<VID_T> meta,
               std::vector<VID_T> current_matching_solution, VID_T u_vid,
               VID_T v_vid,
               minigraph::message::DefaultMessageManager<GID_T, VID_T, VDATA_T,
                                                         EDATA_T>& msg_mngr) {
      if (v_vid == VID_MAX && u_vid == VID_MAX) {
        u_vid = GetNewPredicates(pattern, meta);
        // v_vid = graph.localid2globalid(graph.GetVertexByIndex(0).vid);
        v_vid = graph.localid2globalid(graph.GetVertexByIndex(0).vid);

        std::queue<VID_T>&& candidate =
            GetCandidates(pattern, graph, current_matching_solution);
        while (!candidate.empty()) {
          Match(pattern, graph, partial_match, meta, current_matching_solution,
                v_vid, candidate.front(), msg_mngr);
          candidate.pop();
        }
      } else {
        if (graph.globalid2localid(v_vid) == VID_MAX) {
          // msg_mngr_->x_;
          LOG_INFO("!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
          msg_mngr.BufferPartialResult(current_matching_solution, graph, v_vid);
          return;
        }
        meta.push_back(u_vid);
        current_matching_solution.push_back(v_vid);
        if (meta.size() == pattern.get_num_vertexes()) {
          if (IsGoal(pattern, graph, meta, current_matching_solution)) {
            auto matching_solution = new std::vector<VID_T>;
            matching_solution->swap(current_matching_solution);
            partial_match.vec_matching_solutions->push_back(matching_solution);
          } else {
            return;
          }
        } else {
          if (CheckFeasibilityRules(pattern, graph, meta,
                                    current_matching_solution)) {
            VID_T&& new_predicates = GetNewPredicates(pattern, meta);
            std::queue<VID_T>&& candidate =
                GetCandidates(pattern, graph, current_matching_solution);
            while (!candidate.empty()) {
              Match(pattern, graph, partial_match, meta,
                    current_matching_solution, new_predicates,
                    candidate.front(), msg_mngr);
              candidate.pop();
            }
          } else {
            return;
          }
        }
      }
    }
  };

  bool Init(GRAPH_T& graph) override {}

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
             minigraph::executors::TaskRunner* task_runner) override {
    auto partial_match = new PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>(
        this->context_.pattern->get_num_vertexes(), 0);
    bool* visited = (bool*)malloc(sizeof(bool) * graph.get_num_vertexes());
    std::unordered_map<VID_T, bool*>* M = new std::unordered_map<VID_T, bool*>;

    VF2 vf2_executor;
    std::vector<VID_T>* meta = new std::vector<VID_T>;
    std::vector<VID_T>* current_matching_solution = new std::vector<VID_T>;
    vf2_executor.Match(*this->context_.pattern, graph, *partial_match, *meta,
                       *current_matching_solution, VID_MAX, VID_MAX,
                       *this->msg_mngr_);
    size_t count = 0;
    for (auto& iter : *partial_match->vec_matching_solutions) {
      if (count++ > 10) {
        break;
      }
      for (size_t i = 0; i < partial_match->x_; i++) {
        std::cout << iter->at(i) << ", ";
      }
      std::cout << std::endl;
    }
    return true;
  }

  bool IncEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
               minigraph::executors::TaskRunner* task_runner) override {
    return false;
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
  EDGE_LIST_T* pattern;
};

using SubIsoPIE_T = SubIsoPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;

  std::string pattern_pt = FLAGS_pattern;

  EdgeListPt edgelist_pt;
  edgelist_pt.edges_pt = pattern_pt;

  minigraph::utility::io::DataMngr<gid_t, vid_t, vdata_t, edata_t> data_mngr;
  data_mngr.ReadGraph(0, edgelist_pt);
  Context context;
  context.pattern = (EDGE_LIST_T*)data_mngr.GetGraph(0);
  context.pattern->ShowGraph();
  auto bfs_edge_map = new SubIsoEMap<CSR_T, Context>(context);
  auto bfs_vertex_map = new SubIsoVMap<CSR_T, Context>(context);
  auto bfs_pie =
      new SubIsoPIE<CSR_T, Context>(bfs_vertex_map, bfs_edge_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<SubIsoPIE<CSR_T, Context>, gid_t, vid_t,
                                vdata_t, edata_t>(bfs_pie);

  minigraph::MiniGraphSys<CSR_T, SubIsoPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      app_wrapper);
  minigraph_sys.RunSys();

  minigraph_sys.ShowResult();
  gflags::ShutDownCommandLineFlags();
}