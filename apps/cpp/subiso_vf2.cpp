#include "2d_pie/auto_app_base.h"
#include "2d_pie/edge_map_reduce.h"
#include "2d_pie/vertex_map_reduce.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "message_manager/border_vertexes.h"
#include "message_manager/default_message_manager.h"
#include "message_manager/partial_match.h"
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
    enum TFU { vf2_true, vf2_false, vf2_uncertain };

   public:
    // bool CoversAll(bool* visited, size_t num_vertexes) {
    //   for (size_t i = 0; i < num_vertexes; i++) {
    //     if (visited[i] == 0) return false;
    //   }
    //   return true;
    // }

    bool IsInCurrentMatchingSolution(
        VID_T vid, std::vector<VID_T>& current_matching_solution) {
      for (size_t i = 0; i < current_matching_solution.size(); i++) {
        if (current_matching_solution.at(i) == vid) return true;
      }
      return false;
    }

    template <typename V_T>
    bool IsInVec(std::vector<V_T>& vec, V_T& v) {
      for (size_t i = 0; i < vec.size(); i++)
        if (vec.at(i) == v) return true;
      return false;
    }

    size_t GetIndexOfX(std::vector<VID_T>& vec, VID_T vid) {
      for (size_t i = 0; i < vec.size(); i++)
        if (vec.at(i) == vid) return i;
      return SIZE_MAX;
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

        for (auto& iter : current_matching_solution) {
          auto v_local_vid = graph.globalid2localid(iter);
          if (v_local_vid == VID_MAX) continue;
          auto v = graph.GetVertexByVid(v_local_vid);
          for (size_t j = 0; j < v.outdegree; j++) {
            if (!IsInCurrentMatchingSolution(v.out_edges[j],
                                             current_matching_solution)) {
              num_candidates++;
              candidate.emplace(v.out_edges[j]);
            }
          }
        }

        if (num_candidates == 0) {
          for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
            auto v_global_id = graph.localid2globalid(i);

            if (!IsInCurrentMatchingSolution(v_global_id,
                                             current_matching_solution)) {
              candidate.emplace(v_global_id);
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

    TFU CompareNbrVertexes(std::vector<VID_T>& meta,
                           std::vector<VID_T>& current_matching_solution,
                           EDGE_LIST_T& pattern, GRAPH_T& graph) {
      VertexInfo&& u = pattern.GetVertexByVid(meta.back());
      auto v_local_id =
          graph.globalid2localid(current_matching_solution.back());
      if (v_local_id == VID_MAX) {
        return vf2_uncertain;
      }
      VertexInfo&& v = graph.GetVertexByVid(v_local_id);
      std::vector<size_t> vec_index_meta;
      std::vector<size_t> vec_index_current_matching_solution;
      for (size_t i = 0; i < u.indegree; i++) {
        auto index_nbr_u = GetIndexOfX(meta, u.in_edges[i]);
        if (index_nbr_u == SIZE_MAX) continue;
        vec_index_meta.push_back(index_nbr_u);
      }
      for (size_t i = 0; i < v.indegree; i++) {
        auto index_nbr_v =
            GetIndexOfX(current_matching_solution, v.in_edges[i]);
        if (index_nbr_v == SIZE_MAX) continue;
        vec_index_current_matching_solution.push_back(index_nbr_v);
      }
      bool tag = true;
      for (auto& iter : vec_index_meta) {
        if (!IsInVec(vec_index_current_matching_solution, iter)) {
          tag = false;
          break;
        }
      }
      if (tag) {
        return vf2_true;
      } else {
        return vf2_false;
      }
    }

    // bool IsGoal(EDGE_LIST_T& pattern, GRAPH_T& graph, std::vector<VID_T>&
    // meta,
    //             std::vector<VID_T>& current_matching_solution) {
    //   if (meta.size() < pattern.get_num_vertexes() ||
    //       meta.size() != current_matching_solution.size()) {
    //     return false;
    //   }
    //   for (size_t i = 0; i < current_matching_solution.size(); i++) {
    //     auto u = pattern.GetVertexByVid(meta.at(i));

    //    auto v_local_id =
    //        graph.globalid2localid(current_matching_solution.at(i));
    //    if (v_local_id == VID_MAX) {
    //      return false;
    //    }
    //    auto v = graph.GetVertexByVid(v_local_id);
    //    if (!CompareVertexes(u, v)) return false;
    //    if (!CompareNbrVertexes(meta, current_matching_solution, pattern,
    //                            graph) != vf2_true)
    //      return false;
    //  }
    //  return true;
    //}

    TFU CheckFeasibilityRules(EDGE_LIST_T& pattern, GRAPH_T& graph,
                              std::vector<VID_T>& meta,
                              std::vector<VID_T>& current_matching_solution) {
      auto v_local_id =
          graph.globalid2localid(current_matching_solution.back());
      if (v_local_id == VID_MAX) {
        return vf2_uncertain;
      }

      VertexInfo&& u = pattern.GetVertexByVid(meta.back());
      VertexInfo&& v = graph.GetVertexByVid(v_local_id);
      if (!CompareVertexes(u, v)) return vf2_false;
      return CompareNbrVertexes(meta, current_matching_solution, pattern,
                                graph);
    }

    VID_T GetNewPredicates(EDGE_LIST_T& pattern, std::vector<VID_T>& meta) {
      if (meta.size() < pattern.get_num_vertexes()) {
        if (meta.empty()) {
          return pattern.vertexes_info_->at(0)->vid;
        } else {
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

    void Match(EDGE_LIST_T& pattern, GRAPH_T& graph, std::vector<VID_T> meta,
               std::vector<VID_T> current_matching_solution, VID_T u_vid,
               VID_T v_vid,
               std::vector<std::vector<VID_T>*>& partial_matching_solutions,
               std::vector<std::vector<VID_T>*>& matching_solutions,
               minigraph::message::DefaultMessageManager<GID_T, VID_T, VDATA_T,
                                                         EDATA_T>& msg_mngr) {
      if (u_vid == VID_MAX) {
        u_vid = GetNewPredicates(pattern, meta);
        std::queue<VID_T>&& candidate =
            GetCandidates(pattern, graph, current_matching_solution);
        while (!candidate.empty()) {
          Match(pattern, graph, meta, current_matching_solution, u_vid,
                candidate.front(), partial_matching_solutions,
                matching_solutions, msg_mngr);
          candidate.pop();
        }
      } else {
        // LOG_INFO("u_vid: ", u_vid, ", local: ",
        // graph.globalid2localid(v_vid),
        //          ", global: ", v_vid);
        meta.push_back(u_vid);
        current_matching_solution.push_back(v_vid);
        auto tfu = CheckFeasibilityRules(pattern, graph, meta,
                                         current_matching_solution);
        if (tfu == vf2_true) {
          if (meta.size() == pattern.get_num_vertexes()) {
            auto matching_solution_instance = new std::vector<VID_T>;
            matching_solution_instance->assign(
                current_matching_solution.begin(),
                current_matching_solution.end());
            matching_solutions.push_back(matching_solution_instance);
            //          msg_mngr.BufferResult(current_matching_solution);
          } else {
            VID_T&& new_predicates = GetNewPredicates(pattern, meta);
            std::queue<VID_T>&& candidate =
                GetCandidates(pattern, graph, current_matching_solution);
            if (candidate.size() == 0) return;
            while (!candidate.empty()) {
              Match(pattern, graph, meta, current_matching_solution,
                    new_predicates, candidate.front(),
                    partial_matching_solutions, matching_solutions, msg_mngr);
              candidate.pop();
            }
          }
        } else if (tfu == vf2_false) {
          return;
        } else if (tfu == vf2_uncertain) {
          auto partial_matching_solution_instance = new std::vector<VID_T>;
          partial_matching_solution_instance->assign(
              current_matching_solution.begin(),
              current_matching_solution.end());
          partial_matching_solutions.push_back(
              partial_matching_solution_instance);
          return;
        }
      }
    }
  };

  bool Init(GRAPH_T& graph) override {}

  bool PEval(GRAPH_T& graph, //PARTIAL_RESULT_T* partial_result,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    auto partial_matching_solutions_for_next_round_of_iteration =
        new std::vector<std::vector<VID_T>*>;
    auto matching_solutions = new std::vector<std::vector<VID_T>*>;
    VF2 vf2_executor;
    std::vector<VID_T> meta;
    std::vector<VID_T> current_matching_solution;
    vf2_executor.Match(*this->context_.pattern, graph, meta,
                       current_matching_solution, VID_MAX, VID_MAX,
                       *partial_matching_solutions_for_next_round_of_iteration,
                       *matching_solutions, *this->msg_mngr_);
    this->msg_mngr_->BufferResults(*matching_solutions);
    if (partial_matching_solutions_for_next_round_of_iteration->size() == 0) {
      this->msg_mngr_->partial_match_->ShowMatchingSolutions();
      return false;
    } else {
      this->msg_mngr_->BufferPartialResults(
          *partial_matching_solutions_for_next_round_of_iteration);
      this->msg_mngr_->partial_match_->ShowMatchingSolutions();
      return true;
    }
  }

  bool IncEval(GRAPH_T& graph, //PARTIAL_RESULT_T* partial_result,
               minigraph::executors::TaskRunner* task_runner) override {
    auto partial_matching_solutions =
        this->msg_mngr_->GetPartialMatchingSolutionsofX(graph.gid_);
    if (partial_matching_solutions == nullptr) {
      LOG_INFO("IncEval() - Discarding gid: ", graph.gid_);
      return false;
    }
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    std::vector<VID_T> meta;
    meta.reserve(this->context_.pattern->get_num_vertexes() + 1);
    VF2 vf2_executor;

    auto partial_matching_solutions_for_next_round_of_iteration =
        new std::vector<std::vector<VID_T>*>;
    auto matching_solutions = new std::vector<std::vector<VID_T>*>;

    for (size_t j = 0; j < this->context_.pattern->get_num_vertexes(); j++) {
      auto u_vid = vf2_executor.GetNewPredicates(*this->context_.pattern, meta);
      meta.push_back(u_vid);
    }
    for (size_t i = 0; i < partial_matching_solutions->size(); i++) {
      auto v_vid = partial_matching_solutions->at(i)->back();
      partial_matching_solutions->at(i)->pop_back();
      std::vector<VID_T> current_meta;
      current_meta.assign(
          meta.begin(),
          meta.begin() + partial_matching_solutions->at(i)->size());
      auto u_vid =
          vf2_executor.GetNewPredicates(*this->context_.pattern, current_meta);
      vf2_executor.Match(
          *this->context_.pattern, graph, current_meta,
          *partial_matching_solutions->at(i), u_vid, v_vid,
          *partial_matching_solutions_for_next_round_of_iteration,
          *matching_solutions, *this->msg_mngr_);
    }
    this->msg_mngr_->BufferResults(*matching_solutions);

    if (partial_matching_solutions_for_next_round_of_iteration->size() == 0) {
      this->msg_mngr_->partial_match_->ShowMatchingSolutions();
      return false;
    } else {
      this->msg_mngr_->BufferPartialResults(
          *partial_matching_solutions_for_next_round_of_iteration);
      this->msg_mngr_->partial_match_->ShowMatchingSolutions();
      return true;
    }
  }

  //bool MsgAggr(PARTIAL_RESULT_T* partial_result) override {
  //  if (partial_result->size() == 0) {
  //    return false;
  //  }
  //  for (auto iter = partial_result->begin(); iter != partial_result->end();
  //       iter++) {
  //    auto iter_global = this->global_border_vertexes_info_->find(iter->first);
  //    if (iter_global != this->global_border_vertexes_info_->end()) {
  //      if (iter_global->second->vdata[0] != 1) {
  //        iter_global->second->UpdateVdata(1);
  //      }
  //    } else {
  //      VertexInfo* vertex_info = new VertexInfo(iter->second);
  //      this->global_border_vertexes_info_->insert(
  //          std::make_pair(iter->first, vertex_info));
  //    }
  //  }
  //  return true;
  //}
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