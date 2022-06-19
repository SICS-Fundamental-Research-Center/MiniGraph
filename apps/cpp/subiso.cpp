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
#include <cmath>

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

  static void kernel_check(size_t tid, Frontier* frontier_out, VertexInfo& v,
                           VertexInfo& u, bool* c_set,
                           size_t num_vertexes_graph) {
    bool tag = true;
    if (u.outdegree > v.outdegree || u.indegree > v.indegree) {
      tag = false;
    }
    if (u.vdata != nullptr && v.vdata != nullptr && u.vdata[0] != v.vdata[0]) {
      tag = false;
    }
    if (tag) {
      *(c_set + u.vid * num_vertexes_graph + v.vid) = true;
    }
  }

  static void kernel_explore(size_t tid, Frontier* frontier_out, VertexInfo& v,
                             VertexInfo& u, bool* c_set, EDGE_LIST_T& pattern,
                             GRAPH_T* graph) {
    size_t count = 0;
    for (size_t nbr_u_i = 0; nbr_u_i < u.outdegree; nbr_u_i++) {
      VID_T nbr_u_id = u.out_edges[nbr_u_i];
      VertexInfo&& nbr_u = pattern.GetVertexByVid(nbr_u_id);
      for (size_t nbr_v_i = 0; nbr_v_i < v.outdegree; nbr_v_i++) {
        VID_T nbr_v_id = v.out_edges[nbr_v_i];
        VertexInfo&& nbr_v = graph->GetVertexByVid(nbr_v_id);
        if (nbr_u.outdegree <= nbr_v.outdegree &&
            nbr_u.indegree <= nbr_v.indegree) {
          count++;
          break;
        }
      }
    }

    for (size_t nbr_u_i = 0; nbr_u_i < u.indegree; nbr_u_i++) {
      VID_T nbr_u_id = u.out_edges[nbr_u_i];
      VertexInfo&& nbr_u = pattern.GetVertexByVid(nbr_u_id);
      for (size_t nbr_v_i = 0; nbr_v_i < v.indegree; nbr_v_i++) {
        VID_T nbr_v_id = v.out_edges[nbr_v_i];
        VertexInfo&& nbr_v = graph->GetVertexByVid(nbr_v_id);
        if (nbr_u.outdegree <= nbr_v.outdegree &&
            nbr_u.indegree <= nbr_v.indegree) {
          count++;
          break;
        }
      }
    }

    if (count < u.outdegree + u.indegree) {
      *(c_set + u.vid * graph->get_num_vertexes() + v.vid) = false;
    }
  }

  static void kernel_get_candidate_edges(
      size_t tid, Frontier* frontier_out, VertexInfo& u, bool* c_set,
      EDGE_LIST_T& pattern, GRAPH_T* graph,
      std::unordered_map<std::pair<VID_T, VID_T>,
                         std::vector<std::pair<VID_T, VID_T>>*>*
          candidate_edges,
      std::mutex* mtx) {
    for (size_t candidate_u_vid = 0;
         candidate_u_vid < graph->get_num_vertexes(); candidate_u_vid++) {
      if (!*(c_set + u.vid * graph->get_num_vertexes() + candidate_u_vid)) {
        continue;
      }
      bool tag = false;
      size_t count = 0;
      VertexInfo&& candidate = graph->GetVertexByVid(candidate_u_vid);

      for (size_t nbr_u_index = 0; nbr_u_index < u.outdegree; nbr_u_index++) {
        auto vec_edges = new std::vector<std::pair<VID_T, VID_T>>;
        auto nbr_u_vid = u.out_edges[nbr_u_index];
        for (size_t nbr_candidate_index = 0;
             nbr_candidate_index < candidate.outdegree; nbr_candidate_index++) {
          auto nbr_candidate_vid = candidate.out_edges[nbr_candidate_index];

          // compare the neighbor of u with the neighbor of the candidate of u.
          if (*(c_set + nbr_u_vid * graph->get_num_vertexes() +
                nbr_candidate_vid)) {
            vec_edges->push_back(
                std::make_pair(candidate_u_vid, nbr_candidate_vid));
            count++;
          }
        }
        if (vec_edges->size() != 0) {
          mtx->lock();
          auto iter_candidate_edges =
              candidate_edges->find(std::make_pair(u.vid, nbr_u_vid));
          if (iter_candidate_edges != candidate_edges->end()) {
            for (auto& iter : *vec_edges) {
              iter_candidate_edges->second->push_back(iter);
            }
            delete vec_edges;
          } else {
            candidate_edges->insert(
                std::make_pair(std::make_pair(u.vid, nbr_u_vid), vec_edges));
          }
          mtx->unlock();
        }
      }
    }
  }
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

  static void kernel_first_join(
      size_t tid, size_t t, std::pair<size_t, size_t>& pos_to_hash,
      std::vector<std::pair<VID_T, VID_T>>* candidate,
      PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>& partial_match,
      size_t* config_for_join) {
    auto base_to_join =
        (partial_match.matching_solutions_ + partial_match.x_ * t);
    size_t count = 0;

    for (size_t i = 0; i < candidate->size(); i++) {
      auto candidate_val = pos_to_hash.second == 0 ? candidate->at(i).first
                                                   : candidate->at(i).second;
      if (base_to_join[pos_to_hash.first] == candidate_val) {
        count++;
      }
    }
    *(config_for_join + t * 2) = count;
  }

  static void kernel_second_join(
      size_t tid, size_t t, std::pair<size_t, size_t>& pos_to_hash,
      VID_T* candidate_meta_to_join,
      std::vector<std::pair<VID_T, VID_T>>* candidate, size_t* config_for_join,
      PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>& new_partial_match,
      PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>& partial_match,
      std::atomic<size_t>* offset) {
    VID_T* base_to_join =
        (partial_match.matching_solutions_ + partial_match.x_ * t);
    size_t offset_inner = *(config_for_join + t * 2 + 1);

    for (size_t i = 0; i < candidate->size(); i++) {
      auto candidate_val = pos_to_hash.second == 0 ? candidate->at(i).first
                                                   : candidate->at(i).second;
      if (base_to_join[pos_to_hash.first] == candidate_val) {
        memcpy(new_partial_match.matching_solutions_ +
                   offset_inner * new_partial_match.x_,
               base_to_join, sizeof(VID_T) * partial_match.x_);
        *(new_partial_match.matching_solutions_ +
          offset_inner * new_partial_match.x_ + new_partial_match.x_ - 1) =
            pos_to_hash.second == 0 ? candidate->at(i).second
                                    : candidate->at(i).first;
        offset_inner++;
      }
    }
  }
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

  bool* InitializeCandidateVertices(
      EDGE_LIST_T& pattern, GRAPH_T& graph,
      minigraph::executors::TaskRunner* task_runner) {
    LOG_INFO("InitializeCandidateVertices");
    bool* c_set = (bool*)malloc(sizeof(bool) * pattern.vertexes_info_->size() *
                                graph.get_num_vertexes());
    memset(c_set, 0,
           sizeof(bool) * pattern.vertexes_info_->size() *
               graph.get_num_vertexes());
    bool* initialize =
        (bool*)malloc(sizeof(bool) * pattern.vertexes_info_->size());
    memset(initialize, 0, sizeof(bool) * pattern.vertexes_info_->size());

    std::vector<VID_T> vertexes;
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    graph.ShowGraph(100);
    for (auto iter : *pattern.vertexes_info_) {
      auto vid = iter.first;
      if (initialize[vid] == false) {
        Frontier* frontier_in = new Frontier(graph.get_num_vertexes());

        for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
          VertexInfo&& v = graph.GetVertexByIndex(i);
          frontier_in->enqueue(v);
        }

        // kernel_check;
        VertexInfo&& u = pattern.GetVertexByVid(vid);
        this->vmap_->Map(frontier_in, visited, graph, task_runner,
                         SubIsoVMap<GRAPH_T, CONTEXT_T>::kernel_check, u, c_set,
                         graph.get_num_vertexes());
        initialize[vid] = true;
      }
      // kernel_explore
      Frontier* frontier_in = new Frontier(graph.get_num_vertexes());
      for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
        if (*(c_set + vid * graph.get_num_vertexes() + i)) {
          VertexInfo&& u = graph.GetVertexByVid(i);
          frontier_in->enqueue(u);
        }
      }
      VertexInfo&& u = pattern.GetVertexByVid(vid);
      this->vmap_->Map(frontier_in, visited, graph, task_runner,
                       SubIsoVMap<GRAPH_T, CONTEXT_T>::kernel_explore, u, c_set,
                       pattern, &graph);
    }

    return c_set;
  }

  // bool RefineCandidateVertices() {
  //  TO ADD...;
  //}

  std::unordered_map<std::pair<VID_T, VID_T>,
                     std::vector<std::pair<VID_T, VID_T>>*>*
  FindCandidateEdges(EDGE_LIST_T& pattern, GRAPH_T& graph,
                     minigraph::executors::TaskRunner* task_runner,
                     bool* c_set) {
    Frontier* frontier_in = new Frontier(pattern.get_num_vertexes() + 1);
    auto candidate_edges =
        new std::unordered_map<std::pair<VID_T, VID_T>,
                               std::vector<std::pair<VID_T, VID_T>>*>;
    std::mutex* mtx = new std::mutex;
    for (auto& iter : *pattern.vertexes_info_) {
      frontier_in->enqueue(*iter.second);
    }
    bool* visited = (bool*)malloc(graph.get_num_vertexes());
    this->vmap_->Map(frontier_in, visited, graph, task_runner,
                     SubIsoVMap<GRAPH_T, CONTEXT_T>::kernel_get_candidate_edges,
                     c_set, pattern, &graph, candidate_edges, mtx);
    return candidate_edges;
  }

  std::pair<std::pair<size_t, size_t>,
            std::pair<VID_T*, std::vector<std::pair<VID_T, VID_T>>*>>
  ExactEdgesToJoin(PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>& partial_match,
                   std::unordered_map<std::pair<VID_T, VID_T>,
                                      std::vector<std::pair<VID_T, VID_T>>*>&
                       candidate_edges) {
    std::pair<size_t, size_t> pos_for_hash;
    std::pair<VID_T, VID_T> new_meta;
    std::vector<std::pair<VID_T, VID_T>>* candidate;
    VID_T* candidate_meta_to_join = (VID_T*)malloc(sizeof(VID_T) * 2);

    if (partial_match.x_ == 0) {
      size_t num_min_candidate = std::string::npos;
      size_t capacity = 1;
      std::pair<VID_T, VID_T> edge_min_candidate;
      VID_T meta[2];
      for (auto& iter_candidate_edges : candidate_edges) {
        capacity *= iter_candidate_edges.second->size();
        if (iter_candidate_edges.second->size() < num_min_candidate) {
          num_min_candidate = iter_candidate_edges.second->size();
          meta[0] = iter_candidate_edges.first.first;
          meta[1] = iter_candidate_edges.first.second;
        }
      }
      partial_match.x_ = 2;
      partial_match.y_ = num_min_candidate;
      partial_match.meta_ = (VID_T*)malloc(sizeof(VID_T) * partial_match.x_);
      partial_match.matching_solutions_ =
          (VID_T*)malloc(sizeof(VID_T) * partial_match.x_ * partial_match.y_);

      *(partial_match.meta_) = meta[0];
      *(partial_match.meta_ + 1) = meta[1];
      auto candidate_edges_that_minimum =
          candidate_edges
              .find(std::make_pair(*(partial_match.meta_),
                                   *(partial_match.meta_ + 1)))
              ->second;
      size_t i = 0;
      for (auto& iter_candidate_edges_that_minimum :
           *candidate_edges_that_minimum) {
        *(partial_match.matching_solutions_ + i * partial_match.x_) =
            iter_candidate_edges_that_minimum.first;
        *(partial_match.matching_solutions_ + i * partial_match.x_ + 1) =
            iter_candidate_edges_that_minimum.second;
        i++;
      }
      delete candidate_edges
          .find(std::make_pair(*(partial_match.meta_),
                               *(partial_match.meta_ + 1)))
          ->second;
      candidate_edges.erase(
          std::make_pair(*(partial_match.meta_), *(partial_match.meta_ + 1)));
    }
    for (auto& iter_candidate_edges : candidate_edges) {
      bool tag = false;
      for (size_t i = 0; i < partial_match.x_; i++) {
        if (partial_match.meta_[i] == iter_candidate_edges.first.first) {
          pos_for_hash.first = i;
          pos_for_hash.second = 0;
          candidate_meta_to_join[0] = iter_candidate_edges.first.first;
          candidate_meta_to_join[1] = iter_candidate_edges.first.second;
          tag = true;
          break;
        } else if (partial_match.meta_[i] ==
                   iter_candidate_edges.first.second) {
          pos_for_hash.first = i;
          pos_for_hash.second = 1;
          candidate_meta_to_join[0] = iter_candidate_edges.first.first;
          candidate_meta_to_join[1] = iter_candidate_edges.first.second;
          tag = true;
          break;
        }
      }
      if (tag) {
        candidate = iter_candidate_edges.second;
        candidate_edges.erase(iter_candidate_edges.first);
        return std::make_pair(
            pos_for_hash, std::make_pair(candidate_meta_to_join, candidate));
      }
    }
  }

  bool JoinCandidateEdges(
      std::unordered_map<std::pair<VID_T, VID_T>,
                         std::vector<std::pair<VID_T, VID_T>>*>&
          candidate_edges,
      EDGE_LIST_T& pattern, GRAPH_T& graph,
      minigraph::executors::TaskRunner* task_runner) {
    LOG_INFO("JoinCandidateEdges");

    auto partial_match = new PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>(0, 0);

    while (!candidate_edges.empty()) {
      auto out = ExactEdgesToJoin(*partial_match, candidate_edges);

      size_t* config_for_join = (size_t*)malloc(
          sizeof(size_t) * partial_match->x_ * partial_match->y_);
      memset(config_for_join, 0,
             sizeof(size_t) * partial_match->x_ * partial_match->y_);

      auto pos_to_hash = out.first;
      auto candidate_meta_to_join = out.second.first;
      auto candidate = out.second.second;

      auto frontier_in_first =
          new folly::DMPMCQueue<size_t, false>(partial_match->y_ + 1);
      for (size_t i = 0; i < partial_match->y_; i++) {
        frontier_in_first->enqueue(i);
      }

      this->emap_->Map(frontier_in_first, graph, task_runner,
                       SubIsoEMap<GRAPH_T, CONTEXT_T>::kernel_first_join,
                       pos_to_hash, candidate, *partial_match, config_for_join);

      for (size_t i = 0; i < partial_match->y_; i++) {
        if (i == 0) {
          *(config_for_join + i * partial_match->x_ + 1) = 0;
        } else {
          *(config_for_join + i * partial_match->x_ + 1) =
              *(config_for_join + (i - 1) * partial_match->x_) +
              *(config_for_join + (i - 1) * partial_match->x_ + 1);
        }
      }
      size_t new_y = 0;
      auto frontier_in_second =
          new folly::DMPMCQueue<size_t, false>(partial_match->y_ + 1);
      for (size_t i = 0; i < partial_match->y_; i++) {
        new_y += *(config_for_join + i * partial_match->x_);
        frontier_in_second->enqueue(i);
      }
      auto new_partial_match = new PartialMatch<GID_T, VID_T, VDATA_T, EDATA_T>(
          partial_match->x_ + 1, new_y);
      memcpy(new_partial_match->meta_, partial_match->meta_,
             partial_match->x_ * sizeof(VID_T));
      new_partial_match->meta_[new_partial_match->x_ - 1] =
          candidate_meta_to_join[abs(long(pos_to_hash.second - 1))];
      auto offset = new std::atomic<size_t>(0);
      this->emap_->Map(frontier_in_second, graph, task_runner,
                       SubIsoEMap<GRAPH_T, CONTEXT_T>::kernel_second_join,
                       pos_to_hash, candidate_meta_to_join, candidate,
                       config_for_join, *new_partial_match, *partial_match,
                       offset);
      delete partial_match;
      partial_match = new_partial_match;
      partial_match->ShowPartialMatch();
    }
  };

  bool Init(GRAPH_T& graph) override {}

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
             minigraph::executors::TaskRunner* task_runner) override {
    auto c_set = InitializeCandidateVertices(*this->context_.pattern, graph,
                                             task_runner);
    // for (size_t i = 0; i < this->context_.pattern->get_num_vertexes(); i++) {
    //   for (size_t j = 0; j < graph.get_num_vertexes(); j++) {
    //     std::cout << *(c_set + i * graph.get_num_vertexes() + j) << ", ";
    //   }
    //   std::cout << std::endl;
    // }
    // RefineCandidateVertices();
    auto candidate_edges =
        FindCandidateEdges(*this->context_.pattern, graph, task_runner, c_set);

    JoinCandidateEdges(*candidate_edges, *this->context_.pattern, graph,
                       task_runner);

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