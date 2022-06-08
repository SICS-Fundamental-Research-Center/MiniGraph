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
    if (u.outdegree > v.outdegree) {
      tag = false;
    }
    if (u.indegree > v.indegree) {
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

      for (size_t nbr_candidate_index = 0;
           nbr_candidate_index < candidate.outdegree; nbr_candidate_index++) {
        auto nbr_candidate_vid = candidate.out_edges[nbr_candidate_index];

        // compare the neighbor of u with the neighbor of the candidate of u.
        for (size_t nbr_u_index = 0; nbr_u_index < u.outdegree; nbr_u_index++) {
          auto nbr_u_vid = u.out_edges[nbr_u_index];
          if (*(c_set + nbr_u_vid * graph->get_num_vertexes() +
                nbr_candidate_vid)) {
            LOG_INFO("     pattern:edges: ", u.vid, ", ", nbr_u_vid,
                     " candidate: ", candidate_u_vid, ", ", nbr_candidate_vid);

            auto iter_candidate_edges =
                candidate_edges->find(std::make_pair(u.vid, nbr_u_vid));
            if (iter_candidate_edges != candidate_edges->end()) {
              mtx->lock();
              iter_candidate_edges->second->push_back(
                  std::make_pair(candidate_u_vid, nbr_candidate_vid));
              mtx->unlock();
            } else {
              mtx->lock();
              auto vec_edges = new std::vector<std::pair<VID_T, VID_T>>;
              vec_edges->push_back(
                  std::make_pair(candidate_u_vid, nbr_candidate_vid));
              candidate_edges->insert(
                  std::make_pair(std::make_pair(u.vid, nbr_u_vid), vec_edges));
              mtx->unlock();
            };
            count++;
            break;
          }
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

  struct PartialMatch {
    size_t num_vertexes_of_meta = 0;
    VID_T* meta;
    VID_T* matching_solutions;
  };

 public:
  SubIsoEMap(const CONTEXT_T& context)
      : minigraph::EMapBase<GRAPH_T, CONTEXT_T>(context) {}
  bool C(const VertexInfo& u, const VertexInfo& v) override { return false; }
  bool F(const VertexInfo& u, VertexInfo& v) override { return false; }

  static void kernel_first_join(size_t tid, PartialMatch& partial_match,
                                EDGE_LIST_T& pattern, GRAPH_T& graph) {}

  static void kernel_second_join() {}
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

  struct PartialMatch {
    size_t num_vertexes_of_meta = 0;
    VID_T* meta;
    VID_T* matching_solutions;
  };

  bool* InitializeCandidateVertices(
      EDGE_LIST_T& pattern, GRAPH_T& graph,
      minigraph::executors::TaskRunner* task_runner) {
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
    for (auto iter : *pattern.vertexes_info_) {
      auto vid = iter.first;
      if (initialize[vid] == false) {
        Frontier* frontier_in = new Frontier(graph.get_num_vertexes());

        for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
          VertexInfo&& u = graph.GetVertexByIndex(i);
          frontier_in->enqueue(u);
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

  bool RefineCandidateVertices() {
    // TO ADD...;
  }

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

  bool JoinCandidateEdges(
      std::unordered_map<std::pair<VID_T, VID_T>,
                         std::vector<std::pair<VID_T, VID_T>>*>&
          candidate_edges) {
    size_t num_min_candidate = std::string::npos;
    size_t capacity = 1;
    std::pair<VID_T, VID_T> edge_min_candidate;
    struct Edge {
      std::pair<VID_T, VID_T> src_dst;
    };
    PartialMatch partial_match;
    partial_match.num_vertexes_of_meta = 2;
    partial_match.meta = (VID_T*)malloc(sizeof(VID_T) * 2);

    for (auto& iter_candidate_edges : candidate_edges) {
      capacity *= iter_candidate_edges.second->size();
      if (iter_candidate_edges.second->size() < num_min_candidate) {
        num_min_candidate = iter_candidate_edges.second->size();
        *(partial_match.meta) = iter_candidate_edges.first.first;
        *(partial_match.meta + 1) = iter_candidate_edges.first.second;
      }
    }
    auto frontier_in =
        new folly::DMPMCQueue<size_t, false>(num_min_candidate + 1);

    partial_match.matching_solutions =
        (VID_T*)malloc(sizeof(VID_T) * 2 * num_min_candidate);

    auto candidate_edges_that_minimum =
        candidate_edges
            .find(std::make_pair(*(partial_match.meta),
                                 *(partial_match.meta + 1)))
            ->second;
    size_t i = 0;
    for (auto& iter_candidate_edges_that_minimum :
         *candidate_edges_that_minimum) {
      *(partial_match.matching_solutions +
        i * partial_match.num_vertexes_of_meta) =
          iter_candidate_edges_that_minimum.first;
      *(partial_match.matching_solutions +
        i * partial_match.num_vertexes_of_meta + 1) =
          iter_candidate_edges_that_minimum.second;
    }
    LOG_INFO(*(partial_match.matching_solutions +
               i * partial_match.num_vertexes_of_meta),
             ", ",
             *(partial_match.matching_solutions +
               i * partial_match.num_vertexes_of_meta + 1));
    delete candidate_edges
        .find(std::make_pair(*(partial_match.meta), *(partial_match.meta + 1)))
        ->second;

    candidate_edges.erase(
        std::make_pair(*(partial_match.matching_solutions +
                         i * partial_match.num_vertexes_of_meta),
                       *(partial_match.matching_solutions +
                         i * partial_match.num_vertexes_of_meta + 1)));
    LOG_INFO("XXXXXXXXXX");
    while (!candidate_edges.empty()) {
//      this->emap_->Map<size_t>(frontier_in, );
    }
    //  for (size_t join_time = 0; join_time < candidate_edges.size() - 1;
    //       join_time++) {
    //
    //  }
  };

  bool Init(GRAPH_T& graph) override {}

  bool PEval(GRAPH_T& graph, PARTIAL_RESULT_T* partial_result,
             minigraph::executors::TaskRunner* task_runner) override {
    auto c_set = InitializeCandidateVertices(*this->context_.pattern, graph,
                                             task_runner);
    for (size_t i = 0; i < this->context_.pattern->get_num_vertexes(); i++) {
      for (size_t j = 0; j < graph.get_num_vertexes(); j++) {
        std::cout << *(c_set + i * graph.get_num_vertexes() + j) << ", ";
      }
      std::cout << std::endl;
    }
    // RefineCandidateVertices();
    auto candidate_edges =
        FindCandidateEdges(*this->context_.pattern, graph, task_runner, c_set);

    for (auto& iter : *candidate_edges) {
      LOG_INFO(iter.first.first, ", ", iter.first.second);
    }

    JoinCandidateEdges(*candidate_edges);

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
      new AppWrapper<SubIsoPIE<CSR_T, Context>, gid_t, vid_t, vdata_t, edata_t>(
          bfs_pie);

  minigraph::MiniGraphSys<CSR_T, SubIsoPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      app_wrapper);
  minigraph_sys.RunSys();

  minigraph_sys.ShowResult();
  gflags::ShutDownCommandLineFlags();
}