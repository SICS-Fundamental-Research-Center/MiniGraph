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

template <typename GRAPH_T, typename CONTEXT_T>
class PRAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  CONTEXT_T context_;

  PRAutoMap(CONTEXT_T& context) : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {
    context_ = context;
  }

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step)
      graph->vdata_[i] = 1 / (float)graph->get_num_vertexes();
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class PRPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  PRPIE(minigraph::VMapBase<GRAPH_T, CONTEXT_T>* vmap,
        minigraph::EMapBase<GRAPH_T, CONTEXT_T>* emap, const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(vmap, emap, context) {}

  PRPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
        const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  using Frontier = folly::DMPMCQueue<VertexInfo, false>;

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("Init() - Processing gid: ", graph.gid_);
    Bitmap* visited = new Bitmap(graph.max_vid_);
    visited->fill();
    this->auto_map_->ActiveMap(graph, task_runner, visited,
                               PRAutoMap<GRAPH_T, CONTEXT_T>::kernel_init);
    delete visited;
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_);
    auto start_time = std::chrono::system_clock::now();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();

    auto global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    auto global_vdata = this->msg_mngr_->GetGlobalVdata();
    auto vid_map = this->msg_mngr_->GetVidMap();

    size_t num_iter = 0;
    // while (in_visited->get_num_bit()) {
    while (num_iter++ < this->context_.num_iter) {
      for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
        if (in_visited->get_bit(i) == 0) continue;
        auto u = graph.GetVertexByIndex(i);
        float next = 0;
        size_t count = 0;
        for (size_t j = 0; j < u.indegree; j++) {
          if (!graph.IsInGraph(u.in_edges[j])) continue;
          VertexInfo&& v = graph.GetVertexByVid(vid_map[u.in_edges[j]]);
          next += v.vdata[0];
          count++;
          next = this->context_.gamma * (next / (float)count);
          if ((u.vdata[0] - next) * (u.vdata[0] - next) >
              this->context_.epsilon) {
            if (global_border_vid_map->get_bit(graph.localid2globalid(u.vid)))
              write_min(global_vdata + graph.localid2globalid(u.vid), next);
            write_min(u.vdata, next);
            out_visited->set_bit(u.vid);
            for (size_t j = 0; j < u.outdegree; j++) {
              if (graph.IsInGraph(u.out_edges[j]))
                out_visited->set_bit(vid_map[u.out_edges[j]]);
            }
          }
        }
      }
      // num_iter++;
      std::swap(out_visited, in_visited);
      out_visited->clear();
    }

    auto end_time = std::chrono::system_clock::now();
    std::cout << "Gid " << graph.gid_ << ":  PEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;

    delete in_visited;
    delete out_visited;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_);
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();

    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();

    auto global_border_vid_map = this->msg_mngr_->GetGlobalBorderVidMap();
    auto global_vdata = this->msg_mngr_->GetGlobalVdata();
    auto vid_map = this->msg_mngr_->GetVidMap();

    size_t num_iter = 0;
    // while (in_visited->get_num_bit()) {
    while (num_iter++ < this->context_.num_iter && !in_visited->empty()) {
      for (size_t i = 0; i < graph.get_num_vertexes(); i++) {
        if (in_visited->get_bit(i) == 0) continue;
        auto u = graph.GetVertexByIndex(i);
        float next = 0;
        size_t count = 0;
        for (size_t j = 0; j < u.indegree; j++) {
          if (!graph.IsInGraph(u.in_edges[j]) &&
              global_vdata[u.in_edges[j]] != VDATA_MAX) {
            next += global_vdata[u.in_edges[j]];
            count++;
          } else {
            VertexInfo&& v = graph.GetVertexByVid(vid_map[u.in_edges[j]]);
            next += v.vdata[0];
            count++;
          }
        }
        next = this->context_.gamma * (next / (float)count);
        if ((u.vdata[0] - next) * (u.vdata[0] - next) >
            this->context_.epsilon) {
          if (global_border_vid_map->get_bit(graph.localid2globalid(u.vid)))
            write_min(global_vdata + graph.localid2globalid(u.vid), next);
          write_min(u.vdata, next);
          out_visited->set_bit(u.vid);
          visited.set_bit(u.vid);
          for (size_t j = 0; j < u.outdegree; j++) {
            if (graph.IsInGraph(u.out_edges[j])) {
              out_visited->set_bit(vid_map[u.out_edges[j]]);
            };
          }
        }
      }
      std::swap(out_visited, in_visited);
      out_visited->clear();
    }
    return !visited.empty();
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  size_t num_iter = 5;
 // float epsilon = 0.0000001;
  float epsilon = 0;
  float gamma = 0.01;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using PRPIE_T = PRPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;
  Context context;
  auto pr_auto_map = new PRAutoMap<CSR_T, Context>(context);
  auto pr_pie = new PRPIE<CSR_T, Context>(pr_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<PRPIE<CSR_T, Context>, CSR_T>(pr_pie);

  minigraph::MiniGraphSys<CSR_T, PRPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_iter);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(30);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}