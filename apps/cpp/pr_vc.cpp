#include "2d_pie/auto_app_base.h"
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
    float next = 0;
    size_t count = 0;
    for (size_t i = 0; i < u.indegree; i++) {
      if (graph->IsInGraph(u.in_edges[i])) {
        VID_T local_nbr_id = VID_MAX;
        if (vid_map != nullptr)
          local_nbr_id = vid_map[u.in_edges[i]];
        else
          local_nbr_id = graph->globalid2localid(u.in_edges[i]);
        assert(local_nbr_id != VID_MAX);
        VertexInfo&& v = graph->GetVertexByVid(local_nbr_id);
        next += v.vdata[0];
        count++;
      }
    }
    next = this->context_.gamma * (next / (float)count);
    if ((u.vdata[0] - next) * (u.vdata[0] - next) > this->context_.epsilon) {
      write_min(u.vdata, next);
      return true;
    } else
      return false;
  }

  static bool kernel_init(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                          const size_t step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step)
      graph->vdata_[i] = 1;
    return true;
  }

  static bool kernel_push_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (!global_border_vid_map->get_bit(graph->localid2globalid(i))) continue;
      auto u = graph->GetVertexByIndex(i);
      auto global_id = graph->localid2globalid(u.vid);
      if (*(global_border_vdata + global_id) != u.vdata[0]) {
        write_min((global_border_vdata + global_id), u.vdata[0]);
        visited->set_bit(i);
      }
    }
    return true;
  }

  static bool kernel_pull_border_vertexes(GRAPH_T* graph, const size_t tid,
                                          Bitmap* visited, const size_t step,
                                          Bitmap* in_visited,
                                          Bitmap* global_border_vid_map,
                                          VDATA_T* global_vdata, VID_T* vid_map,
                                          float gamma, float epsilon) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      float next = 0;
      size_t count = 0;
      for (size_t j = 0; j < u.indegree; j++) {
        if (!graph->IsInGraph(u.in_edges[j]) &&
            global_vdata[u.in_edges[j]] != VDATA_MAX) {
          next += global_vdata[u.in_edges[j]];
          count++;
        } else if (graph->IsInGraph(u.in_edges[j])) {
          VID_T local_nbr_id = VID_MAX;
          if (vid_map != nullptr)
            local_nbr_id = vid_map[u.in_edges[i]];
          else
            local_nbr_id = graph->globalid2localid(u.in_edges[i]);
          assert(local_nbr_id != VID_MAX);
          VertexInfo&& v = graph->GetVertexByVid(local_nbr_id);
          next += v.vdata[0];
          count++;
        }
      }
      next = gamma * (next / (float)count);
      if ((u.vdata[0] - next) * (u.vdata[0] - next) > epsilon) {
        u.vdata[0] = next;
        in_visited->set_bit(u.vid);
        visited->set_bit(u.vid);
      }
    }
    return in_visited->get_num_bit();
  }

  static bool kernel_relax(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                           const size_t step, Bitmap* in_visited,
                           Bitmap* out_visited, Bitmap* global_border_vid_map,
                           VDATA_T* global_vdata, VID_T* vid_map, float gamma,
                           float epsilon) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (in_visited->get_bit(i) == 0) continue;
      auto u = graph->GetVertexByIndex(i);
      float next = 0;
      size_t count = 0;

      for (size_t j = 0; j < u.indegree; j++) {
        if (!graph->IsInGraph(u.in_edges[j]) &&
            global_vdata[u.in_edges[j]] != VDATA_MAX) {
          next += global_vdata[u.in_edges[j]];
          count++;
        } else {
          VID_T local_nbr_id = VID_MAX;
          if (vid_map != nullptr)
            local_nbr_id = vid_map[u.in_edges[i]];
          else
            local_nbr_id = graph->globalid2localid(u.in_edges[i]);
          assert(local_nbr_id != VID_MAX);
          VertexInfo&& v = graph->GetVertexByVid(local_nbr_id);
          next += v.vdata[0];
          count++;
        }
        next = gamma * (next / (float)count);
        if ((u.vdata[0] - next) * (u.vdata[0] - next) > epsilon) {
          if (global_border_vid_map->get_bit(graph->localid2globalid(u.vid)))
            write_min(global_vdata + graph->localid2globalid(u.vid), next);
          write_min(u.vdata, next);
          out_visited->set_bit(u.vid);
          visited->set_bit(u.vid);
          for (size_t j = 0; j < u.outdegree; j++) {
            if (graph->IsInGraph(u.out_edges[j])) {
              VID_T local_nbr_id = VID_MAX;
              if (vid_map != nullptr)
                local_nbr_id = vid_map[u.out_edges[i]];
              else
                local_nbr_id = graph->globalid2localid(u.out_edges[i]);
              assert(local_nbr_id != VID_MAX);
              out_visited->set_bit(local_nbr_id);
            }
          }
        }
      }
    }
    return in_visited->get_num_bit();
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class PRPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
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
    auto vid_map = this->msg_mngr_->GetVidMap();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    size_t num_iter = 0;
    while (num_iter++ < this->context_.num_iter) {
      this->auto_map_->ActiveVMap(in_visited, out_visited, graph, task_runner,
                                  vid_map, &visited);
      std::swap(in_visited, out_visited);
    }
    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        PRAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

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
    auto start_time = std::chrono::system_clock::now();
    Bitmap visited(graph.get_num_vertexes());
    visited.clear();
    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    auto vid_map = this->msg_mngr_->GetVidMap();

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        PRAutoMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes, in_visited,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata(), vid_map, this->context_.gamma,
        this->context_.epsilon);

    size_t num_iter = 0;
    while (num_iter++ < this->context_.num_iter && !in_visited->empty()) {
      LOG_INFO("iter:", num_iter);
      this->auto_map_->ActiveMap(graph, task_runner, &visited,
                                 PRAutoMap<GRAPH_T, CONTEXT_T>::kernel_relax,
                                 in_visited, out_visited,
                                 this->msg_mngr_->GetGlobalBorderVidMap(),
                                 this->msg_mngr_->GetGlobalVdata(), vid_map,
                                 this->context_.gamma, this->context_.epsilon);

      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    this->auto_map_->ActiveMap(
        graph, task_runner, &visited,
        PRAutoMap<GRAPH_T, CONTEXT_T>::kernel_push_border_vertexes,
        this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());

    auto end_time = std::chrono::system_clock::now();
    std::cout << "Gid " << graph.gid_ << ":  IncEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;
    delete in_visited;
    delete out_visited;
    return !visited.empty();
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  size_t num_iter = 3;
  float epsilon = 0.001;
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
  size_t num_iter = FLAGS_niters;
  Context context;
  context.num_iter = FLAGS_inner_niters;
  auto pr_auto_map = new PRAutoMap<CSR_T, Context>(context);
  auto pr_pie = new PRPIE<CSR_T, Context>(pr_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<PRPIE<CSR_T, Context>, CSR_T>(pr_pie);

  minigraph::MiniGraphSys<CSR_T, PRPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode, num_iter);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(30);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}