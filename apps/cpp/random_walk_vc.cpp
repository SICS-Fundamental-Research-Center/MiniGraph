#include "2d_pie/auto_app_base.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"

template <typename GRAPH_T, typename CONTEXT_T>
class RWAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  RWAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  // static inline size_t walks_per_source() { return 100; }
  static inline bool is_source(VID_T vid) { return (Hash(vid) % 50 == 0); }

  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    return write_min(v.vdata, u.vdata[0]);
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static bool kernel_init_source(GRAPH_T* graph, const size_t tid,
                                 Bitmap* visited, const size_t step,
                                 Bitmap* in_visited, Bitmap* out_visited,
                                 Bitmap* global_border_vid_map,

                                 VDATA_T* global_border_vdata) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (in_visited->get_bit(i) == 0) continue;
      auto u = graph->GetVertexByIndex(i);
      if (is_source(graph->localid2globalid(u.vid))) {
        if (write_max(u.vdata, (VDATA_T)1)) {
          out_visited->set_bit(u.vid);
          visited->set_bit(u.vid);
        }
        if (global_border_vid_map->get_bit(graph->localid2globalid(u.vid)) !=
            0) {
          write_max(global_border_vdata + graph->localid2globalid(u.vid),
                    (VDATA_T)1);
        }
      }
    }
    return true;
  }

  static bool kernel_update(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                            const size_t step, Bitmap* in_visited,
                            Bitmap* out_visited, Bitmap* global_border_vid_map,
                            VDATA_T* global_border_vdata,
                            const size_t current_step,
                            const size_t walks_per_source) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (in_visited->get_bit(i) == 0) continue;
      auto u = graph->GetVertexByIndex(i);
      for (size_t k = 0; k < walks_per_source; k++) {
        auto global_nbr_id = u.get_random_out_edge();
        if (global_nbr_id == VID_MAX) continue;
        if (graph->IsInGraph(global_nbr_id)) {
          auto local_nbr_id = graph->globalid2localid(global_nbr_id);
          auto nbr = graph->GetVertexByIndex(local_nbr_id);
          if (write_max(nbr.vdata, (VDATA_T)current_step)) {
            visited->set_bit(local_nbr_id);
            out_visited->set_bit(local_nbr_id);
          }
          if (global_border_vid_map->get_bit(global_nbr_id)) {
            write_max(global_border_vdata + global_nbr_id,
                      (VDATA_T)current_step);
          }
        } else {
          write_max(global_border_vdata + global_nbr_id, (VDATA_T)current_step);
        }
      }
    }
    return true;
  }

  static bool kernel_pull_border_vertexes(
      GRAPH_T* graph, const size_t tid, Bitmap* visited, const size_t step,
      Bitmap* in_visited, Bitmap* out_visited, Bitmap* global_border_vid_map,
      VDATA_T* global_border_vdata, const size_t current_step) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (in_visited->get_bit(i) == 0) continue;
      auto u = graph->GetVertexByIndex(i);

      for (size_t j = 0; j < u.indegree; j++) {
        auto in_nbr_id = u.in_edges[j];
        if (!global_border_vid_map->get_bit(in_nbr_id)) continue;
        if (global_border_vdata[in_nbr_id] != current_step - 1) continue;
        auto origin = u.vdata[0];
        if (write_max(u.vdata, global_border_vdata[in_nbr_id] + 1)) {
          visited->set_bit(u.vid);
          out_visited->set_bit(u.vid);
        }
      }
    }
    return true;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class RWPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;

 public:
  RWPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
        const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("Init() - Processing gid: ", graph.gid_);
    graph.InitVdata2AllX(0);
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());
    auto start_time = std::chrono::system_clock::now();

    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* visited = new Bitmap(graph.get_num_vertexes());
    visited->clear();
    in_visited->fill();
    out_visited->clear();

    this->auto_map_->ActiveMap(
        graph, task_runner, visited,
        RWAutoMap<GRAPH_T, CONTEXT_T>::kernel_init_source, in_visited,
        out_visited, this->msg_mngr_->GetGlobalBorderVidMap(),
        this->msg_mngr_->GetGlobalVdata());
    std::swap(in_visited, out_visited);
    out_visited->clear();

    for (auto step = 1; step <= this->context_.inner_niters; step++) {
      this->auto_map_->ActiveMap(graph, task_runner, visited,
                                 RWAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
                                 in_visited, out_visited,
                                 this->msg_mngr_->GetGlobalBorderVidMap(),
                                 this->msg_mngr_->GetGlobalVdata(), step,
                                 this->context_.walks_per_source);
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    delete visited;
    delete in_visited;
    delete out_visited;
    auto end_time = std::chrono::system_clock::now();
    std::cout << "Gid " << graph.gid_ << ":  PEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());
    auto start_time = std::chrono::system_clock::now();

    Bitmap* in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* out_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap* visited = new Bitmap(graph.get_num_vertexes());
    visited->clear();
    in_visited->fill();
    out_visited->clear();

    for (auto step = 2; step <= this->context_.inner_niters; step++) {
      in_visited->fill();

      // pull border vertexes whose vdata = step.
      this->auto_map_->ActiveMap(
          graph, task_runner, visited,
          RWAutoMap<GRAPH_T, CONTEXT_T>::kernel_pull_border_vertexes,
          in_visited, out_visited, this->msg_mngr_->GetGlobalBorderVidMap(),
          this->msg_mngr_->GetGlobalVdata(), step);
      std::swap(in_visited, out_visited);
      out_visited->clear();

      // iteratively walks over active vertexes whose label = step, until
      // reaching a pre-defined niters.
      for (auto inner_step = step; inner_step < this->context_.inner_niters;
           inner_step++) {
        this->auto_map_->ActiveMap(graph, task_runner, visited,
                                   RWAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
                                   in_visited, out_visited,
                                   this->msg_mngr_->GetGlobalBorderVidMap(),
                                   this->msg_mngr_->GetGlobalVdata(),
                                   inner_step, this->context_.walks_per_source);
        std::swap(in_visited, out_visited);
        out_visited->clear();
      }
    }

    delete visited;
    delete in_visited;
    delete out_visited;
    auto end_time = std::chrono::system_clock::now();
    std::cout << "Gid " << graph.gid_ << ":  IncEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;
    return true;
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  vdata_t walks_per_source = 1;
  size_t inner_niters = 10;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using RWPIE_T = RWPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;

  Context context;
  context.inner_niters = FLAGS_inner_niters;
  context.walks_per_source = FLAGS_walks_per_source;
  auto rw_auto_map = new RWAutoMap<CSR_T, Context>();
  auto rw_pie = new RWPIE<CSR_T, Context>(rw_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<RWPIE<CSR_T, Context>, CSR_T>(rw_pie);

  minigraph::MiniGraphSys<CSR_T, RWPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode, FLAGS_niters);
  minigraph_sys.RunSys();
  gflags::ShutDownCommandLineFlags();
  exit(0);
}