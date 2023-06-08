#include "2d_pie/auto_app_base.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/atomic.h"
#include "utility/bitmap.h"
#include "utility/logging.h"

template <typename GRAPH_T, typename CONTEXT_T>
class ColoringAutoMap : public minigraph::AutoMapBase<GRAPH_T, CONTEXT_T> {
  using GID_T = typename GRAPH_T::gid_t;
  using VID_T = typename GRAPH_T::vid_t;
  using VDATA_T = typename GRAPH_T::vdata_t;
  using EDATA_T = typename GRAPH_T::edata_t;
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  ColoringAutoMap() : minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>() {}

  // Push vdata from u to v.
  bool F(const VertexInfo& u, VertexInfo& v,
         GRAPH_T* graph = nullptr) override {
    auto tag = false;
    return tag;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static void kernel_update(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                            const size_t step, VDATA_T* global_border_vdata,
                            Bitmap* out_visited,
                            typename GRAPH_T::vid_t& local_upper_bound,
                            typename GRAPH_T::vid_t& upper_bound) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      if (graph->localid2globalid(i) > upper_bound) continue;
      auto u = graph->GetVertexByIndex(i);
      for (size_t j = 0; j < u.outdegree; ++j) {
        if (graph->localid2globalid(i) < u.out_edges[j]) {
          if (global_border_vdata[graph->localid2globalid(i)] ==
              global_border_vdata[u.out_edges[j]]) {
            ++global_border_vdata[graph->localid2globalid(i)];
            ++u.vdata[0];
            write_max(&local_upper_bound, graph->localid2globalid(i));
            out_visited->set_bit(i);
          }
        }
      }
    }
    return;
  }
};

template <typename GRAPH_T, typename CONTEXT_T>
class ColoringPIE : public minigraph::AutoAppBase<GRAPH_T, CONTEXT_T> {
  using VertexInfo = minigraph::graphs::VertexInfo<typename GRAPH_T::vid_t,
                                                   typename GRAPH_T::vdata_t,
                                                   typename GRAPH_T::edata_t>;

 public:
  ColoringPIE(minigraph::AutoMapBase<GRAPH_T, CONTEXT_T>* auto_map,
              const CONTEXT_T& context)
      : minigraph::AutoAppBase<GRAPH_T, CONTEXT_T>(auto_map, context) {}

  bool Init(GRAPH_T& graph,
            minigraph::executors::TaskRunner* task_runner) override {
    graph.InitVdata2AllX(0);
    write_max(&this->context_.upper_bound, graph.get_max_vid());
    auto vdata = this->msg_mngr_->GetGlobalVdata();
    memset(vdata, 0,
           sizeof(typename GRAPH_T::vdata_t) * graph.get_aligned_max_vid());
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());

    Bitmap *in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap *out_visited= new Bitmap(graph.get_num_vertexes());
    Bitmap visited(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    visited.clear();

    typename GRAPH_T::vid_t local_upper_bound = 0;
    while (!in_visited->empty()) {
      LOG_INFO(in_visited->get_num_bit());
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          ColoringAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
          this->msg_mngr_->GetGlobalVdata(), out_visited, local_upper_bound,
          this->context_.upper_bound);
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }
    write_add(&this->context_.num_graphs, 1);
    delete in_visited;
    delete out_visited;
    return true;
  }

  bool IncEval(GRAPH_T& graph,
               minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("IncEval: ", graph.get_gid());
    Bitmap *in_visited = new Bitmap(graph.get_num_vertexes());
    Bitmap *out_visited= new Bitmap(graph.get_num_vertexes());
    Bitmap visited(graph.get_num_vertexes());
    in_visited->fill();
    out_visited->clear();
    visited.clear();

    typename GRAPH_T::vid_t local_upper_bound = 0;
    while (!in_visited->empty()) {
      LOG_INFO(in_visited->get_num_bit());
      this->auto_map_->ActiveMap(
          graph, task_runner, &visited,
          ColoringAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
          this->msg_mngr_->GetGlobalVdata(), out_visited, local_upper_bound,
          this->context_.upper_bound);
      std::swap(in_visited, out_visited);
      out_visited->clear();
    }

    write_max(&this->context_.sync_bound, local_upper_bound);
    // The last fragment in a BSP step responsible updating upper_bound
    write_add(&this->context_.inc_vote, 1);
    if (this->context_.inc_vote == this->context_.num_graphs) {
      write_min(&this->context_.upper_bound, this->context_.sync_bound);
      this->context_.inc_vote = 0;
      this->context_.sync_bound = 0;
    }
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
  std::vector<Bitmap*> vec_T;
  int inc_step = 0;
  int num_graphs = 0;
  int inc_vote = 0;
  vid_t upper_bound = 0;
  vid_t sync_bound = 0;
};

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using ColoringPIE_T = ColoringPIE<CSR_T, Context>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;
  size_t num_vertexes = FLAGS_vertexes;

  assert(FLAGS_vertexes != 0);
  Context context;
  for (size_t i = 0; i < FLAGS_niters; i++) {
    auto bitmap = new Bitmap(num_vertexes);
    bitmap->clear();
    context.vec_T.push_back(bitmap);
  }

  auto coloring_auto_map = new ColoringAutoMap<CSR_T, Context>();
  auto coloring_pie =
      new ColoringPIE<CSR_T, Context>(coloring_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<ColoringPIE<CSR_T, Context>, CSR_T>(
          coloring_pie);

  minigraph::MiniGraphSys<CSR_T, ColoringPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode, FLAGS_niters, FLAGS_scheduler);
  minigraph_sys.RunSys();
  gflags::ShutDownCommandLineFlags();
  exit(0);
}