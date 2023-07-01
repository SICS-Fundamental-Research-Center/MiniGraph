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
    return false;
  }

  bool F(VertexInfo& u, GRAPH_T* graph = nullptr,
         VID_T* vid_map = nullptr) override {
    return false;
  }

  static inline bool Drop() { return rand() % 2 == 0; }

  static bool kernel_update(GRAPH_T* graph, const size_t tid, Bitmap* visited,
                            const size_t step, VDATA_T* vdata,
                            unsigned short* walks_count,
                            const size_t current_step,
                            const size_t walks_per_source, bool* active) {
    for (size_t i = tid; i < graph->get_num_vertexes(); i += step) {
      auto u = graph->GetVertexByIndex(i);
      if (walks_count[graph->localid2globalid(i)] >= walks_per_source) continue;
      if (vdata[graph->localid2globalid(i)] != current_step) continue;
      for (size_t k = 0; k < u.outdegree; k++) {
        if (walks_count[graph->localid2globalid(i)] >= walks_per_source) break;
        if (Drop()) continue;
        write_max(vdata + u.out_edges[k], (VDATA_T)(current_step + 1));
        write_add(walks_count + graph->localid2globalid(i), (unsigned short)1);
        visited->set_bit(graph->localid2globalid(i));
        if ((*active) == false) *active = true;
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

    auto local_t = __sync_fetch_and_add(&this->context_.t, (unsigned)1);
    if (local_t == 0) {
      auto vdata = this->msg_mngr_->GetGlobalVdata();
      memset(
          vdata, 0,
          sizeof(typename GRAPH_T::vdata_t) * this->msg_mngr_->get_max_vid());
      this->context_.walks_count = (unsigned short*)malloc(
          sizeof(unsigned short) * this->msg_mngr_->get_max_vid());
      memset(this->context_.walks_count, 0,
             sizeof(unsigned short) * this->msg_mngr_->get_max_vid());
    }
    return true;
  }

  bool PEval(GRAPH_T& graph,
             minigraph::executors::TaskRunner* task_runner) override {
    LOG_INFO("PEval() - Processing gid: ", graph.gid_,
             " num_vertexes: ", graph.get_num_vertexes());
    auto start_time = std::chrono::system_clock::now();
    // graph.ShowGraph(3);
    Bitmap* visited = new Bitmap(graph.get_num_vertexes());
    visited->clear();

    bool active = false;

    for (size_t i = 0; i < this->context_.niters; i++) {
      this->auto_map_->ActiveMap(graph, task_runner, visited,
                                 RWAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
                                 this->msg_mngr_->GetGlobalVdata(),
                                 this->context_.walks_count, i,
                                 this->context_.walks_per_source, &active);
      // LOG_INFO(i, "-th active vertices: ", visited->get_num_bit());
      LOG_INFO(i, "-th active vertices: ");
      visited->clear();
      if (!active) {
        break;
      }
    }

    delete visited;
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

    Bitmap* visited = new Bitmap(graph.get_num_vertexes());
    visited->clear();

    bool active = false;
    for (size_t i = 0; i < this->context_.niters; i++) {
      this->auto_map_->ActiveMap(graph, task_runner, visited,
                                 RWAutoMap<GRAPH_T, CONTEXT_T>::kernel_update,
                                 this->msg_mngr_->GetGlobalVdata(),
                                 this->context_.walks_count, i,
                                 this->context_.walks_per_source, &active);
      // LOG_INFO(i, "-th active vertices: ", visited->get_num_bit());
      LOG_INFO(i, "-th active vertices: ");
      visited->clear();
      if (!active) break;
    }

    auto end_time = std::chrono::system_clock::now();
    std::cout << "Gid " << graph.gid_ << ":  IncEval elapse time "
              << std::chrono::duration_cast<std::chrono::microseconds>(
                     end_time - start_time)
                         .count() /
                     (double)CLOCKS_PER_SEC
              << std::endl;
    delete visited;
    return active;
  }

  bool Aggregate(void* a, void* b,
                 minigraph::executors::TaskRunner* task_runner) override {
    if (a == nullptr || b == nullptr) return false;
  }
};

struct Context {
  vdata_t walks_per_source = 5;
  size_t niters = 5;
  unsigned short* walks_count;
  size_t num_vertexes = 0;
  unsigned t = 0;
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
  context.niters = FLAGS_niters;
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