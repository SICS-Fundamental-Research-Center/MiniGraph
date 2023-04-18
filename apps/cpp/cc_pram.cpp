#include "2d_pie/auto_app_base.h"
#include "executors/task_runner.h"
#include "graphs/graph.h"
#include "minigraph_sys.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/bitmap.h"
#include "utility/logging.h"
#include <folly/concurrency/DynamicBoundedQueue.h>


int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::string work_space = FLAGS_i;
  size_t num_workers_lc = FLAGS_lc;
  size_t num_workers_cc = FLAGS_cc;
  size_t num_workers_dc = FLAGS_dc;
  size_t num_cores = FLAGS_cores;
  size_t buffer_size = FLAGS_buffer_size;

  Context context;
  auto wcc_auto_map = new WCCAutoMap<CSR_T, Context>();
  auto bfs_pie = new WCCPIE<CSR_T, Context>(wcc_auto_map, context);
  auto app_wrapper =
      new minigraph::AppWrapper<WCCPIE<CSR_T, Context>, CSR_T>(bfs_pie);

  minigraph::MiniGraphSys<CSR_T, WCCPIE_T> minigraph_sys(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc, num_cores,
      buffer_size, app_wrapper, FLAGS_mode);
  minigraph_sys.RunSys();
  // minigraph_sys.ShowResult(20);
  gflags::ShutDownCommandLineFlags();
  exit(0);
}