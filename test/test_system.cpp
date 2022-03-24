
#include "components/component_base.h"
#include "components/computing_component.h"
#include "components/discharge_component.h"
#include "components/load_component.h"
#include "minigraph_sys.h"
#include <string>
int main(int argc, char* argv[]) {
  XLOG(INFO, "COMMAND: ", argv[1], argv[2], argv[3], argv[4], argv[5]);
  std::string work_space = argv[1];
  size_t num_workers_lc = atoi(argv[2]);
  size_t num_workers_cc = atoi(argv[3]);
  size_t num_workers_dc = atoi(argv[4]);
  size_t num_threads_cpu = atoi(argv[5]);
  size_t max_threads_io = atoi(argv[6]);
  bool is_partition = atoi(argv[7]);
  minigraph::MiniGraphSys<gid_t, vid_t, vdata_t, edata_t> system(
      work_space, num_workers_lc, num_workers_cc, num_workers_dc,
      num_threads_cpu, max_threads_io, is_partition);
  system.RunSys();
}