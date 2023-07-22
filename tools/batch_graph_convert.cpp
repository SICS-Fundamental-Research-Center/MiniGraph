#include <gflags/gflags.h>
#include <sys/stat.h>
#include <iostream>
#include <string>

#include "graphs/edgelist.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/io/edge_list_io_adapter.h"

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

void BatchEdgeList2EdgeList(std::string src_pt, std::string dst_pt,
                            char separator_params = ',',
                            const size_t cores = 1) {
  std::cout << " #Converting " << FLAGS_t << ": input: " << src_pt
            << " output: " << dst_pt << std::endl;
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edge_list_io_adapter;
  auto graph = new EDGE_LIST_T;
  std::string meta_pt = dst_pt + "minigraph_meta" + ".bin";
  std::string data_pt = dst_pt + "minigraph_data" + ".bin";
  std::string vdata_pt = dst_pt + "minigraph_vdata" + ".bin";
  LOG_INFO("Write: ", meta_pt);
  LOG_INFO("Write: ", data_pt);
  LOG_INFO("Write: ", vdata_pt);
  edge_list_io_adapter.BatchParallelRead((GRAPH_BASE_T*)graph, edgelist_csv,
                                         separator_params, 0, cores, src_pt);
  edge_list_io_adapter.Write(*graph, edgelist_bin, meta_pt, data_pt, vdata_pt);
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_i != "" && FLAGS_o != "");
  std::string src_pt = FLAGS_i;
  std::string dst_pt = FLAGS_o;
  std::string init_model = FLAGS_init_model;
  std::size_t cores = FLAGS_cores;
  std::string graph_type = FLAGS_t;

  if (FLAGS_tobin && FLAGS_frombin == false && FLAGS_in_type == "edgelist" &&
      FLAGS_out_type == "edgelist") {
      BatchEdgeList2EdgeList(src_pt, dst_pt, *FLAGS_sep.c_str(), cores);
  }
  gflags::ShutDownCommandLineFlags();
}
