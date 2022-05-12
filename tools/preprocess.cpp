//#include "cxxopts.hpp"
#include "graphs/edge_list.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/io/data_mngr.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_tobin) {
    if (FLAGS_t == "edgelist") {
      assert(FLAGS_i != "" && FLAGS_o != "");
      cout << " #Converting " << FLAGS_t << ": input: " << FLAGS_i
           << " output: " << FLAGS_o << endl;
      std::string src_pt = FLAGS_i;
      std::string dst_pt = FLAGS_o;
      minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
          edge_list_io_adapter;
      auto graph = new EDGE_LIST_T;
      edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edge_list_csv, 0, src_pt);
      edge_list_io_adapter.Write(*graph, edge_list_bin, dst_pt);
      cout << " # end Converting" << endl;
    }
  }

  if (FLAGS_p) {
    assert(FLAGS_i != "" && FLAGS_o != "");
    cout << " #Partitioning: "
         << " input: " << FLAGS_i << " output: " << FLAGS_o
         << " init_model: " << FLAGS_init_model << endl;
    std::string src_pt = FLAGS_i;
    std::string dst_pt = FLAGS_o;
    std::string init_model = FLAGS_init_model;
    unsigned init_val = FLAGS_init_val;

    minigraph::utility::io::DataMgnr<gid_t, vid_t, vdata_t, edata_t> data_mngr;
    minigraph::utility::partitioner::EdgeCutPartitioner<gid_t, vid_t, vdata_t,
                                                        edata_t>
        edge_cut_partitioner(src_pt, dst_pt);
    edge_cut_partitioner.RunPartition(FLAGS_n, init_model, init_val);
    edge_cut_partitioner.GetGlobalBorderVertexes();
    data_mngr.global_border_vertexes_.reset(
        edge_cut_partitioner.GetGlobalBorderVertexes());
    data_mngr.WriteBorderVertexes(*(data_mngr.global_border_vertexes_.get()),
                                  dst_pt + "/border_vertexes/global.bv");
    cout << " # end Partitioning: n = " << FLAGS_n << endl;
  }
  gflags::ShutDownCommandLineFlags();
}