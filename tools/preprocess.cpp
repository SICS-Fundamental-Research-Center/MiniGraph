#include "graphs/edge_list.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "rapidcsv.h"
#include "utility/io/edge_list_io_adapter.h"
#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
int main(int argc, char* argv[]) {
  std::string src_pt = argv[1];
  std::string dst_pt = argv[2];
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edge_list_io_adapter;

  auto graph = new EDGE_LIST_T;
  edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edge_list_csv, 0, src_pt);
  edge_list_io_adapter.Write(*graph, edge_list_bin, dst_pt);
}