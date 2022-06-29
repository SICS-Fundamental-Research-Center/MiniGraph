#include <sys/stat.h>

#include <iostream>
#include <string>

#include <gflags/gflags.h>

#include "graphs/edge_list.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/io/data_mngr.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/paritioner/edge_cut_partitioner.h"

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_tobin) {
    if (FLAGS_t == "edgelist") {
      assert(FLAGS_i != "" && FLAGS_o != "");
      std::cout << " #Converting " << FLAGS_t << ": input: " << FLAGS_i
                << " output: " << FLAGS_o << std::endl;
      std::string src_pt = FLAGS_i;
      std::string dst_pt = FLAGS_o;
      minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
          edge_list_io_adapter;
      auto graph = new EDGE_LIST_T;
      edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edge_list_csv, 0, src_pt);
      edge_list_io_adapter.Write(*graph, edge_list_bin, dst_pt);
      std::cout << " # end Converting" << std::endl;
    }
  }

  if (FLAGS_p) {
    assert(FLAGS_i != "" && FLAGS_o != "");
    std::cout << " #Partitioning: "
              << " input: " << FLAGS_i << " output: " << FLAGS_o
              << " init_model: " << FLAGS_init_model << std::endl;
    std::string src_pt = FLAGS_i;
    std::string dst_pt = FLAGS_o;
    std::string init_model = FLAGS_init_model;
    unsigned init_val = FLAGS_init_val;

    minigraph::utility::io::DataMngr<gid_t, vid_t, vdata_t, edata_t> data_mngr;
    minigraph::utility::partitioner::EdgeCutPartitioner<gid_t, vid_t, vdata_t,
                                                        edata_t>
        edge_cut_partitioner;

    auto graph =
        new minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
    data_mngr.csr_io_adapter_->Read((GRAPH_BASE_T*)graph, edge_list_csv, 0,
                                    src_pt);
    edge_cut_partitioner.RunPartition(*(CSR_T*)graph, FLAGS_n, init_model,
                                      init_val);

    if (!data_mngr.IsExist(dst_pt + "/meta/")) {
      data_mngr.MakeDirectory(dst_pt + "/meta/");
    } else {
      remove((dst_pt + "/meta/").c_str());
      data_mngr.MakeDirectory(dst_pt + "/meta/");
    }
    if (!data_mngr.IsExist(dst_pt + "/data/")) {
      data_mngr.MakeDirectory(dst_pt + "/data/");
    } else {
      remove((dst_pt + "/data/").c_str());
      data_mngr.MakeDirectory(dst_pt + "/data/");
    }
    if (!data_mngr.IsExist(dst_pt + "/border_vertexes/")) {
      data_mngr.MakeDirectory(dst_pt + "/border_vertexes/");
    } else {
      remove((dst_pt + "/border_vertexes/").c_str());
      data_mngr.MakeDirectory(dst_pt + "/border_vertexes/");
    }
    if (!data_mngr.IsExist(dst_pt + "/message/")) {
      data_mngr.MakeDirectory(dst_pt + "/message/");
    } else {
      remove((dst_pt + "/message/").c_str());
      data_mngr.MakeDirectory(dst_pt + "/message/");
    }

    auto fragments = edge_cut_partitioner.GetFragments();
    size_t count = 0;
    for (auto& iter_fragments : *fragments) {
      auto fragment = (CSR_T*)iter_fragments;
      fragment->ShowGraph(99);
      std::string meta_pt = dst_pt + "/meta/" + std::to_string(count) + ".meta";
      std::string data_pt = dst_pt + "/data/" + std::to_string(count) + ".data";
      data_mngr.csr_io_adapter_->Write(*fragment, immutable_csr_bin, meta_pt,
                                       data_pt);
      count++;
    }

    data_mngr.global_border_vertexes_.reset(
        edge_cut_partitioner.GetGlobalBorderVertexes());
    data_mngr.WriteBorderVertexes(*(data_mngr.global_border_vertexes_.get()),
                                  dst_pt + "/border_vertexes/global.bv");
    auto pair_communication_matrix =
        edge_cut_partitioner.GetCommunicationMatrix();
    auto border_vertexes_with_dependencies =
        edge_cut_partitioner.GetBorderVertexesWithDependencies();
    data_mngr.WriteGraphDependencies(
        *border_vertexes_with_dependencies,
        dst_pt + "/border_vertexes/graph_dependencies.bin");
    data_mngr.ReadGraphDependencies(dst_pt +
                                    "/border_vertexes/graph_dependencies.bin");
    data_mngr.WriteCommunicationMatrix(
        dst_pt + "/border_vertexes/communication_matrix.bin",
        pair_communication_matrix.second, pair_communication_matrix.first);

    auto globalid2gid = edge_cut_partitioner.GetGlobalid2Gid();
    data_mngr.WriteGlobalid2Gid(*globalid2gid,
                                dst_pt + "/message/globalid2gid.bin");

    auto global_border_vertexes_by_gid =
        edge_cut_partitioner.GetGlobalBorderVertexesbyGid();
    data_mngr.WriteGlobalBorderVertexesbyGid(
        *global_border_vertexes_by_gid,
        dst_pt + "/message/border_vertexes_by_gid.bin");

    // data_mngr.ReadCommunicationMatrix(
    //     dst_pt + "/border_vertexes/communication_matrix.bin");
    auto out = data_mngr.ReadGlobalid2Gid(dst_pt + "/message/globalid2gid.bin");
    std::cout << " # end Partitioning: n = " << FLAGS_n << std::endl;
  }
  gflags::ShutDownCommandLineFlags();
}