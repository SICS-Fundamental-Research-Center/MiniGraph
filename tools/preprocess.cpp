#include "graphs/edge_list.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/io/data_mngr.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <iostream>
#include <string>

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // if (FLAGS_tobin) {
  //   if (FLAGS_t == "edgelist") {
  //     assert(FLAGS_i != "" && FLAGS_o != "");
  //     std::cout << " #Converting " << FLAGS_t << ": input: " << FLAGS_i
  //               << " output: " << FLAGS_o << std::endl;
  //     std::string src_pt = FLAGS_i;
  //     std::string dst_pt = FLAGS_o;
  //     auto graph = new EDGE_LIST_T;
  //     data_mngr.edge_list_io_adapter_->Read((GRAPH_BASE_T*)graph,
  //     edge_list_csv,
  //                                           0, 0, src_pt);
  //     graph->ShowGraph();
  //     data_mngr.edge_list_io_adapter_->Write(*graph, edge_list_bin, dst_pt);
  //     std::cout << " # end Converting" << std::endl;

  //    data_mngr.edge_list_io_adapter_->Read((GRAPH_BASE_T*)graph,
  //    edge_list_bin,
  //                                          0, 0, FLAGS_edges, dst_pt);
  //  }
  //}

  if (FLAGS_p) {
    assert(FLAGS_i != "" && FLAGS_o != "");
    std::cout << " #Partitioning: "
              << " input: " << FLAGS_i << " output: " << FLAGS_o
              << " init_model: " << FLAGS_init_model << std::endl;
    std::string src_pt = FLAGS_i;
    std::string dst_pt = FLAGS_o;
    std::string init_model = FLAGS_init_model;

    size_t max_vid = FLAGS_vertexes;
    max_vid = (ceil(max_vid / 64) + 1) * 64;
    unsigned init_val = FLAGS_init_val;

    if (FLAGS_gtype == "csr_bin") {
      minigraph::utility::io::DataMngr<CSR_T> data_mngr;
      minigraph::utility::partitioner::EdgeCutPartitioner<CSR_T>
          edge_cut_partitioner(max_vid);
      auto graph =
          new minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
      data_mngr.csr_io_adapter_->Read((GRAPH_BASE_T*)graph, edge_list_csv, 0,
                                      src_pt);
      edge_cut_partitioner.RunPartition(*(CSR_T*)graph, FLAGS_n, init_model,
                                        init_val);

      if (!data_mngr.IsExist(dst_pt + "minigraph_meta/")) {
        data_mngr.MakeDirectory(dst_pt + "minigraph_meta/");
      } else {
        remove((dst_pt + "minigraph_meta/").c_str());
        data_mngr.MakeDirectory(dst_pt + "minigraph_meta/");
      }
      if (!data_mngr.IsExist(dst_pt + "minigraph_data/")) {
        data_mngr.MakeDirectory(dst_pt + "minigraph_data/");
      } else {
        remove((dst_pt + "minigraph_data/").c_str());
        data_mngr.MakeDirectory(dst_pt + "minigraph_data/");
      }
      if (!data_mngr.IsExist(dst_pt + "minigraph_vdata/")) {
        data_mngr.MakeDirectory(dst_pt + "minigraph_vdata/");
      } else {
        remove((dst_pt + "minigraph_vdata/").c_str());
        data_mngr.MakeDirectory(dst_pt + "minigraph_vdata/");
      }
      if (!data_mngr.IsExist(dst_pt + "minigraph_border_vertexes/")) {
        data_mngr.MakeDirectory(dst_pt + "minigraph_border_vertexes/");
      } else {
        remove((dst_pt + "minigraph_border_vertexes/").c_str());
        data_mngr.MakeDirectory(dst_pt + "minigraph_border_vertexes/");
      }
      if (!data_mngr.IsExist(dst_pt + "minigraph_message/")) {
        data_mngr.MakeDirectory(dst_pt + "minigraph_message/");
      } else {
        remove((dst_pt + "minigraph_message/").c_str());
        data_mngr.MakeDirectory(dst_pt + "minigraph_message/");
      }

      auto fragments = edge_cut_partitioner.GetFragments();
      size_t count = 0;
      for (auto& iter_fragments : *fragments) {
        auto fragment = (CSR_T*)iter_fragments;
        fragment->ShowGraph(3);
        std::string meta_pt =
            dst_pt + "minigraph_meta/" + std::to_string(count) + ".bin";
        std::string data_pt =
            dst_pt + "minigraph_data/" + std::to_string(count) + ".bin";
        std::string vdata_pt =
            dst_pt + "minigraph_vdata/" + std::to_string(count) + ".bin";
        data_mngr.csr_io_adapter_->Write(*fragment, csr_bin, false, meta_pt,
                                         data_pt, vdata_pt);
        count++;
      }
      // LOG_INFO("WriteBorderVertexes.");
      // data_mngr.global_border_vertexes_.reset(
      //     edge_cut_partitioner.GetGlobalBorderVertexes());
      // data_mngr.WriteBorderVertexes(*(data_mngr.global_border_vertexes_.get()),
      //                               dst_pt + "/border_vertexes/global.bv");

      LOG_INFO("WriteCommunicationMatrix.");
      auto pair_communication_matrix =
          edge_cut_partitioner.GetCommunicationMatrix();
      data_mngr.WriteCommunicationMatrix(
          dst_pt + "minigraph_border_vertexes/communication_matrix.bin",
          pair_communication_matrix.second, pair_communication_matrix.first);

      // LOG_INFO("WriteGraphDependencies.");
      // auto border_vertexes_with_dependencies =
      //     edge_cut_partitioner.GetBorderVertexesWithDependencies();
      // data_mngr.WriteGraphDependencies(
      //     *border_vertexes_with_dependencies,
      //     dst_pt + "/border_vertexes/graph_dependencies.bin");
      // data_mngr.ReadGraphDependencies(
      //     dst_pt + "/border_vertexes/graph_dependencies.bin");

      // LOG_INFO("WriteGlobalid2Gid.");
      // auto globalid2gid = edge_cut_partitioner.GetGlobalid2Gid();
      // data_mngr.WriteGlobalid2Gid(*globalid2gid,
      //                             dst_pt + "/message/globalid2gid.bin");

      // auto global_border_vertexes_by_gid =
      //     edge_cut_partitioner.GetGlobalBorderVertexesbyGid();
      // data_mngr.WriteGlobalBorderVertexesbyGid(
      //     *global_border_vertexes_by_gid,
      //     dst_pt + "/message/border_vertexes_by_gid.bin");

      auto vid_map = edge_cut_partitioner.GetVidMap();
      data_mngr.WriteVidMap(max_vid, vid_map,
                            dst_pt + "minigraph_message/vid_map.bin");

      LOG_INFO("WriteGlobalBorderVidMap.");
      auto global_border_vid_map = edge_cut_partitioner.GetGlobalBorderVidMap();
      // for (size_t i = 0; i < 34; i++) {
      //   LOG_INFO(global_border_vid_map->get_bit(i));
      // }
      data_mngr.WriteBitmap(
          global_border_vid_map,
          dst_pt + "minigraph_message/global_border_vid_map.bin");
      LOG_INFO("End graph partition#");
    } else if (FLAGS_gtype == "edge_list_bin") {
      minigraph::utility::io::DataMngr<EDGE_LIST_T> data_mngr;
      minigraph::utility::partitioner::EdgeCutPartitioner<EDGE_LIST_T>
          edge_cut_partitioner;
      if (!data_mngr.IsExist(dst_pt + "/minigraph_meta/")) {
        data_mngr.MakeDirectory(dst_pt + "/minigraph_meta/");
      } else {
        remove((dst_pt + "/minigraph_meta/").c_str());
        data_mngr.MakeDirectory(dst_pt + "/minigraph_meta/");
      }
      if (!data_mngr.IsExist(dst_pt + "/minigraph_data/")) {
        data_mngr.MakeDirectory(dst_pt + "/minigraph_data/");
      } else {
        remove((dst_pt + "/minigraph_data/").c_str());
        data_mngr.MakeDirectory(dst_pt + "/minigrph_data/");
      }
      if (!data_mngr.IsExist(dst_pt + "/minigraph_vdata/")) {
        data_mngr.MakeDirectory(dst_pt + "/minigraph_vdata/");
      } else {
        remove((dst_pt + "/minigraph_vdata/").c_str());
        data_mngr.MakeDirectory(dst_pt + "/minigraph_vdata/");
      }
      if (!data_mngr.IsExist(dst_pt + "/minigraph_border_vertexes/")) {
        data_mngr.MakeDirectory(dst_pt + "/minigraph_border_vertexes/");
      } else {
        remove((dst_pt + "/minigraph_border_vertexes/").c_str());
        data_mngr.MakeDirectory(dst_pt + "/minigraph_border_vertexes/");
      }
      if (!data_mngr.IsExist(dst_pt + "/minigraph_message/")) {
        data_mngr.MakeDirectory(dst_pt + "/minigraph_message/");
      } else {
        remove((dst_pt + "/minigraph_message/").c_str());
        data_mngr.MakeDirectory(dst_pt + "/minigraph_message/");
      }
      auto graph = new EDGE_LIST_T;
      data_mngr.edge_list_io_adapter_->Read((GRAPH_BASE_T*)graph, edge_list_csv,
                                            0, 0, src_pt);
      edge_cut_partitioner.RunPartition(*graph, FLAGS_n, init_model, init_val);
      auto fragments = edge_cut_partitioner.GetFragments();
      for (auto& iter_fragments : *fragments) {
        auto fragment = (EDGE_LIST_T*)iter_fragments;
        fragment->ShowGraph(3);
        std::string meta_pt = dst_pt + "/minigraph_meta/" +
                              std::to_string(fragment->gid_) + ".bin";
        std::string data_pt = dst_pt + "/minigraph_data/" +
                              std::to_string(fragment->gid_) + ".bin";
        std::string vdata_pt = dst_pt + "/minigraph_vdata/" +
                               std::to_string(fragment->gid_) + ".bin";
        data_mngr.edge_list_io_adapter_->Write(*fragment, edge_list_bin, false,
                                               meta_pt, data_pt, vdata_pt);
      }
      data_mngr.global_border_vertexes_.reset(
          edge_cut_partitioner.GetGlobalBorderVertexes());
      data_mngr.WriteBorderVertexes(
          *(data_mngr.global_border_vertexes_.get()),
          dst_pt + "/minigraph_border_vertexes/global.bv");
      LOG_INFO("WriteCommunicationMatrix.");
      auto pair_communication_matrix =
          edge_cut_partitioner.GetCommunicationMatrix();
      data_mngr.WriteCommunicationMatrix(
          dst_pt + "/minigraph_border_vertexes/communication_matrix.bin",
          pair_communication_matrix.second, pair_communication_matrix.first);
    }
  }
  gflags::ShutDownCommandLineFlags();
}