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

void EdgeList2CSR(std::string src_pt, std::string dst_pt, std::size_t cores,
                  std::size_t num_partitions, size_t max_vid,
                  std::string init_model, unsigned init_val,
                  char separator_params = ',', const bool frombin = false) {
  max_vid = (ceil(max_vid / 64) + 1) * 64;
  minigraph::utility::io::DataMngr<CSR_T> data_mngr;
  minigraph::utility::partitioner::EdgeCutPartitioner<CSR_T>
      edge_cut_partitioner(max_vid);

  if (frombin) {
    edge_cut_partitioner.ParallelPartitionFromBin(
        src_pt, num_partitions, max_vid, cores, init_model, init_val);
  } else {
    edge_cut_partitioner.ParallelPartition(src_pt, separator_params,
                                           num_partitions, max_vid, cores,
                                           init_model, init_val);
  }

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

  LOG_INFO("WriteCommunicationMatrix.");
  auto pair_communication_matrix =
      edge_cut_partitioner.GetCommunicationMatrix();
  data_mngr.WriteCommunicationMatrix(
      dst_pt + "minigraph_border_vertexes/communication_matrix.bin",
      pair_communication_matrix.second, pair_communication_matrix.first);

  LOG_INFO("WriteVidMap.");
  auto vid_map = edge_cut_partitioner.GetVidMap();
  data_mngr.WriteVidMap(edge_cut_partitioner.GetMaxVid(), vid_map,
                        dst_pt + "minigraph_message/vid_map.bin");

  auto global_border_vid_map = edge_cut_partitioner.GetGlobalBorderVidMap();
  LOG_INFO("WriteGlobalBorderVidMap. size: ", global_border_vid_map->size_);
  data_mngr.WriteBitmap(global_border_vid_map,
                        dst_pt + "minigraph_message/global_border_vid_map.bin");
  LOG_INFO("End graph partition#");
}

void EdgeList2EdgeList(std::string src_pt, std::string dst_pt,
                       char separator_params = ',', const size_t cores = 1) {
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
  edge_list_io_adapter.ParallelRead((GRAPH_BASE_T*)graph, edge_list_csv,
                                    separator_params, 0, cores, src_pt);
  edge_list_io_adapter.Write(*graph, edge_list_bin, false, meta_pt, data_pt,
                             vdata_pt);
}

void EdgeListCSV2EdgeListBin(std::string src_pt, std::string dst_pt,
                             char separator_params = ',') {
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
  edge_list_io_adapter.Read((GRAPH_BASE_T*)graph, edge_list_csv,
                            separator_params, 0, src_pt);
  edge_list_io_adapter.Write(*graph, edge_list_bin, false, meta_pt, data_pt,
                             vdata_pt);
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_i != "" && FLAGS_o != "");
  std::string src_pt = FLAGS_i;
  std::string dst_pt = FLAGS_o;
  std::string init_model = FLAGS_init_model;
  std::size_t cores = FLAGS_cores;
  std::size_t max_vid = FLAGS_vertexes;
  std::string graph_type = FLAGS_t;

  if (FLAGS_p) {
    std::size_t num_partitions = FLAGS_n;
    std::cout << " #Partitioning: "
              << " input: " << FLAGS_i << " output: " << FLAGS_o
              << " init_model: " << FLAGS_init_model
              << " cores: " << FLAGS_cores << std::endl;

    if (graph_type == "csr_bin") {
      EdgeList2CSR(src_pt, dst_pt, cores, num_partitions, max_vid, init_model,
                   FLAGS_init_val, *FLAGS_sep.c_str(), FLAGS_frombin);
    }
  }
  if (FLAGS_tobin) {
    if (graph_type == "edgelist_bin") {
      EdgeList2EdgeList(src_pt, dst_pt, *FLAGS_sep.c_str());
    }
  }
  gflags::ShutDownCommandLineFlags();
}