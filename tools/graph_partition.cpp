#include <sys/stat.h>
#include <iostream>
#include <string>

#include "yaml-cpp/yaml.h"
#include <gflags/gflags.h>

#include "graphs/edgelist.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/io/data_mngr.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/paritioner/2DVC_partitioner.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include "utility/paritioner/hybrid_cut_partitioner.h"
#include "utility/paritioner/partitioner_base.h"
#include "utility/paritioner/vertex_cut_partitioner.h"
#include "utility/thread_pool.h"

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using VID_T = vid_t;
using VertexInfo = minigraph::graphs::VertexInfo<vid_t, vdata_t, edata_t>;

void GraphPartitionEdgeList2CSR(std::string src_pt, std::string dst_pt,
                                std::size_t cores, std::size_t num_partitions,
                                char separator_params = ',',
                                const bool frombin = false,
                                const std::string t_partitioner = "edgecut") {
  assert(t_partitioner == "edgecut" || t_partitioner == "vertexcut" ||
         t_partitioner == "hybridcut" || t_partitioner == "2dvc");

  minigraph::utility::io::DataMngr<CSR_T> data_mngr;
  // Clean dst path.
  if (!data_mngr.Exist(dst_pt + "minigraph_meta/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_meta/");
  } else {
    remove((dst_pt + "minigraph_meta/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_meta/");
  }
  if (!data_mngr.Exist(dst_pt + "minigraph_data/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_data/");
  } else {
    remove((dst_pt + "minigraph_data/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_data/");
  }
  if (!data_mngr.Exist(dst_pt + "minigraph_vdata/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_vdata/");
  } else {
    remove((dst_pt + "minigraph_vdata/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_vdata/");
  }
  if (!data_mngr.Exist(dst_pt + "minigraph_border_vertexes/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_border_vertexes/");
  } else {
    remove((dst_pt + "minigraph_border_vertexes/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_border_vertexes/");
  }
  if (!data_mngr.Exist(dst_pt + "minigraph_message/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_message/");
  } else {
    remove((dst_pt + "minigraph_message/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_message/");
  }
  if (!data_mngr.Exist(dst_pt + "minigraph_message/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_message/");
  } else {
    remove((dst_pt + "minigraph_message/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_message/");
  }
  if (!data_mngr.Exist(dst_pt + "minigraph_si/")) {
    data_mngr.MakeDirectory(dst_pt + "minigraph_si/");
  } else {
    remove((dst_pt + "minigraph_si/").c_str());
    data_mngr.MakeDirectory(dst_pt + "minigraph_si/");
  }

  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edgelist_io_adapter;

  minigraph::utility::partitioner::PartitionerBase<CSR_T>* partitioner =
      nullptr;
  if (t_partitioner == "edgecut")
    partitioner =
        new minigraph::utility::partitioner::EdgeCutPartitioner<CSR_T>();
  else if (t_partitioner == "vertexcut")
    partitioner =
        new minigraph::utility::partitioner::VertexCutPartitioner<CSR_T>();
  else if (t_partitioner == "hybridcut")
    partitioner =
        new minigraph::utility::partitioner::HybridCutPartitioner<CSR_T>();
  else if (t_partitioner == "2dvc")
    partitioner =
        new minigraph::utility::partitioner::TwoDVCPartitioner < CSR_T > ();

  // Read Graph
  auto edgelist_graph = new EDGE_LIST_T;
  if (frombin) {
    std::string meta_pt = src_pt + "minigraph_meta" + ".bin";
    std::string data_pt = src_pt + "minigraph_data" + ".bin";
    std::string vdata_pt = src_pt + "minigraph_vdata" + ".bin";
    edgelist_io_adapter.ReadEdgeListFromBin(edgelist_graph, 0, meta_pt, data_pt,
                                            vdata_pt);
  } else {
    edgelist_io_adapter.ParallelRead((GRAPH_BASE_T*)edgelist_graph,
                                     edgelist_csv, separator_params, 0, cores,
                                     src_pt);
  }

  partitioner->ParallelPartition(edgelist_graph, num_partitions, cores, dst_pt,
                                 true);

  LOG_INFO("WriteCommunicationMatrix.");
  auto pair_communication_matrix = partitioner->GetCommunicationMatrix();

  LOG_INFO("WriteVidMap.");
  auto vid_map = partitioner->GetVidMap();

  auto global_border_vid_map = partitioner->GetGlobalBorderVidMap();
  LOG_INFO("WriteGlobalBorderVidMap. size: ", global_border_vid_map->size_);

  remove(
      (dst_pt + "minigraph_border_vertexes/communication_matrix.bin").c_str());
  remove((dst_pt + "minigraph_message/vid_map.bin").c_str());
  remove((dst_pt + "minigraph_message/global_border_vid_map.bin").c_str());
  data_mngr.WriteCommunicationMatrix(
      dst_pt + "minigraph_border_vertexes/communication_matrix.bin",
      pair_communication_matrix.second, pair_communication_matrix.first);
  data_mngr.WriteVidMap(partitioner->GetMaxVid(), vid_map,
                        dst_pt + "minigraph_message/vid_map.bin");
  data_mngr.WriteBitmap(global_border_vid_map,
                        dst_pt + "minigraph_message/global_border_vid_map.bin");

  auto fragments = partitioner->GetFragments();
  delete partitioner;
  LOG_INFO("Write StatisticInfo.");

  size_t count = 0;
  // Might used in generating training data.
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

  LOG_INFO("End graph partition#");
  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_i != "" && FLAGS_o != "" && FLAGS_p != false);
  std::string src_pt = FLAGS_i;
  std::string dst_pt = FLAGS_o;
  std::size_t cores = FLAGS_cores;
  std::string graph_type = FLAGS_t;

  if (FLAGS_p) {
    std::size_t num_partitions = FLAGS_n;
    std::cout << " #Partitioning: "
              << " input: " << FLAGS_i << " output: " << FLAGS_o
              << " init_model: " << FLAGS_init_model
              << " cores: " << FLAGS_cores << std::endl;

    GraphPartitionEdgeList2CSR(src_pt, dst_pt, cores, num_partitions,
                               *FLAGS_sep.c_str(), FLAGS_frombin,
                               FLAGS_partitioner);
    LOG_INFO("Finished: save at ", dst_pt);
  }

  gflags::ShutDownCommandLineFlags();
}
