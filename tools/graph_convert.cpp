#include "graphs/edgelist.h"
#include "graphs/immutable_csr.h"
#include "message_manager/default_message_manager.h"
#include "portability/sys_data_structure.h"
#include "portability/sys_types.h"
#include "utility/io/data_mngr.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include "utility/paritioner/partitioner_base.h"
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <iostream>
#include <string>

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;

void EdgeListBin2EdgelistCSV(std::string src_pt, std::string dst_pt,
                             std::size_t cores, char separator_params = ',') {
  minigraph::utility::io::DataMngr<CSR_T> data_mngr;

  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edgelist_io_adapter;

  auto edgelist_graph = new EDGE_LIST_T;
  std::string meta_pt = src_pt + "minigraph_meta" + ".bin";
  std::string data_pt = src_pt + "minigraph_data" + ".bin";
  std::string vdata_pt = src_pt + "minigraph_vdata" + ".bin";

  edgelist_io_adapter.ReadEdgeListFromBin((GRAPH_BASE_T*)edgelist_graph, 0,
                                          meta_pt, data_pt, vdata_pt);
  if (data_mngr.Exist(dst_pt)) remove(dst_pt.c_str());

  std::ofstream out_file(dst_pt, std::ios::binary | std::ios::app);

  for (size_t i = 0; i < edgelist_graph->get_num_edges(); i++) {
    out_file << *(edgelist_graph->buf_graph_ + 2 * i) << separator_params
             << *(edgelist_graph->buf_graph_ + 2 * i + 1) << std::endl;
  }
  out_file.close();
  return;
}

void EdgeList2CSR(std::string src_pt, std::string dst_pt, std::size_t cores,
                  char separator_params = ',', const bool frombin = false) {
  minigraph::utility::io::DataMngr<CSR_T> data_mngr;

  minigraph::utility::partitioner::PartitionerBase<CSR_T>* partitioner =
      nullptr;
  partitioner =
      new minigraph::utility::partitioner::EdgeCutPartitioner<CSR_T>();
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edgelist_io_adapter;

  auto edgelist_graph = new EDGE_LIST_T;
  if (frombin) {
    std::string meta_pt = src_pt + "minigraph_meta" + ".bin";
    std::string data_pt = src_pt + "minigraph_data" + ".bin";
    std::string vdata_pt = src_pt + "minigraph_vdata" + ".bin";

    edgelist_io_adapter.ReadEdgeListFromBin((GRAPH_BASE_T*)edgelist_graph, 0,
                                            meta_pt, data_pt, vdata_pt);
  } else {
    edgelist_io_adapter.ParallelReadEdgeListFromCSV(
        (GRAPH_BASE_T*)edgelist_graph, src_pt, 0, separator_params, cores);
  }

  partitioner->ParallelPartition(edgelist_graph, 1, cores);

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

  auto fragments = partitioner->GetFragments();
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
  auto pair_communication_matrix = partitioner->GetCommunicationMatrix();

  LOG_INFO("WriteVidMap.");
  auto vid_map = partitioner->GetVidMap();
  if (vid_map != nullptr)
    data_mngr.WriteVidMap(partitioner->GetMaxVid(), vid_map,
                          dst_pt + "minigraph_message/vid_map.bin");

  auto global_border_vid_map = partitioner->GetGlobalBorderVidMap();
  if (global_border_vid_map != nullptr) {
    LOG_INFO("WriteGlobalBorderVidMap. size: ", global_border_vid_map->size_);
    data_mngr.WriteBitmap(
        global_border_vid_map,
        dst_pt + "minigraph_message/global_border_vid_map.bin");
  }

  remove(
      (dst_pt + "minigraph_border_vertexes/communication_matrix.bin").c_str());
  remove((dst_pt + "minigraph_message/vid_map.bin").c_str());
  remove((dst_pt + "minigraph_message/global_border_vid_map.bin").c_str());
  data_mngr.WriteCommunicationMatrix(
      dst_pt + "minigraph_border_vertexes/communication_matrix.bin",
      pair_communication_matrix.second, pair_communication_matrix.first);
  LOG_INFO("End graph convert#");
}

void EdgeListCSV2EdgeListBin(std::string src_pt, std::string dst_pt,
                             const size_t cores, char separator_params = ',') {
  std::cout << " #Converting " << FLAGS_t << ": input: " << src_pt
            << " output: " << dst_pt << std::endl;
  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edge_list_io_adapter;
  auto graph = new EDGE_LIST_T;
  std::string meta_pt = dst_pt + "minigraph_meta" + ".bin";
  std::string data_pt = dst_pt + "minigraph_data" + ".bin";
  std::string vdata_pt = dst_pt + "minigraph_vdata" + ".bin";
  edge_list_io_adapter.ParallelReadEdgeListFromCSV((GRAPH_BASE_T*)graph, src_pt,
                                                   0, separator_params, cores);
  LOG_INFO("Write: ", meta_pt);
  LOG_INFO("Write: ", data_pt);
  LOG_INFO("Write: ", vdata_pt);
  edge_list_io_adapter.Write(*graph, edgelist_bin, meta_pt, data_pt, vdata_pt);
}

void CSRBin2CSRText(std::string src_pt, std::string dst_pt, std::size_t cores,
                    char separator_params = ',') {
  minigraph::utility::io::DataMngr<CSR_T> data_mngr;

  Path path;
  path.meta_pt = src_pt + "minigraph_meta/0.bin";
  path.data_pt = src_pt + "minigraph_data/0.bin";
  path.vdata_pt = src_pt + "minigraph_vdata/0.bin";

  minigraph::utility::io::CSRIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      csr_io_adapter;

  auto graph = new CSR_T;
  csr_io_adapter.Read(graph, csr_bin, 0, path.meta_pt, path.data_pt,
                      path.vdata_pt);

  graph->ShowGraph(100);

  if (data_mngr.Exist(dst_pt)) remove(dst_pt.c_str());

  std::ofstream out_file(dst_pt, std::ios::binary | std::ios::app);

  out_file << "AdjacencyGraph" << std::endl;
  out_file << graph->get_num_vertexes() << std::endl;
  out_file << graph->get_num_out_edges() << std::endl;
  for (size_t i = 0; i < graph->get_num_vertexes(); i++) {
    out_file << graph->out_offset_[i] << std::endl;
  }
  for (size_t i = 0; i < graph->get_num_out_edges(); i++) {
    out_file << graph->out_edges_[i] << std::endl;
  }
  out_file.close();
}

void MiniGraphCSRBin2PlanarCSR(std::string src_pt, std::string dst_pt,
                               std::size_t cores) {
  auto data_mngr = minigraph::utility::io::DataMngr<CSR_T>();
  auto msg_mngr = minigraph::message::DefaultMessageManager<CSR_T>(
      &data_mngr, src_pt, false);
  auto pt_by_gid = data_mngr.InitPtByGid(src_pt);

  auto max_vid = 0;
  for (auto iter = pt_by_gid.begin(); iter != pt_by_gid.end(); iter++) {
    LOG_INFO("Read GRAPH: ", iter->first);

    auto path = iter->second;
    auto csr = new CSR_T;
    data_mngr.csr_io_adapter_->Read((GRAPH_BASE_T*)csr, csr_bin, iter->first,
                                    path.meta_pt, path.data_pt, path.vdata_pt);

    max_vid = csr->get_max_vid();
    csr->ShowGraph(99);
  }

 // auto communication_matrix = msg_mngr.GetCommunicationMatrix();
 // auto vid_map = msg_mngr.GetVidMap();
 // auto border_bit_map = msg_mngr.GetGlobalBorderVidMap();
//
 // LOG_INFO("X");
 // data_mngr.WriteCommunicationMatrix(
 //     dst_pt + "minigraph_border_vertexes/communication_matrix.bin",
 //     communication_matrix, pt_by_gid.size());
 // LOG_INFO("X");
 // data_mngr.WriteVidMap(max_vid, vid_map,
 //                       dst_pt + "minigraph_message/vid_map.bin");
 // LOG_INFO("X");
 // data_mngr.WriteBitmap(border_bit_map,
 //                       dst_pt + "minigraph_message/global_border_vid_map.bin");

  return;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_i != "" && FLAGS_o != "");
  std::string src_pt = FLAGS_i;
  std::string dst_pt = FLAGS_o;
  std::size_t cores = FLAGS_cores;

  LOG_INFO("In type: ", FLAGS_in_type, " Out type: ", FLAGS_out_type);
  if (FLAGS_tobin && FLAGS_out_type == "edgelist" &&
      FLAGS_in_type == "edgelist")
    EdgeListCSV2EdgeListBin(src_pt, dst_pt, cores, *FLAGS_sep.c_str());

  if (FLAGS_tobin && FLAGS_out_type == "csr" && FLAGS_in_type == "edgelist")
    EdgeList2CSR(src_pt, dst_pt, cores, *FLAGS_sep.c_str(), FLAGS_frombin);

  if (FLAGS_frombin && FLAGS_out_type == "edgelist" &&
      FLAGS_in_type == "edgelist")
    EdgeListBin2EdgelistCSV(src_pt, dst_pt, cores, *FLAGS_sep.c_str());

  if (FLAGS_frombin && FLAGS_tobin == false && FLAGS_out_type == "csr" &&
      FLAGS_in_type == "csr")
    CSRBin2CSRText(src_pt, dst_pt, cores);

  if (FLAGS_frombin && FLAGS_tobin && FLAGS_out_type == "planar_csr" &&
      FLAGS_in_type == "minigraph_csr")
    MiniGraphCSRBin2PlanarCSR(src_pt, dst_pt, cores);

  gflags::ShutDownCommandLineFlags();
}
