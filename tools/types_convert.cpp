#include <gflags/gflags.h>
#include <sys/stat.h>
#include <iostream>
#include <string>

#include "graphs/edgelist.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_types.h"
#include "utility/io/data_mngr.h"
#include "utility/io/edge_list_io_adapter.h"
#include "utility/paritioner/partitioner_base.h"
#include "utility/thread_pool.h"

using CSR_T = minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
using GRAPH_BASE_T = minigraph::graphs::Graph<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_T = minigraph::graphs::EdgeList<gid_t, vid_t, vdata_t, edata_t>;
using EDGE_LIST_UNSIGNED =
    minigraph::graphs::EdgeList<gid_t, unsigned, vdata_t, edata_t>;

EDGE_LIST_UNSIGNED* VID_T2unsigned(EDGE_LIST_T* in_edgelist_graph,
                                   const size_t cores) {
  LOG_INFO("VID_T to unsigned.");

  auto thread_pool = minigraph::utility::CPUThreadPool(cores, 1);
  std::mutex mtx;
  std::condition_variable finish_cv;
  std::unique_lock<std::mutex> lck(mtx);
  std::atomic<size_t> pending_packages(cores);

  auto out_edgelist_graph = new EDGE_LIST_UNSIGNED;
  out_edgelist_graph->gid_ = in_edgelist_graph->get_gid();
  out_edgelist_graph->num_edges_ = in_edgelist_graph->get_num_edges();
  out_edgelist_graph->num_vertexes_ = in_edgelist_graph->get_num_vertexes();
  out_edgelist_graph->aligned_max_vid_ =
      in_edgelist_graph->get_aligned_max_vid();
  out_edgelist_graph->vdata_ = (vdata_t*)malloc(
      sizeof(unsigned) * out_edgelist_graph->get_num_vertexes());
  out_edgelist_graph->globalid_by_localid_ = (unsigned*)malloc(
      sizeof(unsigned) * out_edgelist_graph->get_num_vertexes());

  out_edgelist_graph->buf_graph_ = (unsigned*)malloc(
      sizeof(unsigned) * out_edgelist_graph->get_num_edges() * 2);

  memset(out_edgelist_graph->buf_graph_, 0,
         sizeof(unsigned) * out_edgelist_graph->get_num_edges() * 2);

  LOG_INFO("Convert edges.");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &out_edgelist_graph, &in_edgelist_graph,
                        &pending_packages, &finish_cv]() {
      for (size_t j = tid; j < out_edgelist_graph->get_num_edges();
           j += cores) {
        out_edgelist_graph->buf_graph_[j * 2] =
            in_edgelist_graph->buf_graph_[j * 2];
        out_edgelist_graph->buf_graph_[j * 2 + 1] =
            in_edgelist_graph->buf_graph_[j * 2 + 1];
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Convert vertexes.");
  pending_packages.store(cores);
  for (size_t i = 0; i < cores; i++) {
    size_t tid = i;
    thread_pool.Commit([tid, &cores, &out_edgelist_graph, &in_edgelist_graph,
                        &pending_packages, &finish_cv]() {
      for (size_t j = tid; j < out_edgelist_graph->get_num_vertexes();
           j += cores) {
        out_edgelist_graph->vdata_[j] = in_edgelist_graph->vdata_[j];
        out_edgelist_graph->globalid_by_localid_[j] =
            in_edgelist_graph->globalid_by_localid_[j];
      }
      if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
      return;
    });
  }
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });

  LOG_INFO("Convert VID_T for all vertexes and edges to unsigned");
  return out_edgelist_graph;
}

void TypeConvert(std::string src_pt, std::string dst_pt, std::size_t cores,
                 char separator_params = ',', const bool frombin = false) {
  minigraph::utility::io::DataMngr<CSR_T> data_mngr;

  minigraph::utility::io::EdgeListIOAdapter<gid_t, vid_t, vdata_t, edata_t>
      edgelist_io_adapter;

  auto edgelist_graph = new EDGE_LIST_T;

  std::string meta_pt = src_pt + "minigraph_meta" + ".bin";
  std::string data_pt = src_pt + "minigraph_data" + ".bin";
  std::string vdata_pt = src_pt + "minigraph_vdata" + ".bin";

  edgelist_io_adapter.ReadEdgeListFromBin((GRAPH_BASE_T*)edgelist_graph, 0,
                                          meta_pt, data_pt, vdata_pt);

  std::string dst_meta_pt = dst_pt + "minigraph_meta" + ".bin";
  std::string dst_data_pt = dst_pt + "minigraph_data" + ".bin";
  std::string dst_vdata_pt = dst_pt + "minigraph_vdata" + ".bin";
  auto out_edgelist_graph = VID_T2unsigned(edgelist_graph, cores);

  out_edgelist_graph->ShowGraph();
  std::ofstream meta_file(dst_meta_pt, std::ios::binary | std::ios::app);
  std::ofstream data_file(dst_data_pt, std::ios::binary | std::ios::app);
  std::ofstream vdata_file(dst_vdata_pt, std::ios::binary | std::ios::app);

  size_t* meta_buff = (size_t*)malloc(sizeof(size_t) * 2);
  meta_buff[0] = out_edgelist_graph->num_vertexes_;
  meta_buff[1] = out_edgelist_graph->num_edges_;
  meta_file.write((char*)meta_buff, 2 * sizeof(size_t));
  meta_file.write((char*)&out_edgelist_graph->max_vid_, sizeof(unsigned));

  data_file.write((char*)(out_edgelist_graph->buf_graph_),
                  sizeof(unsigned) * 2 * out_edgelist_graph->get_num_edges());

  LOG_INFO("VID_T size: ", sizeof(unsigned));
  if ((char*)out_edgelist_graph->vdata_ != nullptr)
    vdata_file.write((char*)out_edgelist_graph->vdata_,
                     sizeof(unsigned) * edgelist_graph->get_num_vertexes());

  LOG_INFO("EDGE_UNIT: ", sizeof(unsigned) * 2,
           ", num_edges: ", out_edgelist_graph->get_num_vertexes(),
           ", write size: ",
           sizeof(unsigned) * 2 * out_edgelist_graph->get_num_edges());
  free(meta_buff);
  data_file.close();
  meta_file.close();
  vdata_file.close();
  LOG_INFO("End graph convert#");
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  assert(FLAGS_i != "" && FLAGS_o != "" && FLAGS_t == "edgelist_bin");
  std::string src_pt = FLAGS_i;
  std::string dst_pt = FLAGS_o;
  std::size_t cores = FLAGS_cores;

  TypeConvert(src_pt, dst_pt, cores, FLAGS_frombin);

  gflags::ShutDownCommandLineFlags();
}
