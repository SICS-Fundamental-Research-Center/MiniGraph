#ifndef MINIGRAPH_LOAD_COMPONENT_H
#define MINIGRAPH_LOAD_COMPONENT_H

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/state_machine.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <memory>
#include <string>

#define MAX_THREAD 10;
namespace minigraph::components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class LoadComponent : public ComponentBase<GID_T> {
 public:
  LoadComponent(
      utility::CPUThreadPool* cpu_thread_pool,
      utility::IOThreadPool* io_thread_pool,
      folly::AtomicHashMap<
          GID_T, std::shared_ptr<std::atomic<size_t>>, std::hash<int64_t>,
          std::equal_to<int64_t>, std::allocator<char>,
          folly::AtomicHashArrayQuadraticProbeFcn>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine,
      folly::ProducerConsumerQueue<std::pair<
          GID_T,
          std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
          task_queue,
      folly::AtomicHashMap<GID_T, CSRPt, std::hash<int64_t>,
                           std::equal_to<int64_t>, std::allocator<char>,
                           folly::AtomicHashArrayQuadraticProbeFcn>* pt_by_gid,
      folly::ProducerConsumerQueue<GID_T>* read_trigger)
      : ComponentBase<GID_T>(cpu_thread_pool, io_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    task_queue_ = task_queue;
    pt_by_gid_ = pt_by_gid;
    csr_io_adapter_ =
        new minigraph::utility::io::CSRIOAdapter<gid_t, vid_t, vdata_t,
                                                 edata_t>();
    read_trigger_ = read_trigger;
    XLOG(INFO, "Init LoadComponent: Finish.");
  }
  void Run() override {
    while (this->switch_.load(std::memory_order_relaxed)) {
      GID_T gid;
      while (!read_trigger_->read(gid)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      // process gid
      CSRPt& csr_pt = pt_by_gid_->find(gid)->second;
      XLOG(INFO, "gid: ", gid, " csr_pt: ", csr_pt.vdata_pt, " ",
           csr_pt.meta_in_pt, " ", csr_pt.meta_out_pt, " ", csr_pt.vdata_pt,
           " ", csr_pt.localid2globalid_pt);
      auto immutable_csr =
          new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;

      csr_io_adapter_->Read(
          (graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>*)immutable_csr,
          csr_bin, csr_pt.vertex_pt, csr_pt.meta_in_pt, csr_pt.meta_in_pt,
          csr_pt.vdata_pt, csr_pt.localid2globalid_pt);
    }
  }
  void Stop() override {}
  bool Enqueue();

 private:
  // task queue
  folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
      task_queue_;

  // io adapter
  utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>* csr_io_adapter_;

  folly::AtomicHashMap<GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
                       std::allocator<char>,
                       folly::AtomicHashArrayQuadraticProbeFcn>* pt_by_gid_;

  // read trigger to determine which fragment could be buffered.
  folly::ProducerConsumerQueue<GID_T>* read_trigger_ = nullptr;
};

}  // namespace minigraph::components
#endif  // MINIGRAPH_LOAD_COMPONENT_H
