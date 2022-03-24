#ifndef MINIGRAPH_DISCHARGE_COMPONENT_H
#define MINIGRAPH_DISCHARGE_COMPONENT_H

#include "components/component_base.h"
#include "portability/sys_data_structure.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <string>

namespace minigraph {
namespace components {
template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class DischargeComponent : public ComponentBase<GID_T> {
 public:
  DischargeComponent(
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
          partial_result_queue,
      folly::AtomicHashMap<GID_T, CSRPt, std::hash<int64_t>,
                           std::equal_to<int64_t>, std::allocator<char>,
                           folly::AtomicHashArrayQuadraticProbeFcn>* pt_by_gid)
      : ComponentBase<GID_T>(cpu_thread_pool, io_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    partial_result_queue_ = partial_result_queue;
    pt_by_gid_ = pt_by_gid;
    csr_io_adapter_ =
        new minigraph::utility::io::CSRIOAdapter<gid_t, vid_t, vdata_t,
                                                 edata_t>();
    XLOG(INFO, "Init DischargeComponent: Finish.");
  }
  void Run() override {
    while (this->switch_.load() == true) {
      std::pair<GID_T,
                std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>
          val;
      while (!partial_result_queue_->read(val)) {
        // spin until we get a value
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  void Stop() override { this->switch_.store(false); }
  bool AggregateMsg();
  bool ResoluteMsg();
  bool ProcessPartialResult();

 private:
  // global message in shared memory
  graphs::Message<VID_T, VDATA_T, EDATA_T>* global_msg_;

  folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
      partial_result_queue_;

  utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>* csr_io_adapter_;

  folly::AtomicHashMap<GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
                       std::allocator<char>,
                       folly::AtomicHashArrayQuadraticProbeFcn>* pt_by_gid_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
