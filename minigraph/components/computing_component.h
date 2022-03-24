#ifndef MINIGRAPH_COMPUTING_COMPONENT_H
#define MINIGRAPH_COMPUTING_COMPONENT_H

#include "components/component_base.h"
#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <memory>

namespace minigraph {
namespace components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ComputingComponent : public ComponentBase<GID_T> {
 public:
  ComputingComponent(
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
      folly::ProducerConsumerQueue<std::pair<
          GID_T,
          std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
          partial_result_queue)
      : ComponentBase<GID_T>(cpu_thread_pool, io_thread_pool, superstep_by_gid,
                             global_superstep, state_machine) {
    task_queue_ = task_queue;
    partial_result_queue_ = partial_result_queue;
    XLOG(INFO, "Init ComputingComponent: Finish.");
  };

  void Run() override { ; }
  void Stop() override { ; }
  bool Dequeue();
  bool Enqueue();

 private:
  // global message in shared memory
  graphs::Message<VID_T, VDATA_T, EDATA_T>* global_msg_;

  folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
      task_queue_;

  folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>*
      partial_result_queue_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
