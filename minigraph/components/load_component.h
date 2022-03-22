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
      std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool,
      std::shared_ptr<utility::IOThreadPool> io_thread_pool,
      std::shared_ptr<
          folly::AtomicHashMap<GID_T, std::atomic<size_t>, std::hash<int64_t>,
                               std::equal_to<int64_t>, std::allocator<char>,
                               folly::AtomicHashArrayQuadraticProbeFcn>>
          superstep_by_gid,
      std::shared_ptr<std::atomic<size_t>> global_superstep,
      std::shared_ptr<utility::StateMachine<GID_T>> state_machine)
      : ComponentBase<GID_T>(cpu_thread_pool, io_thread_pool, superstep_by_gid,
                             global_superstep, state_machine){

        };
  //~LoadComponent();
  void Run() override{};
  void Stop() override{};
  bool Enqueue();

 private:
  // task queue
  std::shared_ptr<folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>>
      task_queue_;

  std::shared_ptr<utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_;

  std::shared_ptr<folly::AtomicHashMap<
      GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      pt_by_gid_;
};

}  // namespace minigraph::components
#endif  // MINIGRAPH_LOAD_COMPONENT_H
