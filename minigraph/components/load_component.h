//
// Created by hsiaoko on 2022/3/20.
//

#ifndef MINIGRAPH_LOAD_COMPONENT_H
#define MINIGRAPH_LOAD_COMPONENT_H

#include "portability/sys_data_structure.h"
#include "state_machine/state_machine.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <memory>
#include <string>

#define MAX_THREAD 10;
namespace minigraph {
namespace components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class LoadComponent {
 public:
  LoadComponent(std::string work_space);
  ~LoadComponent();
  bool ListFiles();
  bool Enqueue();

 private:
  std::string work_space_pt_;
  size_t max_threads_ = 0;
  size_t num_threads_ = 0;
  size_t min_threads_ = 0;

  std::shared_ptr<utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_;
  std::shared_ptr<utility::IOThreadPool> io_thread_pool_;
  std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool_;
  std::shared_ptr<folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>>
      task_queue_;
  std::shared_ptr<folly::AtomicHashMap<
      GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      pt_by_gid_;
  std::shared_ptr<StateMachine<GID_T>> state_machine_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_LOAD_COMPONENT_H
