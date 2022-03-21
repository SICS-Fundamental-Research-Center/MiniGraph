//
// Created by hsiaoko on 2022/3/20.
//

#ifndef MINIGRAPH_COMPUTING_COMPONENT_H
#define MINIGRAPH_COMPUTING_COMPONENT_H

#include "graphs/immutable_csr.h"
#include "utility/thread_pool.h"
#include <folly/ProducerConsumerQueue.h>
#include <memory>

namespace minigraph {
namespace components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ComputingComponent {
 public:
  ComputingComponent();
  ~ComputingComponent();

  bool Dequeue();
  bool Enqueue();

 private:
  std::shared_ptr<utility::IOThreadPool> io_thread_pool_;
  std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool_;
  std::shared_ptr<folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>>
      task_queue_;
  std::shared_ptr<folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>>
      partial_result_queue_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H
