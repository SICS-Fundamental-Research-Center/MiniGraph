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
  //ComputingComponent();
  //~ComputingComponent();

  bool Dequeue();
  bool Enqueue();

 private:
  // global message in shared memory
  std::shared_ptr<graphs::Message<VID_T, VDATA_T, EDATA_T>> global_msg;

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
