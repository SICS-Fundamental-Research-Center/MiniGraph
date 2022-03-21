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
  bool AggregateMsg();
  bool ResoluteMsg();
  bool Dequeue();

 private:
  // global message in shared memory
  std::shared_ptr<graphs::Message<VID_T, VDATA_T, EDATA_T>> global_msg;

  std::shared_ptr<folly::ProducerConsumerQueue<std::pair<
      GID_T, std::shared_ptr<graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>>>>>
      partial_result_queue_;

  std::shared_ptr<utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_;

  std::shared_ptr<folly::AtomicHashMap<
      GID_T, CSRPt, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      pt_by_gid_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
