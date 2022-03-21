//
// Created by hsiaoko on 2022/3/20.
//

#ifndef MINIGRAPH_DISCHARGE_COMPONENT_H
#define MINIGRAPH_DISCHARGE_COMPONENT_H

#include "utility/io/csr_io_adapter.h"
#include "utility/thread_pool.h"
#include <portability/sys_data_structure.h>
#include <string>

namespace minigraph {
namespace components {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class DischargeComponent {
 public:
  bool AggregateMsg();
  bool ResoluteMsg();

 private:
  std::shared_ptr<utility::IOThreadPool> io_thread_pool_;
  std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool_;
  // std::shared_ptr<graphs::Message<VID_T, VDATA_T, EDATA_T>>;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_DISCHARGE_COMPONENT_H
