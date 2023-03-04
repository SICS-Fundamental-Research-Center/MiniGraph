
#ifndef MINIGRAPH_SCHEDULER_BASE_H
#define MINIGRAPH_SCHEDULER_BASE_H

#include "Eigen"

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class SchedulerBase {
 public:
  typedef VID_T vid_t;
  typedef GID_T gid_t;
  typedef VDATA_T vdata_t;
  typedef EDATA_T edata_t;

 public:
  SchedulerBase() = default;
  ~SchedulerBase() = default;
};

}  // namespace graphs
}  // namespace minigraph


#endif  // MINIGRAPH_SCHEDULER_BASE_H
