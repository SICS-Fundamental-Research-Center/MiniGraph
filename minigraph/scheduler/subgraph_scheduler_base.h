
#ifndef MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H
#define MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H

#include "Eigen/Dense"
#include "Eigen/Core"


namespace minigraph {
namespace scheduler {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class SubGraphsSchedulerBase {
 public:
  typedef VID_T vid_t;
  typedef GID_T gid_t;
  typedef VDATA_T vdata_t;
  typedef EDATA_T edata_t;

 public:
  SubGraphsSchedulerBase(){
    Eigen::MatrixXf a(10,15);
  };
  ~SubGraphsSchedulerBase() = default;
};

}  // namespace graphs
}  // namespace minigraph


#endif  // MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H
