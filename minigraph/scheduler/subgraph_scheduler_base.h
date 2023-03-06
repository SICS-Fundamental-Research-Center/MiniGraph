
#ifndef MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H
#define MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H

#include "Eigen/Core"
#include "Eigen/Dense"

namespace minigraph {
namespace scheduler {

template <typename GID_T>
class SubGraphsSchedulerBase {
 public:

 public:
  SubGraphsSchedulerBase() { Eigen::MatrixXf a(10, 15); };
  ~SubGraphsSchedulerBase() = default;

  virtual size_t ChooseOne(std::vector<GID_T>& vec_gid) = 0;
};

}  // namespace scheduler
}  // namespace minigraph

#endif  // MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H
