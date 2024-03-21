
#ifndef MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H
#define MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H

#include <vector>

namespace minigraph {
namespace scheduler {

template <typename GID_T>
class SubGraphsSchedulerBase {
 public:
 public:
  SubGraphsSchedulerBase() = default;
  ~SubGraphsSchedulerBase() = default;

  virtual size_t ChooseOne(std::vector<GID_T>& vec_gid) = 0;
};

}  // namespace scheduler
}  // namespace minigraph

#endif  // MINIGRAPH_SUBGRAPH_SCHEDULER_BASE_H
