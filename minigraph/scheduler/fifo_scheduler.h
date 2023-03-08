
#ifndef MINIGRAPH_SUBGRAPH_FIFO_SCHEDULER_H
#define MINIGRAPH_SUBGRAPH_FIFO_SCHEDULER_H

#include "scheduler/subgraph_scheduler_base.h"

namespace minigraph {
namespace scheduler {

template <typename GID_T>
class FIFOScheduler : public SubGraphsSchedulerBase<GID_T> {
 public:
 public:
   FIFOScheduler() {
      LOG_INFO("Init FIFO.");
  };

  ~FIFOScheduler() = default;

  size_t ChooseOne(std::vector<GID_T>& vec_gid) {
    GID_T gid = GID_MAX;
    size_t i = 0;
    gid = vec_gid.at(i);
    vec_gid.erase(vec_gid.begin() + i);
    return gid;
  };
};

}  // namespace scheduler
}  // namespace minigraph

#endif  // MINIGRAPH_SUBGRAPH_FIFO_SCHEDULER_H
