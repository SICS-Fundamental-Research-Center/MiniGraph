#ifndef MINIGRAPH_SUBGRAPH_SMALL_FIRST_SCHEDULER_H
#define MINIGRAPH_SUBGRAPH_SMALL_FIRST_SCHEDULER_H

#include "Eigen/Core"
#include "Eigen/Dense"
#include "portability/sys_data_structure.h"
#include "scheduler/subgraph_scheduler_base.h"
#include "utility/atomic.h"
#include "vector"
#include "yaml-cpp/yaml.h"

namespace minigraph {
namespace scheduler {

template <typename GID_T>
class LargeFirstScheduler : public SubGraphsSchedulerBase<GID_T> {
 private:
  StatisticInfo* si_ = nullptr;

 public:
  LargeFirstScheduler(StatisticInfo* si = nullptr) {
    assert(si != nullptr);
    si_ = si;
  };

  ~LargeFirstScheduler() = default;

  size_t ChooseOne(std::vector<GID_T>& vec_gid) {
    GID_T gid = GID_MAX;

    double rank_max = 0;
    size_t index = 0;
    for (size_t i = 0; i < vec_gid.size(); ++i) {
      auto si = si_[i];
      if (write_max(&rank_max, si.num_active_vertexes)) {
        index = i;
        gid = vec_gid.at(i);
      }
    }

    vec_gid.erase(vec_gid.begin() + index);
    return gid;
  };
};

}  // namespace scheduler
}  // namespace minigraph

#endif  // MINIGRAPH_SUBGRAPH_LEARNED_SCHEDULER_H
