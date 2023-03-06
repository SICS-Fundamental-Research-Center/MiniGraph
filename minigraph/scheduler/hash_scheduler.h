
#ifndef MINIGRAPH_SUBGRAPH_HASH_SCHEDULER_H
#define MINIGRAPH_SUBGRAPH_HASH_SCHEDULER_H

#include "Eigen/Core"
#include "Eigen/Dense"
#include "scheduler/subgraph_scheduler_base.h"
#include "vector"
#include "yaml-cpp/yaml.h"

namespace minigraph {
namespace scheduler {

template <typename GID_T>
class HashScheduler : public SubGraphsSchedulerBase<GID_T> {
 public:
 public:
  HashScheduler() {
    Eigen::MatrixXf a(10, 15);
    // YAML::Node config = YAML::LoadFile("../config.yaml");
  };
  ~HashScheduler() = default;

  size_t ChooseOne(std::vector<GID_T>& vec_gid) {
    GID_T gid = GID_MAX;
    size_t i = rand() % vec_gid.size();
    gid = vec_gid.at(i);
    vec_gid.erase(vec_gid.begin() + i);
    return gid;
  };
};

}  // namespace scheduler
}  // namespace minigraph

#endif  // MINIGRAPH_SUBGRAPH_HAHS_SCHEDULER_H
