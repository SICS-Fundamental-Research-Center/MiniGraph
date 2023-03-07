
#ifndef MINIGRAPH_SUBGRAPH_LEARNED_SCHEDULER_H
#define MINIGRAPH_SUBGRAPH_LEARNED_SCHEDULER_H

#include "Eigen/Core"
#include "Eigen/Dense"
#include "yaml-cpp/yaml.h"

#include "portability/sys_data_structure.h"
#include "scheduler/subgraph_scheduler_base.h"
#include "utility/atomic.h"
#include "vector"

namespace minigraph {
namespace scheduler {

template <typename GID_T>
class LearnedScheduler : public SubGraphsSchedulerBase<GID_T> {
 private:
  StatisticInfo* si_ = nullptr;

  Eigen::MatrixXd relu(Eigen::MatrixXd input) {
    for (int i = 0; i < input.size(); ++i) {
      if (input(i) > 0)
        continue;
      else
        input(i) = 0;
    };
    return input;
  }

  Eigen::MatrixXd model_a(size_t sum_dlv, size_t sum_dgv,
                          size_t sum_dlv_times_dlv, size_t sum_dlv_times_dgv,
                          size_t sum_dgv_times_dgv) {
    Eigen::MatrixXd x(1, 5);
    Eigen::MatrixXd w1(5, 1);
    Eigen::MatrixXd w2(5, 1);
    Eigen::MatrixXd b1(1, 1);
    Eigen::MatrixXd b2(1, 1);

    x << sum_dlv, sum_dgv, sum_dlv_times_dlv, sum_dlv_times_dgv,
        sum_dgv_times_dgv;
    w1 << -0.1898, 0.5267, -0.2173, 0.5433, 0.0131;
    w2 << 0.3575, -0.0915, 0.0856, -0.3408, 0.0801;
    b1 << -0.0752;
    b2 << 0.0425;

    auto y1 = x * w1 + b1;
    auto y2 = x * w2 + b2;
    Eigen::MatrixXd y(1, 1);
    y = y1 + y2;
    return y;
  }

  Eigen::MatrixXd model_b(size_t sum_dlv, size_t sum_dlv_times_dlv) {
    Eigen::MatrixXd x(1, 2);
    Eigen::MatrixXd w1(2, 1);
    Eigen::MatrixXd w2(2, 1);
    Eigen::MatrixXd b1(1, 1);
    Eigen::MatrixXd b2(1, 1);

    x << sum_dlv, sum_dlv_times_dlv;

    w1 << -0.2797, 0.7662;
    w2 << 0.4837, -0.3442;
    b1 << -0.1687;
    b2 << 0.0572;

    auto y1 = x * w1 + b1;
    auto y2 = x * w2 + b2;
    Eigen::MatrixXd y(1, 1);
    y = y1 + y2;
    return y;
  }

  Eigen::MatrixXd model_c(size_t sum_dlv, size_t sum_dgv,
                          size_t sum_dlv_times_dlv, size_t sum_dlv_times_dgv,
                          size_t sum_dgv_times_dgv, size_t cores) {
    Eigen::MatrixXd x(1, 6);
    Eigen::MatrixXd w1(6, 5);
    Eigen::MatrixXd b1(1, 5);
    Eigen::MatrixXd b2(1, 1);
    Eigen::MatrixXd w2(5, 1);

    x << sum_dlv, sum_dgv, sum_dlv_times_dlv, sum_dlv_times_dgv,
        sum_dgv_times_dgv, cores;

    w1 << -0.2864, 0.3247, -0.3302, 0.3522, -0.1752, -0.3372, 0.1929, -0.2168,
        -0.0391, -0.4579, -0.0767, -0.3749, -0.1426, -0.2872, -0.0379, 0.2984,
        -0.1369, -0.3750, 0.3096, 0.1885, -0.1226, -0.3131, -0.1270, -0.2560,
        -0.2824, -0.1183, -0.3180, 0.0814, -0.3789, 0.2296;
    b1 << -0.2948, 0.2531, -0.1102, -0.1362, 0.0069;
    w2 << -0.0535, -0.5602, -0.1710, 0.2712, -0.2997;
    b2 << 0.2190;
    auto y1 = x * w1 + b1;
    Eigen::MatrixXd y(1, 1);
    y = relu(y1) * w2 + b2;
    return y;
  }

 public:
  LearnedScheduler(StatisticInfo* si = nullptr) {
    assert(si != nullptr);
    si_ = si;
  };

  ~LearnedScheduler() = default;

  size_t ChooseOne(std::vector<GID_T>& vec_gid) {
    GID_T gid = GID_MAX;

    double rank_max = 0;
    size_t index = 0;
    for (size_t i = 0; i < vec_gid.size(); ++i) {
      auto si = si_[i];

      //auto rank =
      //    model_a(si_[i].sum_dlv, si_[i].sum_dgv, si_[i].sum_dlv_times_dlv,
      //            si_[i].sum_dlv_times_dgv, si_[i].sum_dgv_times_dgv);
      //auto rank2 = model_b(si_[i].sum_dlv, si_[i].sum_dlv_times_dlv);
      auto rank3 =
          model_c(si_[i].sum_dlv, si_[i].sum_dgv, si_[i].sum_dlv_times_dlv,
                  si_[i].sum_dlv_times_dgv, si_[i].sum_dgv_times_dgv, 5);

      if (write_max(&rank_max, rank3(0, 0))) {
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
