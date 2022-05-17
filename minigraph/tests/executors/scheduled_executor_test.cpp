#include "executors/scheduled_executor.h"

#include <gtest/gtest.h>


namespace minigraph {
namespace executors {

constexpr unsigned int parallelism = 6;

class TaskSubmitter {
 public:
  TaskSubmitter(ScheduledExecutor* executors) {
    executors_ = executors;
    runner_ = executors->RequestTaskRunner({});
  }

  ~TaskSubmitter() {
    executors_->RecycleTaskRunner(runner_);
  }

  ScheduledExecutor* executors_;
  TaskRunner* runner_;
};

class ScheduledExecutorTest : public ::testing::Test {
 protected:
  ScheduledExecutorTest() : executors_(parallelism) {}

  ScheduledExecutor executors_;
};

TEST_F(ScheduledExecutorTest, BasicRequestAndRecycleRunner) {
  auto t1 = std::make_unique<TaskSubmitter>(&executors_);
  auto t2 = std::make_unique<TaskSubmitter>(&executors_);
  auto t3 = std::make_unique<TaskSubmitter>(&executors_);
  EXPECT_EQ(parallelism, t1->runner_->GetParallelism());
  EXPECT_EQ(0, t2->runner_->GetParallelism());
  EXPECT_EQ(0, t3->runner_->GetParallelism());

  t1.reset();
  EXPECT_EQ(parallelism, t2->runner_->GetParallelism());
  EXPECT_EQ(0, t3->runner_->GetParallelism());

  t2.reset();
  EXPECT_EQ(parallelism, t3->runner_->GetParallelism());
}

} // namespace executors
} // namespace minigraph
