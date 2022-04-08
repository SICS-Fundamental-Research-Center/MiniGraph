#include "executors/scheduled_executor.h"

#include <gtest/gtest.h>

#include <chrono>

#include <folly/synchronization/Baton.h>


namespace minigraph {
namespace executors {

using namespace std::chrono_literals;

constexpr auto kSleepTime = 100ms;
constexpr int kTaskBatches = 3;
static const auto kTotalParallelism = std::thread::hardware_concurrency();

class TaskSubmitter {
 public:
  TaskSubmitter(ScheduledExecutor* executors) {
    executors_ = executors;
    runner_ = executors->RequestTaskRunner(this, {});
    counter_ = 0;
  }

  ~TaskSubmitter() {
    executors_->RecycleTaskRunner(this, runner_);
  }

  void SubmitTaskBatch() {
    // Record submission time.
    submission_time_ = std::chrono::system_clock::now();
    for (int i = 0; i < kTotalParallelism * kTaskBatches; i++) {
      runner_->Run([this]() {
        std::this_thread::sleep_for(kSleepTime);
        if (kTotalParallelism * kTaskBatches == ++counter_){
          // Last task to be completed.
          barrier_.post();
        }
      });
    }
  }

  void WaitUntilAllTasksCompleted(int expected_wait_batches = kTaskBatches) {
    barrier_.wait();
    auto runtime = std::chrono::system_clock::now() - submission_time_;
    EXPECT_GE(runtime, expected_wait_batches * kSleepTime);
    EXPECT_LE(runtime, (expected_wait_batches + 1) * kSleepTime);
  }

  int GetCounter() {
    return counter_.load();
  }

 private:
  ScheduledExecutor* executors_;
  TaskRunner* runner_;
  std::atomic_int counter_;
  folly::Baton<> barrier_;
  std::chrono::time_point<std::chrono::system_clock> submission_time_;
};

class ExecutorsIntegrationTest : public ::testing::Test {
 protected:
  ExecutorsIntegrationTest() : executors_(kTotalParallelism) {}

  ScheduledExecutor executors_;
};

TEST_F(ExecutorsIntegrationTest, TasksCanBeExecutedWithFullParallelismInOrder) {
  auto s1 = std::make_unique<TaskSubmitter>(&executors_);
  auto s2 = std::make_unique<TaskSubmitter>(&executors_);
  auto t2 = std::thread([&]() {
    s2->SubmitTaskBatch();
  });
  std::this_thread::sleep_for(kSleepTime);
  auto t1 = std::thread([&]() {
    s1->SubmitTaskBatch();
  });
  s1->WaitUntilAllTasksCompleted(kTaskBatches);

  // No task submitted by s2 should be completed before recycling s1.
  EXPECT_EQ(0, s2->GetCounter());
  s1.reset();
  s2->WaitUntilAllTasksCompleted(2 * kTaskBatches + 1);
  s2.reset();

  // Cleanup.
  t1.join();
  t2.join();
  executors_.Stop();
}

} // namespace executors
} // namespace minigraph
