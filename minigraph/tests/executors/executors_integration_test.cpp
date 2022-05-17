#include "executors/scheduled_executor.h"
#include "utility/logging.h"
#include <gtest/gtest.h>
#include <chrono>
#include <vector>

namespace minigraph {
namespace executors {

using namespace std::chrono_literals;

constexpr auto kSleepTime = 100ms;
constexpr size_t kTaskBatches = 3;
static const auto kTotalParallelism = std::thread::hardware_concurrency();

class TaskSubmitter {
 public:
  explicit TaskSubmitter(ScheduledExecutor* executors)
      : executors_(executors),
        runner_(executors->RequestTaskRunner({})),
        counter_(0) {}

  ~TaskSubmitter() { executors_->RecycleTaskRunner(runner_); }

  void SubmitTaskBatch() {
    std::vector<Task> tasks(kTotalParallelism * kTaskBatches, [this] {
      counter_.fetch_add(1, std::memory_order_acquire);
      std::this_thread::sleep_for(kSleepTime);
    });
    // Record submission time.
    auto submission_time = std::chrono::system_clock::now();
    runner_->Run(tasks, true);
    auto run_duration = std::chrono::system_clock::now() - submission_time;
    LOGF_INFO("Run time: {} ms.", (float)run_duration.count() / 1e3);
  }

  int GetCounter() { return counter_.load(std::memory_order_acquire); }

  ScheduledExecutor* executors_;
  TaskRunner* runner_;
  std::atomic_int counter_;
};

class ExecutorsIntegrationTest : public ::testing::Test {
 protected:
  ExecutorsIntegrationTest() : executors_(kTotalParallelism) {}

  ScheduledExecutor executors_;
};

TEST_F(ExecutorsIntegrationTest, TasksCanBeExecutedWithFullParallelismInOrder) {
  auto s1 = std::make_unique<TaskSubmitter>(&executors_);
  auto s2 = std::make_unique<TaskSubmitter>(&executors_);
  EXPECT_EQ(kTotalParallelism, s1->runner_->GetParallelism());
  EXPECT_EQ(0, s2->runner_->GetParallelism());

  auto t2 = std::thread([&]() { s2->SubmitTaskBatch(); });
  std::this_thread::sleep_for(kSleepTime);
  // No task submitted by s2 should be completed before recycling s1.
  EXPECT_EQ(0, s2->GetCounter());

  s1->SubmitTaskBatch();

  EXPECT_EQ(kTotalParallelism * kTaskBatches, s1->GetCounter());
  t2.join();
  EXPECT_EQ(kTotalParallelism * kTaskBatches, s2->GetCounter());

  // Cleanup.
  executors_.Stop();
}

}  // namespace executors
}  // namespace minigraph
