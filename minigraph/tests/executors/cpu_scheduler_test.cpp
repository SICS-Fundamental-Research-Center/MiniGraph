#include "executors/cpu_scheduler.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <folly/Format.h>

#include "utility/logging.h"


namespace minigraph {
namespace executors {

class DummyTaskRunner : public TaskRunner {
 public:
  DummyTaskRunner() : counter_(0) {}
  ~DummyTaskRunner() {
    for (auto& t : ts_) {
      t.join();
    }
  }

  void Run(Task&& task) override {
    ts_.emplace_back(std::thread([t = std::move(task), this]{
      t();
      counter_++;
    }));
  }
  void Run(Task&& task, bool /*flag*/) override {
    Run(std::move(task));
  }

  void Run(const std::vector<Task>& tasks, bool /*flag*/) override {
    ts_.emplace_back(std::thread([&, this]{
      for (const auto& t : tasks) {
        t();
      }
      counter_++;
    }));
  }

  size_t GetParallelism() const override {
    // Do nothing.
    return 0;
  }

  int CompletedTasks() {
    return counter_.load();
  }

 private:
  std::atomic_int counter_;
  std::list<std::thread> ts_;
};

// Test cases require parallelism >= 5
constexpr size_t parallelism = 6;

class CPUSchedulerTest : public ::testing::Test {
 protected:
  CPUSchedulerTest() : scheduler_(parallelism),
                       factory_(&scheduler_, &downstream_) {}

  CPUScheduler scheduler_;
  DummyTaskRunner downstream_;
  ThrottleFactory factory_;
};

TEST_F(CPUSchedulerTest, SchedulerAllocateThreadsPreemptively) {
  // Create three new throttle in a sequence. The first will be assigned
  // all threads, while the rest two has 0 allocation.
  auto t1 = scheduler_.AllocateNew(&factory_, {});
  auto t2 = scheduler_.AllocateNew(&factory_, {});
  auto t3 = scheduler_.AllocateNew(&factory_, {});
  EXPECT_EQ(parallelism, t1->GetParallelism());
  EXPECT_EQ(0, t2->GetParallelism());
  EXPECT_EQ(0, t3->GetParallelism());

  t1->Run([]{}, true);
  EXPECT_EQ(parallelism - 1, t1->GetParallelism());
  EXPECT_EQ(1, t2->GetParallelism());
  EXPECT_EQ(0, t3->GetParallelism());

  t1->Run([]{}, true);
  EXPECT_EQ(parallelism - 2, t1->GetParallelism());
  EXPECT_EQ(2, t2->GetParallelism());
  EXPECT_EQ(0, t3->GetParallelism());

  t1->Run([]{}, true);
  t2->Run([]{}, true);
  EXPECT_EQ(parallelism - 3, t1->GetParallelism());
  EXPECT_EQ(2, t2->GetParallelism());
  EXPECT_EQ(1, t3->GetParallelism());

  t1->Run([]{}, true);
  EXPECT_EQ(parallelism - 4, t1->GetParallelism());
  EXPECT_EQ(2, t2->GetParallelism());
  EXPECT_EQ(2, t3->GetParallelism());

  t3->Run([]{}, true);
  t1->Run([]{}, true);
  t2->Run([]{}, true);
  EXPECT_EQ(parallelism - 5, t1->GetParallelism());
  EXPECT_EQ(1, t2->GetParallelism());
  EXPECT_EQ(1, t3->GetParallelism());

  // Now allocate t4.
  auto t4 = scheduler_.AllocateNew(&factory_, {});
  EXPECT_EQ(parallelism - 5, t1->GetParallelism());
  EXPECT_EQ(1, t2->GetParallelism());
  EXPECT_EQ(1, t3->GetParallelism());
  EXPECT_EQ(3, t4->GetParallelism());

  t2.reset();
  EXPECT_EQ(4, t4->GetParallelism());
  t4->Run([]{}, true);
  EXPECT_EQ(3, t4->GetParallelism());
  t3.reset();
  EXPECT_EQ(parallelism - 5, t1->GetParallelism());
  EXPECT_EQ(3, t4->GetParallelism());
  t1.reset();
  EXPECT_EQ(3, t4->GetParallelism());
  t4.reset();
}

TEST_F(CPUSchedulerTest, RemovingAThrottleNotManagedTriggersErrorLogging) {
  using ::testing::internal::CaptureStderr;
  using ::testing::internal::GetCapturedStderr;
  using ::testing::StartsWith;
  using ::testing::HasSubstr;

  // Allocate 3 Throttle instances.
  auto t1 = scheduler_.AllocateNew(&factory_, {});
  EXPECT_EQ(parallelism, t1->AllocatedParallelism());
  // Initiate an illegally created t4.
  auto t2 = factory_.New(3, {});

  // Trying to remove t4 results in a fatal error.
  CaptureStderr();
  t2.reset();
  auto e_message = folly::rtrimWhitespace(GetCapturedStderr()).toString();
  EXPECT_THAT(e_message, StartsWith("E"));
  EXPECT_THAT(e_message, HasSubstr("non-existent throttle"));
  EXPECT_THAT(e_message, HasSubstr("illegal operation"));
}

} // namespace executors
} // namespace minigraph
