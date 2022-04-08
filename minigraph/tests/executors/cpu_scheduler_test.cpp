#include "executors/cpu_scheduler.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <folly/Format.h>

#include "utility/logging.h"


namespace minigraph {
namespace executors {

class DummyTaskRunner : public TaskRunner {
 public:
  DummyTaskRunner() = default;

  void Run(Task&& task) override {
    // Do nothing.
  }
  size_t RunParallelism() override {
    // Do nothing.
    return 0;
  }
};

constexpr unsigned int parallelism = 6;

class CPUSchedulerTest : public ::testing::Test {
 protected:
  CPUSchedulerTest() : factory_(&downstream_), scheduler_(parallelism) {}

  DummyTaskRunner downstream_;
  ThrottleFactory factory_;
  CPUScheduler scheduler_;
};

TEST_F(CPUSchedulerTest, SchedulerAllocateThreadsPreemptively) {
  // Create three new throttle in a sequence. The first will be assigned
  // all threads, while the rest two has 0 allocation.
  auto t1 = scheduler_.AllocateNew(&factory_, {});
  auto t2 = scheduler_.AllocateNew(&factory_, {});
  auto t3 = scheduler_.AllocateNew(&factory_, {});
  EXPECT_EQ((size_t) parallelism, t1->RunParallelism());
  EXPECT_EQ(0, t2->RunParallelism());
  EXPECT_EQ(0, t3->RunParallelism());

  // Now remove t1.
  scheduler_.Remove(t1.get());
  EXPECT_EQ(0, t1->RunParallelism());
  EXPECT_EQ((size_t) parallelism, t2->RunParallelism());
  EXPECT_EQ(0, t3->RunParallelism());

  // Now remove t2.
  scheduler_.Remove(t2.get());
  EXPECT_EQ(0, t1->RunParallelism());
  EXPECT_EQ(0, t2->RunParallelism());
  EXPECT_EQ((size_t) parallelism, t3->RunParallelism());

  // Now allocate t4.
  auto t4 = scheduler_.AllocateNew(&factory_, {});
  EXPECT_EQ(0, t1->RunParallelism());
  EXPECT_EQ(0, t2->RunParallelism());
  EXPECT_EQ((size_t) parallelism, t3->RunParallelism());
  EXPECT_EQ(0, t4->RunParallelism());
  // Reschedule prints a warning.
  scheduler_.RescheduleAll();

  // Now remove t3.
  scheduler_.Remove(t3.get());
  EXPECT_EQ(0, t1->RunParallelism());
  EXPECT_EQ(0, t2->RunParallelism());
  EXPECT_EQ(0, t3->RunParallelism());
  EXPECT_EQ((size_t) parallelism, t4->RunParallelism());

  // Now remove t4.
  scheduler_.Remove(t4.get());
  EXPECT_EQ(0, t1->RunParallelism());
  EXPECT_EQ(0, t2->RunParallelism());
  EXPECT_EQ(0, t3->RunParallelism());
  EXPECT_EQ(0, t4->RunParallelism());
  // Reschedule prints a warning.
  scheduler_.RescheduleAll();
}

TEST_F(CPUSchedulerTest, RemovingAThrottleNotAtTheFrontTriggersErrorLogging) {
  using ::testing::internal::CaptureStderr;
  using ::testing::internal::GetCapturedStderr;
  using ::testing::StartsWith;
  using ::testing::HasSubstr;

  // Allocate 3 Throttle instances.
  auto t1 = scheduler_.AllocateNew(&factory_, {});
  auto t2 = scheduler_.AllocateNew(&factory_, {});
  auto t3 = scheduler_.AllocateNew(&factory_, {});
  EXPECT_EQ((size_t) parallelism, t1->AllocatedParallelism());
  EXPECT_EQ(0, t2->AllocatedParallelism());
  EXPECT_EQ(0, t3->AllocatedParallelism());
  // Initiate an illegally created t4.
  auto t4 = factory_.New(3, {});

  // Trying to remove t2 will trigger an error message.
  CaptureStderr();
  scheduler_.Remove(t2.get());
  auto e_message = folly::rtrimWhitespace(GetCapturedStderr()).toString();
  EXPECT_THAT(e_message, StartsWith("E"));
  LOG_INFO(e_message);
  // t2 should be removed from queue regardless of the error.
  scheduler_.Remove(t1.get());
  EXPECT_EQ(0, t2->AllocatedParallelism());
  EXPECT_EQ((size_t) parallelism, t3->AllocatedParallelism());

  // Trying to remove t4 results in a fatal error.
  EXPECT_DEATH(scheduler_.Remove(t4.get()),
               HasSubstr("illegal operation"));
}

} // namespace executors
} // namespace minigraph
