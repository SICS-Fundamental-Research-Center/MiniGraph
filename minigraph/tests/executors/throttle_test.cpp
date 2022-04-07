#include <gtest/gtest.h>

#include "executors/throttle.h"

#include <atomic>
#include <thread>
#include <list>
#include <chrono>


namespace minigraph {
namespace executors {

// For time duration literals like 10ms.
using namespace std::chrono_literals;

// To allow successful execution of parallelism adjustment tests, set
// MAX_PARALLELISM > DELTA_PARALLELISM, and
// TOTAL_TASKS > (MAX_PARALLELISM + DELTA_PARALLELISM) * 3.
#define MAX_PARALLELISM 8
#define DELTA_PARALLELISM 4
#define TOTAL_TASKS 200

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

  size_t RunParallelism() override {
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

class ThrottleTest : public ::testing::Test {
 protected:
  ThrottleTest() : factory_(&dummy_) {
  }

  DummyTaskRunner dummy_;
  ThrottleFactory factory_;
};

TEST_F(ThrottleTest, ThrottleCanLimitMaxParallelism) {
  std::atomic_int running_tasks(0);
  std::mutex mtx;
  std::condition_variable cv;

  auto throttle = factory_.New(MAX_PARALLELISM, {});

  for (int i = 0; i < MAX_PARALLELISM; i++) {
    throttle->Run([&] {
      running_tasks++;
      std::unique_lock<std::mutex> lck(mtx);
      cv.wait(lck);
      running_tasks--;
    });
  }

  // Execution should reach here without being blocked.
  // Now launch more task enqueue operations in parallel. We expect
  // them to block execution because the parallelism is saturated.
  std::vector<std::thread> enqueue_threads;
  enqueue_threads.reserve(TOTAL_TASKS);
  for (int i = 0; i < TOTAL_TASKS - MAX_PARALLELISM; i++) {
    enqueue_threads.emplace_back(std::thread([&] {
      throttle->Run([&] {
        running_tasks++;
        std::unique_lock<std::mutex> lck(mtx);
        cv.wait(lck);
        running_tasks--;
      });
    }));
  }
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(MAX_PARALLELISM, running_tasks.load());
  EXPECT_EQ(0, dummy_.CompletedTasks());

  for (int i = 0; i < TOTAL_TASKS - MAX_PARALLELISM; i++) {
    // Now, use `cv.notify_one()` to unblock one thread.
    cv.notify_one();
    std::this_thread::sleep_for(10ms);
    EXPECT_EQ(
        MAX_PARALLELISM < TOTAL_TASKS-i-1 ? MAX_PARALLELISM : TOTAL_TASKS-i-1 ,
        running_tasks.load());
    EXPECT_EQ(i + 1, dummy_.CompletedTasks());
  }

  // Cleanup.
  while (dummy_.CompletedTasks() < TOTAL_TASKS) {
    cv.notify_all();
    std::this_thread::sleep_for(10ms);
  }
  for (auto& t : enqueue_threads) {
    t.join();
  }
}

TEST_F(ThrottleTest, IncreaseParallelismCanBeAchievedImmediately) {
  std::atomic_int running_tasks(0);
  std::mutex mtx;
  std::condition_variable cv;

  auto throttle = factory_.New(MAX_PARALLELISM, {});

  for (int i = 0; i < MAX_PARALLELISM; i++) {
    throttle->Run([&] {
      running_tasks++;
      std::unique_lock<std::mutex> lck(mtx);
      cv.wait(lck);
      running_tasks--;
    });
  }

  // Execution should reach here without being blocked.
  // Now launch more task enqueue operations in parallel. We expect
  // them to block execution because the parallelism is saturated.
  std::vector<std::thread> enqueue_threads;
  enqueue_threads.reserve(TOTAL_TASKS);
  for (int i = 0; i < TOTAL_TASKS - MAX_PARALLELISM; i++) {
    enqueue_threads.emplace_back(std::thread([&] {
      throttle->Run([&] {
        running_tasks++;
        std::unique_lock<std::mutex> lck(mtx);
        cv.wait(lck);
        running_tasks--;
      });
    }));
  }
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(MAX_PARALLELISM, running_tasks.load());
  EXPECT_EQ(0, dummy_.CompletedTasks());

  // Now improve parallelism and check the number of running tasks.
  throttle->IncreaseParallelism(DELTA_PARALLELISM);
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(MAX_PARALLELISM + DELTA_PARALLELISM, running_tasks.load());
  EXPECT_EQ(0, dummy_.CompletedTasks());

  const size_t current_p = MAX_PARALLELISM + DELTA_PARALLELISM;
  while (dummy_.CompletedTasks() < TOTAL_TASKS) {
    // Now, use `cv.notify_all()` to unblock remaining threads.
    cv.notify_all();
    std::this_thread::sleep_for(1ms);
    EXPECT_GE(current_p, running_tasks.load());
  }

  for (auto& t : enqueue_threads) {
    t.join();
  }
}

TEST_F(ThrottleTest, DecreaseParallelismIsInEffectAfterReturn) {
  std::atomic_int running_tasks(0);
  std::mutex mtx;
  std::condition_variable cv;

  auto throttle = factory_.New(MAX_PARALLELISM, {});

  for (int i = 0; i < MAX_PARALLELISM; i++) {
    throttle->Run([&] {
      running_tasks++;
      std::unique_lock<std::mutex> lck(mtx);
      cv.wait(lck);
      running_tasks--;
    });
  }

  // Execution should reach here without being blocked.
  // Now launch more task enqueue operations in parallel. We expect
  // them to block execution because the parallelism is saturated.
  std::vector<std::thread> enqueue_threads;
  enqueue_threads.reserve(TOTAL_TASKS);
  for (int i = 0; i < MAX_PARALLELISM * 2; i++) {
    enqueue_threads.emplace_back(std::thread([&] {
      throttle->Run([&] {
        running_tasks++;
        std::unique_lock<std::mutex> lck(mtx);
        cv.wait(lck);
        running_tasks--;
      });
    }));
  }
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(MAX_PARALLELISM, running_tasks.load());
  EXPECT_EQ(0, dummy_.CompletedTasks());

  // Now improve parallelism and check the number of running tasks.
  for (int i = 0; i < DELTA_PARALLELISM; i++) {
    std::atomic_bool success(false);
    auto t = std::thread([&] {
      int remaining = throttle->DecrementParallelism();
      EXPECT_EQ(MAX_PARALLELISM - i - 1, remaining);
      success.store(true);
    });
    int notify_count = 0;
    while (!success.load()) {
      cv.notify_one();
      notify_count++;
      std::this_thread::sleep_for(1ms);
    }
    t.join();
  }
  std::this_thread::sleep_for(10ms);

  for (int i = MAX_PARALLELISM * 3; i < TOTAL_TASKS; i++) {
    enqueue_threads.emplace_back(std::thread([&] {
      throttle->Run([&] {
        running_tasks++;
        std::unique_lock<std::mutex> lck(mtx);
        cv.wait(lck);
        running_tasks--;
      });
    }));
  }
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(MAX_PARALLELISM - DELTA_PARALLELISM, running_tasks.load());

  while (dummy_.CompletedTasks() < TOTAL_TASKS) {
    std::vector<std::thread> notifying_threads;
    notifying_threads.reserve(MAX_PARALLELISM);
    for (int i = 0; i < MAX_PARALLELISM; i++) {
      notifying_threads.emplace_back(std::thread([&cv] {
        cv.notify_one();
      }));
    }
    for (auto& t : notifying_threads) {
      t.join();
    }
    EXPECT_GE(MAX_PARALLELISM - DELTA_PARALLELISM, running_tasks.load());
  }

  // Cleanup.
  for (auto& t : enqueue_threads) {
    t.join();
  }
}

TEST_F(ThrottleTest, ThrottleWithZeroParallelismBlockExecution) {
  auto blocking_throttle = factory_.New(0, {});

  std::atomic_int completed_tasks(0);

  auto t = std::thread([&] {
    blocking_throttle->Run([&] {
      completed_tasks++;
    });
  });
  // t should be blocking execution because Run() is blocked.

  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(0, completed_tasks.load());
  EXPECT_EQ(1, blocking_throttle->IncreaseParallelism(1));
  std::this_thread::sleep_for(10ms);
  EXPECT_EQ(1, completed_tasks.load());
  EXPECT_EQ(1, dummy_.CompletedTasks());
  t.join();
}


} // namespace executors
} // namespace minigraph
