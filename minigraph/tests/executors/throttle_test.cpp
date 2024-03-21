#include <gtest/gtest.h>

#include "executors/throttle.h"

#include <atomic>
#include <thread>
#include <list>
#include <chrono>

#include "executors/scheduler.h"
#include "utility/logging.h"


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

class DummyScheduler : public Scheduler<Throttle> {
 public:
  std::unique_ptr<Throttle> AllocateNew(
      const SchedulableFactory<Throttle>* factory,
      Schedulable::Metadata&& metadata) override {
    return nullptr;
  }
  void RecycleOneThread(Throttle* recycler) override {
    recycler->DecrementParallelism();
    recycled_threads_++;
  }
  void RecycleAllThreads(Throttle* recycler) override {
    int parallelism = (int) recycler->GetParallelism();
    for (int i = 0; i < parallelism; i++) {
      recycler->DecrementParallelism();
    }
    recycled_threads_ += parallelism;
  }

  std::atomic_int recycled_threads_{0};

 protected:
  void Remove(Throttle* throttle) override {
    LOG_INFO("DummyScheduler::Remove() called.");
  }
};

class ThrottleTest : public ::testing::Test {
 protected:
  ThrottleTest() : factory_(&scheduler_, &dummy_) {
  }

  DummyScheduler scheduler_;
  DummyTaskRunner dummy_;
  ThrottleFactory factory_;
};

TEST_F(ThrottleTest, ThrottleRunCanRecycleThread) {
  auto throttle = factory_.New(3, {});

  throttle->Run([&] {
    std::this_thread::sleep_for(10ms);
  }, true);
  EXPECT_EQ(1, scheduler_.recycled_threads_.load());
  EXPECT_EQ(2, throttle->GetParallelism());

  throttle->Run([&] {
    std::this_thread::sleep_for(10ms);
  }, true);
  EXPECT_EQ(2, scheduler_.recycled_threads_.load());
  EXPECT_EQ(1, throttle->GetParallelism());

  throttle->Run([&] {
    std::this_thread::sleep_for(10ms);
  }, true);
  EXPECT_EQ(3, scheduler_.recycled_threads_.load());
  EXPECT_EQ(0, throttle->GetParallelism());
}

TEST_F(ThrottleTest, ThrottleRunLimitMaxParallelism) {
  std::atomic_int running_tasks(0);
  auto throttle = factory_.New(MAX_PARALLELISM, {});

  std::vector<std::thread> enqueue_threads;
  enqueue_threads.reserve(TOTAL_TASKS);
  for (int i = 0; i < TOTAL_TASKS; i++) {
    enqueue_threads.emplace_back(std::thread([&] {
      throttle->Run([&] {
        int prev_running = running_tasks.fetch_add(1);
        EXPECT_LT(prev_running, MAX_PARALLELISM);
        std::this_thread::sleep_for(10ms);
        prev_running = running_tasks.fetch_sub(1);
        EXPECT_LE(prev_running, MAX_PARALLELISM);
      }, false);
    }));
  }

  for (auto& t : enqueue_threads) {
    t.join();
  }
  // No thread is recycled.
  EXPECT_EQ(0, scheduler_.recycled_threads_.load());
}

TEST_F(ThrottleTest, ThrottleRunVectorTasksCanRecycleThreads) {
  auto throttle = factory_.New(10, {});

  std::vector<Task> tasks(30, []{
    std::this_thread::sleep_for(10ms);
  });

  throttle->Run(tasks, true);
  EXPECT_EQ(10, scheduler_.recycled_threads_.load());
  EXPECT_EQ(0, throttle->GetParallelism());
  // #Tasks < 4 * #Parallelism, did not partition the vector.
  EXPECT_EQ(dummy_.CompletedTasks(), 30);
}

TEST_F(ThrottleTest, ThrottleRunVectorTasksCanRecycleAllThreads) {
  auto throttle = factory_.New(10, {});

  std::vector<Task> tasks(2, []{
    std::this_thread::sleep_for(10ms);
  });

  throttle->Run(tasks, true);
  EXPECT_EQ(10, scheduler_.recycled_threads_.load());
  EXPECT_EQ(0, throttle->GetParallelism());
  EXPECT_EQ(dummy_.CompletedTasks(), 2);
}

TEST_F(ThrottleTest, ThrottleRunVectorTasksLimitMaxParallelism) {
  std::atomic_int running_tasks(0);
  std::atomic_int counter(0);
  auto throttle = factory_.New(MAX_PARALLELISM, {});

  std::vector<Task> tasks(
      TOTAL_TASKS,
      [&] {
        int prev_running = running_tasks.fetch_add(1);
        EXPECT_LT(prev_running, MAX_PARALLELISM);
        if (prev_running == MAX_PARALLELISM - 1) {
          counter++;
        }
        std::this_thread::sleep_for(10ms);
        prev_running = running_tasks.fetch_sub(1);
        EXPECT_LE(prev_running, MAX_PARALLELISM);
      });
  throttle->Run(tasks, false);
  // No thread is recycled.
  EXPECT_EQ(0, scheduler_.recycled_threads_.load());
  EXPECT_EQ(MAX_PARALLELISM * 4, dummy_.CompletedTasks());

  LOGF_INFO("{} tasks are run at max parallelism.", counter.load());
  EXPECT_LT(0, counter.load());
}

TEST_F(ThrottleTest, IncreaseParallelismCanBeAchievedImmediately) {
  std::atomic_int running_tasks(0);
  std::atomic_int counter(0);
  auto throttle = factory_.New(MAX_PARALLELISM, {});

  std::vector<Task> tasks(TOTAL_TASKS, [&] {
    int prev_running = running_tasks.fetch_add(1);
    EXPECT_LT(prev_running, MAX_PARALLELISM + 10);
    if (prev_running >= MAX_PARALLELISM) {
      counter++;
    }
    std::this_thread::sleep_for(10ms);
    prev_running = running_tasks.fetch_sub(1);
    EXPECT_LE(prev_running, MAX_PARALLELISM + 10);
  });
  auto t = std::thread([&] {
    throttle->Run(tasks, true);
  });

  throttle->IncreaseParallelism(10);
  t.join();
  LOGF_INFO("After increased parallelism, {} tasks are run using additional"
      " threads.", counter.load());
  EXPECT_LT(0, counter.load());
}

TEST_F(ThrottleTest, DecreaseParallelismIsInEffectAfterReturn) {
  std::atomic_int running_tasks(0);
  auto throttle = factory_.New(MAX_PARALLELISM, {});

  std::vector<Task> tasks(TOTAL_TASKS, [&] {
    int prev_running = running_tasks.fetch_add(1);
    EXPECT_LT(prev_running, MAX_PARALLELISM);
    std::this_thread::sleep_for(10ms);
    prev_running = running_tasks.fetch_sub(1);
    EXPECT_LE(prev_running, MAX_PARALLELISM);
  });
  throttle->Run(tasks, false);

  for (int i = 0; i < DELTA_PARALLELISM; i++) {
    throttle->DecrementParallelism();
  }

  std::vector<Task> tasks2(TOTAL_TASKS, [&] {
    int prev_running = running_tasks.fetch_add(1);
    EXPECT_LT(prev_running, MAX_PARALLELISM - DELTA_PARALLELISM);
    std::this_thread::sleep_for(10ms);
    prev_running = running_tasks.fetch_sub(1);
    EXPECT_LE(prev_running, MAX_PARALLELISM - DELTA_PARALLELISM);
  });
  throttle->Run(tasks, true);
}

TEST_F(ThrottleTest, ThrottleWithZeroParallelismBlockExecution) {
  auto blocking_throttle = factory_.New(0, {});

  std::atomic_int completed_tasks(0);

  auto t = std::thread([&] {
    blocking_throttle->Run([&] {
      completed_tasks++;
    }, false);
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
