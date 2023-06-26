#ifndef MINIGRAPH_UTILITY_THREAD_POOL_H_
#define MINIGRAPH_UTILITY_THREAD_POOL_H_

#include <atomic>
#include <cstddef>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <queue>
#include <stdexcept>
#include <vector>

#include <folly/Function.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/EDFThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>


namespace minigraph {
namespace utility {

#define THREADPOOL_MAX_NUM 16

// Base class for implementing threadpool.
class ThreadPool {
 public:
  using Task = std::function<void()>;
  ThreadPool(std::size_t num_thread) { num_threads_ = num_thread; };
  ~ThreadPool() = default;
  std::atomic<bool> run_{true};
  std::size_t get_num_threads() const { return num_threads_; }

 protected:
  std::size_t num_threads_ = 0;
};

// The default queue throws when full (folly::QueueBehaviorIfFull::THROW),
// so commit() can fail. Contains a series of priority queues which get
// constantly picked up by a series of workers. Each worker thread executes
// threadRun() after created. ThreadRun() is essentially an infinite loop
// which pulls one task from task queue and executes it.
// If the task is already expired when it is fetched, then the expire callback
// is executed instead of the task itself.
class CPUThreadPool : virtual public ThreadPool {
 public:
  CPUThreadPool(const std::size_t num_threads, const uint8_t num_priorities)
      : ThreadPool(num_threads) {
    cpu_executor_ =
        std::make_unique<folly::CPUThreadPoolExecutor>(num_threads_);
    num_priorities_ = num_priorities;
  }

  ~CPUThreadPool(){};
  template <class F, class... Args>
  void Commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    cpu_executor_->add(task);
  }
  void Commit(Task task) { cpu_executor_->add(task); };

  void CommitWithPriority(Task task, uint8_t priority) {
    cpu_executor_->addWithPriority(task, priority);
  }

  uint8_t get_num_priorities() const {
    return cpu_executor_->getNumPriorities();
  }

  size_t get_task_queue_size() const {
    return cpu_executor_->getTaskQueueSize();
  }

 private:
  std::unique_ptr<folly::CPUThreadPoolExecutor> cpu_executor_;
  uint8_t num_priorities_ = 1;
};

// Each IO thread of Folly runs its own EventBase. Instead of pulling task from
// task queue like the CPUThreadPoolExecutor, the IOThreadPoolExecutor registers
// an event to the EventBase of next IO thread. Each IO thread then calls
// loopForEver() for its EventBase, which essentially calls epoll() to perform
// async io.
class IOThreadPool : virtual public ThreadPool {
 public:
  IOThreadPool(const size_t max_threads, const size_t min_threads)
      : ThreadPool(max_threads) {
    max_threads_ = max_threads;
    min_threads_ = min_threads;
    io_executor_ =
        std::make_unique<folly::IOThreadPoolExecutor>(max_threads, min_threads);
  }
  ~IOThreadPool() = default;
  size_t get_max_threads() const;

  size_t get_min_threads() const;

  template <class F, class... Args>
  void Commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    io_executor_->add(task, std::chrono::milliseconds(1000));
  }
  void Commit(Task task) {
    io_executor_->add(task, std::chrono::milliseconds(1000));
  };
  folly::EventBase* GetEventBase();

 private:
  size_t max_threads_;
  size_t min_threads_;
  std::unique_ptr<folly::IOThreadPoolExecutor> io_executor_;
};

class EDFThreadPool : virtual public ThreadPool {
 public:
  EDFThreadPool(const std::size_t num_threads) : ThreadPool(num_threads) {
    num_threads_ = num_threads;
    edf_executor_ = std::make_unique<folly::EDFThreadPoolExecutor>(num_threads);
  }

  ~EDFThreadPool() = default;

  template <class F, class... Args>
  void Commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    edf_executor_->add(task);
  }
  void Commit(Task task) { edf_executor_->add(task); };

 private:
  size_t num_threads_;
  std::unique_ptr<folly::EDFThreadPoolExecutor> edf_executor_;
};

class CommonThreadPool : public ThreadPool {
 private:
  std::vector<std::thread> pool_;
  using Task = std::function<void()>;
  std::queue<Task> tasks_;
  std::mutex lock_;
  std::condition_variable task_cv_;
  std::atomic<bool> run_{true};
  std::atomic<int> idl_thr_num_{0};

 public:
  inline CommonThreadPool(const unsigned short num_threads = 4)
      : ThreadPool(num_threads) {
    AddThread(num_threads);
  }
  inline ~CommonThreadPool() {
    run_ = false;
    task_cv_.notify_all();
    for (std::thread& thread : pool_) {
      // thread.detach(); // 让线程“自生自灭”
      if (thread.joinable()) thread.join();
    }
  }
  int get_idl_num() { return idl_thr_num_; }
  int get_threads_num() { return pool_.size(); }

  template <class F, class... Args>
  auto Commit(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
    if (!run_) throw std::runtime_error("commit on ThreadPool is stopped.");
    using RetType = decltype(f(args...));
    auto task = std::make_shared<std::packaged_task<RetType()>>(
        bind(std::forward<F>(f), std::forward<Args>(args)...));
    std::future<RetType> future = task->get_future();
    {
      std::lock_guard<std::mutex> lock{lock_};
      tasks_.emplace([task]() { (*task)(); });
    }
    task_cv_.notify_one();
    return future;
  }

  template <class F, class... Args>
  auto add(F&& f, Args&&... args) -> std::future<decltype(f(args...))> {
    if (!run_) throw std::runtime_error("commit on ThreadPool is stopped.");
    using RetType = decltype(f(args...));
    auto task = std::make_shared<std::packaged_task<RetType()>>(
        bind(std::forward<F>(f), std::forward<Args>(args)...));
    std::future<RetType> future = task->get_future();
    {
      std::lock_guard<std::mutex> lock{lock_};
      tasks_.emplace([task]() { (*task)(); });
    }
    task_cv_.notify_one();
    return future;
  }

  void AddThread(unsigned short size) {
    for (; pool_.size() < THREADPOOL_MAX_NUM && size > 0; --size) {
      pool_.emplace_back([this] {
        while (run_) {
          Task task;
          {
            std::unique_lock<std::mutex> lock{lock_};
            task_cv_.wait(lock, [this] { return !run_ || !tasks_.empty(); });
            if (!run_ && tasks_.empty()) return;
            task = move(tasks_.front());
            tasks_.pop();
          }
          --idl_thr_num_;
          task();
          ++idl_thr_num_;
        }
      });
      ++idl_thr_num_;
    }
  }

  size_t numThreads() const { return num_threads_; };
  void stop() {}
  void join() {}
  void StopAndJoin() {}
};

}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_THREAD_POOL_H_