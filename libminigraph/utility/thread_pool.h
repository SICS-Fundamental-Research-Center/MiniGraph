#ifndef MINIGRAPH_UTILITY_THREAD_POOL_H_
#define MINIGRAPH_UTILITY_THREAD_POOL_H_

#include <folly/Function.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <atomic>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>

namespace minigraph {
namespace utility {

// Base class for implementing threadpool.
class ThreadPool {
 public:
  using Task = std::function<void()>;
  ThreadPool(std::size_t num_thread) { num_threads_ = num_thread; };
  ~ThreadPool(){};
  std::atomic<bool> run_{true};
  virtual void stop() const = 0;
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
  CPUThreadPool(std::size_t num_thread, uint8_t num_priorities);
  ~CPUThreadPool(){};
  uint8_t get_num_priorities() const;
  size_t get_task_queue_size() const;
  template <class F, class... Args>
  void Commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    cpu_executor_->add(task);
  }
  void Commit(Task task) { cpu_executor_->add(task); };
  void CommitWithPriority(Task task, uint8_t priority);

  void stop() const {};

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
  IOThreadPool(std::size_t max_threads, std::size_t min_threads);
  ~IOThreadPool(){};
  size_t get_max_threads() const;
  size_t get_min_threads() const;
  template <class F, class... Args>
  void Commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    io_executor_->add(task);
  }
  void Commit(Task task) { io_executor_->add(task); };
  void stop() const {};
  folly::EventBase* GetEventBase();

 private:
  size_t max_threads_;
  size_t min_threads_;
  std::unique_ptr<folly::IOThreadPoolExecutor> io_executor_;
};

}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_THREAD_POOL_H_