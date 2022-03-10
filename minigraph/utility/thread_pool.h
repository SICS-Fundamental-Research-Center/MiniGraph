#ifndef MINIGRAPH_UTILITY_THREAD_POOL_H_
#define MINIGRAPH_UTILITY_THREAD_POOL_H_

#include <atomic>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

namespace minigraph {
namespace utility {

class ThreadPool {
 public:
  ThreadPool(std::size_t num_thread) { num_threads_ = num_thread; };
  ~ThreadPool(){};
  using Task = std::function<void()>;
  std::atomic<bool> run_{true};
  virtual void stop() const = 0;
  std::size_t get_num_threads() { return num_threads_; }

 private:
  std::size_t num_threads_ = 0;
};

class CpuThreadPool : virtual public ThreadPool {
 public:
  CpuThreadPool(std::size_t num_thread);
  ~CpuThreadPool() { return; };
  template <class F, class... Args>
  void commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    cpu_executor_->add(task);
  }
  void stop() const { return; };

 private:
  std::unique_ptr<folly::CPUThreadPoolExecutor> cpu_executor_;
};

class IOThreadPool : virtual public ThreadPool {
 public:
  IOThreadPool(std::size_t max_threads, std::size_t min_threads);
  ~IOThreadPool() { return; };
  template <class F, class... Args>
  void commit(F&& f, Args&&... args) {
    auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    io_executor_->add(task);
  }
  void stop() const { return; };

 private:
  size_t max_threads_;
  size_t min_threads_;
  std::unique_ptr<folly::IOThreadPoolExecutor> io_executor_;
};
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_THREAD_POOL_H_