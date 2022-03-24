#include "utility/thread_pool.h"

namespace minigraph {
namespace utility {

CPUThreadPool::CPUThreadPool(size_t num_thread, uint8_t num_priorities)
    : ThreadPool(num_thread) {
  cpu_executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(num_threads_);
  num_priorities_ = num_priorities;
}

void CPUThreadPool::CommitWithPriority(Task task, uint8_t priority) {
  // assert(priority < num_priorities_);
  cpu_executor_->addWithPriority(task, priority);
}

uint8_t CPUThreadPool::get_num_priorities() const {
  return cpu_executor_->getNumPriorities();
}

size_t CPUThreadPool::get_task_queue_size() const {
  return cpu_executor_->getTaskQueueSize();
}

IOThreadPool::IOThreadPool(size_t max_threads, size_t min_threads)
    : ThreadPool(max_threads) {
  max_threads_ = max_threads;
  min_threads_ = min_threads;
  io_executor_ =
      std::make_unique<folly::IOThreadPoolExecutor>(max_threads_, min_threads_);
}

folly::EventBase* IOThreadPool::GetEventBase() {
  return io_executor_->getEventBase();
}

}  // namespace utility
}  // namespace minigraph