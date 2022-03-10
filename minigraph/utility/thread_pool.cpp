#include "utility/thread_pool.h"

namespace minigraph {
namespace utility {

CpuThreadPool::CpuThreadPool(size_t num_thread) : ThreadPool(num_thread) {
  cpu_executor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(get_num_threads());
}
IOThreadPool::IOThreadPool(size_t max_threads, size_t min_threads)
    : ThreadPool(max_threads) {
  max_threads_ = max_threads;
  min_threads_ = min_threads;
  io_executor_ =
      std::make_unique<folly::IOThreadPoolExecutor>(max_threads_, min_threads_);
}
}  // namespace utility
}  // namespace minigraph