#include "executors/cpu_scheduler.h"

#include <algorithm>

#include "utility/logging.h"


namespace minigraph {
namespace executors {

CPUScheduler::CPUScheduler(unsigned int num_threads)
    : total_threads_(num_threads),
      next_in_queue_(nullptr),
      num_free_threads_(num_threads) {}

std::unique_ptr<Throttle> CPUScheduler::AllocateNew(
    const SchedulableFactory<Throttle>* factory,
    Schedulable::Metadata&& metadata) {
  std::lock_guard<std::mutex> grd(mtx_);
  if (q_.empty()) {
    std::unique_ptr<Throttle> throttle = factory->New(
        metadata.parallelism, std::forward<Schedulable::Metadata>(metadata));
    // std::unique_ptr<Throttle> throttle = factory->New(
    //     total_threads_, std::forward<Schedulable::Metadata>(metadata));
    q_.push_back(throttle.get());
    num_free_threads_ -= metadata.parallelism;
    // num_free_threads_ = 0;
    return throttle;
  } else {
    std::unique_ptr<Throttle> throttle = factory->New(
        num_free_threads_, std::forward<Schedulable::Metadata>(metadata));
    Throttle* t = throttle.get();
    q_.push_back(t);
    if (next_in_queue_ == nullptr) next_in_queue_ = t;
    num_free_threads_ = 0;
    return throttle;
  }
}

std::unique_ptr<Throttle> CPUScheduler::AllocateNew(
    const SchedulableFactory<Throttle>* factory,
    Schedulable::Metadata&& metadata, const size_t initial_parallelism) {
  std::lock_guard<std::mutex> grd(mtx_);
  if (num_free_threads_ > initial_parallelism) {
    std::unique_ptr<Throttle> throttle = factory->New(
        initial_parallelism, std::forward<Schedulable::Metadata>(metadata));
    q_.push_back(throttle.get());
    LOG_INFO("num_free_threads", num_free_threads_);
    num_free_threads_ -= initial_parallelism;
    LOG_INFO("num_free_threads", num_free_threads_);
    return throttle;
  } else if (num_free_threads_ > 0 && num_free_threads_ < initial_parallelism) {
    std::unique_ptr<Throttle> throttle = factory->New(
        num_free_threads_, std::forward<Schedulable::Metadata>(metadata));
    Throttle* t = throttle.get();
    q_.push_back(t);
    if (next_in_queue_ == nullptr) next_in_queue_ = t;
    num_free_threads_ = 0;
    return throttle;
  }
}

void CPUScheduler::RecycleOneThread(Throttle* recycler) {
  RecycleNThreads(recycler, 1);
}

void CPUScheduler::RecycleAllThreads(Throttle* recycler) {
  if (recycler == nullptr) {
    LOG_ERROR("CPU::Scheduler::RecycleAllThread() called with nullptr.");
    return;
  }
  RecycleNThreads(recycler, recycler->GetParallelism());
}

void CPUScheduler::RecycleNThreads(Throttle* recycler, size_t num_threads) {
  if (recycler == nullptr) {
    LOG_ERROR("CPU::Scheduler::RecycleOneThread() called with nullptr.");
    return;
  }

  for (size_t i = 0; i < num_threads; i++) {
    recycler->DecrementParallelism();
  }

  std::lock_guard<std::mutex> grd(mtx_);
  auto it = q_.cbegin();
  while (it != q_.cend()) {
    if (*it != recycler) {
      it++;
      continue;
    }
    // Now, it points to recycler in the queue.
    if (next_in_queue_ == recycler) {
      // No more resource can be consumed by next_in_queue_, so we can make
      // it point to the next in queue.
      if (it + 1 != q_.cend())
        next_in_queue_ = *(it + 1);
      else {
        next_in_queue_ = nullptr;
      }
    }
    if (next_in_queue_) {
      next_in_queue_->IncreaseParallelism(num_threads);
    } else {
      num_free_threads_ += num_threads;
    }
    return;
  }

  // Execution can only reach here when recycler cannot be found in queue.
  LOG_ERROR(
      "Trying to recycle a thread from a non-existent throttle. "
      "It is an illegal operation.");
}

void CPUScheduler::Remove(Throttle* throttle) {
  if (throttle) {
    if (throttle->GetParallelism() > 0) {
      LOG_WARN("Removing a Throttle without recycling allocated threads.");
      RecycleAllThreads(throttle);
      //      RecycleNThreads(throttle, 11);
    }
    std::lock_guard<std::mutex> grd(mtx_);
    for (auto it = q_.cbegin(); it != q_.cend(); it++) {
      if (throttle == *it) {
        q_.erase(it);
        return;
      }
    }
  } else {
    LOG_WARN("Removing a null Throttle from CPUScheduler.");
  }
}

}  // namespace executors
}  // namespace minigraph
