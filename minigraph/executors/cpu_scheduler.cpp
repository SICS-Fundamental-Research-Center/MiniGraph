#include "executors/cpu_scheduler.h"

#include <algorithm>

#include "utility/logging.h"


namespace minigraph {
namespace executors {

CPUScheduler::CPUScheduler(unsigned int num_threads) :
    total_threads_(num_threads) {}

std::unique_ptr<Throttle> CPUScheduler::AllocateNew(
    SchedulableFactory<Throttle>* factory) {
  auto q = q_.lock();
  if (q->empty()) {
    std::unique_ptr<Throttle> throttle = factory->New(total_threads_, {});
    q->push_back(throttle.get());
    return throttle;
  }
  else {
    std::unique_ptr<Throttle> throttle = factory->New(0, {});
    q->push_back(throttle.get());
    return throttle;
  }
}

void CPUScheduler::Remove(Throttle* throttle) {
  if (throttle == nullptr) {
    return;
  }

  auto q = q_.lock();
  // In normal cases, remove operations should be called on head of q_ once
  // its tasks are all completed. It makes *no sense* to remove a Throttle
  // that has not received any threads. If this happens, print an error.
  if (q->front() != throttle) {
    LOG_ERROR("Removing a Throttle that is not scheduled yet. "
              "Something must be wrong.");
    auto it = std::find(q->begin(), q->end(), throttle);
    if (it == q->end()) {
      LOG_FATAL("Trying to remove a non-existent throttle from scheduler. "
                "It is an illegal operation, exiting...");
    }
    else {
      LOGF_ERROR("Removing a Throttle at {}-th position in queue.",
                 it - q->begin());
      q->erase(it);
      return;
    }
  }

  while (throttle->DecrementParallelism() > 0) {
    // Reclaim all thread allocations from the removed Throttle.
  }
  q->pop_front();
  if (!q->empty()) {
    q->front()->IncreaseParallelism(total_threads_);
  }
}

void CPUScheduler::RescheduleAll() {
  LOGF_WARN("Reschedule threads among {} Throttle instances in queue",
            q_.withLock([](auto& locked) {
              return locked.size();
            }));
}

} // namespace executors
} // namespace minigraph
