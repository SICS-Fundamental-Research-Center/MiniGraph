#ifndef MINIGRAPH_CPU_SCHEDULER_H_
#define MINIGRAPH_CPU_SCHEDULER_H_

#include <thread>
#include <deque>
#include <mutex>

#include "executors/scheduler.h"
#include "executors/throttle.h"

#include <folly/Synchronized.h>


namespace minigraph {
namespace executors {

// A scheduler implementation.
//
// It assigns CPU resources to allocated Throttle instances *preemptively*:
// If there is resource available, it assigns *all* idle threads to the first
// Throttle waiting in line.
class CPUScheduler : public Scheduler<Throttle> {
 public:
  // Default constructor.
  // `num_threads` indicates the total number of available threads. If its value
  // is not provided, hardware concurrency is used.
  explicit CPUScheduler(
      unsigned int num_threads = std::thread::hardware_concurrency());
  virtual ~CPUScheduler() = default;


  // Create a new Throttle and allocate remaining threads to it.
  std::unique_ptr<Throttle> AllocateNew(
      SchedulableFactory<Throttle>* factory) override;

  // Remove a throttle and recycle its assigned resources.
  void Remove(Throttle* throttle) override;

  // Re-allocate threads among existing Throttle instances.
  // For this preemptive implementation, Rescheduling is *never* useful.
  // Calling this function will result in a warning log message with no other
  // side effects.
  void RescheduleAll() override;

 private:
  const size_t total_threads_;

  folly::Synchronized<std::deque<Throttle*>, std::mutex> q_;
};

} // namespace executors
} // namespace minigraph

#endif //MINIGRAPH_CPU_SCHEDULER_H_
