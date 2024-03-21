#ifndef MINIGRAPH_CPU_SCHEDULER_H_
#define MINIGRAPH_CPU_SCHEDULER_H_

#include <deque>
#include <mutex>
#include <thread>

#include "executors/scheduler.h"
#include "executors/throttle.h"


namespace minigraph {
namespace executors {

// A scheduler implementation.
//
// It assigns CPU resources to allocated Throttle instances *preemptively*:
// If there is resource available, it assigns *all* idle threads to the first
// Throttle waiting in line.
class CPUScheduler final : public Scheduler<Throttle> {
 public:
  // Default constructor.
  // `num_threads` indicates the total number of available threads. If its value
  // is not provided, hardware concurrency is used.
  explicit CPUScheduler(
      unsigned int num_threads = std::thread::hardware_concurrency());
  ~CPUScheduler() override = default;

  // Create a new Throttle and allocate remaining threads to it.
  std::unique_ptr<Throttle> AllocateNew(
      const SchedulableFactory<Throttle>* factory,
      Schedulable::Metadata&& metadata) override;

  // Create a new Throttle and allocate user specific threads to it.
  std::unique_ptr<Throttle> AllocateNew(
      const SchedulableFactory<Throttle>* factory,
      Schedulable::Metadata&& metadata, const size_t init_parallelism) override;

  // Recycle one thread from `recycler`, and allocated it to the next Throttle
  // waiting for more threads.
  void RecycleOneThread(Throttle* recycler) override;

  // Recycle all threads from `recycler`, and allocated them to the next
  // Throttle waiting for more threads.
  void RecycleAllThreads(Throttle* recycler) override;

 protected:
  // Remove throttle from being managed by this Scheduler. Callable from the
  // destructor of a throttle only, which is a friend function.
  //
  // Before the call, it is recommended that all threads allocated to throttle
  // be recycled; otherwise, a WARNING message will be logged and the threads
  // are recycled automatically.
  void Remove(Throttle* throttle) override;

 private:
  // A helper function for implementing `RecycleOneThread()` and
  // `RecycleAllThreads()`.
  void RecycleNThreads(Throttle* recycler, size_t num_threads);

  // Total available threads for scheduling.
  const size_t total_threads_;

  // Mutex to synchronize access to the following three data fields.
  std::mutex mtx_;

  // Next throttle that demands more resources.
  // Once a throttle starts recycling threads, it no longer qualifies for more
  // resource allocation.
  Throttle* next_in_queue_;

  size_t num_free_threads_;

  std::deque<Throttle*> q_;
};

}  // namespace executors
}  // namespace minigraph

#endif  // MINIGRAPH_CPU_SCHEDULER_H_
