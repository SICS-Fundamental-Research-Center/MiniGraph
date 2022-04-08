#ifndef MINIGRAPH_EXECUTORS_SCHEDULED_EXECUTOR_H_
#define MINIGRAPH_EXECUTORS_SCHEDULED_EXECUTOR_H_

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "executors/scheduler.h"
#include "executors/throttle.h"


namespace minigraph {
namespace executors {

// An alias of Scheduler for Throttle as Schedulable.
using ThrottleScheduler = Scheduler<Throttle>;

class ScheduledExecutor {
 protected:
  // A wrapper of folly::CPUThreadPoolExecutor.
  // It adapts to the TaskRunner interface.
  class ThreadPool : public TaskRunner {
   public:
    // Parameter `num_threads` determines the number of threads in the pool.
    explicit ThreadPool(unsigned int num_threads);
    ~ThreadPool() = default;

    // Submit a task to the thread pool. Implement the TaskRunner interface.
    void Run(Task&& task) override;

    // Get the total number of threads within the thread pool.
    size_t RunParallelism() override;

    // Stop the thread pool and join all threads.
    void StopAndJoin();

   private:
    folly::CPUThreadPoolExecutor internal_pool_;
  };

 public:
  // Create a ScheduledExecutor, with `num_threads` of threads in the thread
  // pool.
  explicit ScheduledExecutor(
      unsigned int num_threads = std::thread::hardware_concurrency());
  virtual ~ScheduledExecutor() = default;

  // Use the ScheduledExecutor to create a Throttle instance such that
  // the client can submit tasks via the returned TaskRunner.
  //
  // Typically, `this` can be used as `user_ptr`, because it is just an
  // identifier for internal indexing of active Throttles.
  TaskRunner* RequestTaskRunner(
      void* user_ptr,
      Schedulable::Metadata&& metadata);

  // Release and recycle the previously requested Throttle. Called after
  // all tasks are done with the Throttle.
  //
  // Typically, `this` can be used as `user_ptr`, because it is just an
  // identifier for internal indexing of active Throttles.
  void RecycleTaskRunner(void* user_ptr, TaskRunner* runner);

  // Stop the Executor.
  void Stop();

 private:
  // A convenient alias. Its use is restricted to internal implementation use.
  using ThrottlePtr = std::unique_ptr<Throttle>;

  ThreadPool thread_pool_;

  ThrottleFactory factory_;

  std::unique_ptr<ThrottleScheduler> scheduler_;

  // A hash map for managing all active Throttles.
  // The key is the user provided identifier (typically, the caller `this`),
  // the value is the unique_ptr to the Throttle.
  // This indicates full ownership of all active Throttles.
  //
  // We use a folly::Synchronized<> container to protect its access, since
  // contention on this object cannot be too high.
  folly::Synchronized<
      std::unordered_map<void*, ThrottlePtr>,
      std::mutex> throttles_;
};

} // executors
} // minigraph

#endif //MINIGRAPH_EXECUTORS_SCHEDULED_EXECUTOR_H_
