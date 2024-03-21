#ifndef MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_
#define MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_

#include "executors/schedulable.h"
#include "executors/task_runner.h"

#include <folly/synchronization/NativeSemaphore.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

namespace minigraph {
namespace executors {

// Forward declaring the Scheduler interface;
template <typename T>
class Scheduler;

// An intermediary TaskRunner that is used to throttle the maximum
// parallelism of task execution, by limiting the number of tasks being
// processed at the same time.
//
// It supports changing parallelism on-the-fly, by adjusting the parallelism
// limit.
//
// Internally, Throttle uses a NativeSemaphore to limit the number of tasks
// under execution. Once a task is completed, it will `post()` the semaphore
// to release the resources.
class Throttle final : public TaskRunner, public Schedulable {
 public:
  // As an intermediate TaskRunner, it must be constructed by providing
  // a downstream TaskRunner.
  explicit Throttle(ID_Type id,
                    Scheduler<Throttle>* scheduler,
                    TaskRunner* downstream,
                    size_t max_parallelism);

  // A non-default destructor is required to restore semaphore to its original
  // status before destruction. Otherwise, the destructor might raise a
  // EXC_BAD_INSTRUCTION fault on macOS.
  ~Throttle() override;

  /*********************************************************
   * Implement TaskRunner interfaces.
   ********************************************************/

  // Submit a single task for execution in the thread pool.
  // This is *blocking call*.
  // It will not return until the task is *completed*.
  //
  // `release_resource` indicates whether to release one assigned thread
  // after the task is completed.
  // If `release_resource` is set, the available parallelism will decrement
  // by one after this call returns.
  //
  // Note: if multiple Run() is called from different threads, it will result
  // in undefined behaviour.
  //
  [[deprecated(
      "Superseded by the overloads with a release_resource option.")]] void
  Run(Task&& task) override {
    Run(std::move(task), false);
  }
  void Run(Task&& task, bool release_resource) override;

  // Submit a batch of tasks for execution in the thread pool.
  // This is *blocking call*.
  // It will not return until all tasks are *completed*.
  //
  // `release_resource` indicates whether to release a thread after no more
  // parallelism is required. For example, if the TaskRunner is assigned
  // 2 cores and `release_resources` flag is set, completing the second-last
  // task will recycle 1 core for others to use, and completing the last
  // task will recycle the final available core.
  //
  // If `release_resource` is set, the available parallelism will be 0 after
  // this call returns.
  //
  // Note: if multiple Run() is called from different threads, it will result
  // in undefined behaviour.
  void Run(const std::vector<Task>& tasks, bool release_resource) override;

  // Get the current parallelism limit.
  size_t GetParallelism() const override;

  /*********************************************************
   * Implement Schedulable interfaces.
   ********************************************************/

  // Increase the limit on parallelism by `delta`.
  // Return the maximum parallelism after the change.
  //
  // The call will return immediately.
  size_t IncreaseParallelism(size_t delta) override;

  // Decrement the limit on parallelism by 1.
  // Return the maximum parallelism after the change. If the maximum parallelism
  // is 0 before the call, it will return -1.
  //
  // The call will block until its parallelism is reduced successfully.
  // This is achieved on a best-effort basis. It will *try* to recycle
  // computing resource once it is released by task completion
  // by *not* replenishing the semaphore.
  size_t DecrementParallelism() override;

  // Return the const reference to the metadata object.
  const Schedulable::Metadata& metadata() const override;

  // Get the current parallelism limit.
  size_t AllocatedParallelism() const override;

  // Return a mutable pointer to the internal metadata object.
  Schedulable::Metadata* mutable_metadata();

 private:
  // A helper function for Run() implementation.
  std::vector<size_t> PackagedTaskIndices(size_t total_tasks) const;

 private:
  Scheduler<Throttle>* scheduler_;

  TaskRunner* downstream_;

  Schedulable::Metadata metadata_;

  folly::NativeSemaphore sem_;

  // Required for restoring the original semaphore counts. Otherwise, the
  // program will result in an error on semaphore destruction.
  const size_t original_parallelism_;

  // Used to track the allowed parallelism, and to identify illegal operations,
  // e.g., reducing the maximum parallelism below 0.
  std::atomic_size_t allocated_parallelism_;
};

// A companion factory class for Throttle.
class ThrottleFactory final : public SchedulableFactory<Throttle> {
 public:
  ThrottleFactory(Scheduler<Throttle>* scheduler, TaskRunner* downstream);

  // Create a new instance of Throttle, given the initial parallelism
  // and metadata.
  std::unique_ptr<Throttle> New(
      size_t initial_parallelism,
      Schedulable::Metadata&& metadata) const override;

 private:
  Scheduler<Throttle>* scheduler_;

  TaskRunner* downstream_;

  mutable std::atomic<Schedulable::ID_Type> next_id_;
};

}  // namespace executors
}  // namespace minigraph

#endif  // MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_
