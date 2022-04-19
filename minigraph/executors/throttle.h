#ifndef MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_
#define MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_

#include "executors/schedulable.h"
#include "executors/task_runner.h"
#include <folly/synchronization/NativeSemaphore.h>
#include <mutex>
#include <stdexcept>

namespace minigraph {
namespace executors {

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
class Throttle : public TaskRunner, public Schedulable {
 public:
  // As an intermediate TaskRunner, it must be constructed by providing
  // a downstream TaskRunner.
  explicit Throttle(TaskRunner* downstream, size_t max_parallelism);

  // A non-default destructor is required to restore semaphore to its original
  // status before destruction. Otherwise, the destructor might raise a
  // EXC_BAD_INSTRUCTION fault on macOS.
  virtual ~Throttle();

  // Run the task.
  // The call will *block* if its parallelism has been saturated, until
  // previously submitted tasks have been completed.
  void Run(Task&& task) override;

  // Get the current parallelism limit.
  size_t RunParallelism() override;

  // Increase the limit on parallelism by `delta`.
  // Return the maximum parallelism after the change.
  //
  // The call will return immediately.
  int IncreaseParallelism(size_t delta) override;

  // Decrement the limit on parallelism by 1.
  // Return the maximum parallelism after the change. If the maximum parallelism
  // is 0 before the call, it will return -1.
  //
  // The call will block until its parallelism is reduced successfully.
  // This is achieved on a best-effort basis. It will *try* to recycle
  // computing resource once it is released by task completion
  // by *not* replenishing the semaphore.
  int DecrementParallelism() override;

  // Return the const reference to the metadata object.
  const Schedulable::Metadata& metadata() const override;

  // Get the current parallelism limit.
  size_t AllocatedParallelism() override;

  // Return a mutable pointer to the internal metadata object.
  Schedulable::Metadata* mutable_metadata();

 private:
  TaskRunner* downstream_;

  Schedulable::Metadata metadata_;

  folly::NativeSemaphore sem_;

  // Required for restoring the original semaphore counts. Otherwise, the
  // program will result in an error on semaphore destruction.
  const size_t original_parallelism_;

  // Used to track the allowed parallelism, and to identify illegal operations,
  // e.g., reducing the maximum parallelism below 0.
  size_t max_parallelism_;
  // Mutex to protected concurrent access to max_parallelism. Since the variable
  // is only accessed when parallelism is adjusted on-the-fly, which is very
  // rare in practice and contentions are *extremely* low, using a mutex is
  // good enough.
  std::mutex mtx_;
};

// A companion factory class for Throttle.
class ThrottleFactory : public SchedulableFactory<Throttle> {
 public:
  explicit ThrottleFactory(TaskRunner* downstream);

  // Create a new instance of Throttle, given the initial parallelism
  // and metadata.
  std::unique_ptr<Throttle> New(
      size_t initial_parallelism,
      Schedulable::Metadata&& metadata) const override;

 private:
  TaskRunner* downstream_;
};

}  // namespace executors
}  // namespace minigraph

#endif  // MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_
