#ifndef MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_
#define MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_

#include "executors/task_runner.h"

#include <mutex>

#include <folly/synchronization/NativeSemaphore.h>


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
class Throttle : public TaskRunner {
 public:
  // As an intermediate TaskRunner, it must be constructed by providing
  // a downstream TaskRunner.
  explicit Throttle(TaskRunner* downstream, size_t max_parallelism);

  // A non-default destructor is required to restore semaphore to its original
  // status before destruction. Otherwise, the destructor might raise a
  // EXC_BAD_INSTRUCTION fault on macOS.
  ~Throttle();

  // Run the task.
  // The call will *block* if its parallelism has been saturated, until
  // previously submitted tasks have been completed.
  void Run(Task&& task) override;

  // Increase the limit on parallelism by `delta`.
  // Return the maximum parallelism after the change.
  //
  // The call will return immediately.
  int IncreaseParallelism(size_t delta);

  // Decrement the limit on parallelism by 1.
  // Return the maximum parallelism after the change. If the maximum parallelism
  // is 0 before the call, it will return -1.
  //
  // The call will block until its parallelism is reduced successfully.
  // This is achieved on a best-effort basis. It will *try* to recycle
  // computing resource once it is released by task completion
  // by *not* replenishing the semaphore.
  int DecrementParallelism();

 private:
  TaskRunner* downstream_;

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

} // namespace executors
} // namespace minigraph


#endif //MINIGRAPH_MINIGRAPH_EXECUTORS_THROTTLE_H_
