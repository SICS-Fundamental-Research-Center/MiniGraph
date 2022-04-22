#ifndef MINIGRAPH_EXECUTORS_TASK_RUNNER_H_
#define MINIGRAPH_EXECUTORS_TASK_RUNNER_H_

#include <functional>
#include <vector>


namespace minigraph {
namespace executors {

// Alias type name for an executable function.
typedef std::function<void()> Task;

// An interface class, which features a `Run` function that accepts a `Task`
// and run it.
class TaskRunner {
 public:
  // Submit a task for execution.
  // This call may or may *not* block until task is completed. Check for the
  // concrete implementation for more details.
  [[deprecated("Superseded by the overloads with a release_resource option.")]]
  virtual void Run(Task&& task) = 0;
  virtual void Run(Task&& task, bool release_resource) = 0;

  // Submit a batch of tasks for execution.
  // This call may or may *not* block until task is completed. Check for the
  // concrete implementation for more details.
  virtual void Run(const std::vector<Task>& tasks, bool release_resource) = 0;

  // Get the current parallelism for running tasks if a bunch of tasks are
  // submitted via Run().
  //
  // It is useful for task submitters to estimate the currently allowed
  // concurrency, and to batch tiny tasks into larger serial ones while
  // making no compromise on utilization of allocated resources.
  [[nodiscard]]
  virtual size_t GetParallelism() const = 0;
};

}  // namespace executors
}  // namespace minigraph

#endif  // MINIGRAPH_EXECUTORS_TASK_RUNNER_H_
