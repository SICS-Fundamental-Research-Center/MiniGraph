#ifndef MINIGRAPH_EXECUTORS_TASK_PROCESSOR_H_
#define MINIGRAPH_EXECUTORS_TASK_PROCESSOR_H_

#include <functional>


namespace minigraph {
namespace executors {

// Alias type name for an executable function.
typedef std::function<void ()> Task;

// An interface class, which features a `Run` function that accepts a `Task`
// and run it.
class TaskProcessor {
 public:
  // Start executing a task.
  //
  // The call will *block* until executor capacity is available.
  // Returning from the call means the `task` is being scheduled and
  // (probably) running, but not necessarily completed.
  // As a result, the client code is responsible for checking the termination
  // of tasks.
  virtual void Run(Task&& task) = 0;
};

} // namespace executors
} // namespace minigraph

#endif //MINIGRAPH_EXECUTORS_TASK_PROCESSOR_H_
