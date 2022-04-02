#ifndef MINIGRAPH_EXECUTORS_SCHEDULER_H_
#define MINIGRAPH_EXECUTORS_SCHEDULER_H_

#include "executors/schedulable.h"


namespace minigraph {
namespace executors {

// An interface class for a Scheduler that can allocate CPU resources among
// all `allocated` instances of type `Schedulable_T`.
//
// Note: `Schedulable_T` must be a subtype of `Schedulable`. The compiler will
//       check this and report a compilation error if not met.
template <typename Schedulable_T>
class Scheduler {
 public:
  // Interface for creating a new Schedulable and allocating resources on
  // creation. The initialization is done via the companion factory class
  // of the Schedulable.
  // Return a unique pointer to the new Schedulable instance, transferring
  // ownership to the caller. In addition, Scheduler typically keep a raw
  // pointer to the created instance, in case `RescheduleAll()` is called and
  // initially allocated resources need to be adjusted.
  virtual std::unique_ptr<Schedulable_T> AllocateNew(
      SchedulableFactory<Schedulable_T>* factory) = 0;

  // Interface for removing a previously created Schedulable instance, by
  // releasing its allocated resources.
  virtual void Remove(Schedulable_T* schedulable) = 0;

  // Interface for adjust all existing Schedulable instances, by rescheduling
  // the resources among them.
  virtual void RescheduleAll() = 0;
};

} // namespace executors
} // namespace minigraph

#endif //MINIGRAPH_EXECUTORS_SCHEDULER_H_
