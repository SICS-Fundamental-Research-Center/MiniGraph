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
  static_assert(std::is_base_of_v<Schedulable, Schedulable_T>,
                "Scheduler's template type must be a Schedulable.");

  // A default virtual destructor to avoid warning when this interface class
  // is used directly in the code.
  virtual ~Scheduler() = default;

  // Interface for creating a new Schedulable and allocating resources on
  // creation. The initialization is done via the companion factory class
  // of the Schedulable.
  // Return a unique pointer to the new Schedulable instance, transferring
  // ownership to the caller. In addition, Scheduler will keep a raw
  // pointer to the created instance, in case the initially allocated resources
  // need to be adjusted.
  virtual std::unique_ptr<Schedulable_T> AllocateNew(
      const SchedulableFactory<Schedulable_T>* factory,
      Schedulable::Metadata&& metadata) = 0;

  virtual std::unique_ptr<Schedulable_T> AllocateNew(
      const SchedulableFactory<Schedulable_T>* factory,
      Schedulable::Metadata&& metadata, const size_t init_parallelism) = 0;

  // Call to release one allocated thread in recycler to Scheduler.
  virtual void RecycleOneThread(Schedulable_T* recycler) = 0;

  // Call to release all allocated threads in recycler to Scheduler.
  virtual void RecycleAllThreads(Schedulable_T* recycler) = 0;

 protected:
  Schedulable::Metadata metadata_;

  // Allow external call to Remove() via Schedulable_T's destructor only.
  friend Schedulable_T::~Schedulable_T();
  // Interface for removing a previously created Schedulable instance.
  // If it has not released its allocated resources, release it before deletion.
  virtual void Remove(Schedulable_T* schedulable) = 0;
};

} // namespace executors
} // namespace minigraph

#endif //MINIGRAPH_EXECUTORS_SCHEDULER_H_
