#ifndef MINIGRAPH_EXECUTORS_SCHEDULABLE_H_
#define MINIGRAPH_EXECUTORS_SCHEDULABLE_H_

#include <cstddef>
#include <type_traits>
#include <memory>


namespace minigraph {
namespace executors {

// An interface class for a component whose parallelism can be adjusted
// at runtime.
class Schedulable {
 public:
  // Metadata information for effective scheduling.
  struct Metadata {
    int priority;
    //TODO: add more fields to allow scheduler to do some strategic work.
  };

  // Increase the limit on parallelism by `delta`.
  // Return the maximum parallelism after the change.
  virtual int IncreaseParallelism(size_t delta) = 0;

  // Decrement the limit on parallelism by 1.
  // Return the maximum parallelism after the change. If the maximum parallelism
  // is 0 before the call, it will return -1.
  virtual int DecrementParallelism() = 0;

  // Return the const reference to the metadata object.
  virtual const Metadata& metadata() const = 0;

  // Get the current parallelism limit.
  virtual size_t AllocatedParallelism() = 0;
};

// A companion factory class for Schedulable.
template <typename Schedulable_T>
class SchedulableFactory {
 public:
  static_assert(std::is_base_of_v<Schedulable, Schedulable_T>,
                "SchedulableFactory's template type must be a Schedulable.");

  // Create a new instance of Schedulable_T.
  virtual std::unique_ptr<Schedulable_T> New(
      size_t initial_parallelism,
      Schedulable::Metadata&& metadata) const = 0;
};

} // namespace executors
} // namespace minigraph

#endif //MINIGRAPH_EXECUTORS_SCHEDULABLE_H_
