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
  // Typedef for its identifier.
  using ID_Type = int;

  // Metadata information for effective scheduling.
  struct Metadata {
    unsigned priority = 1;
    unsigned parallelism = 1;

    //TODO: add more fields to allow scheduler to do some strategic work.
  };

 public:
  explicit Schedulable(ID_Type id) : id_(id) {}
  virtual ~Schedulable() = default;

  // Increase the limit on parallelism by `delta`.
  // Return the maximum parallelism after the change.
  virtual size_t IncreaseParallelism(size_t delta) = 0;

  // Decrement the limit on parallelism by 1.
  // Return the maximum parallelism after the change. If the maximum parallelism
  // is 0 before the call, it will return -1.
  virtual size_t DecrementParallelism() = 0;

  // Return the const reference to the metadata object.
  [[nodiscard]]
  virtual const Metadata& metadata() const = 0;

  // Get the current parallelism limit.
  [[nodiscard]]
  virtual size_t AllocatedParallelism() const = 0;

  // Get identifier associated to the object.
  [[nodiscard]]
  ID_Type id() const {
    return id_;
  }

 protected:
  // Identifier.
  const ID_Type id_;
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
