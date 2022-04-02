#include "executors/throttle.h"

#include <utility>


namespace minigraph {
namespace executors {


Throttle::Throttle(TaskRunner* downstream, size_t max_parallelism) :
    downstream_(downstream),
    metadata_(),
    sem_(max_parallelism),
    original_parallelism_(max_parallelism),
    max_parallelism_(max_parallelism) {}

Throttle::~Throttle() {
  std::lock_guard<std::mutex> grd(mtx_);
  for (int i = 0;
       original_parallelism_ > max_parallelism_ &&
       i < original_parallelism_ - max_parallelism_;
       i++) {
    sem_.post();
  }
  for (int i = 0;
       original_parallelism_ < max_parallelism_ &&
       i < max_parallelism_ - original_parallelism_;
       i++) {
    sem_.wait();
  }
}

void Throttle::Run(Task&& task) {
  sem_.wait();
  downstream_->Run([t = std::move(task), this] {
    t();
    sem_.post();
  });
}

int Throttle::IncreaseParallelism(size_t delta) {
  size_t after;
  {
    // Change `max_parallelism` before actually increasing semaphore counts.
    std::lock_guard<std::mutex> grd(mtx_);
    max_parallelism_ += delta;
    after = max_parallelism_;
  }
  for (size_t i = 0; i < delta; i++) {
    sem_.post();
  }
  return (int) after;
}

int Throttle::DecrementParallelism() {
  size_t after;
  {
    // Change `max_parallelism` before actually decrement semaphore counts.
    std::lock_guard<std::mutex> grd(mtx_);
    if (max_parallelism_ == 0) {
      return -1;
    }
    after = --max_parallelism_;
  }
  sem_.wait();
  return (int) after;
}

const Schedulable::Metadata& Throttle::metadata() const {
  return metadata_;
}

Schedulable::Metadata* Throttle::mutable_metadata() {
  return &metadata_;
}

size_t Throttle::parallelism() {
  std::lock_guard<std::mutex> grd(mtx_);
  return max_parallelism_;
}

ThrottleFactory::ThrottleFactory(TaskRunner *downstream) :
    downstream_(downstream) {}

std::unique_ptr<Throttle> ThrottleFactory::New(
    size_t initial_parallelism,
    Schedulable::Metadata&& metadata) {
  auto instance = std::make_unique<Throttle>(
      downstream_, initial_parallelism);
  std::swap(*(instance->mutable_metadata()), metadata);
  return instance;
}

} // namespace executors
} // namespace minigraph
