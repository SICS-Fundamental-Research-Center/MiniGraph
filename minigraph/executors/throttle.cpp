#include "executors/throttle.h"


namespace minigraph {
namespace executors {


Throttle::Throttle(TaskProcessor* downstream, size_t max_parallelism) :
    downstream_(downstream),
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

} // namespace executors
} // namespace minigraph