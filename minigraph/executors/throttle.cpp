#include "executors/throttle.h"

#include <utility>

#include "executors/scheduler.h"


namespace minigraph {
namespace executors {

Throttle::Throttle(Scheduler<Throttle>* scheduler,
                   TaskRunner* downstream,
                   size_t max_parallelism) :
    scheduler_(scheduler),
    downstream_(downstream),
    metadata_(),
    sem_(max_parallelism),
    original_parallelism_(max_parallelism),
    max_parallelism_(max_parallelism) {}

Throttle::~Throttle() {
  // Recycle resources, if it has not done so.
  if (max_parallelism_ > 0) scheduler_->RecycleAllThreads(this);
  scheduler_->Remove(this);

  // Reset sem_ status to its initial state, to avoid a destruction error.
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

void Throttle::Run(Task&& task, bool release_resource) {
  std::mutex mtx;
  bool task_completed = false;
  std::condition_variable finish_cv;

  sem_.wait();
  downstream_->Run([&, t = std::move(task)] {
    t();
    sem_.post();
    std::lock_guard grd(mtx);
    task_completed = true;
    finish_cv.notify_one();
  }, false);

  std::unique_lock<std::mutex> lck(mtx);
  while (!task_completed) {
    finish_cv.wait(lck);
  }

  if (release_resource) scheduler_->RecycleOneThread(this);
}

void Throttle::Run(const std::vector<Task>& tasks,
                   bool release_resource) {
  const std::vector<size_t> indices = PackagedTaskIndices(tasks.size());
  const size_t num_packages = indices.size() - 1;

  std::mutex mtx;
  size_t pending_packages = num_packages;
  size_t parallelism = GetParallelism();
  std::condition_variable finish_cv;

  for (size_t i = num_packages; i > 0; i--) {
    sem_.wait();
    downstream_->Run([&] {
      for (size_t j = indices[i-1]; j < indices[i]; j++) {
        tasks[j]();
      }

      // Modify the counter of `pending_packages`.
      bool should_notify;
      {
        std::lock_guard grd(mtx);
        pending_packages--;
        should_notify = (release_resource && pending_packages < parallelism)
            || (!release_resource && pending_packages == 0);
      }
      sem_.post();
      if (should_notify) {
        finish_cv.notify_one();
      }
    }, false);
  }

  std::unique_lock<std::mutex> lck(mtx);
  while (true) {
    finish_cv.wait(lck);
    // Here, cv is waked up after all task packages have been pushed downstream.
    if (release_resource) {
      while (pending_packages < parallelism) {
        parallelism--;
        scheduler_->RecycleOneThread(this);
      }
    }
    if (pending_packages == 0) return;
  }
}

std::vector<size_t> Throttle::PackagedTaskIndices(size_t total_tasks) const {
  const size_t intended_batches = 4;
  const size_t parallelism = GetParallelism();
  const size_t num_packages =
      std::min(parallelism * intended_batches, total_tasks);
  const float step_size =
      (float) total_tasks / (float) num_packages; // step >= 1.0

  std::vector<size_t> indices(num_packages + 1);
  indices[0] = 0;
  indices[num_packages] = total_tasks;
  for (size_t step_num = 1; step_num < num_packages; step_num++) {
    indices[step_num] = (size_t) (step_size * (float) step_num);
  }
  return indices;
}

size_t Throttle::GetParallelism() const {
  return AllocatedParallelism();
}

size_t Throttle::IncreaseParallelism(size_t delta) {
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
  return after;
}

size_t Throttle::DecrementParallelism() {
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
  return after;
}

const Schedulable::Metadata& Throttle::metadata() const {
  return metadata_;
}

Schedulable::Metadata* Throttle::mutable_metadata() {
  return &metadata_;
}

size_t Throttle::AllocatedParallelism() const {
  std::lock_guard<std::mutex> grd(mtx_);
  return max_parallelism_;
}

ThrottleFactory::ThrottleFactory(Scheduler<Throttle>* scheduler,
                                 TaskRunner* downstream) :
    scheduler_(scheduler),
    downstream_(downstream) {}

std::unique_ptr<Throttle> ThrottleFactory::New(
    size_t initial_parallelism,
    Schedulable::Metadata&& metadata) const {
  auto instance = std::make_unique<Throttle>(
      scheduler_, downstream_, initial_parallelism);
  std::swap(*(instance->mutable_metadata()), metadata);
  return instance;
}

} // namespace executors
} // namespace minigraph
