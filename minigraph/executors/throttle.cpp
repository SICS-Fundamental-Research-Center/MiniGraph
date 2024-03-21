#include <thread>
#include <utility>

#include "executors/throttle.h"
#include "executors/scheduler.h"
#include "utility/logging.h"


namespace minigraph {
namespace executors {

Throttle::Throttle(ID_Type id, Scheduler<Throttle>* scheduler,
                   TaskRunner* downstream, size_t max_parallelism)
    : Schedulable(id),
      scheduler_(scheduler),
      downstream_(downstream),
      metadata_(),
      sem_(max_parallelism),
      original_parallelism_(max_parallelism),
      allocated_parallelism_(max_parallelism) {}

Throttle::~Throttle() {
  // Recycle resources, if it has not done so.
  const size_t parallelism = allocated_parallelism_.load();
  if (parallelism > 0) scheduler_->RecycleAllThreads(this);
  scheduler_->Remove(this);

  // Reset sem_ status to its initial state, to avoid a destruction error.
  for (size_t i = 0; original_parallelism_ > 0 && i < original_parallelism_;
       i++) {
    sem_.post();
    // NOTE...
  }
}

void Throttle::Run(Task&& task, bool release_resource) {
  std::mutex mtx;
  bool task_completed = false;
  std::condition_variable finish_cv;

  sem_.wait();
  downstream_->Run(
      [this, &task_completed, &finish_cv, &mtx, t = std::move(task)]() {
        t();
        sem_.post();
        std::lock_guard grd(mtx);
        task_completed = true;
        finish_cv.notify_one();
      },
      false);

  std::unique_lock<std::mutex> lck(mtx);
  while (!task_completed) {
    finish_cv.wait(lck);
  }

  if (release_resource) scheduler_->RecycleOneThread(this);
}

void Throttle::Run(const std::vector<Task>& tasks, bool release_resource) {
  const std::vector<size_t> indices = PackagedTaskIndices(tasks.size());
  const size_t num_packages = indices.size() - 1;
  std::mutex mtx;
  std::atomic<size_t> pending_packages(num_packages);
  std::condition_variable finish_cv;
  for (size_t i = num_packages; i > 0; i--) {
    sem_.wait();
    downstream_->Run(
        [this, &indices, &tasks, &mtx, &pending_packages, &finish_cv,
         index = i]() {
          for (size_t j = indices[index - 1]; j < indices[index]; j++)
            tasks[j]();

          std::lock_guard grd(mtx);
          sem_.post();
          if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
        },
        false);
  }

  std::unique_lock<std::mutex> lck(mtx);
  finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
  //   Final cleaning.
  if (release_resource && GetParallelism() > 0) {
    scheduler_->RecycleAllThreads(this);
  }
}

// void Throttle::Run(const std::vector<Task>& tasks, bool release_resource) {
//   const std::vector<size_t> indices = PackagedTaskIndices(tasks.size());
//   const size_t num_packages = tasks.size();
//   std::mutex mtx;
//   std::atomic<size_t> pending_packages(num_packages);
//   std::condition_variable finish_cv;
//
//   for (size_t i = 0; i < tasks.size(); i++) {
//      sem_.wait();
//     downstream_->Run(
//         [this, &tasks, &mtx, &pending_packages, &finish_cv, index = i]() {
//           tasks[index]();
//           std::lock_guard grd(mtx);
//            sem_.post();
//           if (pending_packages.fetch_sub(1) == 1) finish_cv.notify_all();
//         },
//         false);
//   }
//
//   std::unique_lock<std::mutex> lck(mtx);
//   finish_cv.wait(lck, [&] { return pending_packages.load() == 0; });
//   //   Final cleaning.
//   if (release_resource && GetParallelism() > 0) {
//     scheduler_->RecycleAllThreads(this);
//   }
// }

std::vector<size_t> Throttle::PackagedTaskIndices(size_t total_tasks) const {
  const size_t min_parallelism = 4;
  const size_t intended_batches = 4;
  const size_t parallelism = std::max(GetParallelism(), min_parallelism);
  const size_t num_packages =
      std::min(parallelism * intended_batches, total_tasks);
  const float step_size =
      (float)total_tasks / (float)num_packages;  // step >= 1.0

  std::vector<size_t> indices(num_packages + 1);
  indices[0] = 0;
  indices[num_packages] = total_tasks;
  for (size_t step_num = 1; step_num < num_packages; step_num++) {
    indices[step_num] = (size_t)(step_size * (float)step_num);
  }
  return indices;
}

size_t Throttle::GetParallelism() const { return AllocatedParallelism(); }

size_t Throttle::IncreaseParallelism(size_t delta) {
  // Change `allocated_parallelism_` before actually increasing
  // semaphore counts.
  const size_t before = allocated_parallelism_.fetch_add(delta);
  for (size_t i = 0; i < delta; i++) {
    sem_.post();
  }
  return before + delta;
}

size_t Throttle::DecrementParallelism() {
  // Change `allocated_parallelism_` before actually decrement semaphore counts.
  const size_t before = allocated_parallelism_.fetch_sub(1);
  if (before <= 0) {
    allocated_parallelism_++;
  } else {
    sem_.wait();
  }
  return before - 1;
}

const Schedulable::Metadata& Throttle::metadata() const { return metadata_; }

Schedulable::Metadata* Throttle::mutable_metadata() { return &metadata_; }

size_t Throttle::AllocatedParallelism() const {
  return allocated_parallelism_.load();
}

ThrottleFactory::ThrottleFactory(Scheduler<Throttle>* scheduler,
                                 TaskRunner* downstream)
    : scheduler_(scheduler), downstream_(downstream), next_id_(0) {}

std::unique_ptr<Throttle> ThrottleFactory::New(
    size_t initial_parallelism, Schedulable::Metadata&& metadata) const {
  auto instance = std::make_unique<Throttle>(next_id_++, scheduler_,
                                             downstream_, initial_parallelism);
  std::swap(*(instance->mutable_metadata()), metadata);
  return instance;
}

}  // namespace executors
}  // namespace minigraph
