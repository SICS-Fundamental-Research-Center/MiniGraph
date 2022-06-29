#include "executors/scheduled_executor.h"
#include "executors/cpu_scheduler.h"
#include "utility/logging.h"

namespace minigraph {
namespace executors {

ScheduledExecutor::ThreadPool::ThreadPool(unsigned int num_threads)
    : internal_pool_(num_threads) {}

void ScheduledExecutor::ThreadPool::Run(Task&& task, bool /*flag*/) {
  internal_pool_.add(std::move(task));
}

void ScheduledExecutor::ThreadPool::Run(const std::vector<Task>& tasks,
                                        bool /*release_resource*/) {
  for (const auto& t : tasks) {
    internal_pool_.add(t);
  }
}

size_t ScheduledExecutor::ThreadPool::GetParallelism() const {
  return internal_pool_.numThreads();
}

void ScheduledExecutor::ThreadPool::StopAndJoin() {
  internal_pool_.stop();
  internal_pool_.join();
}

ScheduledExecutor::ScheduledExecutor(unsigned int num_threads)
    : scheduler_(std::make_unique<CPUScheduler>(num_threads)),
      thread_pool_(num_threads),
      factory_(scheduler_.get(), &thread_pool_) {
  std::lock_guard<std::mutex> grd(map_mtx_);
  throttles_.reserve(1024);
}

TaskRunner* ScheduledExecutor::RequestTaskRunner(
    Schedulable::Metadata&& metadata) {
  std::lock_guard<std::mutex> grd(map_mtx_);
  ThrottlePtr throttle = scheduler_->AllocateNew(
      &factory_, std::forward<Schedulable::Metadata>(metadata));
  Schedulable::ID_Type id = throttle->id();
  auto result = throttles_.emplace(throttle->id(), std::move(throttle));
  if (!result.second) {
    LOGF_ERROR("TaskRunner creation did not happen, with throttle id: {}.", id);
    return nullptr;
  }
  return result.first->second.get();
}

TaskRunner* ScheduledExecutor::RequestTaskRunner(
    Schedulable::Metadata&& metadata, const size_t init_parallelism) {
  std::lock_guard<std::mutex> grd(map_mtx_);
  ThrottlePtr throttle = scheduler_->AllocateNew(
      &factory_, std::forward<Schedulable::Metadata>(metadata),
      init_parallelism);
  Schedulable::ID_Type id = throttle->id();
  auto result = throttles_.emplace(throttle->id(), std::move(throttle));
  if (!result.second) {
    LOGF_ERROR("TaskRunner creation did not happen, with throttle id: {}.", id);
    return nullptr;
  }
  return result.first->second.get();
}

void ScheduledExecutor::RecycleTaskRunner(TaskRunner* runner) {
  ThrottlePtr throttle = nullptr;
  Schedulable::ID_Type id = dynamic_cast<Throttle*>(runner)->id();
  std::lock_guard<std::mutex> grd(map_mtx_);
  if (throttles_.find(id) == throttles_.end()) {
    LOG_ERROR("Trying to recycle TaskRunner whose ID is unknown.");
    return;
  }
  throttles_.erase(id);
  //  throttle will destruct here, and get removed from Scheduler automatically.
}

void ScheduledExecutor::Stop() { thread_pool_.StopAndJoin(); }

}  // namespace executors
}  // namespace minigraph
