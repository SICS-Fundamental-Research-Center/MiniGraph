#include "executors/scheduled_executor.h"

#include "executors/cpu_scheduler.h"
#include "utility/logging.h"


namespace minigraph {
namespace executors {

ScheduledExecutor::ThreadPool::ThreadPool(unsigned int num_threads) :
    internal_pool_(num_threads) {}

void ScheduledExecutor::ThreadPool::Run(Task&& task) {
  internal_pool_.add(task);
}

size_t ScheduledExecutor::ThreadPool::RunParallelism() {
  return internal_pool_.numThreads();
}

void ScheduledExecutor::ThreadPool::StopAndJoin() {
  internal_pool_.stop();
  internal_pool_.join();
}

ScheduledExecutor::ScheduledExecutor(unsigned int num_threads) :
    thread_pool_(num_threads),
    factory_(&thread_pool_),
    scheduler_(std::make_unique<CPUScheduler>(num_threads)) {
  throttles_.lock()->reserve(128);
}

TaskRunner* ScheduledExecutor::RequestTaskRunner(
    void* user_ptr,
    Schedulable::Metadata&& metadata) {
  if (user_ptr == nullptr) {
    LOG_ERROR("RequestTaskRunner() called with a nullptr key.");
  }

  auto throttles = throttles_.lock();
  throttles->emplace(
      user_ptr,
      scheduler_->AllocateNew(&factory_,
                              std::forward<Schedulable::Metadata>(metadata)));
  return throttles->at(user_ptr).get();
}

void ScheduledExecutor::RecycleTaskRunner(void* user_ptr, TaskRunner* runner) {
  ThrottlePtr throttle = nullptr;
  {
    auto throttles = throttles_.lock();
    if (throttles->find(user_ptr) == throttles->end()) {
      LOG_ERROR("Trying to recycle TaskRunner with an unknown user_ptr.");
      return;
    }
    throttles->at(user_ptr).swap(throttle);
    throttles->erase(user_ptr);
  }
  scheduler_->Remove(throttle.get());
  if (runner != (TaskRunner*) throttle.get()) {
    LOG_ERROR("user_ptr does not match with the provided runner.");
  }
}

void ScheduledExecutor::Stop() {
  thread_pool_.StopAndJoin();
}

} // executors
} // minigraph
