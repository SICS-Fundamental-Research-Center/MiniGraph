
#ifndef MINIGRAPH_COMPONENT_BASE_H
#define MINIGRAPH_COMPONENT_BASE_H

#include <atomic>
#include <memory>

#include <folly/AtomicHashMap.h>

#include "utility/logging.h"
#include "utility/state_machine.h"
#include "utility/thread_pool.h"

namespace minigraph {
namespace components {

template <typename GID_T>
class ComponentBase {
 public:
  ComponentBase<GID_T>(
      utility::EDFThreadPool* thread_pool,
      folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine) {
    thread_pool_ = thread_pool;
    superstep_by_gid_ = superstep_by_gid;
    global_superstep_ = global_superstep;
    state_machine_ = state_machine;
    global_superstep_mtx_ = new std::mutex;
    super_step_by_gid_mtx_ = new std::mutex;
  }
  ~ComponentBase() = default;

  virtual void Run() = 0;
  virtual void Stop() = 0;

  size_t get_superstep_via_gid(const GID_T& gid) const {
    this->super_step_by_gid_mtx_->lock();
    auto iter = superstep_by_gid_->find(gid);
    if (iter == superstep_by_gid_->end()) {
      XLOG(INFO, "get_super_via_gid Error: ", "gid not found.");
      this->super_step_by_gid_mtx_->unlock();
      return -1;
    } else {
      auto step = iter->second->load();
      this->super_step_by_gid_mtx_->unlock();
      return step;
    }
  };

  void add_superstep_via_gid(const GID_T& gid, const size_t val = 1) {
    this->super_step_by_gid_mtx_->lock();
    auto iter = superstep_by_gid_->find(gid);
    if (iter == superstep_by_gid_->end()) {
      LOG_ERROR("get_super_via_gid Error: ", "gid not found.");
      this->super_step_by_gid_mtx_->unlock();
      return;
    } else {
      iter->second->store(iter->second->load() + val);
    }
    this->super_step_by_gid_mtx_->unlock();
  }

  GID_T get_slowest_gid() {
    this->super_step_by_gid_mtx_->lock();
    size_t step = UINT64_MAX;
    GID_T gid = MINIGRAPH_GID_MAX;
    for (auto& iter : *superstep_by_gid_) {
      auto tmp_step = iter.second->load();
      tmp_step < step ? step = tmp_step, gid = iter.first : 0;
    }
    this->super_step_by_gid_mtx_->unlock();
    return gid;
  }

  size_t get_global_superstep() {
    global_superstep_mtx_->lock();
    auto global_superstep = global_superstep_->load();
    global_superstep_mtx_->unlock();
    return global_superstep;
  };

  void add_global_superstep() {
    global_superstep_mtx_->lock();
    global_superstep_->fetch_add(1);
    global_superstep_mtx_->unlock();
  }

 protected:
  // contral switch.
  std::atomic<bool> switch_ = true;

  // thread pool.
  utility::EDFThreadPool* thread_pool_ = nullptr;

  // superstep
  folly::AtomicHashMap<GID_T, std::atomic<size_t>*>* superstep_by_gid_ =
      nullptr;
  std::atomic<size_t>* global_superstep_ = nullptr;

  // state machine.
  utility::StateMachine<GID_T>* state_machine_ = nullptr;

 private:
  std::mutex *super_step_by_gid_mtx_;
  std::mutex *global_superstep_mtx_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H