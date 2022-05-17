
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
  }
  ~ComponentBase() = default;

  virtual void Run() = 0;
  virtual void Stop() = 0;

  size_t get_superstep_via_gid(const GID_T& gid) const {
    auto iter = superstep_by_gid_->find(gid);
    if (iter == superstep_by_gid_->end()) {
      XLOG(INFO, "get_super_via_gid Error: ", "gid not found.");
      return -1;
    } else {
      return iter->second->load();
    }
  };

  void add_superstep_via_gid(const GID_T& gid, const size_t val = 1) {
    auto iter = superstep_by_gid_->find(gid);
    if (iter == superstep_by_gid_->end()) {
      LOG_ERROR("get_super_via_gid Error: ", "gid not found.");
      return;
    } else {
      iter->second->store(iter->second->load() + val);
    }
  }

  size_t get_global_superstep() { return global_superstep_->load(); };

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
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H