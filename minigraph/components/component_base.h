
#ifndef MINIGRAPH_COMPONENT_BASE_H
#define MINIGRAPH_COMPONENT_BASE_H

#include "utility/logging.h"
#include "utility/state_machine.h"
#include "utility/thread_pool.h"
#include <folly/AtomicHashMap.h>
#include <atomic>
#include <memory>

namespace minigraph {
namespace components {
template <typename GID_T>
class ComponentBase {
 public:
  ComponentBase<GID_T>(
      utility::CPUThreadPool* cpu_thread_pool,
      utility::IOThreadPool* io_thread_pool,
      folly::AtomicHashMap<
          GID_T, std::shared_ptr<std::atomic<size_t>>, std::hash<int64_t>,
          std::equal_to<int64_t>, std::allocator<char>,
          folly::AtomicHashArrayQuadraticProbeFcn>* superstep_by_gid,
      std::atomic<size_t>* global_superstep,
      utility::StateMachine<GID_T>* state_machine) {
    cpu_thread_pool_ = cpu_thread_pool;
    io_thread_pool_ = io_thread_pool;
    superstep_by_gid_ = superstep_by_gid;
    global_superstep_ = global_superstep;
    state_machine_ = state_machine;
    // switch_ = std::atomic<bool>(true);
  }

  virtual void Run() = 0;
  virtual void Stop() = 0;

  size_t get_superstep_via_gid(GID_T gid) {
    auto iter = superstep_by_gid_->load()->find(gid);
    if (iter == superstep_by_gid_->load()->end()) {
      XLOG(INFO, "get_super_via_gid Error: ", "gid not found.");
      return -1;
    } else {
      return iter.second;
    }
  };

  size_t get_global_superstep() { return global_superstep_->load(); };

 protected:
  // contral switch.
  std::atomic<bool> switch_ = true;

  // thread pool.
  utility::IOThreadPool* io_thread_pool_ = nullptr;
  utility::CPUThreadPool* cpu_thread_pool_ = nullptr;

  // superstep
  folly::AtomicHashMap<
      GID_T, std::shared_ptr<std::atomic<size_t>>, std::hash<int64_t>,
      std::equal_to<int64_t>, std::allocator<char>,
      folly::AtomicHashArrayQuadraticProbeFcn>* superstep_by_gid_ = nullptr;
  std::atomic<size_t>* global_superstep_ = nullptr;

  // state machine.
  utility::StateMachine<GID_T>* state_machine_ = nullptr;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H