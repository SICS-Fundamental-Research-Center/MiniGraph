
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
      std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool,
      std::shared_ptr<utility::IOThreadPool> io_thread_pool,
      std::shared_ptr<
          folly::AtomicHashMap<GID_T, std::atomic<size_t>, std::hash<int64_t>,
                               std::equal_to<int64_t>, std::allocator<char>,
                               folly::AtomicHashArrayQuadraticProbeFcn>>
          superstep_by_gid,
      std::shared_ptr<std::atomic<size_t>> global_superstep,
      std::shared_ptr<utility::StateMachine<GID_T>> state_machine) {
    cpu_thread_pool_ = cpu_thread_pool;
    io_thread_pool_ = io_thread_pool;
    superstep_by_gid_ = superstep_by_gid;
    global_superstep_ = global_superstep;
    state_machine_ = state_machine;
  };
  void Run();
  void Stop();
  size_t get_superstep_via_gid(GID_T gid) {
    // auto iter = global_superstep_->load()->find(gid);
    // if (iter == global_superstep_->load()->end()) {
    //   XLOG(INFO, "get_super_via_gid Error: ", "gid not found.");
    // }
    return 0;
  };
  size_t get_global_superstep() { return global_superstep_->load(); };

 private:
  // contral switch.
  std::atomic<bool>* run_;

  // thread pool.
  std::shared_ptr<utility::IOThreadPool> io_thread_pool_;
  std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool_;

  // superstep
  std::shared_ptr<folly::AtomicHashMap<
      GID_T, std::atomic<size_t>, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      superstep_by_gid_;
  std::shared_ptr<std::atomic<size_t>> global_superstep_;

  // state machine.
  std::shared_ptr<utility::StateMachine<GID_T>> state_machine_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H