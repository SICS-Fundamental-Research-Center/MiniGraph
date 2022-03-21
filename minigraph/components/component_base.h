
#ifndef MINIGRAPH_COMPONENT_BASE_H
#define MINIGRAPH_COMPONENT_BASE_H

#include <folly/AtomicHashMap.h>
#include "utility/thread_pool.h"
#include "utility/state_machine.h"
#include <atomic>
#include <memory>

namespace minigraph {
namespace components {
template <typename GID_T>
class ComponentBase {
 public:
  virtual void Run() = 0;
  virtual void Stop() = 0;
  virtual size_t get_superstep_via_gid(GID_T gid) const = 0;
  virtual size_t get_global_superstep() const = 0;

 private:
  // configuration.
  std::string work_space_pt_;
  size_t max_threads_ = 0;
  size_t num_threads_ = 0;
  size_t min_threads_ = 0;

  // contral switch.
  std::atomic<bool>* run_;

  // thread pool.
  std::shared_ptr<utility::IOThreadPool> io_thread_pool_;
  std::shared_ptr<utility::CPUThreadPool> cpu_thread_pool_;

  // state machine.
  //std::shared_ptr<StateMachine<GID_T>> state_machine_;

  std::shared_ptr<folly::AtomicHashMap<
      GID_T, std::atomic<size_t>, std::hash<int64_t>, std::equal_to<int64_t>,
      std::allocator<char>, folly::AtomicHashArrayQuadraticProbeFcn>>
      superstep_by_gid_;

  std::shared_ptr<std::atomic<size_t>> global_superstep_;
};

}  // namespace components
}  // namespace minigraph
#endif  // MINIGRAPH_COMPUTING_COMPONENT_H