#ifndef MINIGRAPH_STATE_MACHINE_H_
#define MINIGRAPH_STATE_MACHINE_H_

#include <boost/sml.hpp>
#include <folly/AtomicHashMap.h>
#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <stdio.h>
#include <unordered_map>
#include <vector>

namespace minigraph {
namespace sml = boost::sml;

// Define Events
struct Load {};
struct Unload {};
struct NothingChanged {};
struct Changed {};
struct Aggregate {};
struct Fixpoint {};
struct GoOn {};
// Define State Machine for each single graph.
struct GraphStateMachine {
  auto operator()() const {
    using namespace sml;
    // Create a transition table.
    // Example:
    //  Idle(state) + Load(event) = Active(state)
    return make_transition_table(*"Idle"_s + event<Load> = "Active"_s,
                                 "Idle"_s + event<Unload> = "Idle"_s,
                                 "Active"_s + event<NothingChanged> = "RT"_s,
                                 "Active"_s + event<Changed> = "RC"_s,
                                 "RC"_s + event<Aggregate> = "Idle"_s,
                                 "RT"_s + event<GoOn> = "Idle"_s,
                                 "RT"_s + event<Fixpoint> = X);
  }
};
// State Machine definition for each graph.
struct SystemStateMachine {
  auto operator()() const {
    using namespace sml;
    return make_transition_table(*"Run"_s + event<GoOn> = "Run"_s,
                                 "Run"_s + event<Fixpoint> = X);
  }
};

// Class for state machine maintained in the system.
// It start from the begining of the system and destroyed when fixpoint is
// reached. At any point of time, a sub-graph is in one of five states:
// Active('A'), Idle('I'), Ready-to-Terminate, i.e RT ('R'),
// Ready-to-be-Collect, i.e RC ('C), and Terminate, i.e X('X').
// The transition from one state to another is triggered by an event.
// There are seven types of events for graph_state_: Load, Unload, ...,
// Fixpoint. and two types of events for system_state_: GoOn and Fixpoint.
//
// On ProcessEvent(GID_T gid, const char event), the state machine of gid
// changed according to transition table of GraphStateMachine.
//
// system_state_ changed from Run to X only if all state of graphs reach RT, i.e
// Fixpoint.
template <typename GID_T>
class StateMachine {
 public:
  StateMachine(const std::vector<GID_T>& vec_gid) {
    for (auto& iter : vec_gid) {
      graph_state_.insert(std::make_pair(
          *iter, std::make_unique<sml::sm<GraphStateMachine>>()));
    }
  };

  ~StateMachine(){};

  void ShowGraphState(const GID_T& gid) const {
    auto iter = graph_state_.find(gid);
    if (iter != graph_state_.end()) {
      iter->second->visit_current_states(
          [](auto state) { std::cout << state.c_str() << std::endl; });
    } else {
    }
  };

  bool GraphIs(const GID_T& gid, const char& event) const {
    assert(event == 'I' || event == 'A' || event == 'R' || event == 'C' ||
           event == 'X');
    auto iter = graph_state_.find(gid);
    using namespace sml;
    if (iter != graph_state_.end()) {
      switch (event) {
        case 'I':
          return iter->second.is("Idle"_s);
        case 'A':
          return iter->second.is("Active"_s);
        case 'R':
          return iter->second.is("RT"_s);
        case 'C':
          return iter->second.is("RC"_s);
        case 'X':
          return iter->second.is(X);
        default:
          break;
      }
    }
    return false;
  };

  bool IsTerminated() {
    using namespace sml;
    unsigned count = 0;
    for (auto& iter : graph_state_) {
      iter.second->is("RT"_s) ? ++count : 0;
    }
    if (count < graph_state_.size()) {
      for (auto& iter : graph_state_) {
        iter.second->process_event(GoOn{});
        assert(iter.second->is("Idle"_s));
      }
      return false;
    } else {
      system_state_.process_event(Fixpoint{});
      assert(system_state_.is(X));
      return true;
    }
  };

  bool ProcessEvent(GID_T gid, const char event) {
    using namespace sml;
    std::cout << event << std::endl;
    assert(event == 'L' || event == 'U' || event == 'N' || event == 'C' ||
           event == 'A' || event == 'F' || event == 'G');
    auto iter = graph_state_.find(gid);
    if (iter != graph_state_.end()) {
      switch (event) {
        case 'L':
          iter->second->process_event(Load{});
          assert(iter->second->is("Active"_s));
          break;
        case 'U':
          iter->second->process_event(Unload{});
          assert(iter->second->is("Idle"_s));
          break;
        case 'N':
          iter->second->process_event(NothingChanged{});
          assert(iter->second->is("RT"_s));
          break;
        case 'C':
          iter->second->process_event(Changed{});
          assert(iter->second->is("RC"_s));
          break;
        case 'A':
          iter->second->process_event(Aggregate{});
          assert(iter->second->is("Idle"_s));
          break;
        case 'F':
          iter->second->process_event(Fixpoint{});
          assert(iter->second->is(X));
          break;
        case 'G':
          iter->second->process_event(GoOn{});
          assert(iter->second->is("Idle"_s));
          break;
        default:
          break;
      }
      return true;
    }
    return false;
  }

 private:
  std::unordered_map<GID_T, std::unique_ptr<sml::sm<GraphStateMachine>>>
      graph_state_;
  sml::sm<SystemStateMachine> system_state_;
};

}  // namespace minigraph

#endif  // MINIGRAPH_STATE_MACHINE_H_
