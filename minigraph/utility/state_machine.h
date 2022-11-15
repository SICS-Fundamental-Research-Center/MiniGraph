#pragma once
#ifndef MINIGRAPH_UTILITY_STATE_MACHINE_H_
#define MINIGRAPH_UTILITY_STATE_MACHINE_H_

#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <stdio.h>
#include <string.h>
#include <unordered_map>
#include <vector>

#include <boost/sml.hpp>
#include <folly/AtomicHashMap.h>

#include "portability/sys_types.h"
#include "utility/logging.h"


namespace minigraph {
namespace utility {

namespace sml = boost::sml;
using namespace sml;

// Define Events
struct Load {};
struct Unload {};
struct NothingChanged {};
struct Changed {};
struct Aggregate {};
struct Fixpoint {};
struct Shortcut {};
struct GoOn {};
struct ShortcutRead {};

// Define State Machine for each single graph.
struct GraphStateMachine {
  auto operator()() const {
    using namespace sml;
    front::event<Load> event_load;
    front::event<ShortcutRead> event_shortcut_read;
    front::event<Unload> event_unload;
    front::event<NothingChanged> event_nothing_changed;
    front::event<Changed> event_changed;
    front::event<Aggregate> event_aggregate;
    front::event<Shortcut> event_shortcut;
    front::event<Fixpoint> event_fix_point;
    front::event<GoOn> event_go_on;
    return make_transition_table(
        *"Idle"_s + event_load = "Active"_s, "Idle"_s + event_unload = "Idle"_s,
        "Idle"_s + event_shortcut_read = "RT"_s,
        "Active"_s + event_nothing_changed = "RT"_s,
        "Active"_s + event_changed = "RC"_s,
        "RC"_s + event_aggregate = "Idle"_s, "RC"_s + event_shortcut = "RTS"_s,
        "RT"_s + event_go_on = "Idle"_s, "RTS"_s + event_go_on = "Idle"_s,
        "RT"_s + event_fix_point = X);
  }
};

// State Machine definition for each graph.
struct SystemStateMachine {
  auto operator()() const {
    using namespace sml;
    front::event<GoOn> event_go_on;
    front::event<Fixpoint> event_fix_point;
    return make_transition_table(*"Run"_s + event_go_on = "Run"_s,
                                 "Run"_s + event_fix_point = X);
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
      graph_state_.insert(
          std::make_pair(iter, std::make_unique<sml::sm<GraphStateMachine>>()));
    }
  };
  StateMachine() {}

  ~StateMachine(){};

  void ShowGraphState(const GID_T& gid) const {
    auto iter = graph_state_.find(gid);
    if (iter != graph_state_.end()) {
      iter->second->visit_current_states(
          [](auto state) { std::cout << state.c_str() << std::endl; });
    }
  };

  char GetState(const GID_T& gid) {
    assert(graph_state_.find(gid) != graph_state_.end());
    auto iter = graph_state_.find(gid);
    char out_state;
    iter->second->visit_current_states([&out_state](auto state) {
      if (strcmp(state.c_str(), "RT") == 0) out_state = RT;
      if (strcmp(state.c_str(), "Idle") == 0) out_state = IDLE;
      if (strcmp(state.c_str(), "RC") == 0) out_state = RC;
      if (strcmp(state.c_str(), "RTS") == 0) out_state = RTS;
    });
    return out_state;
  }

  bool GraphIs(const GID_T& gid, const char& state) const {
    assert(state == IDLE || state == ACTIVE || state == RT || state == RC ||
           state == TERMINATE || state == RTS);
    auto iter = graph_state_.find(gid);
    using namespace sml;
    if (iter != graph_state_.end()) {
      switch (state) {
        case IDLE:
          return iter->second->is("Idle"_s);
        case ACTIVE:
          return iter->second->is("Active"_s);
        case RT:
          return iter->second->is("RT"_s);
        case RTS:
          return iter->second->is("RTS"_s);
        case RC:
          return iter->second->is("RC"_s);
        case TERMINATE:
          return iter->second->is(X);
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
      iter.second->is("RTS"_s) ? ++count : 0;
    }
    if (count < graph_state_.size()) {
      return false;
    } else {
      system_state_.process_event(Fixpoint{});
      assert(system_state_.is(X));
      return true;
    }
  };

  GID_T GetXStateOf(const char state) {
    GID_T gid = MINIGRAPH_GID_MAX;
    for (auto& iter : graph_state_) {
      switch (state) {
        case IDLE:
          if (iter.second->is("Idle"_s)) gid = iter.first;
          break;
        case ACTIVE:
          if (iter.second->is("Active"_s)) gid = iter.first;
          break;
        case RT:
          if (iter.second->is("RT"_s)) gid = iter.first;
          break;
        case RTS:
          if (iter.second->is("RTS"_s)) gid = iter.first;
          break;
        case RC:
          if (iter.second->is("RC"_s)) gid = iter.first;
          break;
        case TERMINATE:
          if (iter.second->is(X)) gid = iter.first;
          break;
        default:
          break;
      }
    }
    return gid;
  }

  bool ProcessEvent(GID_T gid, const char event) {
    using namespace sml;
    assert(event == LOAD || event == UNLOAD || event == NOTHINGCHANGED ||
           event == CHANGED || event == AGGREGATE || event == FIXPOINT ||
           event == GOON || event == SHORTCUT || event == SHORTCUTREAD);
    auto iter = graph_state_.find(gid);
    if (iter != graph_state_.end()) {
      switch (event) {
        case LOAD:
          iter->second->process_event(Load{});
          assert(iter->second->is("Active"_s));
          break;
        case UNLOAD:
          iter->second->process_event(Unload{});
          assert(iter->second->is("Idle"_s));
          break;
        case NOTHINGCHANGED:
          assert(iter->second->is("Active"_s));
          iter->second->process_event(NothingChanged{});
          assert(iter->second->is("RT"_s));
          break;
        case CHANGED:
          assert(iter->second->is("Active"_s));
          iter->second->process_event(Changed{});
          assert(iter->second->is("RC"_s));
          break;
        case SHORTCUT:
          iter->second->process_event(Shortcut{});
          assert(iter->second->is("RTS"_s));
          break;
        case AGGREGATE:
          iter->second->process_event(Aggregate{});
          assert(iter->second->is("Idle"_s));
          break;
        case FIXPOINT:
          iter->second->process_event(Fixpoint{});
          assert(iter->second->is(X));
          break;
        case GOON:
          iter->second->process_event(GoOn{});
          assert(iter->second->is("Idle"_s));
          break;
        case SHORTCUTREAD:
          iter->second->process_event(ShortcutRead{});
          assert(iter->second->is("RT"_s));
          break;
        default:
          break;
      }
      return true;
    }
    return false;
  }

  std::vector<GID_T> GetAllinStateX(const char state) {
    std::vector<GID_T> out;
    switch (state) {
      case RT:
        for (auto& iter : graph_state_) {
          if (iter.second->is("RT"_s)) out.push_back(iter.first);
        }
        break;
      case RC:
        for (auto& iter : graph_state_) {
          if (iter.second->is("RC"_s)) out.push_back(iter.first);
        }
        break;
      case RTS:
        for (auto& iter : graph_state_) {
          if (iter.second->is("RTS"_s)) out.push_back(iter.first);
        }
        break;
      default:
        break;
    }
    return out;
  }

  std::vector<GID_T> EvokeAllX(const char state) {
    std::vector<GID_T> out;
    switch (state) {
      case RT:
        for (auto& iter : graph_state_) {
          if (iter.second->is("RT"_s)) {
            iter.second->process_event(GoOn{});
            assert(iter.second->is("Idle"_s));
            out.push_back(iter.first);
          }
        }
        break;
      case RC:
        for (auto& iter : graph_state_) {
          if (iter.second->is("RC"_s)) {
            iter.second->process_event(Aggregate{});
            assert(iter.second->is("Idle"_s));
            out.push_back(iter.first);
          }
        }
        break;
      case RTS:
        for (auto& iter : graph_state_) {
          if (iter.second->is("RTS"_s)) {
            iter.second->process_event(GoOn{});
            assert(iter.second->is("Idle"_s));
            out.push_back(iter.first);
          }
        }
        break;
      default:
        break;
    }
    return out;
  }

  void EvokeX(const GID_T gid, const char state) {
    std::vector<GID_T> output;
    auto iter = graph_state_.find(gid);
    assert(iter != graph_state_.end());
    switch (state) {
      case RT:
        iter->second->is("RT"_s) ? iter->second->process_event(GoOn{}) : 0;
        assert(iter->second->is("Idle"_s));
        break;
      case RC:
        iter->second->is("RC"_s) ? iter->second->process_event(Aggregate{}) : 0;
        assert(iter->second->is("Idle"_s));
        break;
      case RTS:
        iter->second->is("RTS"_s) ? iter->second->process_event(GoOn{}) : 0;
        assert(iter->second->is("Idle"_s));
        break;
    }
  }

  void ShowAllState() {
    std::cout << "All state: ";
    for (auto& iter : graph_state_) {
      iter.second->visit_current_states(
          [](auto state) { std::cout << state.c_str() << "  "; });
    }
    std::cout << std::endl;
  }

 private:
  std::unordered_map<GID_T, std::unique_ptr<sml::sm<GraphStateMachine>>>
      graph_state_;
  sml::sm<SystemStateMachine> system_state_;
};

}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_STATE_MACHINE_H_
