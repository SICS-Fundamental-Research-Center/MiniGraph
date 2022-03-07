#ifndef MINIGRAPH_STATE_MACHINE_H_
#define MINIGRAPH_STATE_MACHINE_H_
#include <boost/sml.hpp>
#include <assert.h>
#include <iostream>
#include <map>
#include <memory>
#include <stdio.h>
#include <unordered_map>
namespace minigraph {
namespace sml = boost::sml;

struct Load {};
struct Unload {};
struct NothingChanged {};
struct Changed {};
struct Aggregate {};
struct Fixpoint {};
struct GoOn {};
struct GraphSm {
  auto operator()() const {
    using namespace sml;
    return make_transition_table(*"Idle"_s + event<Load> = "Active"_s,
                                 "Idle"_s + event<Unload> = "Idle"_s,
                                 "Active"_s + event<NothingChanged> = "RT"_s,
                                 "Active"_s + event<Changed> = "RC"_s,
                                 "RC"_s + event<Aggregate> = "Idle"_s,
                                 "RT"_s + event<GoOn> = "Idle"_s,
                                 "RT"_s + event<Fixpoint> = X);
  }
};

struct SystemSm {
  auto operator()() const {
    using namespace sml;
    return make_transition_table(*"run"_s + event<GoOn> = "run"_s,
                                 "run"_s + event<Fixpoint> = X);
  }
};

template <typename GID_T, typename GRAPH_T>
class StateMachine {
 public:
  StateMachine(const std::unordered_map<GID_T, GRAPH_T>& id_ptr) {
    graph_state_ = std::make_shared<
        std::unordered_map<GID_T, std::shared_ptr<sml::sm<GraphSm>>>>();
    system_state_ = std::make_shared<sml::sm<SystemSm>>();
    for (auto& iter : id_ptr) {
      graph_state_->insert(
          std::make_pair(iter.first, std::make_shared<sml::sm<GraphSm>>()));
    }
    for (auto& iter : *graph_state_) {
      iter.second->visit_current_states(
          [](auto state) { std::cout << state.c_str() << std::endl; });
    }
  };
  ~StateMachine(){};

  void show_graph_state(const GID_T& gid) {
    auto iter = graph_state_->find(gid);
    if (iter != graph_state_->end()) {
      iter->second->visit_current_states(
          [](auto state) { std::cout << state.c_str() << std::endl; });
    } else {
    }
  };

  bool is_graph_state(const GID_T& gid, const char& event) {
    assert(event == 'I' || event == 'A' || event == 'R' || event == 'C' ||
           event == 'X');
    auto iter = graph_state_->find(gid);
    using namespace sml;
    if (iter != graph_state_->end()) {
      switch (event) {
        case 'I':
          return iter->second->is("Idle"_s);
        case 'A':
          return iter->second->is("Active"_s);
        case 'R':
          return iter->second->is("RT"_s);
          break;
        case 'C':
          return iter->second->is("RC"_s);
          break;
        case 'X':
          return iter->second->is(X);
        default:
          break;
      }
    }
    return false;
  };

  bool ProvessEvent(GID_T gid, const char& event) {
    assert(event == 'L' || event == 'U' || event == 'C' || event == 'A' ||
           event == 'F' || event == 'G');
    auto iter = graph_state_->find(gid);
    if (iter != graph_state_->end()) {
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
  std::shared_ptr<std::unordered_map<GID_T, std::shared_ptr<sml::sm<GraphSm>>>>
      graph_state_ = nullptr;
  std::shared_ptr<sml::sm<SystemSm>> system_state_ = nullptr;
};

}  // namespace minigraph

#endif  // MINIGRAPH_STATE_MACHINE_H_