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
struct Unchanged {};
struct Changed {};
struct Aggregate {};
struct Fixpoint {};
struct GoOn {};
struct GraphSm {
  auto operator()() const {
    using namespace sml;
    return make_transition_table(*"Idle"_s + event<Load> = "Active"_s,
                                 "Idle"_s + event<Unload> = "Idle"_s,
                                 "Active"_s + event<Unchanged> = "RT"_s,
                                 "Active"_s + event<Changed> = "RA"_s,
                                 "RA"_s + event<Aggregate> = "Idle"_s,
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
      std::cout << iter.first << std::endl;
      graph_state_->insert(
          std::make_pair(iter.first, std::make_shared<sml::sm<GraphSm>>()));
    }
  };
  ~StateMachine(){};

  std::shared_ptr<sml::sm<GraphSm>> GetGraphState(const GID_T& gid) {
    auto iter = graph_state_->find(gid);
    if (iter != graph_state_->end()) {
      //graph_state_->find(gid)->second->visit_current_states([](auto state) {
      //  //std::cout << state.c_str() << std::endl;
      //  return state.c_str();
      //});
      return iter->second;
      //return "E";
    } else {
      //return "Error";
      return nullptr;
    }
  };

 private:
  std::shared_ptr<std::unordered_map<GID_T, std::shared_ptr<sml::sm<GraphSm>>>>
      graph_state_ = nullptr;
  std::shared_ptr<sml::sm<SystemSm>> system_state_ = nullptr;
};

}  // namespace minigraph

#endif  // MINIGRAPH_STATE_MACHINE_H_