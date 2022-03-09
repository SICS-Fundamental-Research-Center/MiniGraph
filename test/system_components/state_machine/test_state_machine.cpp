#include <iostream>

#include "state_machine/state_machine.h"

using std::cout;
using std::endl;
int main() {
  std::unordered_map<unsigned, double> id_ptr;
  id_ptr.insert(std::make_pair(1, 99));
  id_ptr.insert(std::make_pair(2, 99));
  minigraph::StateMachine<unsigned, double> minigraph_sm(id_ptr);
  minigraph_sm.ProcessEvent(1, 'L');
  minigraph_sm.ProcessEvent(1, 'N');
  minigraph_sm.ProcessEvent(2, 'L');
  minigraph_sm.ProcessEvent(2, 'N');
  minigraph_sm.ShowGraphState(1);
  cout << "terminate:" << minigraph_sm.IsTerminated() << endl;
}
