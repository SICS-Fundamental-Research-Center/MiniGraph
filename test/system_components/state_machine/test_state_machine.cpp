#include "portability/sys_types.h"
#include "state_machine/state_machine.h"
#include <iostream>
using std::cout;
using std::endl;
int main() {
  std::vector<gid_t> vec_gid;
  minigraph::StateMachine<unsigned> minigraph_sm(vec_gid);
  minigraph_sm.ProcessEvent(1, LOAD);
  minigraph_sm.ProcessEvent(1, NOTHINGCHANGE);
  minigraph_sm.ProcessEvent(2, LOAD);
  minigraph_sm.ProcessEvent(2, NOTHINGCHANGE);
  minigraph_sm.ShowGraphState(1);
  cout << "terminate:" << minigraph_sm.IsTerminated() << endl;
}
