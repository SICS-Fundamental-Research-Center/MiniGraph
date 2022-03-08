#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>
#include <iostream>
using namespace folly;
using namespace std;

void foo(int x) {
  cout << "foo(" << x << ")" << endl;
}

int main() {
  // ...
  folly::ThreadedExecutor executor;
  cout << "making Promise" << endl;
  Promise<int> p;
  Future<int> f = p.getSemiFuture().via(&executor);
  auto f2 = move(f).thenValue(foo);
  cout << "Future chain made" << endl;

  // ... now perhaps in another event callback

  cout << "fulfilling Promise" << endl;
  p.setValue(42);
  move(f2).get();
  cout << "Promise fulfilled" << endl;
  return 0;
}
