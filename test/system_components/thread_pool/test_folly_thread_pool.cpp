
#include <chrono>
#include <iostream>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>

#include "utility/thread_pool.h"

using std::cout;
using std::endl;

int func(int a) {
  cout << 2 << "x" << a << "=" << 2 * a << endl;
  return 2 * a;
}

int main() {
  // CPUThreadPoolExecutor is much more faster than IOThreadPoolExecutor.
  folly::CPUThreadPoolExecutor cpu_executor(3);
  folly::IOThreadPoolExecutor io_executor(3);
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  cpu_executor.add([]() { cout << "CPU" << endl; });
  io_executor.add([]() { cout << "IO" << endl; });
  minigraph::utility::CpuThreadPool cpu_thread_pool(3);
  minigraph::utility::IOThreadPool io_thread_pool(3, 1);
  cpu_thread_pool.commit(func, 3);
  cpu_thread_pool.commit(func, 2);
}
