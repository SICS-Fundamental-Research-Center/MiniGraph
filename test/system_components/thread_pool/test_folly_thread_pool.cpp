
#include "utility/thread_pool.h"
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <chrono>
#include <iostream>

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
  minigraph::utility::CPUThreadPool cpu_thread_pool(3, 2);
  minigraph::utility::IOThreadPool io_thread_pool(3, 1);
  cpu_thread_pool.Commit(func, 3);
  cpu_thread_pool.Commit(func, 2);

  auto task = std::bind(func, 1);
  auto task2 = std::bind(func, 2);
  auto task3 = std::bind(func, 3);
  cpu_thread_pool.CommitWithPriority(task,0);
  cpu_thread_pool.CommitWithPriority(task2,0);
  cpu_thread_pool.CommitWithPriority(task3,2);
}
