#ifndef MINIGRAPH_THREAD_H
#define MINIGRAPH_THREAD_H

#include <atomic>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <queue>
#include <stdexcept>
#include <vector>

namespace minigraph {

using std::atomic;
using std::bind;
using std::condition_variable;
using std::cout;
using std::endl;
using std::forward;
using std::function;
using std::future;
using std::lock;
using std::lock_guard;
using std::make_shared;
using std::mutex;
using std::packaged_task;
using std::queue;
using std::runtime_error;
using std::thread;
using std::unique_lock;
using std::vector;

#define THREADPOOL_MAX_NUM 16

class ThreadPool {
 private:
  vector<thread> _pool;
  using Task = function<void()>;  //定义类型
  queue<Task> _tasks;             //任务队列
  mutex _lock;                    //同步
  condition_variable _task_cv;    //条件阻塞
  atomic<bool> _run{true};        //线程池是否执行
  atomic<int> _idl_thr_num{0};    //空闲线程数量
 public:
  inline ThreadPool(unsigned short size = 4) { AddThread(size); }
  inline ~ThreadPool() {
    _run = false;
    _task_cv.notify_all();  // 唤醒所有线程执行
    for (thread& thread : _pool) {
      // thread.detach(); // 让线程“自生自灭”
      if (thread.joinable())
        thread.join();  // 等待任务结束， 前提：线程一定会执行完
    }
  }
  int get_idl_num() { return _idl_thr_num; }
  int get_threads_num() { return _pool.size(); }

  template <class F, class... Args>
  auto commit(F&& f, Args&&... args)
      -> future<decltype(f(args...))>  // c++ 11 新特性 新的返回语法， auto
                                       // 和真正要返回的 后置。
  {
    if (!_run)  // stoped ??
      throw runtime_error("commit on ThreadPool is stopped.");
    using RetType =
        decltype(f(args...));  // typename std::result_of<F(Args...)>::type,
                               // 函数 f 的返回值类型
    auto task = make_shared<packaged_task<RetType()>>(bind(
        forward<F>(f),
        forward<Args>(
            args)...));  // 把函数入口及参数,打包(绑定)
                         // std::packaged_task包装任何可调用目标(函数、lambda表达式、bind表达式、函数对象)以便它可以被异步调用。
    future<RetType> future = task->get_future();
    {
      // 添加任务到队列
      lock_guard<mutex> lock{
          _lock};  //对当前块的语句加锁  lock_guard 是 mutex 的 stack
                   //封装类，构造的时候 lock()，析构的时候 unlock()
      _tasks.emplace([task]() {  // push(Task{...}) 放到队列后面
        (*task)();
      });
    }
    _task_cv.notify_one();  // 唤醒一个线程执行
    return future;
  }

  void AddThread(unsigned short size) {
    for (; _pool.size() < THREADPOOL_MAX_NUM && size > 0; --size) {
      //增加线程数量,但不超过 预定义数量 THREADPOOL_MAX_NUM
      _pool.emplace_back([this] {  //工作线程函数
        while (_run) {
          Task task;  // 获取一个待执行的 task
          {
            // unique_lock 相比 lock_guard 的好处是：可以随时 unlock() 和 lock()
            unique_lock<mutex> lock{_lock};
            _task_cv.wait(lock, [this] {
              return !_run || !_tasks.empty();
            });  // wait 直到有 task
            if (!_run && _tasks.empty())
              return;
            task = move(_tasks.front());  // 按先进先出从队列取一个 task
            _tasks.pop();
          }
          --_idl_thr_num;
          task();  //执行任务
          ++_idl_thr_num;
        }
      });
      ++_idl_thr_num;
    }
  }
};

}  // namespace miniGraph
#endif