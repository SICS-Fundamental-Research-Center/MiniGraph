//
// Created by chijinxin on 17-7-18.
//
#include <folly/MPMCQueue.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <algorithm>
#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

using namespace std;

class CPUThreadExecutor {
  using Func = folly::Function<void()>;  //定义task仿函数

 private:
  //线程池
  std::vector<std::thread> pool_;
  //任务队列 （无锁队列实现）
  folly::MPMCQueue<Func> tasksQueue_;
  // std::queue<Task> tasks_;
  //是否关闭提交
  std::atomic_bool stop_{false};
  //线程池大小
  size_t size_;
  //空闲线程数量
  std::atomic_int idlThreadNum_;

 public:
  //构造函数 （线程池大小，任务队列最大长度）
  CPUThreadExecutor(size_t s = 0, size_t maxTasks = 20000)
      : size_(s), stop_(false), tasksQueue_(maxTasks), idlThreadNum_(0) {
    if (size_ == 0) {
      size_ = std::thread::hardware_concurrency();  //默认线程池大小等于CPU核数
    }
    idlThreadNum_ = size_;

    //初始化工作线程数量
    for (int i = 0; i < size_; i++) {
      pool_.emplace_back(
          //工作线程函数（死循环，pull任务去执行，stop_为真线程停止）
          [this]() {
            Func func;
            while (!this->stop_) {
              if (!tasksQueue_.isEmpty()) {
                tasksQueue_.blockingRead(func);  //队列阻塞读
                func();                          //执行队列中的任务
              }
            }
            cout << "线程 id=" << std::this_thread::get_id() << " 退出！"
                 << endl;
          });
    }
  }
  //析构函数
  ~CPUThreadExecutor() {
    stop_.store(true);
    for (std::thread& t : pool_) {
      // t.detach();  //分离线程，让线程自生自灭
      if (t.joinable()) {
        t.join();  //等待任务结束，前提是线程一定会结束
      }
    }
  }

  //停止任务的提交
  void shutdown() { this->stop_.store(true); }
  //重启任务提交
  void restart() { this->stop_.store(false); }
  //提交一个任务
  template <class F, class... Args>
  auto commit(F&& f, Args&&... args) -> folly::Future<decltype(f(args...))> {
    auto promise = std::make_shared<folly::Promise<decltype(f(args...))> >();
    if (stop_.load()) {
      promise->setException(
          std::runtime_error("task executor closed commit!!!"));
    }
    this->tasksQueue_.blockingWrite([promise, f]() { promise->setValue(f()); });
    return promise->getFuture();
  }

  //获取空闲线程数量
  int getIdlCount() { return idlThreadNum_; }
};

int task1(int x) {
  cerr << "线程id=" << this_thread::get_id() << "输出：" << x << endl;
  this_thread::sleep_for(chrono::milliseconds(10));
  return x;
}

int main() {
  CPUThreadExecutor taskExecutor(10);
  cout << "空闲线程数量：" << taskExecutor.getIdlCount() << endl;

  //测试collectAll()高阶语义
  std::vector<folly::Future<int> > fs;
  for (int i = 0; i < 10000; i++) {
    fs.push_back(std::move(taskExecutor.commit(std::bind(&task1, i))));
  }
  folly::collectAll(fs).then([](const std::vector<folly::Try<int> >& tries) {
    int result = 0;
    for (int i = 0; i < tries.size(); i++) {
      // cout<<tries[i].value()<<"+";
      result += tries[i].value();
    }
    cout << "result=" << result << endl;
  });

  this_thread::sleep_for(std::chrono::seconds(10000));
  taskExecutor.shutdown();
  return 0;
}