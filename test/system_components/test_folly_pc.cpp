//
// Created by hsiaoko on 2022/3/20.
//
#include <folly/FBString.h>
#include <folly/ProducerConsumerQueue.h>
#include <iostream>
#include <thread>

int main(int argc, char* argv[]) {
  folly::ProducerConsumerQueue<folly::fbstring> queue{10};

  // std::thread reader([&queue] {
  //   for (;;) {
  //     folly::fbstring str;
  //     while (!queue.read(str)) {
  //       // spin until we get a value
  //       continue;
  //     }

  //    //     sink(str);
  //  }
  //});
  queue.write("A");

  folly::fbstring a;
  queue.read(a);
  std::cout << a << std::endl;
  //// producer thread:
  // for (;;) {
  //   folly::fbstring str = "A";
  //   // source();
  //   while (!queue.write(str)) {
  //     // spin until the queue has room
  //     continue;
  //   }
  // }

  // std::thread reader([&queue] {
  //   for (;;) {
  //     folly::fbstring* pval;
  //     do {
  //       pval = queue.frontPtr();
  //     } while (!pval);  // spin until we get a value;

  //    std::sink(*pval);
  //    queue.popFront();
  //  }
  //});
}
