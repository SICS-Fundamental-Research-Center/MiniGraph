#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/memory/arena_allocator.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/memory/Arena.h>
#include <folly/memory/MallctlHelper.h>
#include <folly/memory/Malloc.h>
#include <folly/portability/Asm.h>
#include <folly/portability/GMock.h>
#include <jemalloc/jemalloc.h>
#include <iostream>

using std::cout;
using std::endl;

class Counters {
 private:
  folly::AtomicHashMap<int64_t, int64_t> ahm;

 public:
  explicit Counters(size_t numCounters) : ahm(numCounters) {}

  void increment(int64_t obj_id) {
    auto ret = ahm.insert(std::make_pair(obj_id, 1));
    // if (!ret.second) {
    //   // obj_id already exists, increment
    //   //NoBarrier_AtomicIncrement(&ret.first->second, 1);
    // }
  }

  int64_t getValue(int64_t obj_id) {
    auto ret = ahm.find(obj_id);
    return ret != ahm.end() ? ret->second : 0;
  }

  // Serialize the counters without blocking increments
  // string toString() {
  //  string ret = "{\n";
  //  ret.reserve(ahm.size() * 32);
  //  for (const auto& e : ahm) {
  //    ret += folly::to<string>("  [", e.first, ":", NoBarrier_Load(&e.second),
  //                             "]\n");
  //  }
  //  ret += "}\n";
  //  return ret;
  //}
};

int main() {
  folly::File file("/data/zhuxiaoke/soc-LiveJournal1/workspace/meta/in/0.meta",
                   O_RDONLY);
  int fd = file.fd();
  cout << fd << endl;
  cout << folly::usingJEMalloc() << endl;
  // minigraph::utility::Arena arena();
  // constexpr size_t kArenaBlockOverhead =
  //     folly::Arena<::SysAllocator<void>>::kBlockOverhead;
  // cout << kArenaBlockOverhead << ", "
  //      << folly::Arena<::SysAllocator<void>>::kNoSizeLimit << ","
  //      << folly::Arena<::SysAllocator<void>>::kDefaultMaxAlign << endl;
  // static const size_t requestedBlockSize = 64;
  // static const size_t b = 0;
  // static const size_t c = 64;

  // folly::SysArena arena(requestedBlockSize);
  //  int* buf = (int*)arena.allocate(10);
  // cout << arena.totalSize() << endl;
  int* buf = (int*)malloc(sizeof(unsigned) * 100);
  // int* buf = (int*)calloc(10, sizeof(unsigned));

  folly::preadNoInt(fd, buf, sizeof(unsigned) * 100, sizeof(unsigned));
  //*(buf) = 1;
  // buf[0] = 1;
  // buf[1] = 2;
  // buf[2] = 3;
  // buf[3] = 4;
  for (int i = 0; i < 10; i++) {
    cout << buf[i] << endl;
  }
  //  folly::Arena<SysAllocator<void>>({},
  //  folly::Arena<::SysAllocator<void>>::kNoSizeLimit,
  //  folly::Arena<::SysAllocator<void>>::kNoSizeLimit,
  //  folly::Arena<::SysAllocator<void>>::kDefaultMaxAlign)
  //   SysAllocator<void> &&a;
  //   folly::Arena<SysAllocator<void>>(, 1, 1, 1);
  //    using Alloc = folly::SysAllocator<float>;
  //    folly::ArenaAllocatorTraits<char> alloc;
  //    folly::Arena<::SysAllocator<void>> arena;

  // void *buf = ;
  // folly::readNoInt(fd, buf, 1000);
  // for(int i =0; i <10;i++){
  //     cout<<((int*)buf)[i]<<endl;
  // }
  file.close();
  using minigraph::graphs::ImmutableCSR;
  ImmutableCSR<unsigned, unsigned, unsigned, unsigned> immutable_csr(
      0, "/data/zhuxiaoke/soc-LiveJournal1/workspace/meta/in/0.meta",
      "/data/zhuxiaoke/soc-LiveJournal1/workspace/meta/out/0.meta",
      "/data/zhuxiaoke/soc-LiveJournal1/workspace/label/0.label",
      "/data/zhuxiaoke/soc-LiveJournal1/workspace/nodes_id_map/0.map");
  immutable_csr.init();
  // auto map_local_id_to_global_id_ =
  //      new folly::AtomicHashMap<int64_t, int64_t>(1024);
  //  auto map_local_id_to_global_id_ = new folly::AtomicHashMap<
  //    int64_t,
  //    int64_t,
  //    std::hash<int64_t>,
  //    std::equal_to<int64_t>,
  //    std::allocator<char>,
  //    folly::AtomicHashArrayQuadraticProbeFcn>(1024); // ok
  //  cout << map_local_id_to_global_id_ << endl; // ok
  //  auto hash = map_local_id_to_global_id_->hash_function(); // ok
  //  cout << hash(100) << endl; // ok
  //  cout<<map_local_id_to_global_id_->empty()<<endl; // Segmentation fault
cout<<"########################"<<endl;
  std::allocator<char> alloc;
  auto const mem = alloc.allocate(10);
  mem[0] = 'c';
  cout<<mem[0]<<endl;
cout<<"########################"<<endl;
  static folly::AtomicHashMap<int64_t, int64_t, std::hash<int64_t>,
                              std::equal_to<int64_t>, std::allocator<char>,
                              folly::AtomicHashArrayQuadraticProbeFcn>*
      map_local_id_to_global_id_ = nullptr;  // ok
  map_local_id_to_global_id_ =
      new folly::AtomicHashMap<int64_t, int64_t, std::hash<int64_t>,
                               std::equal_to<int64_t>, std::allocator<char>,
                               folly::AtomicHashArrayQuadraticProbeFcn>(1024);

  if (map_local_id_to_global_id_ == nullptr) {
    return 0;
  }
  cout << map_local_id_to_global_id_ << endl;               // ok
  auto hash = map_local_id_to_global_id_->hash_function();  // ok
  cout << hash(100) << endl;                                // ok
  cout << map_local_id_to_global_id_->empty() << endl;  // Segmentation fault

  // map_local_id_to_global_id_->insert(1, 2);
}