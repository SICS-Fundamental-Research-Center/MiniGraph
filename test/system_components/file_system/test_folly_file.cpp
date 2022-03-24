#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "utility/io/csr_io_adapter.h"
#include "utility/paritioner/edge_cut_partitioner.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/FBVector.h>
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

int main(int argc, char* argv[]) {
  XLOG(INFO, "MAIN()");
  if (argc < 5) {
    XLOG(ERR, "INPUT ERROR");
  }

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

  map_local_id_to_global_id_->insert(1, 2);

  minigraph::utility::io::CSRIOAdapter<unsigned, unsigned, unsigned, unsigned>*
      io_adapter = new minigraph::utility::io::CSRIOAdapter<unsigned, unsigned,
                                                            unsigned, unsigned>;

  /*                                                    unsigned, unsigned>
  minigraph::utility::partitioner::EdgeCutPartitioner < unsigned, unsigned,
      edge_cut(argv[1], argv[2], argv[3], argv[4], argv[5]);
  edge_cut.RunPartition(3);

  auto csr_io_adapter_ = std::make_shared<
      minigraph::utility::io::CSRIOAdapter<gid_t, vid_t, vdata_t, edata_t>>();

  auto immutable_csr =
      new minigraph::graphs::ImmutableCSR<gid_t, vid_t, vdata_t, edata_t>;
  csr_io_adapter_->Read(immutable_csr, 0, argv[2], argv[3], argv[4], argv[5]);
   */
}