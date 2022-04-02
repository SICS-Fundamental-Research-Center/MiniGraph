#ifndef MINIGRAPH_DATA_MNGR_H
#define MINIGRAPH_DATA_MNGR_H

#include "utility/io/csr_io_adapter.h"
#include <folly/AtomicHashMap.h>
#include <memory>

namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class DataMgnr {
  using GRAPH_T = graphs::Graph<GID_T, VID_T, VDATA_T, EDATA_T>;
  using CSR_T = graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
  using MSG_T = graphs::Message<VID_T, VDATA_T, EDATA_T>;

 public:
  DataMgnr() {
    pgraph_by_gid_ =
        std::make_unique<folly::AtomicHashMap<GID_T, GRAPH_T*>>(1024);

    csr_io_adapter_ = std::make_unique<
        utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>();
  };

  GRAPH_T* LoadGraph(GID_T gid, CSRPt csr_pt) {
    auto immutable_csr =
        new graphs::ImmutableCSR<GID_T, VID_T, VDATA_T, EDATA_T>;
    csr_io_adapter_->Read((GRAPH_T*)immutable_csr, csr_bin, gid,
                          csr_pt.vertex_pt, csr_pt.meta_in_pt,
                          csr_pt.meta_out_pt, csr_pt.vdata_pt,
                          csr_pt.localid2globalid_pt);
    pgraph_by_gid_->insert(gid, (GRAPH_T*)immutable_csr);
    return (GRAPH_T*)immutable_csr;
  }

  GRAPH_T* GetGraph(GID_T gid) {
    if (pgraph_by_gid_->count(gid)) {
      return pgraph_by_gid_->find(gid)->second;
    } else {
      return nullptr;
    }
  }

  void EraseGraph(GID_T gid) { pgraph_by_gid_->erase(gid); };

  void EraseMsg(){};
  void InsertMsg(){};

 private:
  std::unique_ptr<MSG_T> global_msg_ = nullptr;
  std::unique_ptr<folly::AtomicHashMap<GID_T, GRAPH_T*>> pgraph_by_gid_ =
      nullptr;
  std::unique_ptr<utility::io::CSRIOAdapter<GID_T, VID_T, VDATA_T, EDATA_T>>
      csr_io_adapter_;
};

}  // namespace io
}  // namespace utility
}  // namespace minigraph
#endif  // MINIGRAPH_DATA_MNGR_H
