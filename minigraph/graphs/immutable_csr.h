
#ifndef MINIGRAPH_GRAPHS_IMMUTABLECSR_H
#define MINIGRAPH_GRAPHS_IMMUTABLESCR_H

#include "graphs/graph.h"
#include "utility/logging.h"
#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>
#include <folly/Benchmark.h>
#include <folly/Conv.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/Range.h>
#include <folly/portability/Atomic.h>
#include <folly/portability/GTest.h>
#include <folly/portability/SysTime.h>
#include <iostream>
#include <map>

using std::cout;
using std::endl;

namespace minigraph {
namespace graphs {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ImmutableCSR : public Graph<GID_T, VID_T, VDATA_T, EDATA_T> {
 public:
  explicit ImmutableCSR(GID_T gid, folly::StringPiece meta_in_pt,
                        folly::StringPiece meta_out_pt,
                        folly::StringPiece vertexes_data_pt,
                        folly::StringPiece localid_to_globalid_pt = NULL)
      : Graph<GID_T, VID_T, VDATA_T, EDATA_T>(gid) {
    meta_in_pt_ = meta_in_pt;
    meta_out_pt_ = meta_out_pt;
    vertexes_data_pt_ = vertexes_data_pt;
    localid_to_globalid_pt_ = localid_to_globalid_pt;
    // this->map_local_id_to_global_id_(19);
  }

  inline void Clear(){};

  bool init() {
    this->ReadCSRBin(meta_in_pt_, meta_out_pt_, vertexes_data_pt_,
                     localid_to_globalid_pt_);
  };

 private:
  folly::StringPiece meta_in_pt_;
  folly::StringPiece meta_out_pt_;
  folly::StringPiece vertexes_data_pt_;
  folly::StringPiece localid_to_globalid_pt_;
  folly::AtomicHashMap<VID_T, VID_T>* map_local_id_to_global_id_;
  folly::AtomicHashMap<VID_T, VID_T>* map_global_id_to_local_id_;

  bool ReadCSRBin(folly::StringPiece meta_in_pt, folly::StringPiece meta_out_pt,
                  folly::StringPiece vertexes_data_pt,
                  folly::StringPiece localid_to_globalid_pt) {
    folly::File meta_in_file(meta_in_pt, O_RDONLY);
    folly::File meta_out_file(meta_out_pt, O_RDONLY);
    folly::File vertexes_data_file(vertexes_data_pt, O_RDONLY);
    folly::File localid_to_globalid_file(localid_to_globalid_pt, O_RDONLY);

    int meta_in_fd = meta_in_file.fd();
    int meta_out_fd = meta_out_file.fd();
    int vertex_data_fd = vertexes_data_file.fd();
    int localid_to_globalid_fd = localid_to_globalid_file.fd();
    folly::readNoInt(meta_in_fd, &this->num_vertexes_, sizeof(VID_T));
    folly::readNoInt(meta_in_fd, &this->num_in_edges_, sizeof(VID_T));
    folly::readNoInt(meta_out_fd, &this->num_vertexes_, sizeof(VID_T));
    folly::readNoInt(meta_out_fd, &this->num_out_edges_, sizeof(VID_T));
    // map_local_id_to_global_id_();

    XLOG(INFO, this->num_vertexes_);
    map_local_id_to_global_id_ =
        new folly::AtomicHashMap<VID_T, VID_T>(this->num_vertexes_);
    map_local_id_to_global_id_ = std::move(map_local_id_to_global_id_);
    cout << std::move(map_local_id_to_global_id_)->capacity() << endl;
    // cout << std::move(map_local_id_to_global_id_)->spaceRemaining() << endl;
    // cout << map_local_id_to_global_id_->spaceRemaining() << endl;
    size_t a = 1024;
    folly::AtomicHashMap<VID_T, VID_T> C(a);
    //   C(a);
    cout << C.capacity() << endl;
    VID_T obj_id = 1;
    // cout << C.spaceRemaining() << endl;
    //C.clear();
    XLOG(INFO, "cp1");
    //for (auto iter = C.begin(); iter != C.end(); iter++) {
    //  cout << "a<<endl;" << endl;
    //}
    // C.count();
    XLOG(INFO, "cp2");
    //C.insert(obj_id, obj_id);
    XLOG(INFO, "cp3");

    //  map_local_id_to_global_id_->insert(1, 1);
    // XLOG(INFO, "cp2");
    // map_local_id_to_global_id_->insert
    if (this->num_in_edges_ == 0 || this->num_vertexes_ == 0) {
      meta_in_file.close();
      meta_out_file.close();
      vertexes_data_file.close();
      localid_to_globalid_file.close();
      return false;
    } else {
      // Load edges
      this->in_offset_ = (VID_T*)malloc(sizeof(VID_T) * this->num_vertexes_);
      folly::readNoInt(meta_in_fd, this->in_offset_,
                       sizeof(VID_T) * this->num_vertexes_);
      this->in_edges_ = (VID_T*)malloc(sizeof(VID_T) * this->num_in_edges_);
      folly::readNoInt(meta_in_fd, this->in_edges_,
                       sizeof(VID_T) * this->num_in_edges_);

      this->out_offset_ = (VID_T*)malloc(sizeof(VID_T) * this->num_vertexes_);
      folly::readNoInt(meta_out_fd, this->out_offset_,
                       sizeof(VID_T) * this->num_vertexes_);

      this->out_edges_ = (VID_T*)malloc(sizeof(VID_T) * this->num_out_edges_);
      folly::readNoInt(meta_out_fd, this->out_edges_,
                       sizeof(VID_T) * this->num_out_edges_);

      // Load vertexes label
      this->vdata_ = (VDATA_T*)malloc(sizeof(VDATA_T) * this->num_vertexes_);
      folly::preadNoInt(vertex_data_fd, this->vdata_,
                        sizeof(VDATA_T) * this->num_vertexes_, sizeof(VDATA_T));

      // Load local_id to global_id
      this->localid_to_globalid_ =
          (VID_T*)malloc(sizeof(VID_T) * this->num_vertexes_ * 2);
      folly::readNoInt(localid_to_globalid_fd, this->localid_to_globalid_,
                       sizeof(VID_T) * this->num_vertexes_ * 2);
      for (int i = 0; i < 10; i++) {
        cout << this->localid_to_globalid_[i] << endl;
        cout << this->localid_to_globalid_[i + this->num_vertexes_] << endl;
        // map_local_id_to_global_id_->insert(1, 2);
      }
    }
  };
};

}  // namespace graphs
}  // namespace minigraph
#endif