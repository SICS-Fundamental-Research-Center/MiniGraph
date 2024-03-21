#pragma once

#include <math.h>

#include <string>
#include <vector>
#include <condition_variable>

#include <folly/AtomicHashMap.h>

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "utility/thread_pool.h"
#include "utility/logging.h"

#define BIG_CONSTANT(x) (x##LLU)

struct Path {
  std::string meta_pt;
  std::string data_pt;
  std::string vdata_pt;
};

//struct EdgeListPt {
//  std::string edges_pt;
//  std::string v_label_pt;
//};

template <typename VID_T, typename GID_T>
class VertexDependencies {
 public:
  VID_T vid_;
  std::vector<GID_T>* who_need_ = nullptr;
  std::vector<GID_T>* who_provide_ = nullptr;

  VertexDependencies(const VID_T vid) {
    vid_ = vid;
    who_need_ = new std::vector<GID_T>;
    who_provide_ = new std::vector<GID_T>;
  };
  ~VertexDependencies() = default;

  void ShowVertexDependencies() {
    std::cout << "___vid: " << vid_ << "___" << std::endl;

    if (who_need_ != nullptr) {
      std::cout << "   who need: ";
      for (auto& iter : *who_need_) {
        std::cout << iter << ", ";
      }
      std::cout << std::endl;
    }

    if (who_provide_ != nullptr) {
      std::cout << "   who provide: ";
      for (auto& iter : *who_provide_) {
        std::cout << iter << ", ";
      }
      std::cout << std::endl;
    }
    return;
  }
};

// reference http://www.cs.cmu.edu/~pbbs/benchmarks/graphIO.html
enum GraphFormat {
  edgelist_csv,
  weight_edgelist_csv,
  edgelist_bin,
  csr_bin,
  immutable_csr_bin,
  batch_relation_csv,
  relation_csv,
  relation_bin
};

template <typename T>
size_t Hash(T k) {
  //k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k = k >> 1;
  return k;
}

template <typename T1, typename T2>
struct IsSameType {
  operator bool() { return false; }
};

template <typename T1>
struct IsSameType<T1, T1> {
  operator bool() { return true; }
};

struct StatisticInfo {
  size_t num_edges = 0;
  size_t num_vertexes = 0;
  size_t num_active_vertexes = 0;
  size_t sum_in_degree = 0;
  size_t sum_out_degree = 0;
  size_t num_iters = 0;
  size_t current_iter = 0;
  size_t sum_active_out_border_vertexes = 0;
  size_t sum_active_in_border_vertexes = 0;
  size_t sum_out_border_vertexes = 0;
  size_t sum_in_border_vertexes = 0;
  size_t sum_dlv_times_dgv = 0;
  size_t sum_dlv_times_dlv = 0;
  size_t sum_dgv_times_dgv = 0;
  size_t sum_dlv = 0;
  size_t sum_dgv = 0;
  size_t level = 0;
  // type denotes the level of statistic information in which type = 0
  // is in Active vertexes level, type = 1 denotes the information is in
  // fragment level, type 2 means in Graph level.
  size_t inc_type = 0;
  // inc_type =0 denotes the statistic information is collected in PEval, while
  // inc_type = 1 means in IncEval.

  float elapsed_time = 0;

  void ShowInfo() {
    LOG_INFO(inc_type, ",", num_iters, ",", num_active_vertexes, ",", sum_dlv,
             ",", sum_dgv, ",", sum_dlv_times_dlv, ",", sum_dlv_times_dgv, ",",
             sum_dgv_times_dgv, ",", sum_in_border_vertexes, ",",
             sum_out_border_vertexes, ",", num_edges, ",", num_vertexes, ",",
             elapsed_time);
    return;
  };

  StatisticInfo(size_t t = 0, size_t l = 0) {
    level = l;
    inc_type = t;
  };

};

inline std::pair<vid_t, vid_t> SplitEdge(const std::string& str,
                                         char* pattern) {
  char* strc = new char[strlen(str.c_str()) + 1];
  strcpy(strc, str.c_str());
  char* tmpStr = strtok(strc, pattern);
  vid_t out[2];
  for (size_t i = 0; i < 2; i++) {
    out[i] = atoll(tmpStr);
    tmpStr = strtok(NULL, pattern);
  }
  delete[] strc;
  return std::make_pair(out[0], out[1]);
};
