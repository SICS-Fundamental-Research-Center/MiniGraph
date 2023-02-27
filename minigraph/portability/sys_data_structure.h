#pragma once

#include <condition_variable>
#include <math.h>
#include <string>
#include <vector>

#include <folly/AtomicHashMap.h>

#include "graphs/graph.h"
#include "graphs/immutable_csr.h"
#include "portability/sys_data_structure.h"
#include "utility/thread_pool.h"

#define BIG_CONSTANT(x) (x##LLU)

struct CSRPt {
  std::string meta_pt;
  std::string data_pt;
  std::string vdata_pt;
};

struct EdgeListPt {
  std::string edges_pt;
  std::string v_label_pt;
};

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
  edge_list_csv,
  weight_edge_list_csv,
  edge_list_bin,
  csr_bin,
  immutable_csr_bin
};

template <typename T>
size_t Hash(T k) {
  // k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  // k = k >> 1;
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

// template <typename T>
// void swap(T& a, T& b) noexcept {
//   T temp = std::move(a);
//   a = std::move(b);
//   b = std::move(temp);
// }

inline std::pair<vid_t, vid_t> SplitEdge(const std::string& str,
                                         char* pattern) {
  char* strc = new char[strlen(str.c_str()) + 1];
  strcpy(strc, str.c_str());
  char* tmpStr = strtok(strc, pattern);
  vid_t out[2];
  for (size_t i = 0; i < 2; i++) {
    out[i] = atoi(tmpStr);
    tmpStr = strtok(NULL, pattern);
  }
  delete[] strc;
  return std::make_pair(out[0], out[1]);
};
