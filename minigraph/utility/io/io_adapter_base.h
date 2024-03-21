//
// Created by hsiaoko on 2022/3/14.
//

#ifndef MINIGRAPH_UTILITY_IO_IO_ADAPTER_BASE_H
#define MINIGRAPH_UTILITY_IO_IO_ADAPTER_BASE_H

#include <iostream>
#include <string>

#include <folly/AtomicHashArray.h>
#include <folly/AtomicHashMap.h>

#include "graphs/immutable_csr.h"

namespace minigraph {
namespace utility {
namespace io {

template <typename GID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class IOAdapterBase {
 public:
  IOAdapterBase(){};
  ~IOAdapterBase(){};

  void MakeDirectory(const std::string& pt) {
    std::string dir = pt;
    int len = dir.size();
    if (dir[len - 1] != '/') {
      dir[len] = '/';
      len++;
    }
    std::string temp;
    for (int i = 1; i < len; i++) {
      if (dir[i] == '/') {
        temp = dir.substr(0, i);
        if (access(temp.c_str(), 0) != 0) {
          if (mkdir(temp.c_str(), 0777) != 0) {
            VLOG(1) << "failed operaiton.";
          }
        }
      }
    }
  }

  bool Exist(const std::string& pt) const {
    struct stat buffer;
    return (stat(pt.c_str(), &buffer) == 0);
  }

  void Touch(const std::string& pt) {
    std::ofstream file(pt, std::ios::binary);
    file.close();
  };

};

}  // namespace io
}  // namespace utility
}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_IO_IO_ADAPTER_BASE_H
