#ifndef MINIGRAPH_MESSAGE_MANAGER_BASE_H
#define MINIGRAPH_MESSAGE_MANAGER_BASE_H

#include <string>

namespace minigraph {
namespace message {

class MessageManagerBase {
 public:
  MessageManagerBase() {}
  virtual ~MessageManagerBase() {}

  virtual void Init(const std::string work_space,
                    const bool load_dependencies) = 0;

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
};

}  // namespace message
}  // namespace minigraph
#endif  // MINIGRAPH_MESSAGE_MANAGER_BASE_H
