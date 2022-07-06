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
};

}  // namespace message
}  // namespace minigraph
#endif  // MINIGRAPH_MESSAGE_MANAGER_BASE_H
