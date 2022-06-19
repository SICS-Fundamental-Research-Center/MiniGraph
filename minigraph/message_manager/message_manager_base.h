#ifndef MINIGRAPH_MESSAGE_MANAGER_BASE_H
#define MINIGRAPH_MESSAGE_MANAGER_BASE_H

#include <string>

namespace minigraph {
namespace message {

class MessageManagerBase {
 public:
  MessageManagerBase() {}
  virtual ~MessageManagerBase() {}

  virtual void Init(std::string work_space) = 0;

  // virtual void BufferMessage() = 0;

  virtual void FlashMessageToSecondStorage() = 0;

  // virtual void Isfull() =0;

  // virtual void receive() = 0;

  // virtual void Serialized() = 0;
};

}  // namespace message
}  // namespace minigraph
#endif  // MINIGRAPH_MESSAGE_MANAGER_BASE_H
