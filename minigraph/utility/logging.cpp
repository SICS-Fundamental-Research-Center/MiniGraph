#include "minigraph/utility/logging.h"

namespace minigraph {

namespace utility {

Logging::~Logging() {
  // All XLOG() statements in this file will log to the category
  // folly.logging.example.lib
  XLOGF(DBG1, "Logging({}) at {} destroyed", value_, fmt::ptr(this));
}
}  // namespace utility

}  // namespace minigraph