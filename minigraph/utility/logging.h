#ifndef MINIGRAPH_UTILITY_LOGGING_H_
#define MINIGRAPH_UTILITY_LOGGING_H_

#include <folly/init/Init.h>
#include <folly/lang/SafeAssert.h>
#include <folly/logging/Init.h>
#include <folly/logging/LogCategoryConfig.h>
#include <folly/logging/LogHandlerConfig.h>
#include <folly/logging/xlog.h>
#include <gflags/gflags.h>
#include <iosfwd>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

namespace minigraph {

namespace utility {

class Logging {
 public:
  explicit Logging(folly::StringPiece str) : value_{str.str()} {
    XLOGF(DBG1, "Logging({}) constructed at {}", value_, fmt::ptr(this));
  }
  ~Logging();

  void doStuff();

 private:
  std::string value_;
};

}  // namespace utility

}  // namespace minigraph

#endif  // MINIGRAPH_UTILITY_LOGGING_H_