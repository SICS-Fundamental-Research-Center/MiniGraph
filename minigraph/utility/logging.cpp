#include "logging.h"

#include <folly/logging/FileHandlerFactory.h>
#include <folly/logging/Init.h>
#include <folly/logging/LogConfigParser.h>
#include <folly/logging/LoggerDB.h>

namespace minigraph {
namespace utility {
namespace logging {

void InitOrDie(const std::string& config_str) {
  folly::LoggerDB::get().registerHandlerFactory(
      std::make_unique<folly::FileHandlerFactory>(), true);
  folly::initLoggingOrDie(config_str);
}

void UpdateConfig(const std::string& config_str) {
  folly::LoggerDB::get().updateConfig(folly::parseLogConfig(config_str));
  folly::LoggerDB::get().flushAllHandlers();
}

void OverrideConfig(const std::string& config_str) {
  folly::LoggerDB::get().resetConfig(folly::parseLogConfig(config_str));
  folly::LoggerDB::get().flushAllHandlers();
}

}  // namespace logging
}  // namespace utility
}  // namespace minigraph
