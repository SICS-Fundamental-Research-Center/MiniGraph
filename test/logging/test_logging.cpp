#include "utility/logging.h"
#include <iostream>
using std::cout;
using std::endl;

#include <folly/logging/Init.h>
#include <folly/logging/LogConfigParser.h>
#include <folly/logging/LoggerDB.h>
//#include <folly/logging/test/ConfigHelpers.h>
#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/logging/FileHandlerFactory.h>
#include <folly/logging/LoggerDB.h>
#include <folly/portability/GFlags.h>
#include <folly/portability/GTest.h>
#include <folly/test/TestUtils.h>

using folly::initLogging;
using folly::LoggerDB;
using folly::parseLogConfig;

static minigraph::utility::Logging staticInitialized("static");

// FOLLY_INIT_LOGGING_CONFIG(
//     ".=WARNING,folly=INFO;default:async=true,sync_level=WARNING");
// replaceExisting = */ true);
// folly::LoggerDB::get()->registerHandlerFactory(
//       std::make_unique<folly::FileHandlerFactory>());

namespace folly {}
FOLLY_INIT_LOGGING_CONFIG(
    "INFO:default:x; default=stream:stream=stderr; x=file:path=log/x.log"
    );

int main(int argc, char** argv) {
  folly::LoggerDB::get().registerHandlerFactory(
      std::make_unique<folly::FileHandlerFactory>(), true);
  XLOG(DBG) << "log messages less than INFO will be ignored before initLogging";
  // XLOG(ERR) << "error messages before initLogging() will be logged to
  // stderr";

  // folly::Init() will automatically initialize the logging settings
  // based on the FOLLY_INIT_LOGGING_CONFIG declaration above and the
  // --logging command line flag.
  folly::Init init(&argc, &argv);

  // All XLOG() statements in this file will log to the category
  // folly.logging.example.main
  XLOG(INFO, "now the normal log settings have been applied");

  XLOG(DBG1, "log arguments are concatenated: ", 12345, ", ", 92.0);
  XLOGF(DBG1, "XLOGF supports {}-style formatting: {:.3f}", "python", 1.0 / 3);
  XLOG(DBG2) << "streaming syntax is also supported: " << 1234;
  XLOG(DBG2, "if you really want, ", "you can even")
      << " mix function-style and streaming syntax: " << 42;
  XLOGF(DBG3, "and {} can mix {} style", "you", "format") << " and streaming";

  minigraph::utility::Logging("foo");
  XLOG(INFO) << "main returning";

  folly::Logger eventLogger("eden.events");
  XLOG(INFO) << "hello world!";

  FB_LOG(eventLogger, INFO) << "something happened";
  XLOG(INFO, "the number is ", 2 + 2);
  return 0;
}