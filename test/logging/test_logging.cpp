#include "utility/logging.h"
#include <iostream>

using folly::initLogging;
using folly::LoggerDB;
using folly::parseLogConfig;
using std::cout;
using std::endl;

namespace folly {}
FOLLY_INIT_LOGGING_CONFIG(
    "DBG:default:x; default=stream:stream=stderr; x=file:path=log/x.log");

int main(int argc, char** argv) {
  folly::LoggerDB::get().registerHandlerFactory(
      std::make_unique<folly::FileHandlerFactory>(), true);
  XLOG(DBG) << "log messages less than INFO will be ignored before initLogging";

  //folly::Init init(&argc, &argv);

  XLOG(INFO, "now the normal log settings have been applied");

  XLOG(DBG1, "log arguments are concatenated: ", 12345, ", ", 92.0);
  XLOGF(DBG1, "XLOGF supports {}-style formatting: {:.3f}", "python", 1.0 / 3);
  XLOG(DBG2) << "streaming syntax is also supported: " << 1234;
  XLOG(DBG2, "if you really want, ", "you can even")
      << " mix function-style and streaming syntax: " << 42;
  XLOGF(DBG3, "and {} can mix {} style", "you", "format") << " and streaming";

  XLOG(INFO) << "main returning";

  folly::Logger eventLogger("eden.events");
  XLOG(INFO) << "hello world!";

  FB_LOG(eventLogger, INFO) << "something happened";
  XLOG(INFO, "the number is ", 2 + 2);
  return 0;
}