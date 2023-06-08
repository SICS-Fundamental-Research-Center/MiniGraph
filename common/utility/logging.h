#ifndef MINIGRAPH_UTILITY_LOGGING_H_
#define MINIGRAPH_UTILITY_LOGGING_H_

#include <folly/logging/xlog.h>

// Default logging configuration for stderr-only logging.
//
// Using this configuration string will set the root log category level to
// INFO, the "async" property to true and "sync_level" property to WARN.
// Setting "async" property ensures that we enable asynchronous logging but
// the "sync_level" flag specifies that all logs of the level WARN and above
// are processed synchronously.
inline constexpr char kDefaultConfigStderr[] =
    "INFO; default:async=true,sync_level=INFO";

// Default logging configuration for debugging (print all to stderr).
inline constexpr char kDebugConfig[] = "DEBUG; default:async=false";

// Default logging configuration for both stdout and file logging.
inline std::string DefaultConfigWithLogFile(const std::string& filepath) {
  return "INFO:default:f; default=stream:stream=stdout,async=true,"
         "sync_level=WARN; f=file:path=" +
         filepath;
}

// Initialize the logging system for a specific application.
// This function should be called at the beginning of a `main()` function, to
// make sure the provided configuration string is applied.
//
// Calling this function will fail if the provided `config_str` is not valid.
//
// If no configuration string is provided, the logging system is initialized
// with default config string: `kDefaultLogConfigStdout`.
void InitOrDie(const std::string& config_str = kDefaultConfigStderr);

// Update the initialized logging system, with the provided configurations
// in `config_str`.
void UpdateConfig(const std::string& config_str);
// Completely override the logging system. Using the provided configurations
// in `config_str` instead.
void OverrideConfig(const std::string& config_str);

// Define wrapper macro to log a FATAL-level string.
#define LOG_FATAL(...) XLOG(FATAL, ##__VA_ARGS__)
// Define wrapper macro to log a FATAL-level string,
// with python-style formatting.
#define LOGF_FATAL(...) XLOGF(FATAL, ##__VA_ARGS__)

// Define wrapper macro to log an ERROR-level string.
#define LOG_ERROR(...) XLOG(ERR, ##__VA_ARGS__)
// Define wrapper macro to log an ERROR-level string,
// with python-style formatting.
#define LOGF_ERROR(...) XLOGF(ERR, ##__VA_ARGS__)

// Define wrapper macro to log a WARN-level string.
#define LOG_WARN(...) XLOG(WARN, ##__VA_ARGS__)
// Define wrapper macro to log a WARN-level string,
// with python-style formatting.
#define LOGF_WARN(...) XLOGF(WARN, ##__VA_ARGS__)

// Define wrapper macro to log an INFO-level string.
#define LOG_INFO(...) XLOG(INFO, ##__VA_ARGS__)
// Define wrapper macro to log an INFO-level string,
// with python-style formatting.
#define LOGF_INFO(...) XLOGF(INFO, ##__VA_ARGS__)

// Define wrapper macro to log a DEBUG-level string.
#define LOG_DEBUG(...) XLOG(DBG, ##__VA_ARGS__)
// Define wrapper macro to log a DEBUG-level string,
// with python-style formatting.
#define LOGF_DEBUG(...) XLOGF(DBG, ##__VA_ARGS__)

#endif  // MINIGRAPH_UTILITY_LOGGING_H_