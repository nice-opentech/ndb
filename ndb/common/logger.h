#ifndef NDB_COMMON_LOGGER_H_
#define NDB_COMMON_LOGGER_H_

#include "ndb/common/result.h"

namespace ndb {

class Logger {
 public:
  enum Level { kDebug, kInfo, kWarn, kError, kFatal };

  struct Options {
    Level level {kInfo};
    std::string filename {"nicedb.log"};
  };
  static const char* FormatLevel(Level level);
  static bool ParseLevel(const std::string& s, Level* level);

  Logger(const Options& options);

  ~Logger();

  Result Open();

  void Printf(Level level, const char *file, int line, const char* fmt, ...);
  void Printf(Level level, const char* fmt, ...);

 private:
  Options options_;
  FILE* fp_ {NULL};
};

}  // namespace ndb

#endif /* NDB_COMMON_LOGGER_H_ */
