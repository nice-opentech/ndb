#ifndef NDB_NDB_H_
#define NDB_NDB_H_

#include "ndb/options.h"

namespace ndb {

struct NDB {
  bool stop {false};
  Options options;
  HashLock hashlock;
  Logger* logger {NULL};
  Server* server {NULL};
  Engine* engine {NULL};
  Command* command {NULL};
  Replica* replica {NULL};

  NDB(int argc, char* argv[]);
  ~NDB();
  Result Run();
};

extern NDB* ndb;

#define NDB_LOG_DEBUG(fmt, ...) if (ndb != NULL) {              \
    ndb->logger->Printf(Logger::kDebug, __FILE__, __LINE__, fmt, ## __VA_ARGS__); \
  }
#define NDB_LOG_INFO(fmt, ...)  if (ndb != NULL) {              \
    ndb->logger->Printf(Logger::kInfo, __FILE__, __LINE__, fmt, ## __VA_ARGS__); \
  }
#define NDB_LOG_WARN(fmt, ...)  if (ndb != NULL) {              \
    ndb->logger->Printf(Logger::kWarn, __FILE__, __LINE__, fmt, ## __VA_ARGS__); \
  }
#define NDB_LOG_ERROR(fmt, ...) if (ndb != NULL) {              \
    ndb->logger->Printf(Logger::kError, __FILE__, __LINE__, fmt, ## __VA_ARGS__); \
  }
#define NDB_LOG_FATAL(fmt, ...) if (ndb != NULL) {              \
    ndb->logger->Printf(Logger::kFatal, __FILE__, __LINE__, fmt, ## __VA_ARGS__); \
  }

}  // namespace ndb

#endif /* NDB_NDB_H_ */
