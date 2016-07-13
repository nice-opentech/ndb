#ifndef NDB_COMMAND_COMMON_H_
#define NDB_COMMAND_COMMON_H_

#include "ndb/server/server.h"
#include "ndb/engine/engine.h"
#include "ndb/command/macros.h"

namespace ndb {

// Command options.
#define NDB_OPTION_NX   (1 << 0)
#define NDB_OPTION_XX   (1 << 1)
#define NDB_OPTION_EX   (1 << 2)
#define NDB_OPTION_CH   (1 << 3)
#define NDB_OPTION_INCR (1 << 4)

// Parse int64.
bool ParseInt64(const std::string& s, int64_t* i);

// Parse uint64.
bool ParseUint64(const std::string& s, uint64_t* u);

// Parse pruning as [min|minscore|max|maxscore].
bool ParsePruning(const std::string& s, Pruning* pruning);

// Parse namespace as [nsname:id|nsname_id].
bool ParseNamespace(const Slice& s, std::string* nsname, std::string* id);

// Parse [LIMIT offset count] from request.
bool ParseLimit(const Request& request, size_t idx, int64_t* offset, int64_t* count);

// Make sure 0 <= start <= stop < length.
bool CheckLimit(int64_t length, int64_t* start, int64_t* stop);

// Parse [min max] or [max min] from request.
bool ParseRange(const Request& request, size_t idx, int64_t* min, int64_t* max, bool reverse = false);

// Check increment for overflow or underflow.
bool CheckIncrementBound(int64_t origin, int64_t increment);

// Convert int64 or bytes value to int64.
bool ConvertValueToInt64(const Value& value, int64_t* int64);

// Convert value to bulk string response.
Response ConvertValueToBulk(const Value& value);

// Change s to a string >= s.
std::string FindNextSuccessor(const std::string& s);

// Get multiple values with different keys of different namespaces.
// WARN: Don't use this to get members of collection data types,
// use Namespace::MultiGet() instead.
std::vector<Result> GenericMGET(Engine* engine, const std::vector<Slice>& keys, std::vector<Value>* values);

// Namespace commands statistics.
class NSStats {
 public:
  void add(const std::string& nsname, const std::string& cmdname) {
    std::unique_lock<std::mutex> lock(lock_);
    nsstats_[nsname]["*"].fetch_add(1, std::memory_order_relaxed);
    nsstats_[nsname][cmdname].fetch_add(1, std::memory_order_relaxed);
  }

  Stats GetStats(const std::string& nsname = "") {
    Stats stats;
    if (nsname == "") {
      for (const auto& cmdstat : nsstats_) {
        for (const auto& it : cmdstat.second) {
          stats.insert(cmdstat.first + "_" + it.first, (uint64_t) it.second);
        }
      }
    } else {
      auto cmdstat = nsstats_.find(nsname);
      if (cmdstat != nsstats_.end()) {
        for (const auto& it : cmdstat->second) {
          stats.insert(cmdstat->first + "_" + it.first, (uint64_t) it.second);
        }
      }
    }
    return stats;
  }

 private:
  std::mutex lock_;
  // <nsname, <cmdname, calls>>
  std::map<std::string, std::map<std::string, std::atomic<uint64_t>>> nsstats_;
};

extern NSStats nsstats;

}  // namespace ndb

#endif /* NDB_COMMAND_COMMON_H_ */
