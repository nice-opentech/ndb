#ifndef NDB_COMMAND_COMMAND_H_
#define NDB_COMMAND_COMMAND_H_

#include "ndb/command/common.h"
#include "ndb/thread/monitor.h"
#include "ndb/thread/synchro.h"

namespace ndb {

class Command {
 public:
  struct Options {
    std::string access_mode {"rw"};
    int max_arguments {4096};
    int slowlogs_maxlen {1024};
    int slowlogs_slower_than_usecs {10000};
  };

  struct Slowlog {
    uint64_t id;
    uint64_t usecs;
    Request request;
    time_t timestamp;
  };

  Command(const Options& options, Engine* engine);

  ~Command();

  Result Run();

  Client* ProcessClient(Client* client);

  std::deque<Slowlog> GetSlowlogs();

  Stats GetStats(const std::string& cmd = "") const;

 private:
  Response ProcessRequest(const Request& request);

  void UpdateCmdStats(const Request& request, uint64_t usecs);

 private:
  Options options_;
  Monitor monitor_;
  Synchro synchro_;

  uint64_t slowlogs_id_ {0};
  std::mutex slowlogs_lock_;
  std::deque<Slowlog> slowlogs_;

  struct Cmd {
    Response (*func)(const Request& request);
    const char* mode;
    int argc;
  };
  std::map<std::string, Cmd> cmds_;

  struct CmdStats {
    std::atomic<uint64_t> calls {0};
    std::atomic<uint64_t> usecs {0};
    std::atomic<uint64_t> slows {0};
  };
  std::map<std::string, CmdStats> cmdstats_;
};

}  // namspace ndb

#endif /* NDB_COMMAND_COMMAND_H_ */
