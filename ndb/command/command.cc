#include "ndb/ndb.h"

namespace ndb {

#define INSTALL(name, func, mode, argc)    \
  Response func(const Request& request);   \
  cmds_[name] = {func, mode, argc};

Command::Command(const Options& options, Engine* engine)
    : options_(options), synchro_(engine) {
  // Server
  INSTALL("PING",               CommandPING,               "",   1);
  INSTALL("ECHO",               CommandECHO,               "",   2);
  INSTALL("INFO",               CommandINFO,               "",  -2);
  INSTALL("BACKUP",             CommandBACKUP,             "r",  2);
  INSTALL("COMPACT",            CommandCOMPACT,            "w", -2);
  INSTALL("SLOWLOG",            CommandSLOWLOG,            "",  -2);
  INSTALL("SHUTDOWN",           CommandSHUTDOWN,           "",   1);

  // Namespace
  INSTALL("NSNEW",              CommandNSNEW,              "w",  2);
  INSTALL("NSDEL",              CommandNSDEL,              "w",  2);
  INSTALL("NSGET",              CommandNSGET,              "r",  3);
  INSTALL("NSSET",              CommandNSSET,              "w",  4);
  INSTALL("NSLIST",             CommandNSLIST,             "r",  1);

  // Keys
  INSTALL("TYPE",               CommandTYPE,               "r",  2);
  INSTALL("META",               CommandMETA,               "r",  2);
  INSTALL("KEYS",               CommandKEYS,               "r", -2);
  INSTALL("EXISTS",             CommandEXISTS,             "r", -2);
  INSTALL("DEL",                CommandDEL,                "w", -2);
  INSTALL("EXPIRE",             CommandEXPIRE,             "w",  3);
  INSTALL("EXPIREAT",           CommandEXPIREAT,           "w",  3);
  INSTALL("PEXPIRE",            CommandPEXPIRE,            "w",  3);
  INSTALL("PEXPIREAT",          CommandPEXPIREAT,          "w",  3);
  INSTALL("TTL",                CommandTTL,                "r",  2);
  INSTALL("PTTL",               CommandPTTL,               "r",  2);

  // Int
  INSTALL("INCR",               CommandINCR,               "w",  2);
  INSTALL("INCRBY",             CommandINCRBY,             "w",  3);
  INSTALL("DECR",               CommandDECR,               "w",  2);
  INSTALL("DECRBY",             CommandDECRBY,             "w",  3);

  // String
  INSTALL("GET",                CommandGET,                "r",  2);
  INSTALL("MGET",               CommandMGET,               "r", -2);
  INSTALL("SET",                CommandSET,                "w", -3);
  INSTALL("SETNX",              CommandSETNX,              "w",  3);
  INSTALL("SETEX",              CommandSETEX,              "w",  4);
  INSTALL("PSETEX",             CommandPSETEX,             "w",  4);
  INSTALL("MSET",               CommandMSET,               "w", -3);
  INSTALL("MSETNX",             CommandMSETNX,             "w", -3);

  // Set
  INSTALL("SADD",               CommandSADD,               "w", -3);
  INSTALL("SREM",               CommandSREM,               "w", -3);
  INSTALL("SCARD",              CommandSCARD,              "r",  2);
  INSTALL("SMEMBERS",           CommandSMEMBERS,           "r",  2);
  INSTALL("SISMEMBER",          CommandSISMEMBER,          "r",  3);

  // OSet
  INSTALL("OADD",               CommandOADD,               "w", -3);
  INSTALL("OREM",               CommandOREM,               "w", -3);
  INSTALL("OREMRANGEBYRANK",    CommandOREMRANGEBYRANK,    "w",  4);
  INSTALL("ORANGE",             CommandORANGE,             "r",  4);
  INSTALL("OREVRANGE",          CommandOREVRANGE,          "r",  4);
  INSTALL("ORANGEBYMEMBER",     CommandORANGEBYMEMBER,     "r", -4);
  INSTALL("OREVRANGEBYMEMBER",  CommandOREVRANGEBYMEMBER,  "r", -4);
  INSTALL("OCARD",              CommandOCARD,              "r",  2);
  INSTALL("OGETMAXLEN",         CommandOGETMAXLEN,         "r",  2);
  INSTALL("OGETFINITY",         CommandOGETMAXLEN,         "r",  2);

  // ZSet
  INSTALL("ZADD",               CommandZADD,               "w", -4);
  INSTALL("ZREM",               CommandZREM,               "w", -3);
  INSTALL("ZREMRANGEBYRANK",    CommandZREMRANGEBYRANK,    "w",  4);
  INSTALL("ZREMRANGEBYSCORE",   CommandZREMRANGEBYSCORE,   "w",  4);
  INSTALL("ZINCRBY",            CommandZINCRBY,            "w",  4);
  INSTALL("ZRANGE",             CommandZRANGE,             "r", -4);
  INSTALL("ZREVRANGE",          CommandZREVRANGE,          "r", -4);
  INSTALL("ZRANGEBYSCORE",      CommandZRANGEBYSCORE,      "r", -4);
  INSTALL("ZREVRANGEBYSCORE",   CommandZREVRANGEBYSCORE,   "r", -4);
  INSTALL("ZSCORE",             CommandZSCORE,             "r",  3);
  INSTALL("ZCARD",              CommandZCARD,              "r",  2);
  INSTALL("ZGETMAXLEN",         CommandZGETMAXLEN,         "r",  2);
  INSTALL("ZGETFINITY",         CommandZGETMAXLEN,         "r",  2);

  // XSet
  INSTALL("XADD",               CommandXADD,               "w", -4);
  INSTALL("XREM",               CommandZREM,               "w", -3);
  INSTALL("XREMRANGEBYRANK",    CommandZREMRANGEBYRANK,    "w",  4);
  INSTALL("XREMRANGEBYSCORE",   CommandZREMRANGEBYSCORE,   "w",  4);
  INSTALL("XINCRBY",            CommandZINCRBY,            "w",  4);
  INSTALL("XRANGE",             CommandZRANGE,             "r", -4);
  INSTALL("XREVRANGE",          CommandZREVRANGE,          "r", -4);
  INSTALL("XRANGEBYSCORE",      CommandZRANGEBYSCORE,      "r", -4);
  INSTALL("XREVRANGEBYSCORE",   CommandZREVRANGEBYSCORE,   "r", -4);
  INSTALL("XSCORE",             CommandZSCORE,             "r",  3);
  INSTALL("XCARD",              CommandZCARD,              "r",  2);
  INSTALL("XGETMAXLEN",         CommandZGETMAXLEN,         "r",  2);
  INSTALL("XGETFINITY",         CommandZGETMAXLEN,         "r",  2);

  // Hash
  INSTALL("HGET",               CommandHGET,               "r",  3);
  INSTALL("HMGET",              CommandHMGET,              "r", -3);
  INSTALL("HSET",               CommandHSET,               "w",  4);
  INSTALL("HSETNX",             CommandHSETNX,             "w",  4);
  INSTALL("HMSET",              CommandHMSET,              "w", -4);
  INSTALL("HDEL",               CommandHDEL,               "w", -3);
  INSTALL("HEXISTS",            CommandHEXISTS,            "r",  3);
  INSTALL("HSTRLEN",            CommandHSTRLEN,            "r",  3);
  INSTALL("HINCRBY",            CommandHINCRBY,            "w",  4);
  INSTALL("HLEN",               CommandHLEN,               "r",  2);
  INSTALL("HKEYS",              CommandHKEYS,              "r",  2);
  INSTALL("HVALS",              CommandHVALS,              "r",  2);
  INSTALL("HGETALL",            CommandHGETALL,            "r",  2);

  // List
  INSTALL("LPUSH",              CommandLPUSH,              "w", -3);
  INSTALL("LPUSHX",             CommandLPUSHX,             "w", -3);
  INSTALL("RPUSH",              CommandRPUSH,              "w", -3);
  INSTALL("RPUSHX",             CommandRPUSHX,             "w", -3);
  INSTALL("LPOP",               CommandLPOP,               "w",  2);
  INSTALL("RPOP",               CommandRPOP,               "w",  2);
  INSTALL("LREM",               CommandLREM,               "w",  4);
  INSTALL("LTRIM",              CommandLTRIM,              "w",  4);
  INSTALL("LSET",               CommandLSET,               "w",  4);
  INSTALL("LINDEX",             CommandLINDEX,             "r",  3);
  INSTALL("LRANGE",             CommandLRANGE,             "r",  4);
  INSTALL("LLEN",               CommandLLEN,               "r",  2);

  // Init commands' stats here so we don't need to protect them later.
  for (const auto& cmd : cmds_) {
    cmdstats_[cmd.first].calls = 0;
    cmdstats_[cmd.first].usecs = 0;
    cmdstats_[cmd.first].slows = 0;
  }
  // "*" is the sum of all commands' stats.
  cmdstats_["*"].calls = 0;
  cmdstats_["*"].usecs = 0;
  cmdstats_["*"].slows = 0;
}

Command::~Command() {
  monitor_.Stop();
  synchro_.Stop();
}

Result Command::Run() {
  NDB_TRY(monitor_.Run());
  NDB_TRY(synchro_.Run());
  return Result::OK();
}

Client* Command::ProcessClient(Client* client) {
  while (client->HasRequest()) {
    const auto& request = client->GetRequest();
    if (request.args(0) == "PSYNC") {
      synchro_.AddClient(client);
      return NULL;
    }
    if (request.args(0) == "MONITOR") {
      monitor_.AddClient(client);
      return NULL;
    }
    monitor_.PutRequest(client);
    client->PutResponse(ProcessRequest(request));
    client->PopRequest();
  }
  return client;
}

Response Command::ProcessRequest(const Request& request) {
  auto it = cmds_.find(request.args(0));
  if (it == cmds_.end()) {
    return Response::InvalidCommand();
  }
  const auto& cmd = it->second;

  if ((int) request.argc() > options_.max_arguments) {
    return Response::InvalidArgument();
  }

  if ((cmd.argc >= 0 && (int) request.argc() != cmd.argc) ||
      (cmd.argc <  0 && (int) request.argc() < -cmd.argc)) {
    return Response::InvalidArgument();
  }

  if (options_.access_mode.find(cmd.mode) == options_.access_mode.npos) {
    return Response::PermissionDenied();
  }

  auto begin = getustime();
  auto response = cmd.func(request);
  UpdateCmdStats(request, getustime() - begin);
  return response;
}

std::deque<Command::Slowlog> Command::GetSlowlogs() {
  std::unique_lock<std::mutex> lock(slowlogs_lock_);
  return slowlogs_;
}

Stats Command::GetStats(const std::string& cmd) const {
  Stats stats;
  if (cmd == "") {
    for (const auto& it : cmdstats_) {
      stats.insert("calls_" + it.first, (uint64_t) it.second.calls);
      stats.insert("usecs_" + it.first, (uint64_t) it.second.usecs);
      stats.insert("slows_" + it.first, (uint64_t) it.second.slows);
    }
  } else {
    auto it = cmdstats_.find(cmd);
    if (it != cmdstats_.end()) {
      stats.insert("calls_" + it->first, (uint64_t) it->second.calls);
      stats.insert("usecs_" + it->first, (uint64_t) it->second.usecs);
      stats.insert("slows_" + it->first, (uint64_t) it->second.slows);
    }
  }
  return stats;
}

#define INCRBY(c, inc) c.fetch_add(inc, std::memory_order_relaxed)

void Command::UpdateCmdStats(const Request& request, uint64_t usecs) {
  const auto& cmd = request.args(0);
  INCRBY(cmdstats_[cmd].calls, 1);
  INCRBY(cmdstats_[cmd].usecs, usecs);
  INCRBY(cmdstats_["*"].calls, 1);
  INCRBY(cmdstats_["*"].usecs, usecs);
  // Slowlogs
  if (usecs > (uint64_t) options_.slowlogs_slower_than_usecs) {
    std::unique_lock<std::mutex> lock(slowlogs_lock_);
    slowlogs_.push_front({slowlogs_id_++, usecs, request, time(NULL)});
    while (slowlogs_.size() > (size_t) options_.slowlogs_maxlen) {
      slowlogs_.pop_back();
    }
    INCRBY(cmdstats_[cmd].slows, 1);
    INCRBY(cmdstats_["*"].slows, 1);
  }
}

}  // namespace ndb
