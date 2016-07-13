#include "ndb/ndb.h"

namespace ndb {

// PING
Response CommandPING(const Request& request) {
  return Response::PONG();
}

// ECHO message
Response CommandECHO(const Request& request) {
  return Response::Bulk(request.args(1));
}

// INFO section [name]
Response CommandINFO(const Request& request) {
  auto name = request.args(1).c_str();

  Stats stats;
  if (strcasecmp(name, "engine") == 0) {
    if (request.argc() == 2) {
      stats = ndb->engine->GetStats();
    } else {
      auto ns = NDB_TRY_GETNS(request.args(2));
      stats = ns->GetStats();
    }
  } else if (strcasecmp(name, "backup") == 0) {
    stats = ndb->engine->GetBackup()->GetStats();
  } else if (strcasecmp(name, "server") == 0) {
    stats = ndb->server->GetStats();
  } else if (strcasecmp(name, "replica") == 0) {
    stats = ndb->replica->GetStats();
  } else if (strcasecmp(name, "command") == 0) {
    if (request.argc() == 2) {
      stats = ndb->command->GetStats();
    } else {
      auto cmd = stoupper(request.args(2));
      stats = ndb->command->GetStats(cmd);
    }
  } else if (strcasecmp(name, "nsstats") == 0) {
    if (request.argc() == 2) {
      stats = nsstats.GetStats();
    } else {
      stats = nsstats.GetStats(request.args(2));
    }
  } else {
    return Response::InvalidArgument();
  }

  std::string info;
  info.append("# ");
  info.append(name);
  info.append("\r\n");
  info.append(stats.Print());
  return Response::Bulk(info);
}

// BACKUP backup
Response CommandBACKUP(const Request& request) {
  auto dbname = request.args(1).c_str();
  auto backup = ndb->engine->GetBackup();
  if (strcasecmp(dbname, "STOP") == 0) {
    backup->Stop();
    return Response::OK();
  }
  return backup->Create(dbname);
}

// COMPACT namespace begin end
Response CommandCOMPACT(const Request& request) {
  auto ns = NDB_TRY_GETNS(request.args(1));
  Slice begin, end;
  if (request.argc() >= 3) begin = request.args(2);
  if (request.argc() >= 4) end = request.args(3);
  return ns->CompactRange(begin, end);
}

// SLOWLOG [LEN|GET] [count]
Response CommandSLOWLOG(const Request& request) {
  auto slowlogs = ndb->command->GetSlowlogs();

  auto name = request.args(1).c_str();
  if (strcasecmp(name, "LEN") == 0) {
    return Response::Int(slowlogs.size());
  }

  if (strcasecmp(name, "GET") == 0) {
    uint64_t count = slowlogs.size();
    if (request.argc() == 3) {
      if (!ParseUint64(request.args(2), &count)) {
        return Response::InvalidArgument();
      }
      if (count > slowlogs.size()) count = slowlogs.size();
    }
    auto res = Response::Size(count);
    for (auto it = slowlogs.begin(); it != slowlogs.end(); it++, count--) {
      if (count <= 0) break;
      res.AppendSize(4);
      res.AppendInt(it->id);
      res.AppendInt(it->timestamp);
      res.AppendInt(it->usecs);
      res.AppendBulks(it->request.args());
    }
    return res;
  }

  return Response::InvalidArgument();
}

// SHUTDOWN
Response CommandSHUTDOWN(const Request& request) {
  kill(getpid(), SIGQUIT);
  return Response::OK();
}

}  // namespace ndb
