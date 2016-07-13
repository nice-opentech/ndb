#ifndef NDB_OPTIONS_H_
#define NDB_OPTIONS_H_

#include "ndb/center.h"
#include "ndb/common/logger.h"
#include "ndb/server/server.h"
#include "ndb/engine/engine.h"
#include "ndb/engine/hashlock.h"
#include "ndb/thread/replica.h"
#include "ndb/command/command.h"

namespace ndb {

struct Options {
  bool daemonize {true};
  std::string restore;
  Center::Options center;
  Logger::Options logger;
  Engine::Options engine;
  Server::Options server;
  Replica::Options replica;
  Command::Options command;

  Options();

  void Usage() const;

  Result Parse(int argc, char* argv[]);

  std::string Get(const std::string& name) const;

  Result Set(const std::string& name, const std::string& value);

  std::string GetNodeID() const;

  Result GetNamespaces(std::map<std::string, Configs>* nss) const;

 private:
  Result ParseOptions(int argc, char* argv[]);

  Result ParseConfigs(const std::string& filename);

  enum Type {
    kBool,
    kInt,
    kSize,
    kString,
  };

  struct Option {
    int id;
    Type type;
    std::string name;
    void* value;
  };

  std::map<std::string, Option> options_;
  std::unique_ptr<Center> center_;
};

}  // namespace ndb

#endif /* NDB_OPTIONS_H_ */
