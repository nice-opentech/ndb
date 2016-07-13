#ifndef NDB_CENTER_H_
#define NDB_CENTER_H_

#include "ndb/common/result.h"
#include "ndb/engine/namespace.h"

namespace ndb {

class Options;

class Center {
 public:
  struct Options {
    std::string address;
    std::string cluster;
    std::string node;
    std::string slaveof;
  };

  Center(const Options& options) : options_(options) {}

  Result GetMaster(std::string* address);

  Result PutMaster(const std::string& address);

  Result PutSlaves(const std::string& address);

  Result GetOptions(ndb::Options* options) const;

  Result GetNamespaces(std::map<std::string, Configs>* nss) const;

 private:
  std::string FormatURL(const char* fmt, ...) const;

  Result PutNode(const std::string& url, const std::string& address);

 private:
  Options options_;
};

}  // namespace ndb

#endif /* NDB_CENTER_H_ */
