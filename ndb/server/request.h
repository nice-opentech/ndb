#ifndef NDB_SERVER_REQUEST_H_
#define NDB_SERVER_REQUEST_H_

#include "ndb/common/common.h"

namespace ndb {

class Request {
 public:
  typedef std::vector<std::string> Arguments;

  Request(Arguments&& args) : args_(std::move(args)) {
    // Transform command to UPPERCASE.
    if (args_.size() > 0) {
      args_[0] = stoupper(args_[0]);
    }
  }

  size_t argc() const { return args_.size(); }

  const Arguments& args() const { return args_; }

  const std::string& args(int i) const { return args_[i]; }

  std::string join() const {
    std::string line;
    for (size_t i = 0; i < argc(); i++) {
      line += args(i) + " ";
    }
    return line;
  }

 private:
  Arguments args_;
};

class RequestBuilder {
 public:
  void Reset();

  ssize_t Parse(const char* input, size_t size);

  Request Finish();

  bool IsFinished() const { return finished_; }

 private:
  ssize_t ParseInline(const char* input, size_t size);
  ssize_t ParseMultiBulk(const char* input, size_t size);

 private:
  enum Type { kUnknown, kInline, kMultiBulk };
  Type type_ {kUnknown};
  ssize_t argc_ {-1};
  ssize_t argz_ {-1};
  Request::Arguments args_;
  bool finished_ {false};
};

}  // namespace ndb

#endif /* NDB_SERVER_REQUEST_H_ */
