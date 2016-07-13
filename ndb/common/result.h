#ifndef NDB_COMMON_RESULT_H_
#define NDB_COMMON_RESULT_H_

#include "ndb/common/common.h"

namespace ndb {

class Result {
 public:
  static Result OK() { return Result(kOk); }
  static Result NotFound() { return Result(kNotFound); }
  static Result Error(const char* fmt, ...);
  static Result Errno(const char* fmt, ...);

  explicit Result(int code = kOk) : code_(code) {}

  bool ok() const { return code() == kOk; }

  bool IsNotFound() const { return code() == kNotFound; }

  int code() const { return code_; }

  const char* message() const { return message_.c_str(); }

 private:
  Result(int code, const char* message) : code_(code), message_(message) {}

 private:
  static const int kOk = 0;
  static const int kError = -1;
  static const int kNotFound = -2;

  int code_ {kOk};
  std::string message_;
};

}  // namespace ndb

#endif /* NDB_COMMON_RESULT_H_ */
