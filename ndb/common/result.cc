#include "ndb/common/result.h"

namespace ndb {

Result Result::Error(const char* fmt, ...) {
  int code = kError;
  char message[1024];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(message, sizeof(message), fmt, ap);
  va_end(ap);
  return Result(code, message);
}

Result Result::Errno(const char* fmt, ...) {
  int code = errno;
  char message[1024];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(message, sizeof(message), fmt, ap);
  va_end(ap);
  snprintf(message+n, sizeof(message)-n, ": %s", strerror(code));
  return Result(code, message);
}

}  // namespace ndb
