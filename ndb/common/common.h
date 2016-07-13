#ifndef NDB_COMMON_COMMON_H_
#define NDB_COMMON_COMMON_H_

// C Headers.
#include <assert.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

// C++ Headers.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// External libraries.
#include <curl/curl.h>
#include <rapidjson/writer.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>

#define NDB_TRY(expression) do {                    \
    auto _r = (expression);                         \
    if (!_r.ok()) return _r;                        \
  } while (0)

#define NDB_ASSERT(condition) if (!(condition)) {                     \
    fprintf(stderr, "[%s:%d] Assertion \"" #condition "\" failed.\n", \
            __FILE__, __LINE__);                                      \
    abort();                                                          \
  }

#define NDB_ASSERT_OK(expression) do {                                          \
    auto _r = (expression);                                                     \
    if (!_r.ok()) {                                                             \
      fprintf(stderr, "[%s:%d] ERROR: %s\n", __FILE__, __LINE__, _r.message()); \
      exit(_r.code());                                                          \
    }                                                                           \
  } while (0)

namespace ndb {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::microseconds;

// Get current time in seconds.
inline uint64_t gettime() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<seconds>(now).count();
}

// Get current time in milliseconds.
inline uint64_t getmstime() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<milliseconds>(now).count();
}

// Get current time in microseconds.
inline uint64_t getustime() {
  auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<microseconds>(now).count();
}

// Transform input to uppercase.
inline std::string stoupper(const std::string& input) {
  auto output = input;
  std::transform(output.begin(), output.end(), output.begin(), toupper);
  return output;
}

}  // namespace ndb

#endif /* NDB_COMMON_COMMON_H_ */
