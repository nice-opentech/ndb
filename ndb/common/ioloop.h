#ifndef NDB_COMMON_IOLOOP_H_
#define NDB_COMMON_IOLOOP_H_

#include "ndb/common/result.h"

namespace ndb {

class IOLoop {
 public:
  typedef int Event;
  static const Event kReadable = (1 << 0);
  static const Event kWritable = (1 << 1);

  static bool Readable(Event event) { return event & kReadable; }
  static bool Writable(Event event) { return event & kWritable; }

  typedef std::function<void (int, Event)> Callback;

  IOLoop();

  ~IOLoop();

  Result Add(int fd, Event event);

  Result Mod(int fd, Event event);

  Result Del(int fd);

  Result Run(Callback cb, milliseconds timeout);

 private:
  struct Rep;
  Rep* rep_ {NULL};
};

}  // namespace ndb

#endif /* NDB_COMMON_IOLOOP_H_ */
