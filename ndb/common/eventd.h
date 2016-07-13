#ifndef NDB_COMMON_EVENTD_H_
#define NDB_COMMON_EVENTD_H_

#include "ndb/common/ioloop.h"
#include "ndb/common/thread.h"

namespace ndb {

class Eventd : public Thread {
 public:
  Eventd() {
    cb_ = [this](int fd, IOLoop::Event event) {
      this->HandleEvent(fd, event);
    };
  }

  ~Eventd() { Join(); }

  void Main() override {
    HandleCron();
    NDB_ASSERT_OK(ioloop_.Run(cb_, milliseconds(1000)));
  }

 protected:
  virtual void HandleCron() {}
  virtual void HandleEvent(int fd, IOLoop::Event event) {}

 protected:
  IOLoop ioloop_;
  IOLoop::Callback cb_;
};

}  // namespace ndb

#endif /* NDB_COMMON_THREAD_H_ */
