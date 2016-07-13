#ifndef NDB_COMMON_THREAD_H_
#define NDB_COMMON_THREAD_H_

#include "ndb/common/result.h"

namespace ndb {

class Thread {
 public:
  virtual ~Thread() { Join(); }

  virtual void Main() = 0;

  Result Loop() {
    thread_ = std::thread([this]() {
        while (!stop_) {
          Main();
        }
      });
    return Result::OK();
  }

  void Stop() {
    stop_ = true;
  }

  void Join() {
    Stop();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  bool stop_ {false};
  std::thread thread_;
};

}  // namespace ndb

#endif /* NDB_COMMON_THREAD_H_ */
