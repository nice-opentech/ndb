#ifndef NDB_COMMON_CHANNEL_H_
#define NDB_COMMON_CHANNEL_H_

#include "ndb/common/common.h"
#include "ndb/common/concurrentqueue.h"

namespace ndb {

template <class T>
class Channel {
 public:
  Channel(size_t maxsize) : maxsize_(maxsize) {
    fd_ = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
    NDB_ASSERT(fd_ != -1);
  }

  ~Channel() {
    while (true) {
      T* item = NULL;
      if (queue_.try_dequeue(item)) {
        delete item;
      } else {
        break;
      }
    }
    close(fd_);
  }

  int fd() const { return fd_; }

  size_t size() const { return queue_.size_approx(); }

  // Caller take ownership of item.
  T* Recv() {
    uint64_t count = 0;
    eventfd_read(fd_, &count);
    T* item = NULL;
    queue_.try_dequeue(item);
    return item;
  }

  // Callee take ownership of item.
  void Send(T* item) {
    if (queue_.size_approx() < maxsize_) {
      queue_.enqueue(item);
      eventfd_write(fd_, 1);
    } else {
      delete item;
    }
  }

 private:
  int fd_;
  size_t maxsize_ {4096};
  moodycamel::ConcurrentQueue<T*> queue_;
};

}  // namespace ndb

#endif /* NDB_COMMON_CHANNEL_H_ */
