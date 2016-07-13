#ifndef NDB_SERVER_BUFFER_H_
#define NDB_SERVER_BUFFER_H_

#include "ndb/common/result.h"

namespace ndb {

class RecvBuf {
 public:
  ~RecvBuf() { free(data_); }

  const char* data() const { return data_; }

  size_t size() const { return size_; }

  Result Recv(int fd, size_t size) {
    if (size > capp_ - size_) {
      capp_ = size_ + size;
      data_ = (char*) realloc(data_, capp_);
    }
    while (size != 0) {
      ssize_t n = read(fd, data_ + size_, size);
      if (n < 0) {
        if (errno == EAGAIN) {
          return Result::OK();
        } else {
          return Result::Errno("read()");
        }
      }
      if (n == 0) {
        return Result::Error("closed.");
      }
      size -= n;
      size_ += n;
    }
    return Result::OK();
  }

  void Skip(size_t pos) {
    if (pos >= size_) {
      size_ = 0;
      return;
    }
    memmove(data_, data_ + pos, size_ - pos);
    size_ -= pos;
  }

 private:
  char* data_ {NULL};
  size_t size_ {0};
  size_t capp_ {0};
};

class SendBuf {
 public:
  const char* data() const { return data_; }

  size_t size() const { return size_; }

  Result Send(int fd, size_t size) {
    if (size > size_) {
      size = size_;
    }
    while (size != 0) {
      ssize_t n = write(fd, data_, size);
      if (n == -1) {
        if (errno == EAGAIN) {
          return Result::OK();
        } else {
          return Result::Errno("write()");
        }
      }
      size -= n;
      data_ += n;
      size_ -= n;
    }
    return Result::OK();
  }

  void Attach(const char* data, size_t size) {
    data_ = data;
    size_ = size;
  }

 private:
  const char* data_ {NULL};
  size_t size_ {0};
};

}  // namespace ndb

#endif /* NDB_SERVER_BUFFER_H_ */
