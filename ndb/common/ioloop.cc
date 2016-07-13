#include "ndb/common/ioloop.h"

namespace ndb {

struct IOLoop::Rep {
  int epfd {-1};
  std::vector<epoll_event> events;

  Rep() {
    epfd = epoll_create(1024);
    NDB_ASSERT(epfd != -1);
    events.resize(1024);
  }

  ~Rep() {
    if (epfd != -1) {
      close(epfd);
    }
  }

  Result Ctl(int op, int fd, Event event) {
    epoll_event e = {0};
    e.data.fd = fd;
    if (Readable(event)) e.events |= EPOLLIN;
    if (Writable(event)) e.events |= EPOLLOUT;
    if (epoll_ctl(epfd, op, fd, &e) == -1) {
      return Result::Errno("epoll_ctl()");
    }
    return Result::OK();
  }

  int Wait(milliseconds timeout) {
    return epoll_wait(epfd, &events[0], events.size(), timeout.count());
  }
};

IOLoop::IOLoop() {
  rep_ = new Rep();
}

IOLoop::~IOLoop() {
  delete rep_;
}

Result IOLoop::Add(int fd, Event event) {
  return rep_->Ctl(EPOLL_CTL_ADD, fd, event);
}

Result IOLoop::Mod(int fd, Event event) {
  return rep_->Ctl(EPOLL_CTL_MOD, fd, event);
}

Result IOLoop::Del(int fd) {
  return rep_->Ctl(EPOLL_CTL_DEL, fd, 0);
}

Result IOLoop::Run(Callback cb, milliseconds timeout) {
  int n = rep_->Wait(timeout);
  if (n == -1) {
    return Result::Errno("epoll_wait()");
  }

  for (int i = 0; i < n; i++) {
    auto fd = rep_->events[i].data.fd;
    auto events = rep_->events[i].events;
    if ((events & EPOLLIN) || (events & EPOLLHUP)) {
      cb(fd, kReadable);
    } else if ((events & EPOLLOUT) || (events & EPOLLERR)) {
      cb(fd, kWritable);
    }
  }

  return Result::OK();
}

}  // namespace ndb
