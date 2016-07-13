#include "ndb/common/socket.h"

namespace ndb {

void Socket::Open(int fd) {
  Close();
  fd_ = fd;
  if (fd_ != -1) {
    SetNonBlock();
  }
}

void Socket::Close() {
  if (fd_ != -1) {
    close(fd_);
  }
  fd_ = -1;
}

Result Socket::Accept(int* connfd) {
  *connfd = accept(fd(), NULL, NULL);
  if (*connfd == -1) {
    if (errno != EAGAIN) {
      return Result::Errno("accept()");
    }
  }
  return Result::OK();
}

Result Socket::Listen(const std::string& address) {
  std::string host, port;
  if (!ParseAddress(address, &host, &port)) {
    return Result::Error("Invalid address");
  }

  addrinfo hint = {0};
  hint.ai_family = AF_UNSPEC;
  hint.ai_socktype = SOCK_STREAM;
  hint.ai_flags = AI_PASSIVE;

  addrinfo* info = NULL;
  int gai = getaddrinfo(host.c_str(), port.c_str(), &hint, &info);
  if (gai == -1) {
    return Result::Error("getaddrinfo(): %s", gai_strerror(gai));
  }
  if (info == NULL) {
    return Result::Error("Invalid address");
  }

  Result r;
  for (auto p = info; p != NULL; p = p->ai_next) {
    fd_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd_ == -1) {
      continue;
    }
    r = Listen(p->ai_addr, p->ai_addrlen);
    break;
  }
  freeaddrinfo(info);
  return r;
}

Result Socket::Listen(const sockaddr* addr, socklen_t addrlen) {
  SetNonBlock();
  SetReuseAddr();
  if (bind(fd(), addr, addrlen) == -1) {
    return Result::Errno("bind()");
  }
  if (listen(fd(), 1024) == -1) {
    return Result::Errno("listen()");
  }
  return Result::OK();
}

Result Socket::Connect(const std::string& address) {
  std::string host, port;
  if (!ParseAddress(address, &host, &port)) {
    return Result::Error("Invalid address");
  }

  addrinfo hint = {0};
  hint.ai_family = AF_UNSPEC;
  hint.ai_socktype = SOCK_STREAM;

  addrinfo* info = NULL;
  int gai = getaddrinfo(host.c_str(), port.c_str(), &hint, &info);
  if (gai == -1) {
    return Result::Error("getaddrinfo(): %s", gai_strerror(gai));
  }

  Result r;
  for (auto p = info; p != NULL; p = p->ai_next) {
    fd_ = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd_ == -1) {
      continue;
    }
    r = Connect(p->ai_addr, p->ai_addrlen);
    break;
  }
  freeaddrinfo(info);
  return r;
}

Result Socket::Connect(const sockaddr* addr, socklen_t addrlen) {
  SetNonBlock();
  if (connect(fd(), addr, addrlen) == -1) {
    if (errno != EINPROGRESS) {
      return Result::Errno("connect()");
    }
  }
  return Result::OK();
}

void Socket::SetNonBlock() {
  int flags = fcntl(fd_, F_GETFL);
  NDB_ASSERT(flags != -1);
  NDB_ASSERT(fcntl(fd_, F_SETFL, flags | O_NONBLOCK) != -1);
}

void Socket::SetReuseAddr() {
  const int on = 1;
  NDB_ASSERT(setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == 0);
}

static std::string GetAddrName(const sockaddr_storage* addr) {
  char host[INET6_ADDRSTRLEN] = {0};
  int port = 0;
  if (addr->ss_family == AF_INET) {
    auto sa = (sockaddr_in*) addr;
    inet_ntop(AF_INET, &sa->sin_addr, host, sizeof(host));
    port = ntohs(sa->sin_port);
  } else if (addr->ss_family == AF_INET6) {
    auto sa = (sockaddr_in6*) addr;
    inet_ntop(AF_INET6, &sa->sin6_addr, host, sizeof(host));
    port = ntohs(sa->sin6_port);
  }
  return std::string(host) + ":" + std::to_string(port);
}

std::string Socket::GetSockName() const {
  sockaddr_storage addr;
  socklen_t len = sizeof(addr);
  if (getsockname(fd(), (sockaddr*) &addr, &len) == -1) {
    return Result::Errno("getsockname()").message();
  }
  return GetAddrName(&addr);
}

std::string Socket::GetPeerName() const {
  sockaddr_storage addr;
  socklen_t len = sizeof(addr);
  if (getpeername(fd(), (sockaddr*) &addr, &len) == -1) {
    return Result::Errno("getpeername()").message();
  }
  return GetAddrName(&addr);
}

bool ParseAddress(const std::string& address, std::string* host, std::string* port) {
  auto pos = address.find(':');
  if (pos == address.npos) {
    return false;
  }
  *host = address.substr(0, pos);
  *port = address.substr(pos+1, address.npos);
  return true;
}

}  // namespace ndb
