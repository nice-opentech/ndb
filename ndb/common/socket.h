#ifndef NDB_COMMON_SOCKET_H_
#define NDB_COMMON_SOCKET_H_

#include "ndb/common/result.h"

namespace ndb {

class Socket {
 public:
  Socket(int fd=-1) { Open(fd); }

  virtual ~Socket() { Close(); }

  int fd() const { return fd_; }

  void Open(int fd);

  void Close();

  bool IsOpen() const { return fd_ != -1; }

  Result Accept(int* connfd);

  Result Listen(const std::string& address);

  Result Connect(const std::string& address);

  void SetNonBlock();

  void SetReuseAddr();

  std::string GetSockName() const;

  std::string GetPeerName() const;

 private:
  Result Listen(const sockaddr* addr, socklen_t addrlen);

  Result Connect(const sockaddr* addr, socklen_t addrlen);

 private:
  int fd_ {-1};
};

bool ParseAddress(const std::string& address, std::string* host, std::string* port);

}  // namespace ndb

#endif /* NDB_COMMON_SOCKET_H_ */
