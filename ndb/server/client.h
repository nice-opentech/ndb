#ifndef NDB_SERVER_CLIENT_H_
#define NDB_SERVER_CLIENT_H_

#include "ndb/common/ioloop.h"
#include "ndb/common/socket.h"
#include "ndb/server/buffer.h"
#include "ndb/server/request.h"
#include "ndb/server/response.h"

namespace ndb {

class Client : public Socket {
 public:
  Client(int fd = -1);

  const char* name() const { return name_.c_str(); }

  void set_bufsize(size_t bufsize) { bufsize_ = bufsize; }

  void set_timeout(seconds timeout) { timeout_ = timeout.count(); }

  bool HasTimeout() const;

  Result HandleEvent(IOLoop::Event event);

  // Request
  const Request& GetRequest() const { return requests_.front(); }
  bool HasRequest() const { return requests_.size() > 0; };
  void PopRequest() { requests_.pop(); }
  void PutRequest(Request&& request) { requests_.push(std::move(request)); }

  // Response
  const Response& GetResponse() const { return responses_.front(); }
  bool HasResponse() const { return responses_.size() > 0; };
  void PopResponse() { responses_.pop(); }
  void PutResponse(Response&& response) { responses_.push(std::move(response)); }

 private:
  Result RecvRequest();
  Result SendResponse();

 private:
  std::string name_;
  size_t bufsize_ {16 << 20};
  uint64_t timeout_ {60};
  uint64_t active_time_;
  RecvBuf rbuf_;
  SendBuf sbuf_;
  RequestBuilder builder_;
  std::queue<Request> requests_;
  std::queue<Response> responses_;
};

}  // namespace ndb

#endif /* NDB_SERVER_CLIENT_H_ */
