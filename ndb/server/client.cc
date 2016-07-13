#include "ndb/server/client.h"

namespace ndb {

Client::Client(int fd)
    : Socket(fd),
      name_(GetPeerName()),
      active_time_(gettime()) {
}

bool Client::HasTimeout() const {
  return (gettime() - active_time_) >= timeout_;
}

Result Client::HandleEvent(IOLoop::Event event) {
  active_time_ = gettime();
  if (IOLoop::Readable(event)) {
    return RecvRequest();
  } else if (IOLoop::Writable(event)) {
    return SendResponse();
  }
  return Result::OK();
}

Result Client::RecvRequest() {
  NDB_TRY(rbuf_.Recv(fd(), 1024 * 1024));
  if (rbuf_.size() > bufsize_) {
    return Result::Error("Read buffer limit exceed %zu", bufsize_);
  }

  size_t pos = 0;
  while (pos < rbuf_.size()) {
    ssize_t n = builder_.Parse(rbuf_.data() + pos, rbuf_.size() - pos);
    if (n < 0) {
      return Result::Error("Invalid request: %zd", n);
    }
    pos += n;

    if (!builder_.IsFinished()) {
      break;
    }

    PutRequest(builder_.Finish());
    builder_.Reset();
  }
  if (pos > 0) {
    rbuf_.Skip(pos);
  }
  return Result::OK();
}

Result Client::SendResponse() {
  if (sbuf_.size() == 0) {
    if (!HasResponse()) {
      return Result::OK();
    }
    const auto& response = GetResponse();
    sbuf_.Attach(response.data(), response.size());
  }

  NDB_TRY(sbuf_.Send(fd(), sbuf_.size()));
  if (sbuf_.size() == 0) {
    PopResponse();
  }
  return Result::OK();
}

}  // namespace ndb
