#include "ndb/ndb.h"

namespace ndb {

Result Replica::Run() {
  if (options_.address.size() == 0) {
    return Result::OK();
  }
  return Loop();
}

void Replica::HandleCron() {
  if (client_ == NULL) {
    OpenClient();
  }
}

void Replica::HandleEvent(int fd, IOLoop::Event event) {
  auto r = client_->HandleEvent(event);
  if (!r.ok()) {
    CloseClient(r.message());
    return;
  }

  auto sequence = GetLatestSequenceNumber();

  if (client_->HasRequest()) {
    r = ProcessUpdates();
    if (!r.ok()) {
      CloseClient(r.message());
      return;
    }
    // Send another PSYNC after processed.
    SendPSYNC();
    last_update_ = getmstime();
  }

  // No progress, wait for a while.
  if (sequence == GetLatestSequenceNumber()) {
    usleep(10 * 1000);
  }
}

void Replica::OpenClient() {
  client_.reset(new Client());

  auto r = client_->Connect(options_.address);
  if (!r.ok()) {
    NDB_LOG_ERROR("*REPLICA* connect: %s", r.message());
    client_.reset();
    return;
  }

  r = ioloop_.Add(client_->fd(), IOLoop::kReadable | IOLoop::kWritable);
  if (!r.ok()) {
    NDB_LOG_ERROR("*REPLICA* add client: %s", r.message());
    client_.reset();
    return;
  }

  SendPSYNC();  // Send a bootstrap PSYNC.
  NDB_LOG_INFO("*REPLICA* connected to %s", options_.address.c_str());
}

void Replica::CloseClient(const char* reason) {
  NDB_LOG_ERROR("*REPLICA* close client: %s", reason);
  ioloop_.Del(client_->fd());
  client_.reset();
  // Link is down, wait a few seconds and then reconnect.
  sleep(5);
}

void Replica::SendPSYNC() {
  if (!client_->HasResponse()) {
    // PSYNC sequence limit
    std::vector<std::string> bulks {"PSYNC"};
    bulks.push_back(std::to_string(GetLatestSequenceNumber()+1));
    bulks.push_back(std::to_string(options_.replicate_limit));
    client_->PutResponse(Response::Bulks(bulks));
  }
}

Result Replica::ProcessNamespace(Slice input) {
  return Result::OK();
}

Result Replica::ProcessUpdates() {
  while (client_->HasRequest()) {
    const auto& request = client_->GetRequest();
    if (request.argc() == 0 || request.args(0) != "UPDATES") {
      return Result::Error("Invalid updates: %s", request.join().c_str());
    }
    auto size = request.argc();
    for (size_t i = 1; i < size; i++) {
      rocksdb::WriteOptions wopts;
      rocksdb::WriteBatch batch(request.args(i));
      auto s = engine_->GetRocksDB()->Write(wopts, &batch);
      if (!s.ok()) return StatusToResult(s);
    }

    if (size > 1) {
      NDB_LOG_DEBUG("PSYNC : seq=%d, size=%d", GetLatestSequenceNumber(), size-1);
    }
    client_->PopRequest();
  }
  return Result::OK();
}

uint64_t Replica::GetLatestSequenceNumber() {
  return engine_->GetRocksDB()->GetLatestSequenceNumber();
}

Stats Replica::GetStats() const {
  Stats stats;
  stats.insert("peer", options_.address);
  stats.insert("link", client_ != NULL ? "up" : "down");
  stats.insert("last_update", last_update_);
  return stats;
}

}  // namespace ndb
