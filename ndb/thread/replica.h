#ifndef NDB_COMMAND_REPLICA_H_
#define NDB_COMMAND_REPLICA_H_

#include "ndb/common/common.h"

namespace ndb {

class Replica : public Eventd {
 public:
  struct Options {
    std::string address;
    size_t replicate_limit {10000};
  };

  Replica(const Options& options, Engine* engine)
      : options_(options), engine_(engine) {}

  Result Run();

  Stats GetStats() const;

 private:
  void HandleCron() override;
  void HandleEvent(int fd, IOLoop::Event event) override;

  void OpenClient();
  void CloseClient(const char* reason);

  void SendPSYNC();
  Result ProcessNamespace(Slice input);
  Result ProcessUpdates();
  uint64_t GetLatestSequenceNumber();

 private:
  Options options_;
  Engine* engine_ {NULL};
  uint64_t last_update_ {0};
  std::unique_ptr<Client> client_;
};

}  // namespace ndb

#endif /* NDB_COMMAND_REPLICA_H_ */
