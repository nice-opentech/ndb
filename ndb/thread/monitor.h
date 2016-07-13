#ifndef NDB_COMMAND_MONITOR_H_
#define NDB_COMMAND_MONITOR_H_

#include "ndb/common/common.h"

namespace ndb {

class Monitor : public Eventd {
 public:
  ~Monitor();

  Result Run();

  // Callee take ownership of client.
  void AddClient(Client* client);

  void PutRequest(const Client* client);

 private:
  void HandleEvent(int fd, IOLoop::Event event) override;

 private:
  Channel<Response> channel_ {4096};
  std::mutex lock_;
  std::set<Client*> clients_;
};

}  // namespace ndb

#endif /* NDB_COMMAND_MONITOR_H_ */
