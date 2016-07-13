#ifndef NDB_COMMAND_SYNCHRO_H_
#define NDB_COMMAND_SYNCHRO_H_

#include "ndb/common/common.h"

namespace ndb {

class Synchro : public Eventd {
 public:
  Synchro(Engine* engine) : engine_(engine) {}

  ~Synchro();

  Result Run();

  // Callee take ownership of client.
  void AddClient(Client* client);

 private:
  void CloseClient(int fd, const char* reason);
  void HandleEvent(int fd, IOLoop::Event event) override;

 private:
  class Replica;
  Engine* engine_ {NULL};
  std::mutex lock_;
  std::map<int, Replica*> replicas_;
};

}  // namespace ndb

#endif /* NDB_COMMAND_SYNCHRO_H_ */
