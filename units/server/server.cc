#include "units/units.h"

Client* Handle(Client* client) {
  while (client->HasRequest()) {
    client->PopRequest();
    client->PutResponse(Response::PONG());
  }
  return client;
}

void Bench(const std::string& address) {
  std::string host, port;
  NDB_ASSERT(ParseAddress(address, &host, &port));
  char bench[1024];
  snprintf(bench, sizeof(bench), "redis-benchmark -n 1024 -t ping -p %s", port.c_str());
  system(bench);
}

int Test(int argc, char *argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <address>\n", argv[0]);
    exit(EXIT_FAILURE);
  }

  Server::Options options;
  options.address = argv[1];
  options.max_clients = 1024;
  options.timeout_secs = 3;

  Server server(options);
  NDB_ASSERT_OK(server.Run(Handle));

  std::thread thread(Bench, options.address);

  for (int i = 0; i < 10; i++) {
    printf("[seconds: %d]\n", i);
    printf("%s\n", server.GetStats().Print());
    sleep(1);
  }

  thread.join();
  return EXIT_SUCCESS;
}
