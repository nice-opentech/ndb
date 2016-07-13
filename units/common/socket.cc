#include "units/units.h"

void ServerMain(const std::string& address) {
  Socket socket;
  NDB_ASSERT_OK(socket.Listen(address));
  NDB_ASSERT(socket.IsOpen());
  printf("listen: %s\n", socket.GetSockName().c_str());

  while (true) {
    int connfd = -1;
    NDB_ASSERT_OK(socket.Accept(&connfd));
    if (connfd == -1) {
      sleep(1);
      continue;
    }
    Socket client(connfd);
    printf("accept: %s\n", client.GetPeerName().c_str());
    char message[1024] = {0};
    read(client.fd(), message, sizeof(message));
    printf("message: %s\n", message);
    break;
  }
}

void ClientMain(const std::string& address) {
  Socket client;
  NDB_ASSERT_OK(client.Connect(address));
  NDB_ASSERT(client.IsOpen());
  printf("server: %s\n", client.GetPeerName().c_str());
  printf("client: %s\n", client.GetSockName().c_str());
  char message[] = "Hello, ndb!";
  NDB_ASSERT(write(client.fd(), message, sizeof(message)) == sizeof(message));
}

int Test(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <address>\n", argv[0]);
    return EXIT_FAILURE;
  }

  std::thread server(ServerMain, argv[1]);
  sleep(1);
  std::thread client(ClientMain, argv[1]);
  client.join();
  server.join();

  return EXIT_SUCCESS;
}
