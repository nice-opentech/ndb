#include "units/units.h"

int Test(int argc, char* argv[]) {
  RequestBuilder builder;

  // Inline.
  {
    char message[] = "PING";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == 0);
  }
  {
    char message[] = "PING\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) < 0);
  }
  {
    char message[] = "PING\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
    NDB_ASSERT(builder.IsFinished());
    auto request = builder.Finish();
    NDB_ASSERT(request.argc() == 1);
    NDB_ASSERT(request.args(0) == "PING");
    builder.Reset();
  }
  {
    char message[] = "ECHO message\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
    NDB_ASSERT(builder.IsFinished());
    auto request = builder.Finish();
    NDB_ASSERT(request.argc() == 2);
    NDB_ASSERT(request.args(0) == "ECHO");
    NDB_ASSERT(request.args(1) == "message");
    builder.Reset();
  }

  // MultiBulk
  {
    char message[] = "*3";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == 0);
  }
  {
    char message[] = "*3\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) < 0);
  }
  {
    char message[] = "$3\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) < 0);
  }
  {
    char message[] = "*3\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
  }
  {
    char message[] = "$3\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
  }
  {
    char message[] = "SET\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
  }
  {
    char message[] = "$3\r\nfoo\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
  }
  {
    char message[] = "$3\r\nbar\r\n";
    NDB_ASSERT(builder.Parse(message, sizeof(message)) == sizeof(message)-1);
  }
  {
    NDB_ASSERT(builder.IsFinished());
    auto request = builder.Finish();
    NDB_ASSERT(request.argc() == 3);
    NDB_ASSERT(request.args(0) == "SET");
    NDB_ASSERT(request.args(1) == "foo");
    NDB_ASSERT(request.args(2) == "bar");
  }

  return EXIT_SUCCESS;
}
