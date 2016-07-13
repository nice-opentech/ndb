#include "units/units.h"

int Test(int argc, char* argv[]) {
  Configs configs;
  configs.set_expire(4096);
  configs.set_maxlen(2048);
  configs.set_pruning(Pruning::MAX);

  Configs options;
  options.set_expire(2048);
  options.set_maxlen(4096);
  options.set_pruning(Pruning::MIN);

  auto a = Value::FromInt64(1024);
  a.mutable_meta()->set_type(Meta::SET);
  a.mutable_meta()->set_maxlen(1);
  a.SetConfigs(configs);
  NDB_ASSERT(a.expire() / 100 == (getmstime() + configs.expire()) / 100);
  NDB_ASSERT(a.meta().maxlen() == configs.maxlen());
  NDB_ASSERT(a.meta().pruning() == configs.pruning());

  auto encoded = a.Encode();

  Value b;
  NDB_ASSERT_OK(b.Decode(encoded));
  b.SetConfigs(configs);
  b.SetConfigs(options);
  NDB_ASSERT(b.int64() == a.int64());
  NDB_ASSERT(b.expire() / 100 == (getmstime() + configs.expire()) / 100);
  NDB_ASSERT(b.meta().maxlen() == options.maxlen());
  NDB_ASSERT(b.meta().pruning() == options.pruning());

  Value c;
  uint64_t start = 0, stop = 0;
  NDB_ASSERT(!c.ExceedMaxlen(&start, &stop));
  c.mutable_meta()->set_maxlen(1000);
  NDB_ASSERT(!c.ExceedMaxlen(&start, &stop));
  c.mutable_meta()->set_length(1000);
  NDB_ASSERT(!c.ExceedMaxlen(&start, &stop));
  c.mutable_meta()->set_length(1090);
  NDB_ASSERT(!c.ExceedMaxlen(&start, &stop));
  c.mutable_meta()->set_length(1110);
  NDB_ASSERT(c.ExceedMaxlen(&start, &stop) && start == 0 && stop == 109);
  c.mutable_meta()->set_pruning(Pruning::MAX);
  NDB_ASSERT(c.ExceedMaxlen(&start, &stop) && start == 1000 && stop == 1109);

  Value d;
  NDB_ASSERT(!d.meta().deleted());
  d.SetLength(1);
  NDB_ASSERT(!d.meta().deleted());
  NDB_ASSERT(d.meta().length() == 1);
  d.SetLength(0);
  NDB_ASSERT(d.meta().deleted());
  NDB_ASSERT(d.meta().length() == 0);

  return EXIT_SUCCESS;
}
