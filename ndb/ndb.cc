#include "ndb/ndb.h"

namespace ndb {

NDB* ndb = NULL;

static void Daemonize(const std::string& node) {
  char name[1024];
  snprintf(name, sizeof(name), "ndb %s", node.c_str());
  NDB_ASSERT(prctl(PR_SET_NAME, name, 0, 0, 0) != -1);
  NDB_ASSERT(daemon(1, 1) == 0);
}

NDB::NDB(int argc, char* argv[]) {
  NDB_ASSERT_OK(options.Parse(argc, argv));

  if (options.restore.size() != 0) {
    NDB_ASSERT_OK(RestoreDB(options.restore, options.engine.dbname));
    exit(EXIT_SUCCESS);
  }

  if (options.daemonize) {
    Daemonize(options.GetNodeID());
  }

  logger = new Logger(options.logger);
  engine = new Engine(options.engine);
  server = new Server(options.server);
  command = new Command(options.command, engine);
  replica = new Replica(options.replica, engine);
}

NDB::~NDB() {
  // Stop server first.
  delete server;
  delete replica;
  delete command;
  delete engine;
  delete logger;
}

Result NDB::Run() {
  NDB_TRY(logger->Open());
  NDB_TRY(engine->Open());
  NDB_TRY(command->Run());
  NDB_TRY(replica->Run());
  return server->Run([this](Client* c) { return command->ProcessClient(c); });
}

// Update namespaces.
// Return true if new namespaces are added.
static bool UpdateNamespaces() {
  std::map<std::string, Configs> nss;
  auto r = ndb->options.GetNamespaces(&nss);
  if (!r.ok()) {
    NDB_LOG_ERROR("*CENTER* Get namespaces: %s", r.message());
    return false;
  }

  // New namespaces.
  bool added = false;
  for (const auto& it : nss) {
    if (ndb->engine->GetNamespace(it.first) != NULL) {
      continue;
    }
    auto r = ndb->engine->NewNamespace(it.first);
    if (!r.ok()) {
      NDB_LOG_ERROR("*CENTER* New namespace [%s]: %s", it.first.c_str(), r.message());
      continue;
    }
    NDB_LOG_INFO("*CENTER* New namespace [%s]", it.first.c_str());
    added = true;
  }

  // Drop namespaces.
  auto nsnames = ndb->engine->ListNamespaces();
  for (const auto& name : nsnames) {
    if (name == "default") continue;
    if (nss.find(name) == nss.end()) {
      auto r = ndb->engine->DropNamespace(name);
      if (!r.ok()) {
        NDB_LOG_ERROR("*CENTER* Drop namespace [%s]: %s", name.c_str(), r.message());
        continue;
      }
      NDB_LOG_INFO("*CENTER* Drop namespace [%s]", name.c_str());
    }
  }

  return added;
}

// Update namespaces' configs.
static void UpdateNSConfigs() {
  std::map<std::string, Configs> nss;
  auto r = ndb->options.GetNamespaces(&nss);
  if (!r.ok()) {
    NDB_LOG_ERROR("*CENTER* Get namespaces: %s", r.message());
    return;
  }

  for (const auto& it : nss) {
    auto ns = ndb->engine->GetNamespace(it.first);
    if (ns == NULL) {
      continue;
    }
    auto configs = ns->GetConfigs();
    if (configs.expire() == it.second.expire() &&
        configs.maxlen() == it.second.maxlen() &&
        configs.pruning() == it.second.pruning()) {
      // Nothing to update.
      continue;
    }
    if (ndb->options.replica.address.size() == 0) {
      // Master's configs are updated from center, we need to save it to db.
      auto r = ns->PutConfigs(it.second);
      if (!r.ok()) {
        NDB_LOG_ERROR("*CENTER* Update namespace [%s]: %s", it.first.c_str(), r.message());
        return;
      }
    } else {
      // Slaves' configs are updated from master, we need to load it from db.
      auto r = ns->LoadConfigs();
      if (!r.ok()) {
        NDB_LOG_ERROR("*CENTER* Update namespace [%s]: %s", it.first.c_str(), r.message());
        return;
      }
    }
    NDB_LOG_INFO("Update namespace [%s]", it.first.c_str());
  }
}

// Contact center every interval seconds and update information.
static void ContactCenter(int interval) {
  while (!ndb->stop) {
    // NOTE: We don't update namespaces and their configs at the same time.
    // If slaves synchronize some namespace's configs from master,
    // but the namespace doesn't exist, slaves will enter an error state.
    // So we wait for a cycle to give slaves a chance to update their namespaces.
    // IF SLAVES HAVE ENTERED ERROR STATE, PLEASE RESTART THEM.
    if (!UpdateNamespaces()) {
      UpdateNSConfigs();
    }
    // Check to stop every second.
    for (int i = 0; !ndb->stop && i < interval; i++) {
      sleep(1);
    }
  }
}

}  // namespace ndb

using namespace ndb;

void Stop(int) {
  if (ndb::ndb != NULL) {
    ndb::ndb->stop = true;
  }
}

int Main(int argc, char* argv[]) {
  signal(SIGINT,  Stop);
  signal(SIGQUIT, Stop);
  signal(SIGTERM, Stop);
  signal(SIGPIPE, SIG_IGN);

  // Init libraries.
  NDB_ASSERT(curl_global_init(CURL_GLOBAL_SSL) == CURLE_OK);

  ndb::ndb = new NDB(argc, argv);
  NDB_ASSERT_OK(ndb::ndb->Run());
  std::thread center(ContactCenter, 60);
  while (!ndb::ndb->stop) {
    sleep(1);
  }
  center.join();
  delete ndb::ndb;

  // Cleanup libraries.
  curl_global_cleanup();
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
