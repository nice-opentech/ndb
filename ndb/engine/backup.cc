#include "ndb/engine/backup.h"

namespace ndb {

using namespace rocksdb;

Backup::Backup(rocksdb::DB* db) : db_(db) {
}

Backup::~Backup() {
  Stop();
  Join();
}

Result Backup::Create(const std::string& backup) {
  std::unique_lock<std::mutex> lock(lock_);

  // Check previous backup.
  if (engine_ != NULL) {
    if (IsFinished()) {
      Join();
    } else {
      return Result::Error("Previous backup is incomplete.");
    }
  }

  if (backup == db_->GetName()) {
    return Result::Error("Backup dbname is the same as the source dbname.");
  }

  BackupableDBOptions options(backup);
  options.backup_rate_limit = 32 << 20;

  auto s = BackupEngine::Open(Env::Default(), options, &engine_);
  if (!s.ok()) return StatusToResult(s);

  backup_ = backup;
  status_ = Status::OK();
  thread_ = std::thread([this] {
      finish_time_ = 0;
      create_time_ = time(NULL);
      status_ = engine_->CreateNewBackup(db_);
      engine_->GarbageCollect();
      engine_->PurgeOldBackups(1);
      finish_time_ = time(NULL);
    });

  return Result::OK();
}

void Backup::Stop() {
  std::unique_lock<std::mutex> lock(lock_);
  if (engine_ == NULL) return;
  if (!IsFinished()) {
    engine_->StopBackup();
  }
}

void Backup::Join() {
  if (engine_ == NULL) return;
  thread_.join();
  delete engine_;
  engine_ = NULL;
}

Stats Backup::GetStats() const {
  Stats stats;
  stats.insert("backup", backup_);
  stats.insert("status", status_.ToString());
  stats.insert("create_time", create_time_);
  stats.insert("finish_time", finish_time_);
  return stats;
}

Result RestoreDB(const std::string& backup, const std::string& dbname) {
  BackupableDBOptions options(backup);
  options.max_background_operations = 4;
  BackupEngineReadOnly* engine = NULL;
  auto s = BackupEngineReadOnly::Open(Env::Default(), options, &engine);
  if (s.ok()) {
    s = engine->RestoreDBFromLatestBackup(dbname, dbname);
  }
  delete engine;
  return StatusToResult(s);
}

}  // namespace ndb
