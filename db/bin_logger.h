#ifndef PORTAL_DB_BIN_LOGGER_H_
#define PORTAL_DB_BIN_LOGGER_H_

#include "util/file.h" // File
#include "portal_db/status.h"
#include "portal_db/piece.h"

#include <cstdint>
#include <atomic>

namespace portal_db {

// Logger for operations including:
// + Put (key, value): key - value (254 byte)
//    additionally, if value shares prefix with marker/0
//    pad 0s in front (268 byte)
// + Delete (key, value): key - marker (12 byte)
class BinLogger : public SequentialFile {
 public:
  BinLogger(std::string name)
    : SequentialFile(name), cursor_(0) { }
  ~BinLogger() { }
  // recovery routine //
  Status Rewind() { cursor_ = 0; return Status::OK(); }
  // read one record at a time
  Status Read(Key& ret, char* alloc_ptr, bool& put);
  // logging routine //
  Status AppendDelete(const Key& key);
  Status AppendPut(const Key& key, const Value& value);
  Status AppendPut(const KeyValue& kv) {
    return AppendPut(kv, kv);
  }
  // checkpoint logging
  void Checkpoint() { // on-going snapshot
    checkpoint_ = cursor_.load();
  }
  // called when snapshot of checkpoint is finished
  Status Compact();
 protected:
  // each page is allocated contiguously
  static constexpr size_t page_ = (1 << 12); // 4 KB page
  // padding for id DEL with PUT
  static constexpr size_t padding_ = 4;
  static const uint32_t marker_ = 0xDEADBEEF;
  // checkpoint log
  size_t checkpoint_ = 0; // offset
  std::atomic<size_t> cursor_;
};

} // namespace portal_db

#endif // PORTAL_DB_BIN_LOGGER_H_