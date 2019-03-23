#ifndef PORTAL_DB_PERSIST_HASH_TRIE_H_
#define PORTAL_DB_PERSIST_HASH_TRIE_H_

#include "hash_trie.h"
#include "bin_logger.h"
#include "util/readwrite_lock.h"
#include "portal_db/port.h"

namespace portal_db {

class PersistHashTrie: public HashTrie {
 public:
  PersistHashTrie(std::string filename) 
      : HashTrie(filename),
        binlogger_(filename + ".bin") {
    StartDaemon();
  }
  Status Get(const Key& key, Value& ret) {
    wrlock_.ReadLock();
    Status status = HashTrie::Get(key, ret);
    wrlock_.ReadUnlock();
    return status;
  }
  Status Put(const Key& key, const Value& value) {
    wrlock_.WriteLock();
    binlogger_.AppendPut(key, value);
    Status ret = HashTrie::Put(key, value);
    mod_ ++;
    wrlock_.WriteUnlock();
    return ret;
  }
  Status Delete(const Key& key) {
    wrlock_.WriteLock();
    binlogger_.AppendDelete(key);
    Status ret = HashTrie::Delete(key);
    mod_ ++;
    wrlock_.WriteUnlock();
    return ret;
  }
  // internal use
  void __persist__() {
    if(mod_ >= snapshot_mod) {
      assert(values_.MakeSnapshot().inspect());
    }
    mod_ = 0;
  }
 private:
  ReadWriteLock wrlock_;
  static constexpr size_t snapshot_interval = 1000; // 1 sec
  static constexpr size_t snapshot_mod = 100; // every 100 mod
  std::atomic<size_t> mod_ = 0;
  BinLogger binlogger_;
  // std::thread daemon_;
  HANDLE sys_timer_;
  void StartDaemon();
  void CloseDaemon();
};

} // namespace portal_db

#endif // PORTAL_DB_PERSIST_HASH_TRIE_H_