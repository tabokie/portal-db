#ifndef PORTAL_DB_PERSIST_HASH_TRIE_H_
#define PORTAL_DB_PERSIST_HASH_TRIE_H_

#include "hash_trie.h"
#include "bin_logger_daemon.h"
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
  ~PersistHashTrie() {
    binlogger_.Close();
    CloseDaemon();
    values_.Close();
  }
  Status Get(const Key& key, Value& ret) {
    wrlock_.ReadLock();
    Status status = HashTrie::Get(key, ret);
    wrlock_.ReadUnlock();
    return status;
  }
  Status Put(const Key& key, const Value& value) {
    wrlock_.WriteLock();
    size_t v = binlogger_.AppendPut(key, value);
    Status ret = HashTrie::Put(key, value);
    mod_ ++;
    wrlock_.WriteUnlock();
    binlogger_.Wait(v);
    return ret;
  }
  Status Scan(const Key& lower, 
              const Key& upper, 
              HashTrieIterator& ret) {
    wrlock_.ReadLock();
    Status status = HashTrie::Scan(lower, upper, ret);
    wrlock_.ReadUnlock();
    return status;
  }
  Status Delete(const Key& key) {
    wrlock_.WriteLock();
    size_t v = binlogger_.AppendDelete(key);
    Status ret = HashTrie::Delete(key);
    mod_ ++;
    wrlock_.WriteUnlock();
    binlogger_.Wait(v);
    return ret;
  }
  Status RecoverBinLog() {
    wrlock_.WriteLock();
    Status status;
    Status op_status;
    Key key;
    Value value;
    char alloc[256];
    bool is_put;
    while(op_status.ok()) {
      status *= binlogger_.Read(key, alloc, is_put);
      if(status.ok()) {
        if(is_put) {
          value.copy<0,256>(alloc);
          op_status *= HashTrie::Put(key, value);
        } else HashTrie::Delete(key);
      } else break;
    }
    wrlock_.WriteUnlock();
    return op_status;
  }
  Status RecoverSnapshot() {
    wrlock_.WriteLock();
    Key key;
    Value value;
    Status op_status;
    op_status *= values_.ReadSnapshot();
    if(!op_status.ok()) return op_status;
    std::cout << "snapshot size: " << values_.size() << std::endl;
    for(int i = 0; i < values_.size() && op_status.ok(); i++) {
      op_status *= PutRecover(i);
    }
    wrlock_.WriteUnlock();
    return op_status;
  }
  // internal use
  void __persist__() {
    if(mod_ >= snapshot_mod) {
      mod_ = 0;
      binlogger_.Checkpoint();
      binlogger_.Compact();
      assert(values_.MakeSnapshot().inspect());
    }
  }
 private:
  ReadWriteLock wrlock_;
  static constexpr size_t snapshot_interval = 500; // 0.5 sec
  static constexpr size_t snapshot_mod = 100; // every 100 mod
  std::atomic<size_t> mod_ = 0;
  BinLoggerDaemon binlogger_;
  // std::thread daemon_;
  HANDLE sys_timer_;
  void StartDaemon();
  void CloseDaemon();
};

} // namespace portal_db

#endif // PORTAL_DB_PERSIST_HASH_TRIE_H_