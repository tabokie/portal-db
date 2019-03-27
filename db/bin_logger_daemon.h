#ifndef PORTAL_DB_BIN_LOGGER_DAEMON_H_
#define PORTAL_DB_BIN_LOGGER_DAEMON_H_

#include "bin_logger.h"
#include "portal_db/piece.h"

#include <atomic>
#include <thread>

namespace portal_db {

class BinLoggerDaemon : public BinLogger {
  // struct for pending operation
  // unsafe reference
  struct OpStruct {
    const Key* key = NULL;
    const Value* value = NULL;
    size_t version;
    std::atomic<OpStruct*> next = NULL;
    OpStruct(size_t ver, const Key& k, const Value& v)
        : version(ver),
          key(&k),
          value(&v) { }
    OpStruct(size_t ver, const Key& k)
        : version(ver),
          key(&k),
          value(NULL) { }
    OpStruct(size_t ver)
        : version(ver), 
          key(NULL), 
          value(NULL) { }
    OpStruct(OpStruct&& rhs)
        : key(rhs.key),
          value(rhs.value),
          version(rhs.version),
          next(rhs.next.load()) { }
    OpStruct(const OpStruct& rhs)
        : key(rhs.key),
          value(rhs.value),
          version(rhs.version),
          next(rhs.next.load()) { }
    OpStruct& operator=(const OpStruct& rhs) {
      key = rhs.key;
      value = rhs.value;
      version = rhs.version;
      next = rhs.next.load();
      return *this;
    }
  };
 public:
  BinLoggerDaemon(std::string name)
      : BinLogger(name),
        close_(false),
        version_(1),
        finished_version_(0) {
    head_ = new OpStruct(0); // dummy
    tail_ = head_;
    daemon_ = std::thread(
      std::mem_fn(&BinLoggerDaemon::DaemonThread), 
      this
    ); // late init
  }
  Status Close() {
    close_.store(true);
    daemon_.join();
    return BinLogger::Close();
  }
  // operation enqueue family //
  size_t AppendDelete(const Key& key) {
    // bug: use stack variable
    OpStruct* op = new OpStruct(std::atomic_fetch_add(&version_, 1), key);
    Enqueue(op);
    return op->version;
  }
  size_t AppendPut(const Key& key, const Value& value) {
    OpStruct* op = new OpStruct(std::atomic_fetch_add(&version_, 1), key, value);
    Enqueue(op);
    return op->version;
  }
  size_t Compact() {
    OpStruct* op = new OpStruct(std::atomic_fetch_add(&version_, 1));
    Enqueue(op);
    return op->version;
  }
  // busy wait
  void Wait(size_t version) {
    while(true) {
      size_t v = finished_version_.load();
      if(v >= version || version-v > 0x7fffffff) return;
      else std::this_thread::yield();
    }
  }
 private:
  OpStruct* head_;
  std::atomic<OpStruct*> tail_;
  std::atomic<bool> close_;
  std::atomic<size_t> version_; // start from 1
  std::atomic<size_t> finished_version_; // latest finished op
  std::thread daemon_;
  // concurrent enqueue
  bool Enqueue(OpStruct* node);
  // single-thread dequeue
  bool Dequeue(OpStruct& ret);
  // instantiates as daemon thread
  void DaemonThread();
};

} // namespace portal_db

#endif // PORTAL_DB_BIN_LOGGER_DAEMON_H_