#ifndef PORTAL_DB_BIN_LOGGER_DAEMON_H_
#define PORTAL_DB_BIN_LOGGER_DAEMON_H_

#include "bin_logger.h"
#include "portal_db/piece.h"

#include <atomic>
#include <thread>

namespace portal_db {

class BinLoggerDaemon : public BinLogger {
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
    daemon_ = std::thread(std::mem_fn(&BinLoggerDaemon::DaemonThread), this); // late init
  }
  Status Close() {
    close_.store(true);
    daemon_.join();
    return BinLogger::Close();
  }
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
  std::atomic<size_t> finished_version_;
  std::thread daemon_;
  bool Enqueue(OpStruct* node) {
    OpStruct* n = node;
    OpStruct *t, *s;
    n->next = NULL;
    while(true) {
      t = tail_.load();
      s = t->next;
      if( t == tail_) {
        if(s == NULL) {
          if(std::atomic_compare_exchange_strong(&t->next, &s, n)){
            std::atomic_compare_exchange_strong(&tail_, &t, n);
            return true;
          }
        } else {
          std::atomic_compare_exchange_strong(&tail_, &t, s);
        }
      }
    }
  }
  bool Dequeue(OpStruct& ret) { // single-thread
    OpStruct *first;
    first = head_->next;
    if(first == NULL) return false;
    first = head_;
    head_ = head_->next.load();
    ret = *head_;
    delete first;
    return true;
  }
  void DaemonThread() {
    OpStruct cur(0);
    while(!close_.load()) {
      if(Dequeue(cur)) {
        size_t v = cur.version;
        if(cur.key == NULL) {
          BinLogger::Compact();
          finished_version_ = v; // bug
        }
        else if(cur.value == NULL){
          BinLogger::AppendDelete(*cur.key);
          finished_version_ = v;
        } 
        else {
          BinLogger::AppendPut(*cur.key, *cur.value);
          finished_version_ = v;
        }
      } else std::this_thread::yield();
    }
  }
};

} // namespace portal_db

#endif // PORTAL_DB_BIN_LOGGER_DAEMON_H_