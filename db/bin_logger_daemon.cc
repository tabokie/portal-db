#include "bin_logger_daemon.h"

namespace portal_db {

bool BinLoggerDaemon::Enqueue(OpStruct* node) {
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
bool BinLoggerDaemon::Dequeue(OpStruct& ret) { // single-thread
  OpStruct *first;
  first = head_->next;
  if(first == NULL) return false;
  first = head_;
  head_ = head_->next.load();
  ret = *head_;
  delete first;
  return true;
}
void BinLoggerDaemon::DaemonThread() {
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

} // namespace portal_db