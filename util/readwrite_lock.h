#ifndef PORTAL_UTIL_READWRITE_LOCK_H_
#define PORTAL_UTIL_READWRITE_LOCK_H_

#include "util.h"

#include <mutex>
#include <atomic>

namespace portal_db {

// unfair version (read first)
class ReadWriteLock : public NoMove {
 public:
  void ReadLock() {
    std::lock_guard<std::mutex> lk(read_);
    reader_ ++;
    if(reader_ == 1) write_.lock();
  }
  void ReadUnlock() {
    std::lock_guard<std::mutex> lk(read_);
    reader_ --;
    if(reader_ == 0) write_.unlock();
  }
  void WriteLock() {
    write_.lock();
  }
  void WriteUnlock() {
    write_.unlock();
  }
 private:
  std::mutex read_;
  std::atomic<size_t> reader_;
  std::mutex write_;
};

} // namespace portal_db

#endif // PORTAL_UTIL_READWRITE_LOCK_H_