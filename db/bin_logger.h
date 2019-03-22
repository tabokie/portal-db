#ifndef PORTAL_DB_BIN_LOGGER_H_
#define PORTAL_DB_BIN_LOGGER_H_

#include "util/file.h" // File
#include "portal_db/status.h"
#include "portal_db/piece.h"

#include <cstdint>
#include <atomic>

namespace portal_db {

// logger for operations including:
// Put (key, value): key - value (254 byte)
//  additionally, if value shares prefix with marker/0
//  pad 0s in front (268 byte)
// Delete (key, value): key - marker (12 byte)
class BinLogger : public SequentialFile {

 public:
  BinLogger(std::string name)
    : SequentialFile(name), cursor_(0) { }
  ~BinLogger() { }
  // recovery routine //
  Status Rewind() { cursor_ = 0; return Status::OK(); }
  Status Read(const Key& ret, char*& alloc_ptr, bool& put) {
    put = false;
    if(!opened()) {
      Status ret = Open();
      if(!ret.ok()) return ret;
    }
    size_t cur = std::atomic_fetch_add(&cursor_, 12);
    Status ret = Read(cur, 12, alloc_ptr);
    if(!ret.ok()) return ret;
    for(int i = 0; i < 8; i++) ret[i] = alloc_ptr[i];
    uint32_t tmp = *(reinterpret_cast<uint32_t*>(alloc_ptr + 4));
    if( tmp == 0 ) {
      cur = std::atomic_fetch_add(&cursor_, 256);
      ret *= Read(cur, 256, alloc_ptr);
      if(ret.ok()) put = true;
      return ret;
    } else if(tmp == marker_) {
      return Status::OK();
    } else {
      memcpy(alloc_ptr, alloc_ptr + 8, 4);
      cur = std::atomic_fetch_add(&cursor_, 256 - 4);
      ret *= Read(cur, 256 - 4, alloc_ptr + 4);
      if(ret.ok()) put = true;
      return ret;
    }
  }
  // logging routine //
  Status AppendDelete(const Key& key) {
    if(!opened()) {
      Status ret = Open();
      if(!ret.ok()) return ret;
    }
    size_t cur = std::atomic_fetch_add(&cursor_, 8 + padding_);
    if(size() <= cur + 8 + padding_) SetEnd(size() + page_);
    Status ret = Write(cur, 8, key.raw_ptr());
    if(ret.ok()) ret *= Write(cur + 8, 4, 
      reinterpret_cast<const char*>(&marker_));
    return ret;
  }
  Status AppendPut(const Key& key, const Value& value) {
    if(!opened()) {
      Status ret = Open();
      if(!ret.ok()) return ret;
    }
    size_t len = 8 + 256;
    uint32_t tmp = *((uint32_t*)value.pointer_to_slice<0,8>());
    if(tmp == 0 || tmp == marker_) len += padding_; // padding
    tmp = 0;
    size_t cur = std::atomic_fetch_add(&cursor_, len);
    if(size() <= cur + len) SetEnd(size() + page_);
    Status ret = Write(cur, 8, key.raw_ptr());
    if(len > 8 + 256 && ret.ok()) {
      ret *= Write(cur + 8, 4,
       reinterpret_cast<const char*>(&tmp)); // 0 pad
      tmp = 4;
    }
    if(ret.ok()) {
      ret *= Write(cur + tmp + 8, 256,
        value.pointer_to_slice<0, 256>());
    }
    return ret;
  }
  Status AppendPut(const KeyValue& kv) {
    return AppendPut(kv, kv);
  }
  void Checkpoint() { // on-going snapshot
    checkpoint_ = cursor_.load();
  }
  Status Compact() { // snapshot is finished
    if(!opened()) {
      Status ret = Open();
      if(!ret.ok()) return ret;
    }
    size_t tmp;
    do {
      tmp = cursor_.load();
    } while(!std::atomic_compare_exchange_strong(&cursor_, &tmp, tmp - checkpoint_));
    char* buffer = new char[tmp - checkpoint_];
    Status ret = Read(checkpoint_, tmp - checkpoint_, buffer);
    if(ret.ok()) ret *= Write(0, tmp - checkpoint_, buffer);
    if(ret.ok()) ret *= SetEnd(max((cursor_.load() + page_) / page_ * page_, (size() / 2 + page_ ) / page_ * page_ ));
    return ret;
  }
 private:
  static constexpr size_t page_ = (1 << 12); // 4 KB page
  static constexpr size_t padding_ = 4;
  static const uint32_t marker_ = 0xDEADBEEF;
  size_t checkpoint_ = 0; // offset
  std::atomic<size_t> cursor_;
};

} // namespace portal_db

#endif // PORTAL_DB_BIN_LOGGER_H_