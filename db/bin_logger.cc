#include "bin_logger.h"

namespace portal_db {

Status BinLogger::Read(Key& ret, char* alloc_ptr, bool& put) {
  put = false;
  if(!opened()) {
    Status status = Open();
    if(!status.ok()) return status;
  }
  size_t cur = std::atomic_fetch_add(&cursor_, 12);
  if(cur + 256 + 8 > SequentialFile::size()) return Status::NotFound("EOF");
  Status status = SequentialFile::Read(cur, 12, alloc_ptr);
  if(!status.ok()) return status;
  for(int i = 0; i < 8; i++) ret[i] = alloc_ptr[i];
  uint32_t tmp = *(reinterpret_cast<uint32_t*>(alloc_ptr + 4));
  if( tmp == 0 ) {
    cur = std::atomic_fetch_add(&cursor_, 256);
    if(cur + 256 > SequentialFile::size()) return Status::NotFound("EOF");
    status *= SequentialFile::Read(cur, 256, alloc_ptr);
    if(status.ok()) put = true;
    return status;
  } else if(tmp == marker_) {
    return Status::OK();
  } else {
    memcpy(alloc_ptr, alloc_ptr + 8, 4);
    cur = std::atomic_fetch_add(&cursor_, 256 - 4);
    status *= SequentialFile::Read(cur, 256 - 4, alloc_ptr + 4);
    if(status.ok()) put = true;
    return status;
  }
}
Status BinLogger::AppendDelete(const Key& key) {
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
Status BinLogger::AppendPut(const Key& key, const Value& value) {
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

Status BinLogger::Compact() { // snapshot is finished
  if(!opened()) {
    Status ret = Open();
    if(!ret.ok()) return ret;
  }
  size_t tmp;
  do {
    tmp = cursor_.load();
  } while(!std::atomic_compare_exchange_strong(&cursor_, &tmp, tmp - checkpoint_));
  char* buffer = new char[tmp - checkpoint_];
  Status ret = SequentialFile::Read(checkpoint_, tmp - checkpoint_, buffer);
  if(ret.ok()) ret *= Write(0, tmp - checkpoint_, buffer);
  if(ret.ok()) ret *= SetEnd(max((cursor_.load() + page_) / page_ * page_, (size() / 2 + page_ ) / page_ * page_ ));
  checkpoint_ = 0;
  return ret;
}

} // namespace portal_db