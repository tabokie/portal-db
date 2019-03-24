#ifndef PORTAL_DB_PAGED_POOL_H_
#define PORTAL_DB_PAGED_POOL_H_

#include "util/file.h"

#include <atomic>
#include <iostream>

namespace portal_db {

// test::MaximumSize = 2 ^ 29 = 512 MB (tested to hold million)
// MaximumSize = 2 ^ 33 = 8 GB
// PageSize = 2 ^ 22 = 4 KB
template <
  size_t SliceSize, 
  size_t MaximumPower = 29, // = 33,
  size_t PagePower = 12>
class PagedPool: public SequentialFile {
 public:
  PagedPool(std::string filename): SequentialFile(filename) {
    for(int i = 0; i < bucket_num_; i++)
      bucket_[i].store(NULL);
    size_.store(0);
    bucket_size_.store(0);
  }
  ~PagedPool() {
    for(int i = 0; i < bucket_num_; i++){
      delete [] bucket_[i].load();
    }
  }
  // unsafe, must be initialized
  char* Get(size_t offset) {
    size_t bucket = offset / per_bucket_num_;
    if(bucket >= bucket_size_) {
      return NULL;
    }
    size_t subslot = offset - bucket * per_bucket_num_;
    char* p = bucket_[bucket].load();
    if(p != NULL) return p + subslot * SliceSize;
    return NULL;
  }
  const char* Get(size_t offset) const {
    size_t bucket = offset / per_bucket_num_;
    if(bucket >= bucket_size_) {
      return NULL;
    }
    size_t subslot = offset - bucket * per_bucket_num_;
    char* p = bucket_[bucket].load();
    if(p != NULL) return p + subslot * SliceSize;
    return NULL;
  }
  size_t New() {
    size_t slot = std::atomic_fetch_add(&size_, 1);
    size_t bucket = slot / per_bucket_num_;
    if(bucket >= bucket_num_) {
      return 0x0fffffff;
    }
    size_t subslot = slot - bucket * per_bucket_num_;
    AllocBucket(bucket);
    return slot;
  }
  Status MakeSnapshot () {
    if(!opened()) {
      Status ret = Open();
      if(!ret.ok()) return ret;
    }
    uint32_t sliceSize = size_.load();
    uint32_t bucketSize = bucket_size_.load();
    if(SequentialFile::size() < per_bucket_bytes_ * bucketSize + snapshot_header_) {
      SetEnd(per_bucket_bytes_ * bucketSize + snapshot_header_);
    }
    Status ret = Write(0, sizeof(uint32_t), reinterpret_cast<char*>(&sliceSize));
    size_t offset = snapshot_header_;
    for(int i = 0; i < bucketSize && ret.ok(); i++) {
      ret *= Write(offset, per_bucket_bytes_, bucket_[i]);
      offset += per_bucket_bytes_;
    }
    return ret;
  }
  Status DeleteSnapshot() {
    Status ret = Status::OK();
    if(opened()) ret *= Close();
    if(!ret.ok()) return ret;
    ret *= Delete();
    return ret;
  }
  Status ReadSnapshot() {
    if(!opened()) {
      Status ret = Open();
      if(!ret.ok()) return ret;
    }
    size_t fileSize = SequentialFile::size();
    uint32_t sliceSize;
    Status ret = Read(0, sizeof(uint32_t), reinterpret_cast<char*>(&sliceSize));
    std::cout << "Slice Size = " << sliceSize << std::endl;
    size_t offset = snapshot_header_;
    for(size_t bucket = 0; 
      bucket < bucket_num_ && 
      offset + per_bucket_bytes_ <= fileSize &&
      ret.ok();
      bucket++) {
      AllocBucket(bucket);
      ret *= Read(offset, per_bucket_bytes_, bucket_[bucket]);
      offset += per_bucket_bytes_;
    }
    if(ret.ok()) size_.store(sliceSize); // take effetch
    return ret;
  }
  size_t size() const {
    return size_.load();
  }
  size_t capacity() const {
    return bucket_num_ * per_bucket_num_;
  }
  // for Debug
  void inspect() const {
    size_t size = size_.load();
    char tmp[SliceSize + 1];
    tmp[SliceSize] = '\0';
    for(int i = 0; i < size; i++) {
      const char* p = Get(i);
      memcpy(tmp, p, SliceSize);
      for(int j = 0; j < 8; j++) {
        std::cout << tmp[j];
      }
      std::cout << std::endl;
    }
  }
 private:
  static constexpr size_t per_bucket_num_ = (1 << PagePower) / SliceSize; // slice
  static constexpr size_t per_bucket_bytes_ = per_bucket_num_ * SliceSize; // byte
  static constexpr size_t bucket_num_ = (1 << (MaximumPower - PagePower));
  static constexpr size_t snapshot_header_ = sizeof(uint32_t); // store `size_` field
  std::atomic<size_t> size_; // size of slices
  std::atomic<size_t> bucket_size_; // size of buckets
  std::atomic<char*> bucket_[bucket_num_];
  void AllocBucket(size_t idx) {
    size_t tmp;
    while((tmp = bucket_size_.load()) <= idx) {
      if(std::atomic_compare_exchange_strong(&bucket_size_, &tmp, tmp + 1)) {
        bucket_[tmp] = new char[per_bucket_bytes_];
      }
    }
  }
};

} // namespace portal_db

#endif // PORTAL_DB_PAGED_POOL_H_
