#ifndef PORTAL_DB_PAGED_POOL_H_
#define PORTAL_DB_PAGED_POOL_H_

#include "util/file.h"

#include <atomic>

namespace portal_db {

// MaximumSize = 2 ^ 33 = 8 GB
// PageSize = 2 ^ 22 = 4 MB
template <
	size_t SliceSize, 
	size_t MaximumSize = (1ULL << 33),
	size_t PageSize = (1 << 22)>
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
 		if(bucket_ >= bucket_size_) {
 			return NULL;
 		}
 		size_t subslot = offset - bucket * per_bucket_num_;
 		char* p = bucket_[bucket].load();
 		if(p != NULL) return p[subslot * SliceSize];
 		return NULL;
 	}
 	char* New() {
 		size_t slot = std::atomic_fetch_add(&size_, 1);
 		size_t bucket = slot / per_bucket_num_;
 		if(bucket >= bucket_num_) {
 			return NULL;
 		}
 		size_t subslot = slot - bucket * per_bucket_num_;
 		size_t tmp;
 		while((tmp = bucket_size_.load()) <= bucket) {
 			if(std::atomic_compare_exchange_strong(&bucket_size_, &tmp, tmp + 1)) {
 				bucket_[tmp] = new char[per_bucket_bytes_];
 			}
 		}
 		return bucket_[bucket] + subslot * SliceSize;
 	}
 	Status MakeSnapshot () {
 		if(!opened()) {
 			Status ret = Open();
 			if(!ret.ok()) return ret;
 		}
 		size_t size = bucket_size_;
 		if(SequentialFile::size() < per_bucket_bytes_ * size) {
 			SetEnd(per_bucket_bytes_ * size);
 		}
 		size_t offset = 0;
 		Status ret = Status::OK();
 		for(int i = 0; i < size && ret.ok(); i++) {
 			ret *= Write(offset, per_bucket_bytes_, bucket_[i]);
 			offset += per_bucket_bytes_;
 		}
 		return ret;
 	}
 	Status DeleteSnapshot() {
 		Status ret = Close();
 		if(!ret.ok()) return ret;
 		ret *= Delete();
 		return ret;
 	}
 	size_t size() const {
 		return size_.load();
 	}
 	size_t capacity() const {
 		return bucket_num_ * per_bucket_num_;
 	}
 private:
 	static constexpr size_t per_bucket_num_ = PageSize / SliceSize; // slice
 	static constexpr size_t per_bucket_bytes_ = PageSize / SliceSize * SliceSize; // byte
 	static constexpr size_t bucket_num_ = (MaximumSize + SliceSize) / SliceSize;
 	std::atomic<size_t> size_; // size of slices
 	std::atomic<size_t> bucket_size_; // size of buckets
 	std::atomic<char*> bucket_[bucket_num_];
};

} // namespace portal_db

#endif // PORTAL_DB_PAGED_POOL_H_
