#ifndef PORTAL_DB_PAGED_POOL_H_
#define PORTAL_DB_PAGED_POOL_H_

#include <atomic>

namespace portal_db_impl {

// MaximumSize = 2 ^ 33 = 8 GB
// PageSize = 2 ^ 22 = 4 MB
template <
	size_t SliceSize, 
	size_t MaximumSize = (1ULL << 33),
	size_t PageSize = (1 << 22)>
class PagedPool {
 public:
 	PagedPool() {
 		for(int i = 0; i < bucket_size_; i++)
 			bucket_[i].store(NULL);
 		size_.store(0);
 	}
 	~PagedPool() {
 		for(int i = 0; i < bucket_size_; i++){
			delete [] bucket_[i].load();
		}
 	}
 	// unsafe, must be initialized
 	char* Get(size_t offset) {
 		size_t bucket = offset / per_bucket_size_;
 		if(bucket >= bucket_size_) {
 			return NULL;
 		}
 		size_t subslot = offset - bucket * per_bucket_size_;
 		char* p = bucket_[bucket].load();
 		if(p != NULL) return p + subslot * SliceSize;
 		return NULL;
 	}
 	const char* Get(size_t offset) const {
 		size_t bucket = offset / per_bucket_size_;
 		if(bucket_ >= bucket_size_) {
 			return NULL;
 		}
 		size_t subslot = offset - bucket * per_bucket_size_;
 		char* p = bucket_[bucket].load();
 		if(p != NULL) return p[subslot * SliceSize];
 		return NULL;
 	}
 	char* New() {
 		size_t slot = std::atomic_fetch_add(&size_, 1);
 		size_t bucket = slot / per_bucket_size_;
 		if(bucket >= bucket_size_) {
 			return NULL;
 		}
 		size_t subslot = slot - bucket * per_bucket_size_;
 		char* old_pointer;
 		char* lock = reinterpret_cast<char*>(1u);
 		if((old_pointer = bucket_[bucket].load()) == NULL) {
 			if(std::atomic_compare_exchange_strong(&bucket_[bucket], &old_pointer, lock)) {
 				char* tmp = new char[per_bucket_len_];
 				bucket_[bucket] = tmp;
 				return tmp + subslot * SliceSize;
 			} 
 		}
 		while((old_pointer = bucket_[bucket].load()) <= lock);
 		return old_pointer + subslot * SliceSize;
 	}
 	size_t size() const {
 		return size_.load();
 	}
 	size_t capacity() const {
 		return bucket_size_ * per_bucket_size_;
 	}
 private:
 	static constexpr size_t per_bucket_size_ = PageSize / SliceSize; // slice
 	static constexpr size_t per_bucket_len_ = PageSize / SliceSize * SliceSize; // byte
 	static constexpr size_t bucket_size_ = (MaximumSize + SliceSize) / SliceSize;
 	std::atomic<size_t> size_;
 	std::atomic<char*> bucket_[bucket_size_];
};

} // namespace portal_db_impl

#endif // PORTAL_DB_PAGED_POOL_H_
