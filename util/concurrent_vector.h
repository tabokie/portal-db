#ifndef PORTAL_UTIL_CONCURRENT_VECTOR_H_
#define PORTAL_UTIL_CONCURRENT_VECTOR_H_

#include <memory>
#include <atomic>
#include <mutex>
#include <vector>

namespace portal_db {

template <typename ElementType>
class ConcurrentVector {
 public:
 	using ItemType = std::unique_ptr<ElementType>;
 	ConcurrentVector() : size_(0) { }
 	~ConcurrentVector() {
 		for(int i = 0; i < buffer_.size(); i++) {
 			delete[] buffer_[i];
 		}
 	}
 	size_t push_back(ItemType&& element) {
 		size_t token = std::atomic_fetch_add(&size_, 1);
 		while(token + 1 >= buffer_.size() * buffer_size) {
 			mutate();
 		}
 		ItemType* p = buffer_[token / buffer_size];
 		p[token % buffer_size] = std::move(element);
 		return token;
 	}
 	ElementType* operator[](size_t idx) const {
 		return buffer_[idx / buffer_size][idx % buffer_size].get();
 	}
 	size_t size() const {
 		return size_.load();
 	}
 	std::unique_ptr<ElementType>&& own(size_t idx) {
 		// std::unique_ptr<ElementType> ret;
 		// ret.swap(buffer_[idx / buffer_size][idx % buffer_size]);
 		// return std::move(ret);
 		return std::move(buffer_[idx / buffer_size][idx % buffer_size]);
 	}
 private:
 	static constexpr size_t buffer_size = 25;
 	std::atomic<size_t> size_;
 	std::mutex lock_;
 	std::vector<ItemType*> buffer_;
 	void mutate() {
 		std::lock_guard<std::mutex> lk(lock_);
 		buffer_.push_back(new ItemType[buffer_size]);
 	}
};

} // namespace portal_db

#endif // PORTAL_UTIL_CONCURRENT_VECTOR_H_