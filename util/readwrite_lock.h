#ifndef PORTAL_UTIL_READWRITE_LOCK_H_
#define PORTAL_UTIL_READWRITE_LOCK_H_

#include "util.h"

#include <mutex>
#include <atomic>

namespace portal_db {

// unfair version
class ReadWriteLock : public NoCopy{
 public:
 	class ReadHandle : public Handle {
 	 public:
 		ReadHandle(ReadWriteLock* l) {
 			l->ReadLock();
 			ref_ = l;
 		}
 		ReadHandle(ReadHandle&& rhs)
 			: ref_(rhs.ref_), 
 				released_(rhs.released_) {
 				rhs.ref_ = NULL;
 				rhs.released_ = true;
 			}
 		~ReadHandle() {
 			if(!released_ && ref_) ref_->ReadUnlock();
 		}
 		void release() {
 			if(!released_ && ref_) ref_->ReadUnlock();
 			ref_ = NULL; // release reference
 		}
 	 private:
 	 	ReadWriteLock* ref_ = NULL;
 	 	bool released_ = false;;
 	};
 	class WriteHandle : public Handle {
 	 public:
 		WriteHandle(ReadWriteLock* l) {
 			l->WriteLock();
 			ref_ = l;
 		}
 		WriteHandle(WriteHandle&& rhs)
 			: ref_(rhs.ref_), 
 				released_(rhs.released_) {
 				rhs.ref_ = NULL;
 				rhs.released_ = true;
 			}
 		~WriteHandle() {
 			if(!released_ && ref_) ref_->WriteUnlock();
 		}
 		void release() {
 			if(!released_ && ref_) ref_->WriteUnlock();
 			ref_ = NULL; // release reference
 		}
 	 private:
 	 	ReadWriteLock* ref_ = NULL;
 	 	bool released_ = false;;
 	};
 	void ReadLock() {
 		std::lock_guard<std::mutex> lk(reader_lock_);
 		size_t tmp = std::atomic_fetch_add(&reader_count_, 1);
 		if(tmp == 0) writer_lock_.lock();
 	}
 	void ReadUnlock() {
 		size_t tmp = std::atomic_fetch_add(&reader_count_, -1);
 		if(tmp == 1) writer_lock_.unlock();
 	}
 	void WriteLock() {
 		writer_lock_.lock();
 	}
 	void WriteUnlock() {
 		writer_lock_.unlock();
 	}
 	ReadHandle ReadGuard() {
 		return ReadHandle(this);
 	}
 	WriteHandle WriteGuard() {
 		return WriteHandle(this);
 	}
 private:
 	std::mutex reader_lock_;
 	std::mutex writer_lock_;
 	std::atomic<size_t> reader_count_;
};

} // namespace portal_db

#endif // PORTAL_UTIL_READWRITE_LOCK_H_