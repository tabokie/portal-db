#ifndef PORTAL_UTIL_ATOMIC_LOCK_H_
#define PORTAL_UTIL_ATOMIC_LOCK_H_

#include "util.h"

#include <atomic>

namespace portal_db {

class AtomicLock : public NoCopy {
 public:
 	AtomicLock() { } // dummy lock
 	AtomicLock(std::atomic<bool>& lock): ref_(&lock) {
 		bool tmp = false;
    // compete for mutation lock
    while(!std::atomic_compare_exchange_strong(
      ref_,
      &tmp,
      true
      )) { tmp = false; }
 	}
 AtomicLock(AtomicLock&& rhs): ref_(rhs.ref_) {
 	rhs.ref_ = NULL;
 }
	~AtomicLock() {
		if(ref_) {
			assert(ref_->load());
			ref_->store(false);	
		}
	}
 private:
 	std::atomic<bool>* ref_ = NULL;
};

} // namespace portal_db

#endif // PORTAL_UTIL_ATOMIC_LOCK_H_