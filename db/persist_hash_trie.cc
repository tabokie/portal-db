#include "persist_hash_trie.h"
#include "portal_db/port.h"

namespace portal_db {
  
#ifdef WIN_PLATFORM

void __stdcall snapshot_callback(
  PVOID dwUser,
  BOOLEAN timerOrWaitFired){
  // use dwUser to point to trigger data
  PersistHashTrie* p = reinterpret_cast<PersistHashTrie*>(dwUser);
  p->__persist__();
} 

void PersistHashTrie::StartDaemon() {
  assert(CreateTimerQueueTimer(
    &sys_timer_, 
    NULL, 
    snapshot_callback, 
    (PVOID)(this), 
    snapshot_interval,
    snapshot_interval, 
    WT_EXECUTEDEFAULT));
}

void PersistHashTrie::CloseDaemon() {
  if(sys_timer_ != NULL) {
    assert(DeleteTimerQueueTimer(NULL, sys_timer_, INVALID_HANDLE_VALUE));
    sys_timer_ = NULL;
  }
}

#else
#error    "no implementation for atimer on this platform"
#endif

} // namespace portal_db
