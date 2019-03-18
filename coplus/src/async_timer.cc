#include "coplus/async_timer.h"
#include "coplus/coroutine.h"

namespace coplus{

#ifdef WIN_PORT
void __stdcall timer_callback(
	PVOID dwUser,
	BOOLEAN timerOrWaitFired){
	// use dwUser to point to trigger data
	TriggerData* trigger = (TriggerData*)dwUser;
	notify(trigger);
}	
// use a minimun accuracy
void AsyncTimer::await(float seconds, Trigger trigger){
	HANDLE hTimer = NULL;
	CreateTimerQueueTimer(&hTimer, NULL, timer_callback, (PVOID)(trigger.get()), seconds * 1000, 0, WT_EXECUTEDEFAULT);
}
void AsyncTimer::hr_await(float seconds, Trigger trigger){
	HANDLE hTimer = NULL;
	CreateTimerQueueTimer(&hTimer, NULL, timer_callback, (PVOID)(trigger.get()), seconds * 1000, 0, WT_EXECUTEDEFAULT);
}
#endif // not implemented on other platform


} // namespace coplus
