#include "coplus/fiber_port.h"
#include "coplus/fiber_task.h"

namespace coplus{
	
// Function Implement //
#ifdef WIN_PORT
void InitEnv(void){
	ConvertThreadToFiber(NULL);
}
RawFiber CurrentFiber(void){
	return GetCurrentFiber();
}
RawFiber NewFiber(void (__stdcall *f)(void*) , void* p){
	return CreateFiber(0, f, p);
}
// Assertion for correctness //
// for coroutine block A{}
// enter: ToFiber(A) and exit: ToFiber(ret_addr) happen in the same thread continuously
// even for nested coroutine, 
// child routine is a new task that executes in another thread or after parent task yields
void ToFiber(RawFiber f){
	RawFiber temp = FiberData::caller_fiber;
	FiberData::caller_fiber = CurrentFiber();
	if(f == NilFiber){
		SwitchToFiber(temp); // return
	}
	else{
		SwitchToFiber(f);
	}
}
#elif defined(POSIX_PORT)
// not implemented
#endif	

}
