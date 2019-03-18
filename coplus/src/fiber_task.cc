#include "coplus/fiber_task.h"

namespace coplus{

void __stdcall _co_proxy(void* pData){
	Task* p = (Task*)pData;
	p->raw_call(); // /*yield();*/ return;
	ToFiber();
	// return; // if return here, fiber is ended / undefined
}

thread_local Task::TaskStatus FiberData::public_status = Task::kUnknown;
thread_local RawFiber FiberData::caller_fiber = NilFiber;
thread_local std::shared_ptr<TriggerData> FiberData::trigger = nullptr;
thread_local std::shared_ptr<Task> FiberData::wait_for_task = nullptr;

Trigger NewTrigger(void){
	return std::make_shared<TriggerData>();
}

}