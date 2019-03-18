#pragma once

#include <mutex>
#include <memory>

#include "coplus/task.h" // base
#include "coplus/fiber_port.h"

namespace coplus{

void __stdcall _co_proxy(void* pData);
// {
// 	Task* p = (Task*)pData;
// 	p->raw_call(); // /*yield();*/ return;
// 	ToFiber();
// 	// return; // if return here, fiber is ended / undefined
// }

struct TriggerData{
	std::mutex lk;
	std::vector<int> waiters;
	TriggerData() = default;
	~TriggerData() { }
	void Register(int id){
		std::lock_guard<std::mutex> local(lk);
		waiters.push_back(id);
	}
};
using Trigger = std::shared_ptr<TriggerData>;
Trigger NewTrigger(void);

struct FiberData { // work-around for context-free function task
	static thread_local Task::TaskStatus public_status;
	static thread_local RawFiber caller_fiber;
	// for await task
	static thread_local std::shared_ptr<Task> wait_for_task;
	static thread_local Trigger trigger;
	FiberData() = default;
	~FiberData() { }
};

template <typename FunctionType, typename test = typename std::result_of<FunctionType()>::type>
struct FiberTask: public Task{
	RawFiber ret_routine = NilFiber;
	RawFiber work_routine = NilFiber;
	FiberTask(FunctionType&& f): f_(std::move(f)) { }
	~FiberTask() { }
	Task::TaskStatus call(void){
		status = kReady;
		ret_routine = CurrentFiber();
		if(work_routine == NilFiber)work_routine = NewFiber(_co_proxy, this);
		FiberData::public_status = status;
		ToFiber(work_routine);
		status = FiberData::public_status;
		return status;
	}
	void raw_call(){
		f_();
		FiberData::public_status = kFinished;
	}
	FunctionType f_;
};	

}

