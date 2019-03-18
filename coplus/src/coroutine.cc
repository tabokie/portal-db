#include "coplus/coroutine.h"
#include "coplus/async_timer.h"

namespace coplus{
void yield(){
	ToFiber(); // no input
	FiberData::public_status = Task::kReady; // reschedule to ready queue
}

void await(Trigger trigger){
	FiberData::trigger = trigger;
	FiberData::public_status = Task::kWaiting;
	FiberData::wait_for_task = nullptr;
	ToFiber();
	FiberData::public_status = Task::kReady;
	return ;
}

void await(float seconds){
	Trigger temp = NewTrigger();
	FiberData::trigger = temp;
	AsyncTimer::await(seconds, temp); // assign trigger to system callback
	FiberData::public_status = Task::kWaiting;
	FiberData::wait_for_task = nullptr;
	ToFiber(); // exit and save context
	FiberData::public_status = Task::kReady;
	return ;
}

void notify(Trigger trigger){
	if(!trigger)return ;
	std::lock_guard<std::mutex> local(trigger->lk);
	// std::lock_guard<std::mutex> protect_waiters_local(kPool.protect_waiters);
	FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&kPool.tasks);
	std::shared_ptr<Task> pTask;
	for(auto id: trigger->waiters){
		bool status = kPool.wait_queue.get(id, pTask);
		if(!status){
			colog << "wait queue fail";
			break;
		}
		status = handle.push_hard(pTask);
		// bool status = handle.push_hard(kPool.wait_queue[id]);
		if(!status){
			colog << "push fail";
		}
	}
	return ;
}	

// naked pointer fallback
void notify(TriggerData* trigger){
	if(!trigger)return ;
	std::lock_guard<std::mutex> local(trigger->lk);
	// std::lock_guard<std::mutex> protect_waiters_local(kPool.protect_waiters);
	FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&kPool.tasks);
	std::shared_ptr<Task> pTask;
	for(auto id: trigger->waiters){
		bool status = kPool.wait_queue.get(id, pTask);
		if(!status){
			colog << "wait queue fail";
			break;
		}
		status = handle.push_hard(pTask);
		// bool status = handle.push_hard(kPool.wait_queue[id]);
		if(!status){
			colog << "push fail";
		}
	}
	return ;
}	


}


