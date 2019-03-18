#pragma once

#include <vector>
#include <thread>
#include <queue>
#include <type_traits>
#include <future>
#include <chrono>
#include <algorithm>
#include <typeinfo>

#include "coplus/util.h"
#include "coplus/colog.h"
#include "coplus/fast_queue.h"
#include "coplus/task.h"
#include "coplus/fiber_task.h"
#include "coplus/machine.h"

namespace coplus {

class ThreadPool: public NoMove{
	// Table<std::shared_ptr<Task>> global_tasks;
	std::vector<Machine> machines;
	size_t threadSize;
	std::mutex mutating; // for structure mutating, lock all
 public:
	FastQueue<std::shared_ptr<Task>> tasks;
	PushOnlyFastStack<std::shared_ptr<Task>> wait_queue;
 	// std::mutex protect_waiters;
	// std::vector<std::shared_ptr<Task>> wait_queue;
 	ThreadPool(): wait_queue(1000) {
 		threadSize = std::thread::hardware_concurrency(); // one for main thread
 		for(int i = 0; i < threadSize - 1; i++){
 			machines.push_back( //std::thread(
 				[&]() -> bool {
 					std::shared_ptr<Task> current;
 					bool ret = tasks.pop_hard(current);
 					if(ret){
 						switch(current->call()){
 							case Task::kFinished:
 							break;
 							case Task::kReady:
 							{
	 							// back to pool
	 							FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
								if(!handle.push_hard(current)){
									colog << "push fail";
								}	
 							}
 							break;
 							case Task::kWaiting:
 							{
 								int id = wait_queue.push(current);
	 							// protect_waiters.lock();
	 							// int id = wait_queue.size();
	 							// wait_queue.push_back(current);
	 							// protect_waiters.unlock();
	 							FiberData::trigger->Register(id);
	 							if(FiberData::wait_for_task){ // depend on new job
									FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
									if(!handle.push_hard(FiberData::wait_for_task)){
										colog << "push fail";
									}	
								}
 							}
 							break;
 						}
 						return true;
 					}
 					// fail to execute
 					return false;
 				}
 			); // worker thread end
 			// main thread put at detor
 		}
 	}
 	void as_machine(std::future<int>&& trace){
 		InitEnv();
		int task_count = 0;
		int retry_count = 0;
		// int starve_count = 0;
		int limit = 5000;
		while( retry_count < limit || trace.wait_for(std::chrono::seconds(0)) != std::future_status::ready ){ // no thread is closing it
			std::shared_ptr<Task> current;
			bool ret = tasks.pop_hard(current);
			if(ret){
				switch(current->call()){
					case Task::kFinished:
					break;
					case Task::kReady:
					{
						// back to pool
						FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
						if(!handle.push_hard(current)){
							colog << "push fail";
						}	
					}
					break;
					case Task::kWaiting:
					{
						int id = wait_queue.push(current);
						// protect_waiters.lock();
						// int id = wait_queue.size();
						// wait_queue.push_back(current);
						// protect_waiters.unlock();
						FiberData::trigger->Register(id);
						if(FiberData::wait_for_task){ // depend on new job
							FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
							if(!handle.push_hard(FiberData::wait_for_task)){
								colog << "push fail";
							}	
						}
					}
					break;
				}
				retry_count = 0;
				task_count ++;
			}
			else{
				retry_count ++;
			}
		}
		colog << std::to_string(task_count) + " jobs scheduled by main()";
 	}
 	~ThreadPool(){
		close();
 		report();
 	}
 	void resize(size_t newSize){
 		std::lock_guard<std::mutex> local(mutating);
 		if(newSize < threadSize){
			std::for_each(machines.begin() + newSize, machines.end(), std::mem_fn(&Machine::join));
 			// machines.erase(machines.begin() + newSize, machines.end()); // cannot use erase
 			machines.resize(newSize);
 		}
 		else if(newSize > threadSize){
 			for(int i = threadSize; i < newSize; i++){
 				machines.push_back(
	 				[&]()-> bool{
	 					std::shared_ptr<Task> current;
	 					bool ret = tasks.pop_hard(current);
	 					if(ret){
	 						switch(current->call()){
	 							case Task::kFinished:
	 							break;
	 							case Task::kReady:
	 							{
		 							// back to pool
		 							FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
									if(!handle.push_hard(current)){
										colog << "push fail";
									}	
	 							}
	 							break;
	 							case Task::kWaiting:{
 									int id = wait_queue.push(current);
		 							// protect_waiters.lock();
		 							// int id = wait_queue.size();
		 							// wait_queue.push_back(current);
		 							// protect_waiters.unlock();
		 							FiberData::trigger->Register(id);
		 							if(FiberData::wait_for_task){ // depend on new job
										FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
										if(!handle.push_hard(FiberData::wait_for_task)){
											colog << "push fail";
										}	
									}
	 							}
	 							break;
	 						}
	 						return true;
	 					}
	 					return false;
	 				}
	 			);
 			}
 		}
 		threadSize = newSize;
 		return ;
 	}
 	void close(void){
		std::for_each(machines.begin(), machines.end(), std::mem_fn(&Machine::join));
 	}
 	void report(void){
 		colog << std::to_string(tasks.element_count()) + " jobs left";
		std::for_each(machines.begin(), machines.end(), std::mem_fn(&Machine::report));
 	}
	// for function with return value
	template<
	typename FunctionType, 
	typename test = typename std::enable_if<!bool(std::is_void<typename std::result_of<FunctionType()>::type>::value)>::type>
	std::future<typename std::result_of<FunctionType()>::type> submit(FunctionType&& f){
		// @BUG: thread_local outlive the FastQueue object, use temporary handle instead
		FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
		using ResultType = typename std::result_of<FunctionType()>::type;
		std::packaged_task<ResultType()> packaged_f(std::move(f));
		auto ret = (packaged_f.get_future());
		bool status = handle.push_hard(std::make_shared<FunctionTask<std::packaged_task<ResultType()>>>(std::move(packaged_f)));
		if(!status){
			colog << "Push failed";
		}
		return (ret);
	}
	// for void return function
	template<
	typename FunctionType, 
	typename test = std::enable_if<std::is_void<typename std::result_of<FunctionType()>::type>::value>::type >
	void submit(FunctionType&& f){
		FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
		bool status = handle.push_hard(std::make_shared<FunctionTask<FunctionType>>(std::move(f)));
		if(!status){
			colog << "Push failed";
		}
	}
	// for void fiber
	template<
	typename FunctionType, 
	typename test = typename std::enable_if<std::is_void<typename std::result_of<FunctionType()>::type>::value>::type >
	void go(FunctionType&& f){
		FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
		bool status = handle.push_hard(std::make_shared<FiberTask<FunctionType>>(std::move(f)));
		if(!status){
			colog << "Push failed";
		}
	}
	// for fiber with return value
	template<
	typename FunctionType, 
	typename test = typename std::enable_if<!bool(std::is_void<typename std::result_of<FunctionType()>::type>::value)>::type >
	std::future<typename std::result_of<FunctionType()>::type> go(FunctionType&& f){
		FastQueue<std::shared_ptr<Task>>::ProducerHandle handle(&tasks);
		using ResultType = typename std::result_of<FunctionType()>::type;
		std::packaged_task<ResultType()> packaged_f(std::move(f));
		auto ret = (packaged_f.get_future());
		bool status = handle.push_hard(std::make_shared<FiberTask<std::packaged_task<ResultType()>>>(std::move(packaged_f)));
		if(!status){
			colog << "Push failed";
		}
		return (ret);
	}

}; // ThreadPool

extern ThreadPool kPool;

} // namespace coplus


