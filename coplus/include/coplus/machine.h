#pragma once

#include <atomic>
#include <thread>

namespace coplus{

class Machine: public NoCopy{ // simple encap of thread
	std::thread t_;
	std::atomic<bool>* close_; // using pointer for MoveAssign used in container
	int* job_count_;
	// asserting access to close happen in valid range
	// asserting thread.empty == close_
 public:
	template <typename FunctionType>
	Machine(FunctionType&& scheduler): close_(new std::atomic<bool>(false)), job_count_(new int(0)){
		t_ = std::move(std::thread([&, close=close_, schedule=std::move(scheduler), count=job_count_]() mutable{
			InitEnv();
	 		while( !close->load() ){
				if (schedule()){
					(*count) ++;
				}
	 		}
		}));
	}
	// required for container
	Machine(Machine&& rhs) : t_(std::move(rhs.t_)) {
		close_ = rhs.close_;
		rhs.close_ = nullptr;
		job_count_ = rhs.job_count_;
		rhs.job_count_ = nullptr;
	}
	// for container resize larger which is deprecated
 	Machine(): close_(new std::atomic<bool>(true)) { }
 	// just a matching interface
 	template <typename FunctionType>
 	bool restart(FunctionType&& scheduler){
 		if(!close_->load())return false;
 		t_ = std::move(std::thread([&, close=close_, schedule=std::move(scheduler), count=job_count_]() mutable{
			InitEnv();
	 		while( !close->load() ){
				if (schedule()){
					(*count) ++;
				}
	 		}
		}));
		return true;
 	}
	~Machine() { delete close_; }
	void join(void){
		if(close_->load())return; // already closed
		atomic_store(close_, true); // send out signal to thread
		t_.join();
		return ;
	}
	void report(void){
		colog << (std::to_string(*job_count_) + " jobs scheduled");
		return ;
	}
};

}