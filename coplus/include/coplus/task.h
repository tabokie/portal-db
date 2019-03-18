#pragma once

#include <type_traits>
#include <vector>

#include "coplus/util.h"

namespace coplus{

struct Task: public NoMove {
	enum TaskStatus{
		kUnknown = 0,
		kFinished = 1,
		kReady = 2,
		kWaiting = 3
	};
	TaskStatus status = kUnknown;
	virtual TaskStatus call() = 0; // return false if not completed
	virtual ~Task() {}
	virtual void raw_call() = 0;
};

// basic wrapper of function
template <typename FunctionType>
struct FunctionTask: public Task	{
	FunctionTask(FunctionType&& f): f_(std::move(f)) { }
	~FunctionTask(){ }
	Task::TaskStatus call(){
		f_();
		return kFinished;
	}
	void raw_call(){f_();}
	FunctionType f_;
};

}