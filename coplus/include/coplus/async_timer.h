#pragma once

#include "coplus/port.h"
#include "coplus/fiber_task.h"

namespace coplus{

class AsyncTimer{
 public:
	static void await(float seconds, Trigger trigger);
	static void hr_await(float seconds, Trigger trigger);
};

} // namespace coplus
