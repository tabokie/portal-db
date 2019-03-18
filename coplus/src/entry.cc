#include "coplus/coroutine.h"

int main(int argc, char** argv){
	auto x = go(std::bind(coplus::main, argc, argv));
	coplus::kPool.as_machine(std::move(x));
	return x.get();
}