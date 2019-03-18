#pragma once

#include <iostream>
#include <ctime> /* clock_t, clock() */
#include <chrono>

namespace coplus{

namespace {

class Cotimer{
	using ClockType = std::chrono::time_point<std::chrono::high_resolution_clock>;
 public:
 	Cotimer() { }
 	~Cotimer() { }
 	void start(void){
  	variables(true) = std::chrono::high_resolution_clock::now();
 	}
 	double end(void){
 		auto& a = variables(true);
 		auto& b = variables(false);
 		b = std::chrono::high_resolution_clock::now();
  	std::chrono::duration<double> elapsed = b - a;
  	return elapsed.count();
 	}
 	std::string describe(void) {
		time_t t = time(NULL);
		tm info;
		auto err = localtime_s(&info, &t);
 		return std::to_string(info.tm_year + 1900) + "-" +
		  std::to_string(info.tm_mon + 1) + "-" +
		  std::to_string(info.tm_mday) + " " +
		  std::to_string(info.tm_hour) + ":" +
		  std::to_string(info.tm_min) + ":" +
		  std::to_string(info.tm_sec);
 	} 
 private:
 	inline ClockType& variables(bool flag){ // true for start, false for end
 		thread_local ClockType start_ = std::chrono::high_resolution_clock::now();
 		thread_local ClockType end_ = std::chrono::high_resolution_clock::now();
 		return flag ? start_ : end_;
 	}
};	

} // anonymous np

Cotimer cotimer;	

} // namespace coplus


