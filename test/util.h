#ifndef TEST_UTIL_H_
#define TEST_UTIL_H_

#include <random>
#include <string>
#include <iostream>
#include <ctime> /* clock_t, clock() */
#include <chrono>

namespace test {

class Random{
  std::mt19937 gen_;
  std::uniform_real_distribution<double> double_dist_; // [0,1)
 public:
  Random(){Shuffle();}
  void Shuffle(void){
    gen_.seed(std::random_device()());
  }
  void SetSeed(int seed){
    gen_.seed(seed);
  }
  unsigned int UInt(unsigned int max){
    return static_cast<unsigned int>(double_dist_(gen_) * max);
  }
  int Int(int max){ // [-max, max]
    return (double_dist_(gen_) * 2 - 1) * max;
  }
  float UFloat(void){
    return double_dist_(gen_);
  }
  float Float(void){
    return (double_dist_(gen_) * 2 - 1);
  }
  bool Bool(void){
    return double_dist_(gen_) > 0.5f;
  }
  std::string NumericString(int digit){
    std::string ret;
    // 5 digit a time
    while(ret.size() < digit){
      ret += std::to_string( static_cast<unsigned int>((UFloat()*9+1.0f) * 10000));
    }
    return ret.substr(0, digit);
  }
};

class Timer{
  using ClockType = std::chrono::time_point<std::chrono::high_resolution_clock>;
 public:
  Timer() { }
  ~Timer() { }
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

} // namespace test

extern test::Random rnd;
extern test::Timer timer;

#endif // TEST_UTIL_H_