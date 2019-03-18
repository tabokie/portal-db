#ifndef COPLUS_CHANNEL_H_
#define COPLUS_CHANNEL_H_

#include <thread>
#include <functional>
#include <memory>
#include <vector>
#include <future>
#include <type_traits>
#include <iostream>
#include <queue>
#include <cassert>

#include "coplus/colog.h"

namespace coplus{

using Alert = std::shared_ptr<std::condition_variable>;

// ordered capcity-limited queue with enhanced sync
// apart from basic interface <<, >>
// provide blocking and interrupt, provide reader Capacity accessor
// implement using strong locked buffer
template <typename ElementType, size_t BufferCapacity, typename SchedulerType = std::thread>
class Channel {
	std::mutex recvq_lock;
	std::queue<Alert> recvq;
	std::mutex sendq_lock;
	std::queue<Alert> sendq;
	std::mutex buffer_lock;
	std::queue<ElementType> buffer;
	SchedulerType scheduler;
	std::atomic<bool> closed; // 1 for closed
 public:
	Channel(): scheduler([&](){
		std::atomic_store_explicit(&closed, false, std::memory_order_relaxed);
		// std::atomic_init(&closed, false);
		// assert(std::atomic_load(&closed) == false);
		while(!closed.load()){
			// std::lock_quard<std::mutex> lock_buffer(buffer_lock);
			buffer_lock.lock();
			if(buffer.size() < BufferCapacity){ // refer to sendq
				// std::lock_quard<std::mutex> lock_send(sendq_lock);
				sendq_lock.lock(); // last sendq grated has done sending
				if(sendq.size() > 0){
					sendq.front()->notify_one();
					sendq.pop();
				}
				sendq_lock.unlock();
			}
			if(buffer.size() > 0){
				// std::lock_quard<std::mutex> lock_recv(recvq_lock);
				recvq_lock.lock(); // last receiver has done receiving
				if(recvq.size() > 0){
					recvq.front()->notify_one();
					recvq.pop();	
				}
				recvq_lock.unlock();
			}
			buffer_lock.unlock();
		}
		return ;
	}) { scheduler.detach();}

	Channel& operator>>(ElementType& ret){
		// static already implied
		thread_local Alert alert_recv = std::make_shared<std::condition_variable>();
		recvq_lock.lock();
		recvq.push(alert_recv);
		recvq_lock.unlock();
		std::unique_lock<std::mutex> local(buffer_lock);
		alert_recv->wait(local, [&]()-> bool{return buffer.size() > 0;});
		// buffer_lock.lock();
		ret = buffer.front();
		buffer.pop();
		// buffer_lock.unlock();
		return *this;
	}
	Channel& operator<<(ElementType item){
		// static already implied
		thread_local Alert alert_send = std::make_shared<std::condition_variable>();
		sendq_lock.lock();
		sendq.push(alert_send);
		sendq_lock.unlock();
		std::unique_lock<std::mutex> local(buffer_lock);
		alert_send->wait(local, [&]()-> bool{return buffer.size() < BufferCapacity;});
		// buffer_lock.lock();
		buffer.push(item);
		// buffer_lock.unlock();
		return *this;
	}
	void close(void){
		std::atomic_store(&closed, true);
		return ;
	}

};

template <typename ElementType, typename SchedulerType>
class Channel<ElementType, 0, SchedulerType> {
	std::mutex recvq_lock;
	std::queue<Alert> recvq;
	std::mutex sendq_lock;
	std::queue<Alert> sendq;
	std::mutex buffer_lock;
	ElementType* buffer = nullptr;
	SchedulerType scheduler;
	std::atomic<bool> closed; // 1 for closed
 public:
	Channel(): scheduler([&](){
		// std::atomic_init(&closed, false); // not implemented in gcc 4.9!
		std::atomic_store_explicit(&closed, false, std::memory_order_relaxed);
		// assert(std::atomic_load(&closed) == false);
		while(true){
			if(!std::atomic_load(&closed) ){
				// std::lock_quard<std::mutex> lock_buffer(buffer_lock);
				sendq_lock.lock();
				if(sendq.size() > 0){
					recvq_lock.lock();
					if(recvq.size() > 0){
						sendq.front()->notify_one();
						recvq.front()->notify_one();
						recvq.pop();
						sendq.pop();
					}
					recvq_lock.unlock();
				}
				sendq_lock.unlock();
			}
			else{
				colog << "Channel closed.";
				break ;
			}	
		}
		return ;
	}) { scheduler.detach();}

	Channel& operator>>(ElementType& ret){
		// static already implied
		thread_local Alert alert_recv = std::make_shared<std::condition_variable>();
		recvq_lock.lock();
		recvq.push(alert_recv);
		recvq_lock.unlock();
		std::unique_lock<std::mutex> local(buffer_lock);
		int wakeonly = 0;
		alert_recv->wait(local, [&]()-> bool{return (wakeonly++) && buffer != nullptr;});
		ret = *buffer;
		delete buffer;
		buffer = nullptr;
		return *this;
	}
	Channel& operator<<(ElementType item){
		// static already implied
		thread_local Alert alert_send = std::make_shared<std::condition_variable>();
		sendq_lock.lock();
		sendq.push(alert_send);
		sendq_lock.unlock();
		std::unique_lock<std::mutex> local(buffer_lock);
		int wakeonly = 0;
		alert_send->wait(local, [&]()-> bool{return (wakeonly++) && buffer == nullptr;});
		buffer = new ElementType(item);
		return *this;
	}
	void close(void){
		std::atomic_store(&closed, true);
		return ;
	}

};

template <typename ElementType, typename SchedulerType = std::thread>
using SyncChannel = Channel<ElementType, 0, SchedulerType>;

} // namespace coplus

#endif // COPLUS_CHANNEL_H_