#ifndef COPLUS_FAST_QUEUE_H_
#define COPLUS_FAST_QUEUE_H_

#include <thread> // thread
#include <functional> // function
#include <memory> // shared_ptr
#include <vector> // vector
#include <future> // std::future
#include <type_traits> // std::enable_if , result_of, is_void
#include <iostream>    
#include <queue> // std::queue
#include <cassert> // assert
#include <algorithm> // std::min
#include <cstdint>

#include "coplus/util.h"
#include "coplus/corand.h"

namespace coplus{
// Design Overview //
// Top: [Queue list & Free list]
// Queue List (In Stack, wait-free)
// Free List (In-Out Stack, non-busy structure use lock)
// Bottom: [Single-Producer Multi-Comsumer Queue]
// Procedure //
// Produce: Ask Handle -> Ask Free Queue -> Fetch or Create
// -> Produce -> Full and Handle Transfer(need send in handle) -> Ask Free Queue and Free before
// Consume: Ask Port -> Random and Probe (hard probe serially, weak probe 2-order)

template <typename ElementType, size_t unitSize = 10>
class PushOnlyFastStack: public NoMove{
	std::atomic<ElementType*>* stacks;
	std::atomic<int> end; // point to the first empty slot, meaning size
	size_t max_size;
 public:
 	PushOnlyFastStack(size_t maxSize = 100): max_size(maxSize){
 		stacks = new std::atomic<ElementType*>[(maxSize+unitSize-1) / unitSize];
 		for(int i = 0; i < (maxSize+unitSize-1) / unitSize; i++)
 			stacks[i].store((ElementType*)NULL);
 		end.store(0);
 	}
	~PushOnlyFastStack(){
		for(int i = 0; i < (max_size+unitSize-1) / unitSize; i++){
			delete [] stacks[i].load();
		}
		delete [] stacks;
	}
	// get element, no delete
	bool get(int token, ElementType& ret){
		if(token > max_size)return false;
		int mainSlot = token / unitSize;
		int subSlot = token - mainSlot * unitSize;
		ElementType* oldStack;
		if((oldStack = stacks[mainSlot].load()) == NULL){
			return false;
		}
		else{
			ret = oldStack[subSlot];
			return true;
		}
	}
	// return new token
	int push(ElementType newItem){
		int slot = std::atomic_fetch_add(&end, 1) ; // start from 0
		if(slot >= max_size)return -1;
		int mainSlot = slot / unitSize;
		int subSlot = slot - mainSlot * unitSize;
		ElementType* oldStack;
		if((oldStack = stacks[mainSlot].load()) == NULL){
			ElementType* newStack = new ElementType[unitSize];
			if(std::atomic_compare_exchange_strong(&stacks[mainSlot], &oldStack, newStack)){
				// first to new
				oldStack = newStack;
			}
			else{
				delete [] newStack;
			}
		}
		oldStack[subSlot] = (newItem);
		return slot;
	}

	int size(void){
		return end.load();
	}

};

template <typename ElementType, size_t unitSize>
class PushOnlyFastStack<std::unique_ptr<ElementType>, unitSize>{
	using OwnerPtr = std::unique_ptr<ElementType>;
	using RefPtr = ElementType*;
	std::atomic<OwnerPtr*>* stacks;
	std::atomic<int> end; // point to the first empty slot, meaning size
	size_t max_size;
 public:
 	PushOnlyFastStack(size_t maxSize): max_size(maxSize){
 		stacks = new std::atomic<OwnerPtr*>[(maxSize+unitSize-1) / unitSize]();
 		for(int i = 0; i < (maxSize+unitSize-1) / unitSize; i++)
 			stacks[i].store(nullptr);
 		end.store(0);
 	}
	~PushOnlyFastStack(){
		for(int i = 0; i < (max_size+unitSize-1) / unitSize; i++){
			delete [] stacks[i].load();
		}
		delete [] stacks;
	}
	// get element, no delete
	bool get(int token, RefPtr& ret){
		if(token > max_size)return false;
		int mainSlot = token / unitSize;
		int subSlot = token - mainSlot * unitSize;
		OwnerPtr* oldStack;
		if((oldStack = stacks[mainSlot].load()) == nullptr){
			return false;
		}
		else{
			ret = oldStack[subSlot].get();
			return true;
		}
	}
	// return new token
	int push(RefPtr newItem){
		int slot = std::atomic_fetch_add(&end, 1) ; // start from 0
		if(slot >= max_size)return -1;
		int mainSlot = slot / unitSize;
		int subSlot = slot - mainSlot * unitSize;
		OwnerPtr* oldStack;
		if((oldStack = stacks[mainSlot].load()) == nullptr){
			OwnerPtr* newStack = new OwnerPtr[unitSize]();
			if(std::atomic_compare_exchange_strong(&stacks[mainSlot], &oldStack, newStack)){
				// first to new
				oldStack = newStack;
			}
			else{
				delete [] newStack;
			}
		}
		oldStack[subSlot].reset(newItem);
		return slot;
	}
	int push(OwnerPtr&& newItem){
		int slot = std::atomic_fetch_add(&end, 1) ; // start from 0
		if(slot >= max_size)return -1;
		int mainSlot = slot / unitSize;
		int subSlot = slot - mainSlot * unitSize;
		OwnerPtr* oldStack;
		if((oldStack = stacks[mainSlot].load()) == nullptr){
			OwnerPtr* newStack = new OwnerPtr[unitSize]();
			if(std::atomic_compare_exchange_strong(&stacks[mainSlot], &oldStack, newStack)){
				// first to new
				oldStack = newStack;
			}
			else{
				delete [] newStack;
			}
		}
		oldStack[subSlot] = (std::move(newItem));
		return slot;
	}

	int size(void){
		return end.load();
	}

};

template <typename ElementType>
class SlowQueue: public NoMove{
	int max_size;
	std::vector<ElementType> ring;
	int head, tail; // head pop if < tail, tail push
	std::mutex lock;
 public:
 	// only head=tail=0 is empty, other is full
 	SlowQueue(int maxSize): max_size(maxSize), head(0), tail(0), ring(maxSize) {	}
 	~SlowQueue() { }
 	bool pop(ElementType& ret){ // get and delete
 		std::lock_guard<std::mutex> local(lock);
 		if(head == tail && tail == 0){
 			return false;
 		}
 		int slot = head;
 		head = (head==max_size-1) ? 0 : head+1;
 		if(head == tail){
 			head = tail = 0; // empty
 		}
 		ret = ring[slot];
 		return true;
 	}
 	bool push(ElementType item){
 		std::lock_guard<std::mutex> local(lock);
 		if(head == tail){
 			if(head == 0){
 				tail ++;
 				ring[0] = (item);
 				return true;
 			}
 			else{
 				return false;
 			}
 		}
 		ring[tail] = (item);
 		tail = (tail==max_size-1) ? 0 : tail+1;
 		if(head == tail && tail == 0){
 			head = tail = 1; // no equal 0
 		}
 		return true;
 	}
 	size_t size(void){
 		std::lock_guard<std::mutex> local(lock);
 		if(head == tail && tail == 0)return 0;
 		if(head == tail)return max_size;
 		if(head > tail)return tail - head + max_size;
 		return tail - head;
 	}
};

// Push: atomic add push count, if push-pop < QueueSize, pass; else add push rependent count
// Pop: atomic add pop count, if push-pop > 0, pass; else 
template <typename ElementType, size_t QueueSize = 32>
class FastLocalQueue: public NoMove{
	// all monotonically increasing
	// outer layer
	std::atomic<uint32_t> enqueue_count_;
	std::atomic<uint32_t> enqueue_revoke_;
	std::atomic<uint32_t> dequeue_count_;
	std::atomic<uint32_t> dequeue_revoke_;
	// inner layer
	std::atomic<uint32_t> head_; // dequeue from head
	std::atomic<uint32_t> tail_; // enqueue to tail
	// for accessing slot
	std::atomic<uint32_t> head_fast_;
	std::atomic<uint32_t> tail_fast_;
 	// data layer
	ElementType data_[QueueSize];
 public:
	FastLocalQueue(): 
		head_(0), tail_(0), 
		head_fast_(0), tail_fast_(0),
		enqueue_count_(0), dequeue_count_(0), 
		enqueue_revoke_(0), dequeue_revoke_(0) { }
	~FastLocalQueue() { }
	// race condition if using enq_c, deq_c to deduce state
	// thread A: enq ++; --> switch context
	// thread B: --> calc enq - enq_r - deq + deq_r == 0;
	bool push(ElementType item){
		// first check outer layer
		uint32_t enq_revoke = enqueue_revoke_.load();
		uint32_t enq = std::atomic_fetch_add(&enqueue_count_, 1) + 1;
		// probable bug here, assuming that QueueSize + hazard << uint32_t.max
		if( (enq - enq_revoke) - head_.load() > QueueSize){
			std::atomic_fetch_add(&enqueue_revoke_, 1);
			return false;
		}
		data_[std::atomic_fetch_add(&tail_fast_, 1) % QueueSize] = (item);
		std::atomic_fetch_add(&tail_, 1);
		return true;
	}
	bool pop(ElementType& ret){
		// first check outer layer
		uint32_t deq_revoke = dequeue_revoke_.load(); // safe for its increasing nature
		uint32_t deq = std::atomic_fetch_add(&dequeue_count_, 1) + 1;
		// if exactly empty, evaluate to uint32_t.max
		// if exactly full, evaluate to QueueSize - 1
		// here we assump the hazard is limited in half the max
		if(tail_.load() - (deq - deq_revoke) > 2147483647){
			std::atomic_fetch_add(&dequeue_revoke_, 1);
			return false;
		}
		ret = data_[std::atomic_fetch_add(&head_fast_, 1) % QueueSize];
		std::atomic_fetch_add(&head_, 1);
		return true;
	}
	inline size_t size(void){
		return (tail_.load() - head_.load());
		// if(current == 0){
		// 	// rough assumption of concurrency
		// 	if( (enqueue_count_.load() - enqueue_revoke_.load() - dequeue_count_.load() + dequeue_revoke_.load()) > QueueSize / 2){
		// 		current = tail_.load() - head_.load();
		// 		return (current == 0) ? QueueSize : current;
		// 	}
		// 	else{
		// 		return tail_.load() - head_.load();
		// 	}
		// }
		// return current;
	}
	inline void log(void){
		colog.put_batch(enqueue_count_.load(),
			enqueue_revoke_.load(),dequeue_count_.load(),dequeue_revoke_.load(),head_.load(),tail_.load());
	}
};

constexpr size_t kLocalQueueSize = 32;
constexpr size_t kFreeListSize = 50;
constexpr size_t kQueueMaxCount = 100;
// unordered unlimited queue without sync needed
// maintain thread local order
// provide basic interface of <<, >>, non-blocking
// implementation using thread_local buffer
template <typename ElementType>
class FastQueue: public NoMove{
	using Token = int;
	using LocalQueue = FastLocalQueue<ElementType, kLocalQueueSize>;
 	using LocalQueueOwnerPtr = std::unique_ptr<FastLocalQueue<ElementType, kLocalQueueSize>>;
 	using LocalQueueRefPtr = FastLocalQueue<ElementType, kLocalQueueSize>*;
	PushOnlyFastStack<LocalQueueOwnerPtr> QueueList;
	SlowQueue<Token> FreeList;
 public:
 	FastQueue(): FreeList(kFreeListSize), QueueList(kQueueMaxCount) { }
 	~FastQueue() { } // causing all local queues' destruction, MUST ensure all handle ref are deleted
 	size_t queue_count(void){
 		return QueueList.size();
 	}
 	size_t element_count(void){
 		// sequential scan
 		LocalQueueRefPtr local = nullptr;
 		bool status;
 		int approximateSize = QueueList.size();
 		size_t totalCount = 0;
 		for(int index = 0; index < approximateSize; index ++){
 			status = QueueList.get(index, local);
 			if(!status || !local)continue;
 			totalCount += local->size();
 			if(index == approximateSize - 1){
 				approximateSize = QueueList.size(); // update size at end
 			}
 		}
 		return totalCount;
 	}
 	// for handle producer
	// when delete a handle, queue freed only for new producer
	// consumer still keep probing
 	class ProducerHandle{
 		const int kProducerRetry = 10;
 		Token token;
 		FastQueue<ElementType>* queue_;
 		LocalQueueRefPtr local_; // no one is to delete queue, use shared
 	 public:
		ProducerHandle(FastQueue<ElementType>* queue): queue_(queue), local_(nullptr){
			// create a local queue here
			int retry = kProducerRetry;
			while(--retry && !apply()) { }
			if(retry == 0){
				retry = kProducerRetry;
				while(--retry && !preempt()) { }
			}
		}
		bool push(ElementType item){
			if(!local_) return false; // no retry
			return local_->push(item);
		}
		bool push_hard(ElementType item){ // skip to new local queue
			int retry;
			if(!local_){
				retry = kProducerRetry;
				while(--retry && !apply()) {  } 
				if(retry == 0){
					retry = kProducerRetry;
					while(--retry && !preempt()) {  }
				}
			}
			if(!local_)return false;
			
			retry = kProducerRetry;
			while(--retry && !local_->push(item)) {  }
			if(retry == 0){
				queue_->FreeList.push(token);
				retry = kProducerRetry;
				while(--retry && !preempt()) {  }
				if(!local_)return false;
			}
			else{ // @BUG
				return true;
			}
			
			retry = kProducerRetry;
			while(--retry && !local_->push(item)) {  }
			return retry != 0;
		}
		~ProducerHandle(){
			// free or delete the local queue here
			if(local_){
				queue_->FreeList.push(token);
			}
		}
	 private:
		bool apply(void){
			bool ret = queue_->FreeList.pop(token);
			if(!ret)return false;
			local_ = nullptr;
			ret = queue_->QueueList.get(token, local_);
			return ret;
		}
		bool preempt(void){
			bool ret = queue_->create_queue(token);
			if(!ret)return false;
			local_ = nullptr;
			ret = queue_->QueueList.get(token, local_);
			return ret;
		}
		
	};
 	// pop fail if approximate empty
 	bool pop(ElementType& ret){
 		// randomly choose a local
 		int index = corand.UInt(QueueList.size());
 		int cur = index;
 		LocalQueueRefPtr local = nullptr;
 		bool status;
 		for(int i = 0; i==0 || cur != index; i++, cur += 2*i+1){
 			cur = cur % QueueList.size();
 			status = QueueList.get(cur, local);
 			if(!status || !local)continue;
 			status = local->pop(ret);
 			if(status)return true;
 		}
 		return false;
 	}
 	// probing in sequential way
 	bool pop_hard(ElementType& ret){
 		LocalQueueRefPtr local = nullptr;
 		bool status;
 		int approximateSize = QueueList.size();
 		int middle = corand.UInt(approximateSize);
 		for(int index = middle; index >= 0; index --){ // up to low
 			status = QueueList.get(index, local);
 			if(!status || !local)continue; // don't break yet
 			status = local->pop(ret);
 			if(status)return true;
 		}
 		for(int index = middle + 1; index < approximateSize; index ++){ // lo to up
 			status = QueueList.get(index, local);
 			if(!status || !local){
 				return false; // break
 			}
 			status = local->pop(ret);
 			if(status)return true;
 			if(index == approximateSize - 1){
 				// update size at end
 				approximateSize = QueueList.size();
 			}
 		}
 		return false;
 	}
 private:
 	bool create_queue(Token& newToken){
 		auto token = QueueList.push(new LocalQueue());
 		if(token < 0)return false;
 		newToken = token;
 		return true;
 	}
};

} // namespace coplus

#endif // COPLUS_FAST_QUEUE_H_