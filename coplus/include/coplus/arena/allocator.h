#ifndef COPLUS_ARENA_ALLOCATOR_H_
#define COPLUS_ARENA_ALLOCATOR_H_

#include <cstring>

// #define _ARENA_DEBUG_
#include "coplus/arena/arena.h"

namespace coplus {
namespace arena {

class base_allocator{
 protected:
  static Arena __arena;
};

Arena base_allocator::__arena;

template <class T>
class allocator: public base_allocator{
 public:
  typedef T          	value_type;
  typedef T*          pointer;
  typedef const T*    const_pointer;
  typedef T&          reference;
  typedef const T&    const_reference;
  typedef size_t      size_type;
  typedef ptrdiff_t   difference_type;

  allocator(){ }
  template <class U>
  allocator(const allocator<U>& c){ }
	template<class _Other>
	allocator<T>& operator=(const allocator<_Other>&){	// assign from a related allocator (do nothing)
		return (*this);
	}

  template <class U>
  struct rebind { typedef allocator<U> other; };

  pointer allocate(size_type n, const_pointer = 0) {
    return (pointer)__arena.Malloc( (n*sizeof(T)) );
  }
  void deallocate(pointer p, size_type n) {
    __arena.Free((char*)(p));
  }
  void construct(pointer p) {
    // std::cout << "construct" << std::endl;
    new(p) T(); 
  }
  void construct(pointer p, const T& value) {
    // std::cout << "construct" << std::endl;
    new(p) T(value);
  }
  void destroy(pointer p) {
    // std::cout << "destroy" << std::endl;
    p->~T();
  }
	template<class _Uty>
	void destroy(_Uty *_Ptr){	// destroy object at _Ptr
    // std::cout << "destroy" << std::endl;
		_Ptr->~_Uty();
	}

  const_pointer address(const_reference x) const noexcept { return ::std::addressof(x);}
  pointer address(reference x) const noexcept { return ::std::addressof(x);}
  const_pointer const_address(const_reference x) const noexcept { return (const_pointer)&x; }

  size_type max_size() const noexcept { return size_type(kTotalSizeByte / sizeof(T)); }   
};


} // namespace arena
} // namespace coplus

#endif // COPLUS_ARENA_ALLOCATOR_H_