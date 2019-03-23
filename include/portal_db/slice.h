#ifndef PORTAL_SLICE_H_
#define PORTAL_SLICE_H_

#include "piece.h"

namespace portal_db {

template <typename ElementType>
class ReadIterator {
 public:
 	ReadIterator() = default;
 	virtual ~ReadIterator() { }
  virtual bool Next() = 0; // true of succeed
  virtual const ElementType& Peek() const = 0; // non-consume
  virtual size_t size() const = 0;
};

template <typename ElementType>
class Iterator: public ReadIterator<ElementType> {
 public:
 	Iterator() = default;
 	virtual ~Iterator() { }
  virtual bool Append(ElementType&&) = 0;
  virtual bool Append(const ElementType&) = 0;
};

} // namespace portal_db

#endif // PORTAL_SLICE_H_