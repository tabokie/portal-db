#ifndef PORTAL_SLICE_H_
#define PORTAL_SLICE_H_

#include <piece.h>

namespace portal_db {

template <typename ElementType>
class ReadIterator {
 public:
  bool Next() = 0; // true of succeed
  const ElementType& Peek() const = 0; // non-consume
  size_t size() = 0;
};

template <typename ElementType>
class Iterator: public ReadIterator<ElementType> {
 public:
  bool Append(ElementType&&) = 0;
  bool Append(const ElementType&) = 0;
};

} // namespace portal_db

#endif // PORTAL_SLICE_H_