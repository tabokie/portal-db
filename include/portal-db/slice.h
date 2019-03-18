#ifndef PORTAL_SLICE_H_
#define PORTAL_SLICE_H_

#include <piece.h>

namespace portal_db {

template <typename ElementType>
class Iterator {
 public:
 	bool HasNext() = 0;
 	ElementType Next() = 0;
 	bool Append(ElementType&&) = 0;
 	bool Append(const ElementType&) = 0;
};

using KeyValueSlice = Iterator<KeyValue>;
using ValueSlice = Iterator<Key>;
using KeySlice = Iterator<Value>;

} // namespace portal_db

#endif // PORTAL_SLICE_H_