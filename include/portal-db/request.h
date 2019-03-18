#ifndef PORTAL_REQUEST_H_
#define PORTAL_REQUEST_H_

namespace portal_db {

// request for service on local machine //
// null or data //
// each request is waited by unique holder
// and granted by unique worker
// this opens opportunities for optimization
// for now use plain mutex
class Request: public KeyValueSlice {
 public:
 	enum RequestType {
 		kNil = 0,
 		kGet = 1,
 		kPut = 2,
 		kScan = 3
 	};
 	RequestType type() { return type_; }
 	const Key& key() { return key0_; };
 	const Key& second_key() { return key1_; };
 	const Value& value() { return value_; };
 	static Request Get(const Key& key); // Value
 	static Request Put(const Key& key, const Value& value); // void
 	static Request Scan(const KeyRange& range); // KeyValueSlice
 protected:
 	RequestType type_ = kNil;
 	// lazy initialized data holder
 	Key key0_;
 	Key key1_;
 	Value value_;
 	Request(const Key& key): key0_(key), type_(kGet) { }
 	Request(const Key& key, const Value& value)
 		: key0_(key), value_(value), type_(kPut) { }
 	Request(const KeyRange& range)
 		: key0_(range), key1_(range.pointer_to_slice<8,8>()), type_(kScan) { }
};

} // namespace portal_db

#endif // PORTAL_REQUEST_H_