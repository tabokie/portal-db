#ifndef PORTAL_DISPATCHER_H_
#define PORTAL_DISPATCHER_H_

namespace portal_db {

// dispatch request to local / remote machines //
class Dispatcher {
 public:
 	// connect to database //
 	Status Connect(std::string& name);
 	Status Connect(IpAddr& addr);
 	Status ShutDown(std::string& name);
 	Status ShutDown(IpAddr& addr);
 	Status ShutDown(); // shutdown all connection
 	// query and update //
  Status Get(const Key& key, ValueSlice& ret);
  Status Get(const Key& key, Value& ret);
  Status Put(const Key& key, const Value& value);
  Status Scan(const KeyRange& range, KeyValueSlice& ret);
  Status Scan(const KeyRange& range, ValueSlice& ret);
  Status Scan(const KeyRange& range, KeySlice& ret);
  // statistic //
  StorageSize local_mem_size(); // local cache
  StorageSize storage_disk_size(); // local / remote storage archive size
  size_t keyvalue_size();
};

} // namespace portal_db

#endif // PORTAL_DISPATCHER_H_