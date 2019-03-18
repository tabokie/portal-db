
namespace portal_db {

class DBBuilder {
 public:
  enum DBType {

  };
  DB Build(DBType type);
};

class DB {
 public:
  DB() { }
  virtual ~DB();
  virtual Status Close() = 0;
  // request pool //
  virtual Status Enqueue(Request& request) = 0;
  // statistics //
  virtual size_t active_request() = 0;
  virtual size_t served_request() = 0;
  virtual size_t failed_request() = 0;
};

} // namespace portal_db