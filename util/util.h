#ifndef PORTAL_UTIL_UTIL_H_
#define PORTAL_UTIL_UTIL_H_

namespace portal_db {

class NoCopy {
 public:
  NoCopy() = default;
  NoCopy(const NoCopy&) = delete;
  NoCopy& operator=(const NoCopy&) = delete;
};

class NoMove {
 public:
  NoMove() = default;
  NoMove(const NoMove&) = delete;
  NoMove& operator=(const NoMove&) = delete;
  NoMove(NoMove&&) = delete;
  NoMove& operator=(NoMove&&) = delete;
};

class Handle : public NoCopy {
 public:
  Handle() = default;
  virtual ~Handle() { }
  virtual void release() = 0;
};

} // namespace portal_db

#endif // PORTAL_UTIL_UTIL_H_