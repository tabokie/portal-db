#ifndef PORTAL_UTIL_FILE_H_
#define PORTAL_UTIL_FILE_H_

#include "util/port.h"
#include "portal-db/status.h"

#include <memory>
#include <iostream>

namespace portal_db {

// Define Handle Type & Header // 
#ifdef WIN_PLATFORM
#include <windows.h>
typedef HANDLE OsFileHandle;
typedef HANDLE OsMapHandle;
inline void CaptureError(void){::std::cout << "OS raise error code: " << GetLastError() << ::std::endl;}
#endif // win

#ifdef LINUX_PLATFORM
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
typedef int OsFileHandle;
typedef bool OsMapHandle; // custom
inline void CaptureError(void){std::cout << "OS raise unknown error." << std::endl;}
#endif // linux

// no synchronization //
class WritableFile{
 protected:
  OsFileHandle fhandle_;
  size_t file_end_;
  bool is_opened_ = false;
  // extern data
  const ::std::string fileName;
 public:
  using WritableFilePtr = std::shared_ptr<WritableFile>;
  WritableFile(const char* name):fileName(name),file_end_(0){ }
  WritableFile(const ::std::string name):fileName(name),file_end_(0){ }
  WritableFile():file_end_(0){ }
  bool Empty(void){return file_end_ == 0;}
  virtual ~WritableFile(){ };
  virtual Status Open(void);
  virtual Status Close(void);
  // Close then Delete
  virtual Status Delete(void);
  virtual Status Read(size_t offset, size_t size, char* alloc_ptr) = 0;
  virtual Status Write(size_t offset, size_t size, char* data_ptr) = 0;
  virtual Status SetEnd(size_t offset) = 0;
  inline bool opened() const { return is_opened_; }
  inline const ::std::string name(void) const{return fileName;}
  inline size_t size(void) const{return file_end_;}
};

class SequentialFile : public WritableFile{
 public:
  SequentialFile(const char* name):WritableFile(name){ }
  SequentialFile(const ::std::string name):WritableFile(name){ }
  SequentialFile():WritableFile(){ }
  virtual ~SequentialFile(){ }
  Status Read(size_t offset, size_t size, char* alloc_ptr);
  Status Write(size_t offset, size_t size, char* data_ptr);
  Status SetEnd(size_t offset);
};



} // namespace portal_db

#endif // PORTAL_UTIL_FILE_H_