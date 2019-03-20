#include "util/port.h"
#include "portal-db/status.h"
#include "util/file.h"

namespace portal_db {

#ifdef WIN_PLATFORM

// Base Type :: Writabel File //
Status WritableFile::Open(void){
  fhandle_ = CreateFile(fileName.c_str(), 
    GENERIC_READ | GENERIC_WRITE, 
    0,  // share mode
    NULL,  // security
    OPEN_ALWAYS, 
    FILE_ATTRIBUTE_NORMAL, 
    NULL); // template file handle
  if(fhandle_ == INVALID_HANDLE_VALUE)return Status::IOError("Invalid File Handle");
  is_opened_ = true;
  file_end_ = GetFileSize(fhandle_, NULL);
  return Status::OK();
}

Status WritableFile::Close(void){
	is_opened_ = false;
  if(!CloseHandle(fhandle_))return Status::IOError("Close File Failed");
  return Status::OK();
}

Status WritableFile::Delete(void){
	if(is_opened_) return Status::IOError("File Still Opened");
  if(!DeleteFile(fileName.c_str())){
    std::cout << "Windows error: " << GetLastError() << std::endl;
    return Status::IOError("Cannot Delete File");
  }
  return Status::OK();
}

// Derived Type :: Sequential File //
Status SequentialFile::Read(size_t offset, size_t size, char* alloc_ptr){
	if(!is_opened_) return Status::IOError("File Not Opened");
  if(!alloc_ptr)return Status::InvalidArgument("Null data pointer.");
  if(offset + size > file_end_)return Status::InvalidArgument("Exceed file length.");
  DWORD dwPtr = SetFilePointer(fhandle_, 
    offset, 
    NULL, 
    0); // 0 for starting from beginning
  DWORD dwError;
  if(dwPtr == INVALID_SET_FILE_POINTER \
    && (dwError = GetLastError())!=NO_ERROR)return Status::IOError("Set File Pointer Failed");
  DWORD numByteRead;
  bool rfRes = ReadFile(fhandle_, 
    alloc_ptr, 
    size, 
    &numByteRead, // num of bytes read
    NULL); // overlapped structure
  if(!rfRes)return Status::IOError("Read File Failed");
  return Status::OK();
}
Status SequentialFile::Write(size_t offset, size_t size, const char* data_ptr) {
	if(!is_opened_) return Status::IOError("File Not Opened");
  if(!data_ptr)return Status::InvalidArgument("Null data pointer.");
  if(offset >= file_end_)return Status::OK();
  if(offset + size > file_end_)size = file_end_-offset;
  DWORD dwPtr = SetFilePointer(fhandle_, 
    offset, 
    NULL, 
    0); // 0 for starting from beginning
  DWORD dwError;
  if(dwPtr == INVALID_SET_FILE_POINTER \
    && (dwError = GetLastError())!=NO_ERROR)return Status::IOError("Set File Pointer Failed");
  DWORD numByteWritten;
  bool rfRes = WriteFile(fhandle_, 
    data_ptr, 
    size, 
    &numByteWritten,  // num of bytes read
    NULL); // overlapped structure
  if(!rfRes){
    std::cout << "Windows error: " << GetLastError() << std::endl;
    return Status::IOError("Write File Failed");
  }
  return Status::OK();
}

Status SequentialFile::SetEnd(size_t offset) {
	if(!is_opened_) return Status::IOError("File Not Opened");
  DWORD dwPtr = SetFilePointer(fhandle_, \
    offset, \
    NULL, \
    0); // 0 for starting from beginning
  DWORD dwError;
  if(dwPtr == INVALID_SET_FILE_POINTER \
    && (dwError = GetLastError())!=NO_ERROR)return Status::IOError("Set File Pointer Failed");
  SetEndOfFile(fhandle_);
  file_end_ = offset;
  // SetFileValidData(fhandle_, offset+block_size_)
  return Status::OK();
}

#endif

} // namespace portal_db