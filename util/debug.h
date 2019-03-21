#ifndef PORTAL_UTIL_DEBUG_H_
#define PORTAL_UTIL_DEBUG_H_

namespace portal_db {

#ifdef PORTAL_DEBUG
#define COMMENT(str)					std::cout << __LINE__ << ": " << str << std::endl;
#else
#define COMMENT(str)
#endif

} // namespace portal_db

#endif // PORTAL_UTIL_DEBUG_H_