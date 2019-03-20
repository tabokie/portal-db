#ifndef PORTAL_UTIL_PORT_H_
#define PORTAL_UTIL_PORT_H_

namespace portal_db {

// system platform
#if defined(__WIN32) || defined(__WIN64) || defined(_MSC_VER)
	#define WIN_PLATFORM
#elif defined(__linux)
	#define LINUX_PLATFORM
#else
	#define UNKNOWN_PLATFORM
#endif

// assum little endian
#ifndef LITTLE_ENDIAN
	#define LITTLE_ENDIAN
#endif

} // namespace portal_db

#endif // PORTAL_UTIL_PORT_H_