/* Copyright (C)  2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free  Software Foundation.

This program is distributed in the hope that  it will be useful, but
WITHOUT ANY WARRANTY; without even  the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License version 2.0 for more details.

You should have received a  copy of the GNU General Public License
version 2.0  along with this  program; if not, write to the Free
Software Foundation,  Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA  */

#ifndef _STDAFX_H_
#define _STDAFX_H_

#ifndef __GNUC__
#ifndef _WIN32_WINNT		// Allow use of features specific to Windows XP or later.
#define _WIN32_WINNT 0x0501	// Change this to the appropriate value to target other versions of Windows.
#endif
#include <winsock2.h>
#include <windows.h>
#pragma warning(disable : 4800)  // data type conversion performance warning
#pragma warning(disable : 4290)  // C++ exception specification ignored except to indicate a function is not __declspec(nothrow)
#else
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <dirent.h>
#include <semaphore.h>
#endif

// section: ANSI C
#include <ctype.h>
#include <stdio.h>	// for mysql
#include <stddef.h> // for mysql
#include <errno.h>	// for mysql
#include <stdarg.h>	// for mysql
#include <signal.h>	// for mysql
#include <float.h>	// for mysql
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <memory.h>
#include <search.h>

// section STL
//#ifdef __cplusplus
#include <sstream>
#include <iostream>
#include <locale>	// for std::collate
#include <iterator>	// for stdext::checked_array_iterator
#include <limits>	// for std::numeric_limits
#include <queue>	// for std::priority_queue
#include <stack>	// for std::stack
#include <map>		// for std::map
#include <vector>
#include <iomanip>
#include <cstdlib>
#include <cstdio>
#include <cctype>
#include <memory>
#include <cmath>
#include <ios>
#include <iosfwd>
#include <cstring>
#include <string>
#include <bitset>
#include <cassert>
#include <typeinfo>
#include <algorithm>

#include "common/CommonDefinitions.h"

#ifdef __GNUC__
#include <ext/hash_map>
#include <ext/hash_set>
#include "system/linux/hash_set_ext.h"
#ifndef stdext
#define stdext __gnu_cxx
#endif
#else
#include <hash_map>
#include <hash_set>
#endif

#include <set>

#ifdef __GNUC__
#define NOGDICAPMASKS
#define NOVIRTUALKEYCODES
//#define NOWINMESSAGES
#define NOWINSTYLES
#define NOSYSMETRICS
#define NOMENUS
#define NOICONS
#define NOKEYSTATES
#define NOSYSCOMMANDS
#define NORASTEROPS
//#define NOSHOWWINDOW
#define OEMRESOURCE
#define NOATOM
#define NOCLIPBOARD
#define NOCOLOR
#define NOCTLMGR
#define NODRAWTEXT
#define NOGDI
#define NOKERNEL
//#define NOMB
#define NOMEMMGR
#define NOMETAFILE
#define NOMINMAX
#define NOOPENFILE
#define NOSCROLL
//#define NOSERVICE
#define NOSOUND
#define NOTEXTMETRIC
#define NOWH
#define NOWINOFFSETS
#define NOCOMM
#define NOKANJI
#define NOHELP
#define NOPROFILER
#define NODEFERWINDOWPOS
#define NOMCX
//#define NONLS
//#define _WIN32_WINNT 02000
#endif

#ifdef __GNUC__
// string compare functions
#else
#define gcvt      _gcvt
#define atoll     _atoi64
#endif

#ifdef __GNUC__
// unicode related functions,
#ifdef  UNICODE
#ifndef _TCHAR_DEFINED
typedef WCHAR TCHAR, _TCHAR, *PTCHAR;
#define _TCHAR_DEFINED
#endif /* !_TCHAR_DEFINED */
#define __TEXT(quote) L##quote
#define _T(quote) L##quote

#define _tcslen wcslen
#define _tcsnlen wcsnlen
#else   /* UNICODE */
#ifndef _TCHAR_DEFINED
typedef char TCHAR, _TCHAR, *PTCHAR;
#define _TCHAR_DEFINED
#endif /* !_TCHAR_DEFINED */

#define __TEXT(quote) quote
#define _T(quote) quote
#define _tcslen strlen
#define _tcsnlen strnlen
#endif
#define TEXT(quote) __TEXT(quote)
#else
#include <tchar.h>
#endif

#ifndef SEM_SEMUN_DEFINED
#ifndef __FreeBSD__
#define SEM_SEMUN_DEFINED
union semun
{
	int val;
	struct semid_ds *buf;
	unsigned short *array;
};
#endif
#endif


// WINPORT
#ifdef __WIN__
	#pragma warning( disable : 4530 )
	#pragma warning( disable : 4800 )

	#include <list>
	typedef std::list<int> dummy_int_list_;

	#include <boost/program_options.hpp>
	#include <boost/logic/tribool.hpp>
#endif // __WIN__

#ifdef __GNUC__
	#pragma GCC diagnostic ignored "-Wparentheses"
	#pragma GCC diagnostic ignored "-Wreorder"
#endif

#include "common/mysql_gate.h"

#ifdef __GNUC__
	#pragma GCC diagnostic error "-Wreorder"
#endif

#endif


