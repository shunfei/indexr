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

#ifndef _MSC_VER
#include <sys/stat.h>
#endif /* #ifndef _MSC_VER */

#include "BHToolkit.h"
#include "time.h"
#include "ib_system.h"

uint64 srand_initializer = time(0);

uint GenerateTransactionNumber()
{
	static IBMutex sid_guard;
	{
		IBGuard guard(sid_guard);
		srand(uint(++srand_initializer));
	}
	int current_session=0;						// as random as possible
	current_session|=((rand()%1024)<<21);	// rand() initialized in RCEngine
	current_session|=((rand()%1024)<<11);
	current_session^=(rand()%8192);
	current_session+=1;						// just in case (should not be 0)]
	//std::cerr << "current_session = " << current_session << std::endl;
	return current_session;
}

bool IsPipe(const char* name)
{
#ifdef _MSC_VER

	if (*name=='\\') {
		if (strncasecmp(name, "\\\\", 2)!=0) return false;
		const char *rest = strstr(name+2, "\\");
		if ((!rest) || (rest == name+2)) return false;   // backslash not found or found at the beginning of name+2
		if (strncasecmp(rest, "\\pipe\\", 6)!=0) return false;
		return true;
	} else if (*name=='/') {
		if (strncasecmp(name, "//", 2)!=0) return false;
		const char *rest = strstr(name+2, "/");
		if ((!rest) || (rest == name+2)) return false;   // slash not found or found at the beginning of name+2
		if (strncasecmp(rest, "/pipe/", 6)!=0) return false;
		return true;
	}
	return false;

#else

	struct stat statbuf;
	int status;
	status = stat(name, &statbuf);
	return S_ISFIFO(statbuf.st_mode);

#endif
}
