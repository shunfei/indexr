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

#ifndef WINTOOLS_H_INCLUDED
#define WINTOOLS_H_INCLUDED
#ifdef _MSC_VER
#include <winsock2.h>
#include <Windows.h>
#include <Winnt.h>
#include <psapi.h>
#endif
#include "../system/RCSystem.h"

std::string DisplayError();

class SystemInfo
{
public:
//TODO: actually this function has never been used
#ifdef _MSC_VER
	static unsigned long NoPageFaults()
	{
		PROCESS_MEMORY_COUNTERS pmc;
		GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc));
		return pmc.PageFaultCount;
	}
#endif
};
#endif /* not WINTOOLS_H_INCLUDED */

