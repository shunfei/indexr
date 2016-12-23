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

#ifndef _SYSTEM_INFORMATION_H_
#define _SYSTEM_INFORMATION_H_

struct _IBMemInfo
{
    long long MemTotal;
    long long MemFree;
    long long MemCached;
    long long SwapTotal;
    long long SwapFree;
    long long VTotal;
    long long VFree;
};

typedef _IBMemInfo IBMemInfo;

int GetSystemMemInfo(IBMemInfo &meminfo);
unsigned long long GetSystemFreeMemory();
unsigned long long GetSystemVirtualMemory();
unsigned long long GetProcessVirtualMemory();

// debug purpose
void PrintMemoryInfo();

#endif // _SYSTEM_INFORMATION_H_
