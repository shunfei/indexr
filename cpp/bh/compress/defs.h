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

/* Definitions for compression methods */

#ifndef MV_COMPRESSION_DEFS_H
#define MV_COMPRESSION_DEFS_H

// Maximum no. of records allowed in compression routines
#define  CPRS_MAXREC	65536

// Compression and decompression errors
enum CprsErr {
	CPRS_SUCCESS = 0,
	CPRS_ERR_BUF = 1, 		// buffer overflow error
	CPRS_ERR_PAR = 2,		// bad parameters
	CPRS_ERR_SUM = 3,		// wrong cumulative-sum table (for arithmetic coding)
	CPRS_ERR_VER = 4,		// incorrect version of compressed data
	CPRS_ERR_COR = 5,		// compressed data are corrupted
	CPRS_ERR_MEM = 6,		// memory allocation error
	CPRS_ERR_OTH = 100		// other error (unrecognized)
};

// Attribute types treated specially by compression routines
enum CprsAttrType/*Enum*/ {
	CAT_OTHER = 0,
	CAT_DATA  = 1,
	CAT_TIME  = 2,
};

#ifdef _MAKESTAT_
#	define IFSTAT(ins) ins
#	pragma message("-----------------  Storing STATISTICS is on!!!  -----------------")
#else
#	define IFSTAT(ins) ((void)0)
#endif

#include <iostream>

class _SHIFT_CHECK_
{
public:
	unsigned long long v;
	explicit _SHIFT_CHECK_(unsigned long long a): v(a)	{}
};

template<class T>
inline T operator>>(T a, _SHIFT_CHECK_ b)
{
	if(b.v >= sizeof(T)*8) {
		return 0;
	}
	return a >> b.v;
}
template<class T>
inline T& operator>>=(T& a, _SHIFT_CHECK_ b)
{
	if(b.v >= sizeof(T)*8) {
		a = (T)0;
		return a;
	}
	return a >>= b.v;
}

template<class T>
inline T operator<<(T a, _SHIFT_CHECK_ b)
{
	if(b.v >= sizeof(T)*8) {
		return 0;
	}
	return a << b.v;
}
template<class T>
inline T& operator<<=(T& a, _SHIFT_CHECK_ b)
{
	if(b.v >= sizeof(T)*8) {
		a = (T)0;
		return a;
	}
	return a <<= b.v;
}

#ifndef _SHR_
#define _SHR_			>>(_SHIFT_CHECK_)(unsigned long long)
#endif
#ifndef _SHR_ASSIGN_
#define _SHR_ASSIGN_	>>=(_SHIFT_CHECK_)(unsigned long long)
#endif
#ifndef _SHL_
#define _SHL_			<<(_SHIFT_CHECK_)(unsigned long long)
#endif
#ifndef _SHL_ASSIGN_
#define _SHL_ASSIGN_	<<=(_SHIFT_CHECK_)(unsigned long long)
#endif

#endif

