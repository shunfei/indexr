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

#ifndef IB_COMPRESSION_TOOLS_H
#define IB_COMPRESSION_TOOLS_H

#include "defs.h"
#include "common/CommonDefinitions.h"

inline uint GetBitLen(uint x)
{
#ifdef __WIN__
#ifdef _M_X64
	auto DWORD bit;
	if(_BitScanReverse(&bit, x)) return bit + 1;
	else return 0;
#else
	__asm
	{
		cmp dword ptr x, 0
		jne NOTZERO
		mov eax, 0
		jmp EXIT
	NOTZERO:
		BSR eax, dword ptr x
		inc eax
	EXIT:
	}
#endif
#elif defined(__linux__) || defined(__sun__) || defined(__FreeBSD__)
#ifdef __x86_64__
	uint position;
	if (!x)
		return 0;
	asm ("bsrl %1, %0" : "=r" (position) : "r" (x));
	return position + 1;
#else //x86
	register uint b = 0;
	while (((x >> b) != 0) && (b < sizeof(x)*8))
		b++;
	return b;
#endif
#else
	register uint b = 0;
	while (((x >> b) != 0) && (b < sizeof(x)*8))
		b++;
	return b;
	// #error "Unsupported operating system."
#endif
}


inline uint GetBitLen(_uint64 x)
{
#ifdef __WIN__
#ifdef _M_IA64
	__asm
	{
		cmp qword ptr x, 0
		jne NOTZERO
		mov rax, 0
		jmp EXIT
	NOTZERO:
		BSR rax, qword ptr x
		inc rax
	EXIT:
	}
#elif defined _M_X64
	auto DWORD bit;
	if(_BitScanReverse64(&bit, x)) return bit + 1;
	else return 0;
#else
	__asm
	{
		cmp dword ptr [x+4], 0
		jne OVER32
		cmp dword ptr x, 0
		jne NOTZERO
		mov eax, 0
		jmp EXIT
	OVER32:
		BSR eax, dword ptr [x+4]
		add eax, 33
		jmp EXIT
	NOTZERO:
		BSR eax, dword ptr x
		inc eax
	EXIT:
	}
#endif
#elif defined(__linux__) || defined(__sun__) || defined(__FreeBSD__)
#ifdef __x86_64__
	_uint64 position;
	if (!x)
		return 0;
	asm ("bsr %1, %0" : "=r" (position) : "r" (x));
	return (uint)position + 1;
#else //x86
	register uint b = 0;
	while (((x >> b) != 0) && (b < sizeof(x)*8))
		b++;
	return b;
#endif
#else
	register uint b = 0;
	while (((x >> b) != 0) && (b < sizeof(x)*8))
		b++;
	return b;
	// #error "Unsupported operating system."
#endif
}

inline uint GetBitLen(ushort x)		{ return GetBitLen((uint)x); }
inline uint GetBitLen(uchar x)		{ return GetBitLen((uint)x); }

// The smallest no. of bits of x which must be shifted right to make x <= limit
inline uint GetShift(uint x, uint limit)
{
	if(x <= limit) return 0;
	uint shift = GetBitLen(x) - GetBitLen(limit);
	if((x _SHR_ shift) > limit)
		shift++;
	return shift;
}

// Find Greatest Common Divisor of 2 integers with Euclid's algorithm.
// It's slightly faster if you set a <= b
template<class T>
inline T GCD(T a, T b)
{
	T t;
	if(a > b) {		// force a <= b
		t = a;
		a = b;
		b = t;
	}
	while(a != 0) {
		t = a;
		a = b % a;
		b = t;
	}
	return b;
}

#endif
