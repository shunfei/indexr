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

#ifndef _BINTOOLS_H_
#define _BINTOOLS_H_

#include <boost/shared_ptr.hpp>
#include <boost/assign/std/vector.hpp>
#include <vector>

#include "common/CommonDefinitions.h"

/////// Number of "1"-s in 32-bit value //////////////////////////////

int CalculateBinSum(unsigned int n);
int CalculateBinSize(unsigned int n);
int CalculateBinSize(_uint64 n);
int CalculateByteSize(_uint64 n);

/////// Additional tools ////////////////////////////////////////////extern ////
_int64  FloatMinusEpsilon(_int64 v);		// modify the number to the nearest down (precision permitting)
_int64  FloatPlusEpsilon(_int64 v);		// modify the number to the nearest up (precision permitting)
_int64  FloatMinusEpsilon(_int64 v);		// modify the number to the nearest down (precision permitting)
_int64  FloatPlusEpsilon(_int64 v);		// modify the number to the nearest up (precision permitting)
_int64  DoubleMinusEpsilon(_int64 v);	// modify the number to the nearest down (precision permitting)
_int64  DoublePlusEpsilon(_int64 v);		// modify the number to the nearest up (precision permitting)
_int64  MonotonicDouble2Int64(_int64 d);	// encode double value (stored bitwise as _int64) into _int64 to prevent comparison directions
_int64  MonotonicInt642Double(_int64 d);	// reverse encoding done by MonotonicDouble2Int64

_int64 SafeMultiplication(_int64 x, _int64 y);	// return a multiplication of two numbers or NULL_VALUE_64 in case of overflow

uint	HashValue(unsigned char const* buf, int n);	// CRC-32 checksum for a given memory buffer, n - number of bytes
#define HASH_FUNCTION_BYTE_SIZE 16
void HashMD5( unsigned char const* buf, int n, unsigned char* hash );

RSValue Or(RSValue f, RSValue s);
RSValue And(RSValue f, RSValue s);

inline bool IsDoubleNull(const double d) {return *(_int64*)&d == NULL_VALUE_64;}
//////////////////////////////////////////////////////////////////////////////////
//
// Easy hash set for 64-bit values

#define HASH64_STEPS_MASK 0x1F

class Hash64
{
private:
	_int64 FindAddress(_int64 n)
	{
		n ^= n << 17;
		n += n >> 8;
		n ^= n << 13;
		n += n >> 42;
		n ^= n << 33;
		n += n >> 25;
		return n & address_mask;
	}

	_int64 HashStep(_int64 n)
	{
		n ^= n << 14;
		n += n >> 23;
		n += n >> 33;
		return hash_step[n & HASH64_STEPS_MASK];
	}

public:
	Hash64(int set_size)				// do not exceed the set_size numbers of input values
	{
		_int64 local_step[] = {	1,  5,  7,  11, 13, 17, 19,  23,  29,  31,  37,  41,  43,  47,  53,  59, 
								71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149 };
		int step_size = (HASH64_STEPS_MASK + 1);
		hash_step = new _int64 [step_size];
		for(int i = 0; i < step_size; i++)
			hash_step[i] = local_step[i];			

		int size;
		for(size = 4; size < set_size + 2; size <<= 1);
		size <<= 1;			// at least twice as much as set_size, typically 3 times
		t = new _int64 [size];
		memset(t, 0, size * sizeof(_int64)); 
		address_mask = size - 1;
		zero_inserted = false;
	}
	Hash64(Hash64& sec)
	{
		address_mask = sec.address_mask;
		zero_inserted = sec.zero_inserted;
		t = new _int64 [address_mask + 1];
		memcpy(t, sec.t, (address_mask + 1) * sizeof(_int64));
		int step_size = (HASH64_STEPS_MASK + 1);
		hash_step = new _int64 [step_size];
		memcpy(hash_step, sec.hash_step, step_size * sizeof(_int64));
	}

	~Hash64()
	{ 
		delete [] t;
		delete [] hash_step;
	}

	inline bool Find(_int64& n)
	{
		if(n == 0)
			return zero_inserted;
		_int64 i = FindAddress(n);
		_int64 tv = t[i];
		if(tv == n)
			return true;
		if(tv == 0)
			return false;
		_int64 s = HashStep(n);
		while(1) {
			i = (i + s) & address_mask;
			tv = t[i];
			if(tv == n)
				return true;
			if(tv == 0)
				return false;
		}
		return false;
	}

	void Insert(_int64 n) 
	{
		if(n == 0) {
			zero_inserted = true;
			return;
		}
		_int64 i = FindAddress(n);
		if(t[i] == 0) {
			t[i] = n;
			return;
		}
		if(t[i] == n)
			return;
		_int64 s = HashStep(n);
		while(1) {
			i = (i + s) & address_mask;
			if(t[i] == 0) {
				t[i] = n;
				return;
			}
			if(t[i] == n)
				return;
		}
		return;
	}

private:
	_int64*		t;
	_int64		address_mask;
	bool		zero_inserted;
	_int64*		hash_step;
};

#endif

