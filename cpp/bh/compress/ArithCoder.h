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

#ifndef __COMPRESSION_ARITHCODER_H
#define __COMPRESSION_ARITHCODER_H

#include <limits.h>
#include "DataStream.h"
#include "defs.h"

// The functions in ArithCoder throw ErrBufOverrun() or return CPRS_ERR_BUF,
// when the buffer is too short (during reading or writing).
//
// NOTE: cumulative count of symbols ('total') must be smaller than MAX_BaseT / 4 (=16383) !!!

class ArithCoder
{
public:
	typedef unsigned short 		BaseT;
	typedef unsigned long 		WideT;
	static const BaseT 			MAX_TOTAL= USHRT_MAX/4;

	// Saves encoded data to 'dest'.
	//   sum - array of cumulated counts, starting with value 0.
	//      Its size = highest_index_of_character_to_encode + 2 (1 for total)
	//      (contains 'low' for every character plus total),
	//		e.g. for characters 0 and 1 it is: { low0, low1, total }
	//   total - total sum of counts
	CprsErr CompressBytes(BitStream* dest, char* src, int slen, BaseT* sum, BaseT total);

	// dlen - the number of characters to decode ('dest' must be at least 'dlen' large)
	CprsErr DecompressBytes(char* dest, int dlen, BitStream* src, BaseT* sum, BaseT total);

	// For de/compression of bit strings.
	//   sum[] = { low0, low1, total } = { 0, count0, count0 + count1 }
	CprsErr CompressBits(BitStream* dest, BitStream* src, BaseT* sum, BaseT total);
	// dest->len is the number of bits to decode
	CprsErr DecompressBits(BitStream* dest, BitStream* src, BaseT* sum, BaseT total);

	// De/compression of bits with dynamic probabilities
	//CprsErr CompressBitsDyn(BitStream* dest, BitStream* src, uint num0, uint num1);
	//CprsErr DecompressBitsDyn(BitStream* dest, BitStream* src, uint num0, uint num1);

private:
	BaseT 	low;
	BaseT 	high;
	BaseT 	code;
	WideT 	range;
	int 	underflow_bits;
	int 	added; // number of '0' bits virtually "added" to the source during decompr. (added > 16, means error)

	// constants for uniform encoding:
	static const uint 	uni_nbit = 13;
	static const BaseT 	uni_mask = (1 << uni_nbit) - 1;
	static const BaseT 	uni_total = 1 << uni_nbit;

public:
	ArithCoder(void)
	{
		low = high = code = 0;
		range = 0;
		underflow_bits = added = 0;
	}
	~ArithCoder(void)
	{
	}

	// compression methods
	void InitCompress();
	CprsErr ScaleRange(BitStream* dest, BaseT s_low, BaseT s_high, BaseT total);
	void EndCompress(BitStream* dest);

	// decompression methods
	void InitDecompress(BitStream* src);
	BaseT GetCount(BaseT total);
	CprsErr RemoveSymbol(BitStream* src, BaseT s_low, BaseT s_high, BaseT total);

	// uniform compression and decompression
	// bitmax - bit length of maxval
	template<class T> CprsErr EncodeUniform(BitStream* dest, T val, T maxval, uint bitmax);
	template<class T> CprsErr EncodeUniform(BitStream* dest, T val, T maxval);
	template<class T> CprsErr DecodeUniform(BitStream* src, T& val, T maxval, uint bitmax);
	template<class T> CprsErr DecodeUniform(BitStream* src, T& val, T maxval);
};

#endif

