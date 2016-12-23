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

#ifndef __COMPRESS_RANGECODER_H
#define __COMPRESS_RANGECODER_H

#include <typeinfo>

#include "defs.h"
#include "tools.h"
#include "system/RCException.h"
#include "common/bhassert.h"

/* Decoder always reads exactly the same no. of bytes as the encoder saves */
class RangeCoder
{
#ifdef SOLARIS
    static const uint TOP = 16777216;
    static const uint BOT = 65536;
#else
    static const uint TOP = 1u << 24;
    static const uint BOT = 1u << 16;
    //static const uint BOT = 1u << 15;
#endif

	uint  low, code, range, _tot, _sh;
	uchar *buf, *pos, *stop;

	// constants for uniform encoding:
	static const uint uni_nbit = 16;
	static const uint uni_mask = (1u << uni_nbit) - 1;
	static const uint uni_total = 1u << uni_nbit;

	uchar InByte ()             { if(pos>=stop) throw CPRS_ERR_BUF; return *pos++; }
	void OutByte (uchar c)      { if(pos>=stop) throw CPRS_ERR_BUF; *pos++ = c; }

public:
	static const uint MAX_TOTAL = BOT;

	RangeCoder()      { buf = pos = stop = 0; }
	uint GetPos()     { return (uint)(pos - buf); }		// !!! position in BYTES !!!

	void InitCompress(void* b, uint len, uint p=0)		{ buf=(uchar*)b; pos=buf+p; stop=buf+len;  low=0; range=(uint)-1; }
	void EndCompress()									{ for(int i=0;i<4;i++)  OutByte(low>>24), low<<=8; }
	void InitDecompress(void* b, uint len, uint p=0)	{ buf=(uchar*)b; pos=buf+p; stop=buf+len;  low=code=0; range=(uint)-1;
														  for(int i=0;i<4;i++) code= code<<8 | InByte();
														}
#pragma warning(push)
#pragma warning(disable: 4146)	// unary minus applied to uint

	void Encode(uint cumFreq, uint freq, uint total) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(freq && cumFreq+freq<=total && total<=MAX_TOTAL);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(range>=BOT && low+range-1>=low);
		low   += (range/= total) * cumFreq;
		range *= freq;
		while ((low ^ (low+range))<TOP || range<BOT && ((range= -low & (BOT-1)),1))
		   OutByte(low>>24), low <<= 8, range <<= 8;
	}

	uint GetCount(uint total) {
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
		_tot = total;
#endif
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(range>=BOT && low+range-1>=code && code>=low);
		uint tmp = (code-low) / (range/= total);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(tmp < total);
		if(tmp >= total) throw CPRS_ERR_COR;
		return tmp;
	}

	void Decode(uint cumFreq, uint freq, uint total) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(_tot==total && freq && cumFreq+freq<=total && total<=MAX_TOTAL);
		low   += range * cumFreq;
		range *= freq;
		while ((low ^ (low+range))<TOP || range<BOT && ((range= -low & (BOT-1)),1))
		   code = code<<8 | InByte(), low <<= 8, range <<= 8;
			// NOTE: after range<BOT we might check for data corruption
	}

	void EncodeShift(uint cumFreq, uint freq, uint shift) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(cumFreq+freq<=(1u _SHL_ shift) && freq && (1u _SHL_ shift)<=MAX_TOTAL);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(range>=BOT && low+range-1>=low);
		low   += (range _SHR_ASSIGN_ shift) * cumFreq;
		range *= freq;
		while ((low ^ (low+range))<TOP || range<BOT && ((range= -low & (BOT-1)),1))
		   OutByte(low>>24), low <<= 8, range <<= 8;
	}

	uint GetCountShift(uint shift) {
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
		_sh = shift;
#endif
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(range>=BOT && low+range-1>=code && code>=low);
		uint tmp = (code-low) / (range _SHR_ASSIGN_ shift);
		if(tmp >= (1u<<shift)) throw CPRS_ERR_COR;
		return tmp;
	}

	void DecodeShift(uint cumFreq, uint freq, uint shift) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(_sh==shift && cumFreq+freq<=(1u _SHL_ shift) && freq && (1u _SHL_ shift)<=MAX_TOTAL);
		low   += range * cumFreq;
		range *= freq;
		while ((low ^ (low+range))<TOP || range<BOT && ((range= -low & (BOT-1)),1))
		   code = code<<8 | InByte(), low <<= 8, range <<= 8;
	}
#pragma warning(pop)

	// uniform compression and decompression (must be: val <= maxval)
	template<class T> void EncodeUniform(T val, T maxval, uint bitmax)
	{
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT((val <= maxval));
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT((((_uint64)maxval >> bitmax) == 0) || bitmax >= 64);
		if(maxval == 0) return;

		// encode groups of 'uni_nbit' bits, from the least significant
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(uni_total <= MAX_TOTAL);
		while(bitmax > uni_nbit) {
			EncodeShift((uint)(val & uni_mask), 1, uni_nbit);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(uni_nbit < sizeof(T)*8);
			val >>= uni_nbit;
			maxval >>= uni_nbit;
			bitmax -= uni_nbit;
		}

		// encode the most significant group
		//BHASSERT(maxval < MAX_TOTAL, "should be 'maxval < MAX_TOTAL'"); // compiler figure out as allways true
		Encode((uint)val, 1, (uint)maxval + 1);
	}

	template<class T> void EncodeUniform(T val, T maxval)
	{
		EncodeUniform<T>(val, maxval, GetBitLen((_uint64)maxval));
	}

	template<class T> void DecodeUniform(T& val, T maxval, uint bitmax)
	{
		val = 0;
		if(maxval == 0) return;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT((((_uint64)maxval >> bitmax) == 0) || bitmax >= 64 );

		// decode groups of 'uni_nbit' bits, from the least significant
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(uni_total <= MAX_TOTAL);
		uint v, shift = 0;
		while(shift + uni_nbit < bitmax) {
			v = GetCountShift(uni_nbit);
			DecodeShift(v, 1, uni_nbit);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(shift < 64);
			val |= (_uint64)v << shift;
			shift += uni_nbit;
		}

		// decode the most significant group
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(shift < sizeof(maxval)*8);
		uint total = (uint)(maxval >> shift) + 1;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(total <= MAX_TOTAL);
		v = GetCount(total);
		Decode(v, 1, total);
		val |= (_uint64)v << shift;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(val <= maxval);
	}

	template<class T> void DecodeUniform(T& val, T maxval)
	{
		DecodeUniform<T>(val, maxval, GetBitLen((_uint64)maxval));
	}

	// uniform compression by shifting
	template<class T> void EncodeUniShift(T x, uint shift) {
		if(shift <= uni_nbit)
			EncodeShift((uint)x, 1, shift);
		else {
			EncodeShift((uint)x & uni_mask, 1, uni_nbit);
			EncodeUniShift(x >> uni_nbit, shift - uni_nbit);
		}
	}

	template<class T> void DecodeUniShift(T& x, uint shift) {
		if(shift <= uni_nbit) {
			x = (T)GetCountShift(shift);
			DecodeShift((uint)x, 1, shift);
		}
		else {
			uint tmp = GetCountShift(uni_nbit);
			DecodeShift(tmp, 1, uni_nbit);
			DecodeUniShift(x, shift - uni_nbit);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(uni_nbit < sizeof(x)*8);
			x = (x << uni_nbit) | (T)tmp;
		}
	}
};

template<> inline void RangeCoder::EncodeUniShift<uchar>(uchar x, uint shift)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(shift <= uni_nbit);
	EncodeShift((uint)x, 1, shift);
}

template<> inline void RangeCoder::EncodeUniShift<ushort>(ushort x, uint shift)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(shift <= uni_nbit);
	EncodeShift((uint)x, 1, shift);
}


#endif

