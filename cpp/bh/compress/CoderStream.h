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

#ifndef __COMPRESSION_CODERSTREAM_H
#define __COMPRESSION_CODERSTREAM_H

#include "ArithCoder.h"
#include "defs.h"


// All the methods may throw exceptions of type CprsErr or ErrBufOverrun
class CoderStream : protected ArithCoder
{
	BitStream my_stream;
	BitStream* str;

public:
	void Reset(char* buf, uint len, uint pos = 0)		{ my_stream.Reset(buf, len, pos); str = &my_stream; }
	void Reset(BitStream* str_ = 0)						{ str = str_; }

	CoderStream()										{ Reset(); }
	CoderStream(char* buf, uint len, uint pos = 0)		{ Reset(buf, len, pos); }
	CoderStream(BitStream* str_)						{ Reset(str_); }
	virtual ~CoderStream() {}
	
	using ArithCoder::MAX_TOTAL;
	using ArithCoder::BaseT;

	// stream access methods
	uint GetPos()		{ return str->GetPos(); }

	// compression methods
	void InitCompress()												{ ArithCoder::InitCompress(); }
	void Encode(BaseT low, BaseT high, BaseT total)					{ CprsErr err=ArithCoder::ScaleRange(str, low, high, total); if(err) throw err; }
	void EndCompress()												{ ArithCoder::EndCompress(str); }

	// decompression methods
	void InitDecompress()											{ ArithCoder::InitDecompress(str); }
	BaseT GetCount(BaseT total)										{ return ArithCoder::GetCount(total); }
	void Decode(BaseT low, BaseT high, BaseT total)					{ CprsErr err=ArithCoder::RemoveSymbol(str, low, high, total); if(err) throw err; }

	// uniform compression and decompression
	template<class T> void EncodeUniform(T val, T maxval, uint bitmax)	
	{ 
		CprsErr err=ArithCoder::EncodeUniform<T>(str, val, maxval, bitmax); 
		if (err) 
			throw err; 
	}
	
	template<class T> void EncodeUniform(T val, T maxval)				
	{ 
		CprsErr err=ArithCoder::EncodeUniform<T>(str, val, maxval); 
		if (err) 
			throw err; 
	}
	
	template<class T> void DecodeUniform(T& val, T maxval, uint bitmax)	
	{ 
		CprsErr err=ArithCoder::DecodeUniform<T>(str, val, maxval, bitmax); 
		if (err) 
			throw err; 
	}
	
	template<class T> void DecodeUniform(T& val, T maxval)				
	{ 
		CprsErr err=ArithCoder::DecodeUniform<T>(str, val, maxval); 
		if (err) 
			throw err; 
	}
};


#endif
