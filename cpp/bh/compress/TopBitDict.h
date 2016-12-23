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

#ifndef __COMPRESSION_TOPBITDICT_H
#define __COMPRESSION_TOPBITDICT_H

#include <limits.h>
#include "DataFilt.h"
#include "RangeCoder.h"
#include "Dictionary.h"


// Full dictionary of highest bits
template<class T> class TopBitDict : public DataFilt<T>
{
public:
	static const uint MAXTOTAL = RangeCoder::MAX_TOTAL;
	static const uint MAXLEN = 65536;
	static const uint BITSTART = 7;
	static const uint BITSTEP = 2;
	static const uint KEYOCCUR = 8;
	static const double MINPREDICT;
	enum TopBottom { tbTop, tbBottom };		// which part of bits is compressed

private:
	const TopBottom topbottom;

	//uint* levels[2];		// for temporary use in FindOptimum()
	//T* datasort;

	//struct KeyRange {
	//	uint64 key;
	//	uint count, low, high;
	//	void Set(uint64 k, uint c, uint l, uint h)	{ key=k; count=c; low=l; high=h; }
	//};
	//uint total;

	typedef Dictionary<T> Dict;
	Dict counters[2];
	T bitlow;				// no. of bits in lower part
	
	// for merging data during decompression
	T* decoded;			// decoded 'highs' of values
	T maxval_merge;

	// Finds optimum no. of bits to store in the dictionary.
	uint FindOptimum(DataSet<T>* dataset, uint nbit, uint& opt_bit, Dict*& opt_dict);
	bool Insert(Dict* dict, T* data, uint nbit, uint bit, uint nrec, uint skiprec);
	static const uint INF = UINT_MAX;

	virtual void LogCompress(FILE* f)	{ fprintf(f, "%u %u", this->codesize[0], this->codesize[1]); }

public:
	TopBitDict(bool top);
	virtual ~TopBitDict();
	virtual char const* GetName()		{ return topbottom == tbTop ? (char*)"top" : (char*)"low"; }

	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};


#endif

