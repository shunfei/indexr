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

#ifndef __COMPRESSION_DATAFILT_H
#define __COMPRESSION_DATAFILT_H

#include <stdio.h>
#include "RangeCoder.h"
#include "QuickMath.h"


template<class T> struct DataSet {
	static const uint MAXLEN = 65536;
	T* data;			// array of size at least MAXLEN
	T maxval;			// maximum of 'data' or something larger
	uint nrec;			// no. of elements in 'data', <= MAXLEN
	//CprsAttrType cat;
};


/* Part of DataFilt not dependent on template type */
class DataFiltNoTemp
{
public:
	uint codesize[2];		// size of encoded data: description part [0], data part [1] (in bytes)
	virtual void ClearStats()			{ IFSTAT(codesize[0] = codesize[1] = 0); }
	virtual char const* GetName() = 0;
	virtual void LogCompress(FILE* dump) {}
};

/* Abstract class to represent atomic compression algorithms for numeric data */
template<class T> class DataFilt : public DataFiltNoTemp
{
protected:
	// utility function for descendants, to estimate size (in BITS) of data compressed uniformly
	uint PredictUni(DataSet<T>* ds)			{ return (uint)(ds->nrec * QuickMath::log2(ds->maxval+1.0)); }

public:
	virtual ~DataFilt() {}

	// Takes 'data', checks if compression is useful (otherwise returns false, nothing is written to 'coder'),
	// compresses to 'coder', the rest of data puts into 'data' for further compression, returns true.
	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset) = 0;

	// 1st pass of decompression: read compressed data from 'coder',
	// partially fill in 'dataset' (maxval, nrec), for futher stages of compression;
	// if possible, fill 'dataset->data' as well
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset) = 0;
	// 2nd pass of decompession (in reverse order), data reconstruction:
	// fill 'dataset->data', reconstruct descriptory values (maxval, nrec).
	// Decode() and Merge() are allowed to access only 'dataset->nrec' first elements of
	// 'dataset->data' (where 'nrec' is the one passed to Decode()).
	virtual void Merge(DataSet<T>* dataset) = 0;
};


#define TEMPLATE_CLS(cls)			\
	template class cls<uchar>;		\
	template class cls<ushort>;		\
	template class cls<uint>;		\
	template class cls<_uint64>;


#endif

