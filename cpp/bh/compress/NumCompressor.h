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

#ifndef __COMPRESSION_NUMCOMPRESSOR_H
#define __COMPRESSION_NUMCOMPRESSOR_H

#include "system/fet.h"

//#define _MAKESTAT_

#include "defs.h"
#include "RangeCoder.h"
#include "PartDict.h"
#include "TopBitDict.h"
#include "BasicDataFilt.h"
#include "QuickMath.h"
#include "common/bhassert.h"
#include "core/tools.h"

// NOTE: if possible, REUSE the same NumCompressor object instead of destroying
// and allocating a new one!!! This is more efficient, as it prevents memory reallocation
// (every object allocates the same amount of memory - several MB - during creation)

// Non-templated interface of NumCompressor
class NumCompressorBase
{
public:
	static const uint NFILTERS = 7;
	FILE* dump;

	// statistics
	struct Stat {
		clock_t tc, td;					// times of compression and decomp.
		void Clear()		{ memset(this,0,sizeof(Stat)); }
		Stat()				{ Clear(); }
		Stat& operator+= (Stat& s)		{ tc += s.tc; td += s.td; return *this; }
	};
	struct Stats : public std::vector<Stat> {
		Stats(): std::vector<Stat>(NumCompressorBase::NFILTERS) {}
		//Stats(size_type cnt): std::vector<Stat>(cnt) {}
		Stats& operator+= (Stats& s) {
			BHASSERT(size() == s.size(), "should be 'size() == s.size()'");
			for(size_type i = 0; i < size(); i++)
				(*this)[i] += s[i];
			return *this;
		}
		Stats& operator=(Stats& s)	{ assign(s.begin(), s.end()); return *this; }
		void Clear()				{ for(size_type i = 0; i < size(); i++) (*this)[i].Clear(); }
	};
	Stats stats;

	//virtual void GetFilters(std::vector<DataFiltNoTemp*>& v) {}

	// non-templated type-safe compression methods
	virtual CprsErr Compress(char* dest, uint& len, const uchar*  src, uint nrec, uchar  maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Compress(char* dest, uint& len, const ushort* src, uint nrec, ushort maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Compress(char* dest, uint& len, const uint*   src, uint nrec, uint   maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Compress(char* dest, uint& len, const _uint64* src, uint nrec, _uint64 maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Decompress(uchar*  dest, char* src, uint len, uint nrec, uchar  maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Decompress(ushort* dest, char* src, uint len, uint nrec, ushort maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Decompress(uint*   dest, char* src, uint len, uint nrec, uint   maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	virtual CprsErr Decompress(_uint64* dest, char* src, uint len, uint nrec, _uint64 maxval, CprsAttrType cat = CAT_OTHER)
	{
		BHERROR("method should not be invoked");
		return CPRS_ERR_OTH;
	}

	// non-templated NON-type-safe compression methods - use them carefully!
	// make sure that you invoke them for object of appropriate type
	virtual CprsErr Compress(char* dest, uint& len, const void* src, uint nrec, _uint64 maxval, CprsAttrType cat = CAT_OTHER) = 0;
	virtual CprsErr Decompress(void* dest, char* src, uint len, uint nrec, _uint64 maxval, CprsAttrType cat = CAT_OTHER) = 0;
};

// THE NumCompressor (templated)
template<class T>
class NumCompressor : public NumCompressorBase
{
private:
	T* data;				// buffer of constant size CPRS_MAXREC elements
	bool copy_only;
	//DataFilt_Min<T>		dfMin;
	//DataFilt_GCD<T>		dfGCD;
	//DataFilt_Diff<T>	dfDiff;
	//PartDict<T>			dfDict;
	//TopBitDict<T>		dfTopBit;
	//DataFilt_Uniform<T>	dfUniform;

	// compress by simple copying the data
	CprsErr CopyCompress(char* dest, uint& len, const T* src, uint nrec);
	CprsErr CopyDecompress(T* dest, char* src, uint len, uint nrec);

	void DumpData(DataSet<T>* data, uint f);

public:
	// Filters - compression algorithms:
	std::vector<DataFilt<T>*> filters;
	//virtual void GetFilters(std::vector<DataFiltNoTemp*>& v)		{ v.resize(filters.size()); for(uint i=0; i < (uint)v.size(); i++) v[i] = filters[i]; }

	NumCompressor(bool copy_only = false);
	~NumCompressor();

	// 'len' - length of 'dest' in BYTES; upon exit contains actual size of compressed data,
	//         which is at most size_of_original_data+20
	// 'nrec' must be at most CPRS_MAXREC
	// 'maxval' - maximum value in the data
	// Data in 'src' are NOT changed.
	CprsErr CompressT(char* dest, uint& len, const T* src, uint nrec, T maxval, CprsAttrType cat = CAT_OTHER);

	// 'len' - the value of 'len' returned from Compress()
	// 'dest' must be able to hold at least 'nrec' elements of type T
	CprsErr DecompressT(T* dest, char* src, uint len, uint nrec, T maxval, CprsAttrType cat = CAT_OTHER);

	virtual CprsErr Compress(char* dest, uint& len, const T* src, uint nrec, T maxval, CprsAttrType cat = CAT_OTHER)
	{
		MEASURE_FET("NumCompressor::Compress(...)");
		return CompressT(dest, len, src, nrec, maxval, cat);
	}

	virtual CprsErr Decompress(T*  dest, char* src, uint len, uint nrec, T  maxval, CprsAttrType cat = CAT_OTHER)
	{
		MEASURE_FET("NumCompressor::Decompress(...)");
		return DecompressT(dest, src, len, nrec, maxval, cat);
	}

	virtual CprsErr Compress(char* dest, uint& len, const void* src, uint nrec, _uint64 maxval, CprsAttrType cat = CAT_OTHER)
	{
		MEASURE_FET("NumCompressor::Compress(...)");
		return CompressT(dest, len, (T*)src, nrec, (T)maxval, cat);
	}

	virtual CprsErr Decompress(void* dest, char* src, uint len, uint nrec, _uint64 maxval, CprsAttrType cat = CAT_OTHER)
	{
		MEASURE_FET("NumCompressor::Decompress(...)");
		return DecompressT((T*)dest, src, len, nrec, (T)maxval, cat);
	}
};


//-------------------------------------------------------------------------

// The same routines as in NumCompressor, but without the need to manually create an object of appropriate type T
template<class T> inline CprsErr NumCompress(char* dest, uint& len, const T* src, uint nrec, _uint64 maxval)
{
	NumCompressor<T> nc;
	return nc.Compress(dest, len, src, nrec, (T)maxval);
}
template<class T> inline CprsErr NumDecompress(T* dest, char* src, uint len, uint nrec, _uint64 maxval)
{
	NumCompressor<T> nc;
	return nc.Decompress(dest, src, len, nrec, (T)maxval);
}

//-------------------------------------------------------------------------

template<class T> NumCompressor<T>::NumCompressor(bool copy_only) : copy_only(copy_only)
{
	data = new T[CPRS_MAXREC];
	dump = NULL;

	// Create filters
	filters.reserve(NFILTERS);
	//filters.push_back(new DataFilt_RLE<T>);
	filters.push_back(new DataFilt_Min<T>);
	filters.push_back(new DataFilt_GCD<T>);
	filters.push_back(new DataFilt_Diff<T>);
	filters.push_back(new PartDict<T>);
	filters.push_back(new TopBitDict<T>(true));			// top bits
	filters.push_back(new TopBitDict<T>(false));		// low bits
	filters.push_back(new DataFilt_Uniform<T>);
	BHASSERT(filters.size() == NFILTERS, "should be 'filters.size() == NFILTERS'");
	//nfilt = 6;
	//filters = new DataFilt<T>*[nfilt];
	//filters[0] = new DataFilt_Min<T>;
	//filters[1] = new DataFilt_GCD<T>;
	//filters[2] = new DataFilt_Diff<T>;
	//filters[3] = new PartDict<T>;
	//filters[4] = new TopBitDict<T>;
	//filters[5] = new DataFilt_Uniform<T>;
	//filters[0] = &dfMin;
	//filters[1] = &dfGCD;
	//filters[2] = &dfDiff;
	//filters[3] = &dfDict;
	//filters[4] = &dfTopBit;
	//filters[5] = &dfUniform;
	IFSTAT(stats.resize(filters.size()));
}

template<class T> NumCompressor<T>::~NumCompressor()
{
	for(uint i = 0; i < (uint)filters.size(); i++)
		delete filters[i];
	delete [] data;
}

//-------------------------------------------------------------------------

template<class T> void NumCompressor<T>::DumpData(DataSet<T>* data, uint f)
{
	if(!dump) return;
	uint nbit = GetBitLen(data->maxval);
	fprintf(dump, "%u:  %u %I64u %u ;  ", f, data->nrec, (_uint64)data->maxval, nbit);
	if(f) filters[f-1]->LogCompress(dump);
	fprintf(dump, "\n");
}
//-------------------------------------------------------------------------

template<class T> CprsErr NumCompressor<T>::CopyCompress(char* dest, uint& len, const T* src, uint nrec)
{
	uint datalen = nrec * sizeof(T);
	if(len < 1 + datalen) return CPRS_ERR_BUF;
	*dest = 0;				// ID of copy compression
	memcpy(dest + 1, src, datalen);
	len = 1 + datalen;
	return CPRS_SUCCESS;
}

template<class T> CprsErr NumCompressor<T>::CopyDecompress(T* dest, char* src, uint len, uint nrec)
{
	uint datalen = nrec * sizeof(T);
	if(len < 1 + datalen) throw CPRS_ERR_BUF;
	memcpy(dest, src + 1, datalen);
	return CPRS_SUCCESS;
}

template<class T> CprsErr NumCompressor<T>::CompressT(char* dest, uint& len, const T* src, uint nrec, T maxval, CprsAttrType cat)
{
	// Format:    <ver>[1B] <compressID>[1B] <compressed_data>[...]
	// <ver>=0  - copy compression
	// <ver>=1  - current version

	if(!dest || !src || (len < 3)) return CPRS_ERR_BUF;
	if((nrec == 0) || (maxval == 0)) return CPRS_ERR_PAR;

	if(copy_only)
		return CopyCompress(dest, len, src, nrec);

	*dest = 1;						// version
	uint posID = 1, pos = 3;		// 1 byte reserved for compression ID
	ushort ID = 0;
	BHASSERT(filters.size() <= 8*sizeof(ID), "should be 'filters.size() <= 8*sizeof(ID)'");

	DataSet<T> dataset = { data, maxval, nrec };
	memcpy(data, src, nrec*sizeof(*src));

	CprsErr err = CPRS_SUCCESS;
	try {
		RangeCoder coder;
		coder.InitCompress(dest, len, pos);
		IFSTAT(stats.Clear());

		// main loop: apply all compression algorithms from 'filters'
		uint f = 0;
		IFSTAT(DumpData(&dataset, f));
		for(; (f < filters.size()) && dataset.nrec; f++) {
			IFSTAT(clock_t t1 = clock());
			if(filters[f]->Encode(&coder, &dataset))
				ID |= 1 << f;
			IFSTAT(stats[f].tc += clock() - t1);
			IFSTAT(if(ID & (1<<f)) DumpData(&dataset, f+1));
		}

		// finish and store compression ID
		coder.EndCompress();
		pos = coder.GetPos();
		*(ushort*)(dest+posID) = ID;
	} catch(CprsErr& err_)  { 
		err = err_; 
	}

	// if compression failed or the size is bigger than the raw data, use copy compression
	if(err || (pos >= 0.98*nrec*sizeof(T)))
		return CopyCompress(dest, len, src, nrec);

	len = pos;
	return CPRS_SUCCESS;
}

template<class T> CprsErr NumCompressor<T>::DecompressT(T* dest, char* src, uint len, uint nrec, T maxval, CprsAttrType cat)
{
	if(!src || (len < 1)) return CPRS_ERR_BUF;
	if((nrec == 0) || (maxval == 0)) return CPRS_ERR_PAR;

	uchar ver = (uchar)src[0];
	if(ver == 0) return CopyDecompress(dest, src, len, nrec);
	if(len < 3) return CPRS_ERR_BUF;
	ushort ID = *(ushort*)(src+1);
	uint pos = 3;

	DataSet<T> dataset = { dest, maxval, nrec };

	try {
		RangeCoder coder;
		coder.InitDecompress(src, len, pos);
		uint f = 0, nfilt = (uint)filters.size();
		IFSTAT(stats.Clear());

		// 1st stage of decompression
		for(; (f < nfilt) && dataset.nrec; f++) {
			IFSTAT(clock_t t1 = clock());
			if(ID & (1 << f)) filters[f]->Decode(&coder, &dataset);
			IFSTAT(stats[f].td += clock() - t1);
		}

		// 2nd stage
		for(; f > 0;) {
			IFSTAT(clock_t t1 = clock());
			if(ID & (1 << --f)) filters[f]->Merge(&dataset);
			IFSTAT(stats[f].td += clock() - t1);
		}
	} catch(CprsErr& err)  { 
		return err; 
	}

	return CPRS_SUCCESS;
}


#endif

