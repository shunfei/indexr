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

#ifndef __COMPRESSION_BASICDATAFILT_H
#define __COMPRESSION_BASICDATAFILT_H

#include "tools.h"
#include "QuickMath.h"
#include "DataFilt.h"
#include "Dictionary.h"

/* RLE */
template<class T> class DataFilt_RLE : public DataFilt<T>
{
	static const ushort MAXBLEN = 16;
	ushort *lens;
	uint nblk;
	uint *lencnt;
	//uint *vals;
	uint merge_nrec;
	Dictionary<T> dict;

	void Clear()
	{
		memset(lencnt, 0, (MAXBLEN+1)*sizeof(*lencnt));
		nblk = 0;
	}
	void AddLen(ushort len)
	{
		lens[nblk++] = len;
		lencnt[len]++;
	}
public:
	DataFilt_RLE()
	{
		lens = new ushort[CPRS_MAXREC];
		lencnt = new uint[MAXBLEN+1]; /*vals = new uint[CPRS_MAXREC];*/
	}

	virtual ~DataFilt_RLE()
	{ /*delete[] vals;*/
		delete[] lencnt;
		delete[] lens;
	}

	virtual char const* GetName() { return "rle"; }

	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};

/* recoding date and time */
template<class T> class DataFilt_DateTime : public DataFilt<T>
{
	T* newdata;
public:
	DataFilt_DateTime()	{ newdata = new T[CPRS_MAXREC];	}
	virtual ~DataFilt_DateTime() { delete[] newdata; }
	virtual char const* GetName() { return "dt"; }

	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};

/* Subtracting minimum from data */
template<class T> class DataFilt_Min : public DataFilt<T>
{
	T minval;
public:
	virtual char const* GetName() { return "min"; }
	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};

/* Dividing data by GCD */
template<class T> class DataFilt_GCD : public DataFilt<T>
{
	T gcd;
public:
	virtual char const* GetName()	{ return "gcd"; }
	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};

/* Data differencing */
template<class T> class DataFilt_Diff : public DataFilt<T>
{
	static const uint MAXSAMP = 65536/20;
	static const uchar BITDICT = 8;
	T* sample;
	Dictionary<uchar> dict;
	//T newmin, newmax, merge_maxval;
	//static int compare(const void* p1, const void* p2);		// for sorting array in increasing order
	double Entropy(T* data, uint nrec, uchar bitdict, uchar bitlow, bool top);
	double Measure(DataSet<T>* dataset, bool diff);
public:
	DataFilt_Diff()	{ sample = new T[MAXSAMP]; }
	virtual ~DataFilt_Diff() { delete[] sample; }
	virtual char const* GetName() { return "diff"; }
	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};

/* Uniform compression */
template<class T> class DataFilt_Uniform : public DataFilt<T>
{
public:
	virtual char const* GetName() { return "uni"; }
	virtual bool Encode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Decode(RangeCoder* coder, DataSet<T>* dataset);
	virtual void Merge(DataSet<T>* dataset);
};

#endif
