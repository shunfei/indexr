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

#include <cstring>

#include "BasicDataFilt.h"
#include "common/bhassert.h"

template<class T> bool DataFilt_RLE<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	T* data = dataset->data;
	uint nrec = dataset->nrec;
	BHASSERT(MAXBLEN < 65536, "should be 'MAXBLEN < 65536'");

	// use RLE?
	dict.InitInsert();
	uint nrep = 0, nsamp = 0;
	for (uint i = 1; i < nrec; i += 5)
		if (dict.Insert(data[i-1])) {
			nsamp++;
			if (data[i] == data[i-1])
				nrep++;
		} else
			break;

	BHASSERT(nsamp <= 65535, "should be 'nsamp <= 65535'");
	short nkey;
	uint sum2 = 0;
	typename Dictionary<T>::KeyRange* keys = dict.GetKeys(nkey);
	for (short k = 0; k < nkey; k++)
		sum2 += keys[k].count * keys[k].count;
	if (nrep*nsamp < 5*sum2)
		return false;

	// make blocks
	Clear();
	ushort len = 1;
	T last = data[0];
	for (uint i = 1; i < nrec; i++)
		if ((data[i] == last) && (len < MAXBLEN))
			len++;
		else {
			AddLen(len);
			len = 1;
			last = data[nblk] = data[i];
		}
	AddLen(len);
	dataset->nrec = nblk;

	// save version using 2 bits
	coder->EncodeUniShift((uchar)0, 2);

	// calculate and save cum counts
	// TODO: histogram alignment to power of 2
	lencnt[0] = 0;
	uint bitmax = GetBitLen(nrec);
	for (ushort i = 1; i <= MAXBLEN; i++)
		coder->EncodeUniShift(lencnt[i] += lencnt[i-1], bitmax);
	uint total = lencnt[MAXBLEN];

	// encode block lengths
	for (uint b = 0; b < nblk; b++) {
		len = lens[b];
		coder->Encode(lencnt[len-1], lencnt[len]-lencnt[len-1], total); // TODO: EncodeShift
	}

	return true;
}
template<class T> void DataFilt_RLE<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	uchar ver;
	coder->DecodeUniShift(ver, 2);
	if (ver > 0)
		throw CPRS_ERR_COR;

	// read cum counts
	lencnt[0] = 0;
	merge_nrec = dataset->nrec;
	uint bitmax = GetBitLen(merge_nrec);
	for (ushort i = 1; i <= MAXBLEN; i++)
		coder->DecodeUniShift(lencnt[i], bitmax);
	uint total = lencnt[MAXBLEN];

	// decode block lengths
	nblk = 0;
	uint sumlen = 0;
	while (sumlen < merge_nrec) {
		uint c = coder->GetCount(total);
		ushort len = 1;
		while (c >= lencnt[len])
			len++;
		coder->Decode(lencnt[len-1], lencnt[len]-lencnt[len-1], total);
		sumlen += (lens[nblk++] = len);
	}
	if (sumlen > merge_nrec)
		throw CPRS_ERR_COR;
	dataset->nrec = nblk;
}
template<class T> void DataFilt_RLE<T>::Merge(DataSet<T>* dataset)
{
	T* data = dataset->data;
	uint nrec = merge_nrec;
	nblk = dataset->nrec;
	while (nblk > 0) {
		T val = data[--nblk];
		for (ushort i = lens[nblk]; i > 0; i--) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(nrec > 0);
			data[--nrec] = val;
		}
	}
	dataset->nrec = merge_nrec;
}

//--------------------------------------------------------------------------------------------

template<class T> bool DataFilt_DateTime<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	if ((typeid(T) == typeid(uchar)) || (typeid(T) == typeid(ushort)))
		return false;

	T* data = dataset->data;
	uint nrec = dataset->nrec;

	memcpy(data, newdata, nrec*sizeof(T));

	// save version using 2 bits
	coder->EncodeUniShift((uchar)0, 2);

	return true;
}

template<class T> void DataFilt_DateTime<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
}

template<class T> void DataFilt_DateTime<T>::Merge(DataSet<T>* dataset)
{
}

//--------------------------------------------------------------------------------------------

template<class T> bool DataFilt_Min<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	// TODO: if(dataset->maxval < 100) return false;
	T* data = dataset->data;
	uint nrec = dataset->nrec;

	// find minimum
	T minval = (T)0-(T)1;
	for (uint i = 0; i < nrec; i++)
		//if(data[i] < minval) minval = data[i];
		if ((data[i] < minval) && ((minval = data[i]) == 0))
			break;
	if (minval == 0)
		return false;
	if (minval > dataset->maxval)
		throw CPRS_ERR_PAR;

	// save 'minval'
	coder->EncodeUniform(minval, dataset->maxval);

	// subtract minimum
	for (uint i = 0; i < nrec; i++)
		data[i] -= minval;
	dataset->maxval -= minval;

	return true;
}
template<class T> void DataFilt_Min<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	coder->DecodeUniform(minval, dataset->maxval);
	if (minval == 0)
		throw CPRS_ERR_COR;
	dataset->maxval -= minval;
}
template<class T> void DataFilt_Min<T>::Merge(DataSet<T>* dataset)
{
	BHASSERT(minval > 0, "should be 'minval > 0'");
	T* data = dataset->data;
	uint nrec = dataset->nrec;
	for (uint i = 0; i < nrec; i++)
		data[i] += minval;
	dataset->maxval += minval;
}

//--------------------------------------------------------------------------------------------

template<class T> bool DataFilt_GCD<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	// TODO: if(dataset->maxval < 100) return false;
	T* data = dataset->data;
	uint nrec = dataset->nrec;

	// find GCD
	T gcd = 0;
	for (uint i = 0; i < nrec; i++)
		if ((gcd = GCD(gcd, data[i])) == 1)
			break;
	if (gcd <= 1)
		return false;
	if (gcd > dataset->maxval)
		throw CPRS_ERR_PAR;

	// save
	coder->EncodeUniform(gcd, dataset->maxval);

	// divide by GCD
	for (uint i = 0; i < nrec; i++)
		data[i] /= gcd;
	dataset->maxval /= gcd;

	return true;
}
template<class T> void DataFilt_GCD<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	coder->DecodeUniform(gcd, dataset->maxval);
	if (gcd <= 1)
		throw CPRS_ERR_COR;
	dataset->maxval /= gcd;
}
template<class T> void DataFilt_GCD<T>::Merge(DataSet<T>* dataset)
{
	BHASSERT(gcd > 1, "should be 'gcd > 1'");
	T* data = dataset->data;
	uint nrec = dataset->nrec;
	for (uint i = 0; i < nrec; i++)
		data[i] *= gcd;
	dataset->maxval *= gcd;
}

//--------------------------------------------------------------------------------------------

template<class T> double DataFilt_Diff<T>::Entropy(T* data, uint nrec, uchar bitdict, uchar bitlow, bool top)
{
	uchar mask = ((uchar)1 _SHL_ bitlow) - 1;
	BHASSERT(bitlow < sizeof(T)*8, "should be 'bitlow < sizeof(T)*8'");
	dict.InitInsert();
	if (top)
		for (uint i = 0; i < nrec; i++)
			dict.Insert((uchar)(data[i] _SHR_ bitlow));
	else
		for (uint i = 0; i < nrec; i++)
			dict.Insert((uchar)data[i] & mask);

	double len = QuickMath::nlog2n(nrec);
	short nkey;
	Dictionary<uchar>::KeyRange* keys = dict.GetKeys(nkey);
	for (short k = 0; k < nkey; k++)
		len -= QuickMath::nlog2n(keys[k].count);
	return len;
}
template<class T> double DataFilt_Diff<T>::Measure(DataSet<T>* dataset, bool diff)
{
	T* data = dataset->data;
	uint nrec = dataset->nrec;
	uint nsamp = (nrec-1 < MAXSAMP) ? nrec-1 : MAXSAMP;
	uint step = (nrec-1) / nsamp;
	uint i = 1, j = 0;

	if (diff) {
		T maxval1 = dataset->maxval + 1;
		for (; j < nsamp; i += step, j++) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i < nrec);
			if ((sample[j] = data[i] - data[i-1]) > data[i])
				sample[j] += maxval1;
		}
	} else
		for (; j < nsamp; i += step, j++) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i < nrec);
			sample[j] = data[i];
		}

	BHASSERT(j <= MAXSAMP, "should be 'j <= MAXSAMP'");

	uint nbit = GetBitLen(dataset->maxval);
	if (nbit <= BITDICT)
		return Entropy(sample, j, nbit, 0, true);

	double x = Entropy(sample, j, BITDICT, nbit-BITDICT, true);
	if (nbit <= 2*BITDICT)
		x += Entropy(sample, j, nbit-BITDICT, nbit-BITDICT, false);
	else
		x += Entropy(sample, j, BITDICT, BITDICT, false);
	return x;
}

template<class T> bool DataFilt_Diff<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	uint nrec = dataset->nrec;
	if (nrec < 100)
		return false;

	double m_orig = Measure(dataset, false);
	double m_diff = Measure(dataset, true);

	if (m_diff >= 0.99 * m_orig)
		return false;

	// save version of this routine, using 2 bits (3 = 2^2-1)
	coder->EncodeUniform((uchar)0, (uchar)3);

	T* data = dataset->data;
	T last = 0, curr, maxval1 = dataset->maxval + 1;
	//newmin = newmax = data[0];

	// make full differencing
	for (uint i = 0; i < nrec; i++) {
		data[i] = (curr = data[i]) - last;
		if (last > curr)
			data[i] += maxval1;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(data[i] <= dataset->maxval);
		last = curr;
	}

	return true;
}

template<class T> void DataFilt_Diff<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	uchar ver;
	coder->DecodeUniform(ver, (uchar)3);
	if (ver > 0)
		throw CPRS_ERR_COR;
}

template<class T> void DataFilt_Diff<T>::Merge(DataSet<T>* dataset)
{
	T* data = dataset->data;
	uint nrec = dataset->nrec;

	// integrate
	T last = 0, curr;
	T maxval = dataset->maxval, maxval1 = maxval + 1;
	for (uint i = 0; i < nrec; i++) {
		curr = last + data[i];
		if ((curr > maxval) || (curr < last))
			curr -= maxval1;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(curr <= maxval);
		last = data[i] = curr;
	}
}

//--------------------------------------------------------------------------------------------

template<class T> bool DataFilt_Uniform<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	// save version of this routine, using 2 bits (3 = 2^2-1)
	coder->EncodeUniform((uchar)0, (uchar)3);

	T* data = dataset->data;
	T maxval = dataset->maxval;
	uint nrec = dataset->nrec;

	if (maxval) {
		uint bitmax = GetBitLen(maxval);
		if (maxval == ((T)1 _SHL_ bitmax) - (T)1)
			for (uint i = 0; i < nrec; i++)
				coder->EncodeUniShift(data[i], bitmax);
		else
			for (uint i = 0; i < nrec; i++)
				coder->EncodeUniform(data[i], maxval, bitmax);
	}

	dataset->nrec = 0; // no other compression can be used after Uniform
	return true;
}
template<class T> void DataFilt_Uniform<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	// read version
	uchar ver;
	coder->DecodeUniform(ver, (uchar)3);
	if (ver > 0)
		throw CPRS_ERR_COR;

	T* data = dataset->data;
	T maxval = dataset->maxval;
	uint nrec = dataset->nrec;

	// decompress data directly to 'dataset->data' - because Uniform is always the last stage of compression
	if (maxval) {
		uint bitmax = GetBitLen(maxval);
		if (maxval == ((T)1 _SHL_ bitmax) - (T)1)
			for (uint i = 0; i < nrec; i++)
				coder->DecodeUniShift(data[i], bitmax);
		else
			for (uint i = 0; i < nrec; i++)
				coder->DecodeUniform(data[i], maxval, bitmax);
	} else
		memset(data, 0, nrec*sizeof(*data));
}
template<class T> void DataFilt_Uniform<T>::Merge(DataSet<T>* dataset)
{
}

//--------------------------------------------------------------------------------------------

TEMPLATE_CLS(DataFilt_RLE)
TEMPLATE_CLS(DataFilt_Min)
TEMPLATE_CLS(DataFilt_GCD)
TEMPLATE_CLS(DataFilt_Diff)
TEMPLATE_CLS(DataFilt_Uniform)
