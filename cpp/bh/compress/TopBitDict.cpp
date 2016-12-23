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

#include "TopBitDict.h"
#include "QuickMath.h"
#include "tools.h"
#include "common/bhassert.h"


template<class T> const double TopBitDict<T>::MINPREDICT = 0.97;


template<class T> TopBitDict<T>::TopBitDict(bool top) : topbottom(top ? tbTop : tbBottom)
{
	//levels[0] = new uint[MAXLEV+1];
	//levels[1] = new uint[MAXLEV+1];
	decoded = new T[MAXLEN];
	//datasort = new T[MAXLEN];
}

template<class T> TopBitDict<T>::~TopBitDict()
{
	//delete[] datasort;
	delete[] decoded;
	//delete[] levels[0];
	//delete[] levels[1];
}

//-------------------------------------------------------------------------------------

template<class T> uint TopBitDict<T>::FindOptimum(DataSet<T>* dataset, uint nbit, uint& opt_bit, Dict*& opt_dict)
{
	if(nbit <= 2) return INF;

	T* data = dataset->data;
	uint nrec = dataset->nrec;
	T maxval = dataset->maxval;

	QuickMath math;
	opt_bit = 0;
	opt_dict = &counters[0];
	double uni_pred = nrec * math.log2(maxval+1.0);
	double opt_pred = uni_pred;
	double min_pred = MINPREDICT * uni_pred;
	double max_pred = 0.99 * uni_pred;

	uint bitstart = (nbit/2 < BITSTART) ? nbit/2 : BITSTART;
	uint bit = bitstart;
	Dict* dict = &counters[1];
	double pred;
	uint maxkey = 1u _SHL_ bitstart;					// maximum no. of keys in the next loop
	uint skiprec, opt_skip;
	//uint skiprec = (nrec >> bitstart) / KEYOCCUR;	// when skipping, each possible key occurs KEYOCCUR times on avg.

	while(bit <= nbit) {
		skiprec = nrec / (maxkey * KEYOCCUR);
		if(!skiprec) skiprec = 1;

		// insert truncated bits to dictionary (counter)
		if(!Insert(dict, data, nbit, bit, nrec, skiprec)) break;

		// make prediction
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(nrec <= MAXTOTAL);
		double uplen = 0.0;
		short nkey;
		typename Dict::KeyRange* keys = dict->GetKeys(nkey);
		for(short i = 0; i < nkey; i++)
			uplen -= math.nlog2n(keys[i].count);
		if(skiprec > 1) uplen = skiprec * uplen - nrec * math.log2(skiprec);
		uplen += math.nlog2n(nrec);
		double cntlen = math.log2(MAXTOTAL) - math.log2((uint)nkey);
		pred = (nbit - bit) * nrec			// bits encoded uniformly
			 + uplen						// bits encoded with dictionary
			 + (bit + cntlen) * nkey;		// dictionary

		if((pred > max_pred) || (pred > 1.03*opt_pred)) break;
		if(pred < opt_pred) {
			//if((skiprec > 1) && (pred < min_pred)) {
			//	skiprec = 1;			// recalculate dictionary without skipping
			//	continue;
			//}
			opt_pred = pred;
			opt_bit = bit;
			opt_skip = skiprec;
			if(opt_skip == 1) {
				Dict* tmp = dict;
				dict = opt_dict;
				opt_dict = tmp;
			}
		}
		//skiprec >>= BITSTEP;
		//if(!skiprec) skiprec = 1;
		//if((skiprec == 1) && (opt_pred > min_pred)) break;
		bit += BITSTEP;
		maxkey = (uint)nkey << BITSTEP;		// upper bound for the no. of keys in the next loop
	}

	if(!opt_bit || (opt_pred >= min_pred)) return INF;
	if(opt_skip > 1) {
		bool ok = Insert(opt_dict, data, nbit, opt_bit, nrec, 1);
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ok);
		if(ok == false) return INF;
		// don't recalculate prediction
	}
	return (uint)opt_pred + 1;
}

template<class T> inline bool TopBitDict<T>::Insert(Dict* dict, T* data, uint nbit, uint bit, uint nrec, uint skiprec)
{
	dict->InitInsert();
	if(topbottom == tbTop) {				// top bits
		uchar bitlow = (uchar)(nbit - bit);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bitlow < sizeof(T)*8);
		for(uint i = 0; i < nrec; i += skiprec)
			if(!dict->Insert(data[i] >> bitlow)) return false;
	}
	else {									// low bits
		T mask = (T)1 _SHL_ (uchar)bit; mask--;
		for(uint i = 0; i < nrec; i += skiprec)
			if(!dict->Insert(data[i] & mask)) return false;
	}
	return true;
}


//template<class T> uint TopBitDict<T>::Create(DataSet<T>* dataset)
//{
//	T* data = dataset->data;
//	uint len = dataset->nrec;
//	if(len == 0) return INF;
//
//	memcpy(datasort, data, len * sizeof(*data));
//	qsort(datasort, len, sizeof(*datasort), compare);
//
//	// find optimum no. of bits to hold in the dictionary
//	uint predict = FindOptimum(datasort, len, dataset->maxval);
//	if(predict == INF) return INF;
//
//	T key = datasort[0] >> bitlow;
//	uint count = 1;
//	//uint shift = GetShift(len, maxtotal - nlev);
//
//	// create dictionary in a hash table
//	dict.InitInsert();
//	for(uint i = 1; i <= len; i++) {
//		if((i == len) || (key != (datasort[i] >> bitlow))) {
//			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(count > 0);
//			if(!dict.Insert(key, count)) BHERROR("insert");
//			if(i < len) { key = datasort[i] >> bitlow; count = 1; }
//			//scnt = cnt >> shift;
//			//if(scnt == 0) scnt = 1;
//			//hash.insert(key, AVal(cnt, low, low + scnt));
//			//low += scnt;
//		}
//		else count++;
//	}
//	dict.SetLows();
//
//	return predict;
//}
//template<class T> int TopBitDict<T>::compare(const void* p1, const void* p2)
//{
//	if(*(T*)p1 < *(T*)p2) return -1;
//	else if(*(T*)p1 > *(T*)p2) return 1;
//	else return 0;
//}
//template<class T> uint TopBitDict<T>::FindOptimum(T* datasort, uint len, T maxval)
//{
//	uint *lev1 = levels[0], *lev2 = levels[1];
//	uint nlev1 = 1, nlev2;
//	lev1[0] = 0;
//	lev1[1] = len;
//
//	uint nbit = GetBitLen(maxval);
//	uint opt_b = 0, opt_nlev = 0;
//
//	QuickMath math;
//	double predict, cntlen, uplen;
//	double opt_pred = len * math.log2(maxval+1.0);
//
//	for(uint b = 1; b < nbit; b++) {
//		nlev2 = 0;
//		uint first, last, mid;
//		T mask = (T)1 << (nbit - b);		// the bit we test currently
//
//		// find new levels by spliting levels from the previous stage
//		for(uint i = 0; i < nlev1; i++) {
//			lev2[nlev2++] = first = lev1[i];
//			last = lev1[i+1]-1;
//			if(nlev2 > MAXLEV) break;
//
//			// should we create a new level? if so, make binary search for its beginning
//			if((datasort[first] ^ datasort[last]) & mask) {
//				while(last > first + 1) {
//					mid = (first + last + 1) / 2;
//					if(datasort[mid] & mask)  last = mid;
//					else                      first = mid;
//				}
//				BHASSERT_WITH_NO_PERFORMANCE_IMPACT((datasort[last] & mask) && !(datasort[last-1] & mask));
//				lev2[nlev2++] = last;
//				if(nlev2 > MAXLEV) break;
//			}
//		}
//		if(nlev2 > MAXLEV) break;
//
//		// switch arrays
//		lev2[nlev2] = len;
//		nlev1 = nlev2;
//		uint* tmp = lev1;
//		lev1 = lev2;
//		lev2 = tmp;
//
//		// compute size of the upper-bits coding
//		if(len > MAXTOTAL) {
//			uplen = 0.0;
//			uint total = 0;
//			uint cnt, scnt, shift = GetShift(len, MAXTOTAL - nlev1);
//			for(uint i = 0; i < nlev1; i++) {
//				cnt = lev1[i+1] - lev1[i];
//				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(cnt > 0);
//				scnt = cnt >> shift;
//				if(scnt == 0) scnt = 1;
//				total += scnt;
//				uplen -= cnt * math.log2(scnt);
//			}
//			uplen += len * math.log2(total);
//		}
//		else {
//			uplen = math.nlog2n(len);
//			for(uint i = 0; i < nlev1; i++)
//				uplen -= math.nlog2n(lev1[i+1] - lev1[i]);
//		}
//
//		// make prediction
//		cntlen = math.log2(MAXTOTAL) - math.log2(nlev1);
//		predict = (nbit - b) * len			// encoding of lower bits (uniform)
//				+ uplen						// encoding of upper bits (dictionary)
//				+ (b + cntlen) * nlev1;		// dictionary
//		if(predict > 1.05 * opt_pred) break;
//		if(predict < opt_pred) {
//			opt_pred = predict;
//			opt_b = b;
//			opt_nlev = nlev1;
//		}
//	}
//
//	bitlow = nbit - opt_b;
//	nlev = opt_nlev;
//	if(opt_b == 0) return INF;
//	return (uint)opt_pred + 1;
//}

template<class T> bool TopBitDict<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	T maxval = dataset->maxval;
	if(maxval == 0) return false;

	// find optimum dictionary
	Dict* dict;
	uint nbit = GetBitLen(maxval), bitdict;
	if(FindOptimum(dataset, nbit, bitdict, dict) >= 0.98 * this->PredictUni(dataset)) return false;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bitdict);

	dict->SetLows();

	// save version of this routine, using 3 bits (7 = 2^3-1)
	IFSTAT(uint pos0 = coder->GetPos());
	coder->EncodeUniform((uchar)0, (uchar)7);

	// save no. of lower bits
	bitlow = (topbottom == tbTop) ? (T)(nbit - bitdict) : (T)bitdict;
	coder->EncodeUniform(bitlow, (T)64);

	// save dictionary
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bitlow < sizeof(maxval)*8);
	T maxhigh = maxval >> bitlow, maxlow = ((T)1 _SHL_ bitlow) - (T)1;
	T dictmax = (topbottom == tbTop) ? maxhigh : maxlow;
	dict->Save(coder, dictmax);

	IFSTAT(uint pos1 = coder->GetPos());
	IFSTAT(codesize[0] = pos1 - pos0);
	T* data = dataset->data;
	uint nrec = dataset->nrec;
	bool esc;

	// encode data
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bitlow < sizeof(T)*8);
	if(topbottom == tbTop)
		for(uint i = 0; i < nrec; i++) {
			esc = dict->Encode(coder, data[i] >> bitlow);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!esc);
			data[i] &= maxlow;
		}
	else
		for(uint i = 0; i < nrec; i++) {
			esc = dict->Encode(coder, data[i] & maxlow);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!esc);
			data[i] >>= bitlow;
		}

	IFSTAT(codesize[1] = coder->GetPos() - pos1);
	dataset->maxval = (topbottom == tbTop) ? maxlow : maxhigh;
	return true;
}

template<class T> void TopBitDict<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	// read version
	uchar ver;
	coder->DecodeUniform(ver, (uchar)7);
	if(ver > 0) throw CPRS_ERR_COR;

	// read no. of lower bits
	coder->DecodeUniform(bitlow, (T)64);

	// load dictionary
	Dict* dict = counters;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bitlow < sizeof(dataset->maxval)*8);
	T maxhigh = dataset->maxval >> bitlow, maxlow = ((T)1 _SHL_ bitlow) - (T)1;
	T dictmax = (topbottom == tbTop) ? maxhigh : maxlow;
	dict->Load(coder, dictmax);

	// decode data
	bool esc;
	uint nrec = dataset->nrec;
	for(uint i = 0; i < nrec; i++) {
		esc = dict->Decode(coder, decoded[i]);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!esc);
	}

	maxval_merge = dataset->maxval;
	dataset->maxval = (topbottom == tbTop) ? maxlow : maxhigh;
}

template<class T> void TopBitDict<T>::Merge(DataSet<T>* dataset)
{
	T* data = dataset->data;
	uint nrec = dataset->nrec;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bitlow < sizeof(T)*8);
	if(topbottom == tbTop)
		for(uint i = 0; i < nrec; i++)
			data[i] |= decoded[i] << bitlow;
	else
		for(uint i = 0; i < nrec; i++)
			(data[i] <<= bitlow) |= decoded[i];
	dataset->maxval = maxval_merge;			// recover original maxval
}

//-------------------------------------------------------------------------------------

TEMPLATE_CLS(TopBitDict)
