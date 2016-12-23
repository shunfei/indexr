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

#include "Dictionary.h"
#include "tools.h"
#include "util/BHQSort.h"

#ifdef SOLARIS
static const uint MAXTOTAL = 65536;//RangeCoder::MAX_TOTAL;
#endif

template<> const ushort Dictionary<uchar>::  MAXKEYS = 256;
template<> const ushort Dictionary<ushort>:: MAXKEYS = 1024;
template<> const ushort Dictionary<uint>::   MAXKEYS = 4096;
template<> const ushort Dictionary<_uint64>:: MAXKEYS = 4096;

template<> const uint Dictionary<uchar>::  NBUCK = 256;
template<> const uint Dictionary<ushort>:: NBUCK = 65536;
template<> const uint Dictionary<uint>::   NBUCK = 65536;
template<> const uint Dictionary<_uint64>:: NBUCK = 65536;



template<class T> inline bool Dictionary<T>::Insert(T key, uint count)
{
	uint b = hash(key);
	short k = buckets[b];
	while((k >= 0) && (keys[k].key != key))
		k = next[k];
	
	if(k < 0) {
		if(nkeys >= MAXKEYS) return false;
		keys[nkeys].key = key;
		keys[nkeys].count = count;
		next[nkeys] = buckets[b];			// TODO: time - insert new keys at the END of the list
		buckets[b] = nkeys++;
	}
	else keys[k].count += count;
	return true;
}

template<class T> inline bool Dictionary<T>::Encode(RangeCoder* dest, T key)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(compress);
	
	// find the 'key' in the hash
	uint b = hash(key);
	short k = buckets[b];
	while((k >= 0) && (keys[k].key != key))
		k = next[k];
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(k >= 0);			// TODO: handle ESC encoding
	
	dest->EncodeShift(keys[k].low, keys[k].count, tot_shift);
	return false;
}

template<class T> inline bool Dictionary<T>::Decode(RangeCoder* src, T& key)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(decompress);
	uint count = src->GetCountShift(tot_shift);
	short k = cnt2val[count];		// TODO: handle ESC decoding
	key = keys[k].key;
	src->DecodeShift(keys[k].low, keys[k].count, tot_shift);
	return false;
}


template<> inline uint Dictionary<uchar>::hash(uchar key) {
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NBUCK >= 256); 
	return key;
}


template<class T> void Dictionary<T>::Clear()
{
	BHASSERT(MAXTOTAL > MAXKEYS+1, "should be 'MAXTOTAL > MAXKEYS+1'");
	nkeys = 0;
	memset(buckets, -1, NBUCK*sizeof(*buckets));
	compress = decompress = false;
}

template<class T> Dictionary<T>::Dictionary(bool fullalloc)
{
	BHASSERT(MAXKEYS < SHRT_MAX, "should be 'MAXKEYS < SHRT_MAX'");
	keys = new KeyRange[MAXKEYS];								// <= 16B *  4K =  64 KB
	buckets = new short[NBUCK];									// <=  2B * 64K = 128 KB
	next = new short[MAXKEYS];									// <=  2B *  4K =   8 KB
	cnt2val = fullalloc ? (new short[MAXTOTAL]) : NULL;			//  =  2B * 64K = 128 KB  (!)
	order = fullalloc ? (new KeyRange*[MAXKEYS]) : NULL;		// <=  4B *  4K =  16 KB
	Clear();
}

template<class T> Dictionary<T>::~Dictionary()
{
	if(order) delete[] order;
	if(cnt2val) delete[] cnt2val;
	delete[] next;
	delete[] buckets;
	delete[] keys;
}

//-------------------------------------------------------------------------------------

template<class T> void Dictionary<T>::SetLows()
{
	// sort keys by descending 'count'
	uint sumcnt = 0, total = 0;
	for(short i = 0; i < nkeys; i++) {
		order[i] = &keys[i];
		sumcnt += keys[i].count;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(keys[i].count > 0);
	}
	qsort_ib(order, nkeys, sizeof(*order), compare);


	BHASSERT(sumcnt <= MAXTOTAL, "should be 'sumcnt <= MAXTOTAL'");
// set short counts
//	if(sumcnt > MAXTOTAL) {
//		assert(0);
//		uint shift = GetShift(sumcnt, MAXTOTAL - nkeys);
//		for(short i = 0; i < nkeys; i++) {
//			if((order[i]->count _SHR_ASSIGN_ shift) == 0) order[i]->count = 1;
//			total += order[i]->count;
//		}
//	}
//	else total = sumcnt;

	total = sumcnt;


	tot_shift = GetBitLen(total-1);
	BHASSERT((total <= MAXTOTAL) && (total > 0) && (1u _SHL_ tot_shift) >= total,
			"should be '(total <= MAXTOTAL) && (total > 0) && (1u _SHL_ tot_shift) >= total'");

	// correct counts to sum up to power of 2; set lows
	uint high = (1u _SHL_ tot_shift), rest = high - total, d;
	for(short i = nkeys; i > 0;) {
		rest -= (d = rest / i--);
		order[i]->low = (high -= (order[i]->count += d));
	}
	BHASSERT(high == 0, "should be 'high == 0'");

	compress = true;
}

template<class T> int Dictionary<T>::compare(const void* p1, const void* p2)
{
	typedef KeyRange KR;
	if(((*(KR**)p1)->count) < ((*(KR**)p2)->count)) return 1;
    else if(((*(KR**)p1)->count) > ((*(KR**)p2)->count)) return -1;
    else return 0;
}

template<class T> void Dictionary<T>::Save(RangeCoder* dest, T maxkey)
{
	BHASSERT(compress, "'compress' should be true");

	// save no. of keys
	dest->EncodeUniform(nkeys, (short)MAXKEYS);

	uint bitmax = GetBitLen(maxkey);
	uint c, prevc = MAXTOTAL-1;
	for(short i = 0; i < nkeys; i++) {
		// save the key and its short count-1 (which is not greater than the previous count-1)
		dest->EncodeUniform(order[i]->key, maxkey, bitmax);
		dest->EncodeUniform(c = order[i]->count - 1, prevc);
		prevc = c;
	}
}

template<class T> void Dictionary<T>::Load(RangeCoder* src, T maxkey)
{
	compress = decompress = false;

	// load no. of keys
	src->DecodeUniform(nkeys, (short)MAXKEYS);

	// load keys, their 'lows' and 'highs'; fill 'cnt2val' array
	uint bitmax = GetBitLen(maxkey);
	uint c, prevc = MAXTOTAL-1;
	uint total = 0;
	for(short i = 0; i < nkeys; i++) {
		src->DecodeUniform(keys[i].key, maxkey, bitmax);
		src->DecodeUniform(c, prevc);
		prevc = c++;
		keys[i].count = c;
		keys[i].low = total;
		if(total + c > MAXTOTAL) throw CPRS_ERR_COR;
		for(; c > 0; c--) cnt2val[total++] = i;
	}

	tot_shift = GetBitLen(total-1);
	BHASSERT((total <= MAXTOTAL) && (total > 0) && (1u _SHL_ tot_shift) >= total,
			"should be '(total <= MAXTOTAL) && (total > 0) && (1u _SHL_ tot_shift) >= total'");

	decompress = true;
}

//-------------------------------------------------------------------------------------

template class Dictionary<uchar>;
template class Dictionary<ushort>;
template class Dictionary<uint>;
template class Dictionary<_uint64>;
