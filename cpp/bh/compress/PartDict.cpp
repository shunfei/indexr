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

#include "PartDict.h"
#include "QuickMath.h"
#include "tools.h"
#include "util/BHQSort.h"

#ifdef SOLARIS
static const uint MAXLEN = 65536;
//static const uint MAXTOTAL = ArithCoder::MAX_TOTAL;
static const uint MAXTOTAL = 65536;//RangeCoder::MAX_TOTAL;

static const uint MINOCCUR = 4;                         // how many times a value must occur to be frequent value
static const uint MAXFREQ = (MAXTOTAL+20)/MINOCCUR;     // max no. of frequent values = max size of dictionary; must be smaller than USHRT_MAX
#endif

template<> const unsigned int PartDict<unsigned long long>::HashTab::nbuck = 0x20000;
template<> const unsigned int PartDict<unsigned int>  ::HashTab::nbuck = 0x20000;
template<> const unsigned int PartDict<unsigned short>::HashTab::nbuck = 65536;
template<> const unsigned int PartDict<unsigned char> ::HashTab::nbuck = 256;

template<> const unsigned int PartDict<unsigned long long>::HashTab::mask = 0x20000 - 1;
template<> const unsigned int PartDict<unsigned int>  ::HashTab::mask = 0x20000 - 1;
template<> const unsigned int PartDict<unsigned short>::HashTab::mask = 65536 - 1;
template<> const unsigned int PartDict<unsigned char> ::HashTab::mask = 256 - 1;


template<class T> inline void PartDict<T>::HashTab::insert(T key)
{
	//AKey* p = keys + fun(key);
	//while(p->count && (p->key != key))
	//	if(++p == stop) p = keys;
	//if(p->count++) return NULL;
	//p->key = key;
	//return p;

	uint b = fun(key);
	int k = buckets[b];
	if(k < 0) nusebuck++;
	else while((k >= 0) && (keys[k].key != key))
		k = keys[k].next;

	if(k < 0) {
#ifdef SOLARIS
        BHASSERT(nkeys < 65536, "should be 'nkeys < PartDict::MAXLEN'");
#else
        BHASSERT(nkeys < PartDict::MAXLEN, "should be 'nkeys < PartDict::MAXLEN'");
#endif
		AKey& ak = keys[nkeys];
		ak.key = key;
		ak.count = 1;
		ak.low = (uint)-1;
		//ak.high = 0;
		ak.next = buckets[b];			// TODO: time - insert new keys at the END of the list
		buckets[b] = nkeys++;
	}
	else keys[k].count++;
}

template<class T> inline int PartDict<T>::HashTab::find(T key)
{
	//AKey* p = keys + fun(key);
	//while(p->count && (p->key != key))
	//	if(++p == stop) p = keys;
	//return p->count ? p : NULL;

	uint b = fun(key);
	int k = buckets[b];
	while((k >= 0) && (keys[k].key != key))
		k = keys[k].next;
	return k;
}


template<> inline unsigned int PartDict<unsigned long long>::HashTab::fun(unsigned long long key) {
	uint x = ((uint)key ^ *((uint*)&key+1));
	BHASSERT(topbit < sizeof(uint)*8, "should be 'topbit < sizeof(uint)*8'");
	return (x&mask)^(x>>topbit);
}
template<> inline unsigned int PartDict<unsigned int>::HashTab::fun(unsigned int key) {
	BHASSERT(topbit < sizeof(uint)*8, "should be 'topbit < sizeof(uint)*8'");
	return (key&mask)^(key>>topbit);
}
template<> inline unsigned int PartDict<unsigned short>::HashTab::fun(unsigned short key) {
	BHASSERT(key <= mask, "should be 'key <= mask'");
	return key;
}
template<> inline unsigned int PartDict<unsigned char>::HashTab::fun(unsigned char key) {
	BHASSERT(key <= mask, "should be 'key <= mask'");
	return key;
}


template<class T> inline bool PartDict<T>::GetRange(T val, uint& low, uint& count)
{
	//BHASSERT(compress, "'compress' should be true");
	int k = hash.find(val);
	//HashTab::AKey* p = hash.find(val);
	// OK under assumption that PartDict is built on the whole data to be compressed
	BHASSERT(k >= 0, "should be 'k >= 0'");
	//high = hash.keys[k].high;
	low = hash.keys[k].low;
	if(low == (uint)-1) {			// ESCape
		low = esc_low;
		count = esc_usecnt;
		//high = esc_high;
		return true;
	}
	count = hash.keys[k].count;
	//low = hash.keys[k].low;
	//BHASSERT(low < high, "should be 'low < high'");
	return false;
}

template<class T> inline bool PartDict<T>::GetVal(uint c, T& val, uint& low, uint& count)
{
	//BHASSERT(decompress, "'decompress' should be true");
	if(c >= esc_low) {		// ESCape
		low = esc_low;
		count = esc_usecnt;
		//high = esc_high;
		return true;
	}
	ValRange& vr = freqval[cnt2val[c]];
	val = vr.val;
	low = vr.low;
	count = vr.count;
	//high = vr.high;
	return false;
}

template<class T> inline void PartDict<T>::HashTab::Clear()
{
	nkeys = nusebuck = 0;
	memset(buckets, -1, nbuck*sizeof(*buckets));
}

template<class T> PartDict<T>::HashTab::HashTab()
{
	buckets = new int[nbuck];				// 128K * 4B = 512 KB
#ifdef SOLARIS
    keys = new AKey[MAXLEN];                // 64K * 24B = 1.5 MB
#else
    keys = new AKey[PartDict::MAXLEN];      // 64K * 24B = 1.5 MB
#endif
	Clear();
}

template<class T> PartDict<T>::HashTab::~HashTab()
{
	delete[] keys;
	delete[] buckets;
}

template<class T> void PartDict<T>::Clear()
{
	//compress = decompress = false;
}

template<class T> PartDict<T>::PartDict()
{
	freqkey = new typename HashTab::AKey*[MAXLEN];	// 64K * 4B = 256 KB
	freqval = new ValRange[MAXFREQ];	// 8K * 16B = 128 KB
	cnt2val = new ushort[MAXTOTAL];		// 16K * 2B =  32 KB
	decoded = new T[MAXLEN];
	isesc = new bool[MAXLEN];
	Clear();
}

template<class T> PartDict<T>::~PartDict()
{
	delete[] isesc;
	delete[] decoded;
	delete[] cnt2val;
	delete[] freqval;
	delete[] freqkey;
}

//-------------------------------------------------------------------------------------

template<class T> void PartDict<T>::Create(DataSet<T>* dataset)
{
	T* data = dataset->data;
	uint len = dataset->nrec;
	BHASSERT(len <= MAXLEN, "should be 'len <= MAXLEN'");

	// put the data into hash table - to count no. of occurences of each value
	nfreq = 0;
	hash.Clear();
	//HashTab::AKey* p;
	for(uint i = 0; i < len; i++)
		//if(p = hash.insert(data[i])) freqkey[nfreq++] = p;
		hash.insert(data[i]);

	// leave only frequent keys in the 'freqkey'
	//int k = 0;
	//while(k < nfreq)
	//	if(freqkey[k]->count < MINOCCUR) freqkey[k] = freqkey[--nfreq];
	//	else k++;

	for(int k = 0; k < hash.nkeys; k++)
		if(hash.keys[k].count >= MINOCCUR)
			freqkey[nfreq++] = &hash.keys[k];

	// sort the array of pointers to frequent values in descending order
	if(nfreq > 0) qsort_ib(freqkey, nfreq, sizeof(*freqkey), compare);

	// find the best size of dictionary
	//QuickMath math;
	//double logmax = math.log2(maxval+1.0);			// no. of bits to encode a value uniformly
	//double logtot = math.log2(MAXTOTAL), diff;
	//int c, rest, sumc = 0, f = 0;
	//for(; f < (int)nfreq; f++) {
	//	c = freqkey[f]->count;
	//	rest = len - sumc;
	//	diff = (1-c)*logmax + logtot			// dict:value - uniform + dict:count
	//		 + (math.nlog2n(rest) - math.nlog2n(c) - math.nlog2n(rest-c));
	//	if(diff > 0) break;
	//	sumc += c;
	//}
	//nfreq = f;

	if(nfreq > MAXTOTAL/2) nfreq = MAXTOTAL/2;		// there is no sense to hold big dictionary with all counts small and equal
	BHASSERT(nfreq <= MAXFREQ, "should be 'nfreq <= MAXFREQ'");

	// TODO: find the best size of dictionary
	// TODO: if no. of different values is small (<100 ?), force the use of full dictionary

	// compute lows and highs
	if(len > MAXTOTAL) {
		// how much the counts must be shifted to fit into 'maxtotal'
		uint shift = GetShift(len, MAXTOTAL - nfreq - 1);
		uint low = 0, scount;
		uint sumcnt = 0;
		for(uint f = 0; f < nfreq; f++) {
//			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(shift < sizeof(freqkey[f]->count)*8);
			scount = freqkey[f]->count _SHR_ shift;
			if(scount == 0) scount = 1;
			freqkey[f]->low = low;
			freqkey[f]->count = scount;
			//freqkey[f]->high = (low += scount);
			low += scount;
			sumcnt += freqkey[f]->count;
		}
		BHASSERT(sumcnt <= len, "should be 'sumcnt <= len'");

		esc_count = len - sumcnt;
//		BHASSERT(shift < sizeof(esc_count)*8, "should be 'shift < sizeof(esc_count)*8'");
		scount = esc_count _SHR_ shift;
		if(((esc_count > 0) && (scount == 0)) || (sumcnt == 0)) scount = 1;
		esc_low = low;
		esc_high = low + scount;
		BHASSERT(esc_high <= MAXTOTAL, "should be 'esc_high <= MAXTOTAL'");
	}
	else {
		uint sumcnt = 0;
		for(uint f = 0; f < nfreq; f++) {
			freqkey[f]->low = sumcnt;
			sumcnt += freqkey[f]->count;
			//freqkey[f]->high = (sumcnt += freqkey[f]->count);
		}
		BHASSERT(sumcnt <= len, "should be 'sumcnt <= len'");
		esc_count = len - sumcnt;
		esc_low = sumcnt;
		esc_high = len;
	}

	// TODO CAUTION: difference between 2^n and real total is put fully to ESC count
	// -- this may lead to much worse compression for partially filled packs (e.g. with nulls)
	esc_high = 1u _SHL_ GetBitLen(esc_high-1);
	esc_usecnt = esc_high - esc_low;
	BHASSERT(esc_high <= MAXTOTAL, "should be 'esc_high <= MAXTOTAL'");
}

template<class T> int PartDict<T>::compare(const void* p1, const void* p2)
{
	typedef struct PartDict<T>::HashTab::AKey AK;
	if(((*(AK**)p1)->count) < ((*(AK**)p2)->count)) return 1;
	else if(((*(AK**)p1)->count) > ((*(AK**)p2)->count)) return -1;
	else return 0;
}

template<class T> void PartDict<T>::Save(RangeCoder* coder, T maxval)
{
	// save no. of frequent values
	coder->EncodeUniform(nfreq, MAXFREQ);

	// save frequent values
	uint bitmax = GetBitLen(maxval);
	uint c, prevc = MAXTOTAL-1;
	for(uint f = 0; f < nfreq; f++) {
		// save frequent value and its short count-1 (which is not greater than the previous count-1)
		coder->EncodeUniform(freqkey[f]->key, maxval, bitmax);
		coder->EncodeUniform(c = freqkey[f]->count - 1, prevc);
		prevc = c;
	}

	// save 'esc_high' = total
	coder->EncodeUniform(esc_high, MAXTOTAL);
}

template<class T> void PartDict<T>::Load(RangeCoder* coder, T maxval)
{
	// load no. of frequent values
	coder->DecodeUniform(nfreq, MAXFREQ);

	// load frequent values, their 'lows' and 'highs'; fill 'cnt2val' array
	uint bitmax = GetBitLen(maxval);
	uint c, prevc = MAXTOTAL-1;
	uint low = 0;
	for(ushort f = 0; f < nfreq; f++) {
		coder->DecodeUniform(freqval[f].val, maxval, bitmax);
		//src->DecodeUniform(c, maxc);
		//maxc -= ++c;
		coder->DecodeUniform(c, prevc);
		prevc = c++;
		freqval[f].low = low;
		freqval[f].count = c;
		if(low + c > MAXTOTAL) throw CPRS_ERR_COR;
		for(; c > 0; c--) cnt2val[low++] = f;
		//freqval[f].high = low;
	}

	// load range of ESC
	esc_low = low;
	coder->DecodeUniform(esc_high, MAXTOTAL);
	if(esc_low > esc_high) throw CPRS_ERR_COR;
	esc_usecnt = esc_high - esc_low;
}

template<class T> uint PartDict<T>::Predict(DataSet<T>* ds)
{
	QuickMath math;
	double x = 0;
	double logmax = math.log2(ds->maxval+1.0);		// no. of bits to encode a value uniformly

	// dictionary:  nfreq + vals_and_counts + esc_high
	x += 16 + nfreq*(logmax + math.log2(MAXTOTAL) - math.log2(nfreq)) + 16;

	// encoding of data:  frequent values + ESC + uniform model for rare values
	double data = ds->nrec * math.log2(esc_high);
	if(esc_count > 0)
		data -= esc_count * (math.log2(esc_usecnt) - logmax);
	BHASSERT(MAXTOTAL >= ds->nrec, "should be 'MAXTOTAL >= ds->nrec'");
	for(uint f = 0; f < nfreq; f++)
		data -= math.nlog2n(freqkey[f]->count);
		//data -= freqkey[f]->count * math.log2(freqkey[f]->high - freqkey[f]->low);

	x += data;
	return (uint)x + 20;		// cost of the use of arithmetic coder
}

template<class T> bool PartDict<T>::Encode(RangeCoder* coder, DataSet<T>* dataset)
{
	// build dictionary
	Create(dataset);
	if(Predict(dataset) > 0.98 * this->PredictUni(dataset)) return false;

	// save version of this routine, using 3 bits (7 = 2^3-1)
	coder->EncodeUniform((uchar)0, (uchar)7);

	// save dictionary
	Save(coder, dataset->maxval);

	// encode data
	T* data = dataset->data;
	uint len = dataset->nrec;
	uint low, cnt;
	bool esc;
	uint shift = GetBitLen(esc_high-1);

	lenrest = 0;
	for(uint i = 0; i < len; i++) {
		esc = GetRange(data[i], low, cnt);				// encode frequent symbol or ESC
		//dest->Encode(low, high-low, esc_high);
		coder->EncodeShift(low, cnt, shift);
		if(esc)	{										// ESC -> encode rare symbol
			data[lenrest++] = data[i];
			//if(data[i] > maxrest) maxrest = data[i];
		}
	}
	dataset->nrec = lenrest;
	// maxval is not changed
	return true;
}

template<class T> void PartDict<T>::Decode(RangeCoder* coder, DataSet<T>* dataset)
{
	// read version
	uchar ver;
	coder->DecodeUniform(ver, (uchar)7);
	if(ver > 0) throw CPRS_ERR_COR;

	// load dictionary
	Load(coder, dataset->maxval);

	// decode data
	uint len = dataset->nrec;
	uint c, low, count;
	uint shift = GetBitLen(esc_high-1);

	lenall = len;
	lenrest = 0;
	for(uint i = 0; i < len; i++) {
		//c = coder->GetCount(esc_high);
		c = coder->GetCountShift(shift);
		isesc[i] = GetVal(c, decoded[i], low, count);
		if(isesc[i]) lenrest++;
		coder->DecodeShift(low, count, shift);
		//coder->Decode(low, high-low, esc_high);
	}
	dataset->nrec = lenrest;
}

template<class T> void PartDict<T>::Merge(DataSet<T>* dataset)
{
	T* data = dataset->data;
	BHASSERT(dataset->nrec == lenrest, "should be 'dataset->nrec == lenrest'");
	uint i = lenall;
	while(i-- > 0)
		if(isesc[i]) data[i] = data[--lenrest];
		else data[i] = decoded[i];
	BHASSERT(lenrest == 0, "should be 'lenrest == 0'");
	dataset->nrec = lenall;
}

//-------------------------------------------------------------------------------------

// template<> const unsigned int PartDict<unsigned long long>::HashTab::nbuck = 0x20000;
// template<> const unsigned int PartDict<unsigned int>  ::HashTab::nbuck = 0x20000;
// template<> const unsigned int PartDict<unsigned short>::HashTab::nbuck = 65536;
// template<> const unsigned int PartDict<unsigned char> ::HashTab::nbuck = 256;

// template<> const unsigned int PartDict<unsigned long long>::HashTab::mask = 0x20000 - 1;
// template<> const unsigned int PartDict<unsigned int>  ::HashTab::mask = 0x20000 - 1;
// template<> const unsigned int PartDict<unsigned short>::HashTab::mask = 65536 - 1;
// template<> const unsigned int PartDict<unsigned char> ::HashTab::mask = 256 - 1;

//-------------------------------------------------------------------------------------

TEMPLATE_CLS(PartDict)

