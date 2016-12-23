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

#include "AggregatorBasic.h"
#include "system/RCSystem.h"
#include "system/ConnectionInfo.h"

#ifndef PURE_LIBRARY
extern MYSQL_ERROR *IBPushWarning(THD *thd, MYSQL_ERROR::enum_warning_level level, uint code, const char *msg);
#endif

void AggregatorSum64::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	stats_updated = false;
	_int64* p = (_int64*)buf;
	if(*p == NULL_VALUE_64)	{
		*p = 0;
	}
	double overflow_check = double(*p) + double(v) * factor;
	if(overflow_check > 9.223372037e+18 || overflow_check < -9.223372037e+18)
		throw NotImplementedRCException("Aggregation overflow.");
	*p += v * factor;
}

void AggregatorSum64::Merge(unsigned char *buf, unsigned char *src_buf)
{
	_int64* p = (_int64*)buf;
	_int64* ps = (_int64*)src_buf;
	if(*ps == NULL_VALUE_64)
		return;
	stats_updated = false;
	if(*p == NULL_VALUE_64)	{
		*p = 0;
	}
	double overflow_check = double(*p) + double(*ps);
	if(overflow_check > 9.223372037e+18 || overflow_check < -9.223372037e+18)
		throw NotImplementedRCException("Aggregation overflow.");
	*p += *ps;
}

void AggregatorSum64::SetAggregatePackSum(_int64 par1, _int64 factor)
{
	double overflow_check = double(par1) * factor;
	if(overflow_check > 9.223372037e+18 || overflow_check < -9.223372037e+18)
		throw NotImplementedRCException("Aggregation overflow.");
	pack_sum = par1 * factor;
}

bool AggregatorSum64::AggregatePack(unsigned char *buf)
{
	assert(pack_sum != NULL_VALUE_64);
	PutAggregatedValue(buf, pack_sum, 1);
	return true;
}

///////////////

void AggregatorSumD::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	stats_updated = false;
	double_int_t* p = (double_int_t*)buf;
	if((*p).i == NULL_VALUE_64)	{
		(*p).i = 0;
	}
	(*p).d += *((double*)(&v)) * factor;
}

void AggregatorSumD::Merge(unsigned char *buf, unsigned char *src_buf)
{
	double_int_t* p = (double_int_t*)buf;
	double_int_t* ps = (double_int_t*)src_buf;
	if((*ps).i == NULL_VALUE_64)
		return;
	stats_updated = false;
	if((*p).i == NULL_VALUE_64)	{
		(*p).i = 0;
	}
	(*p).d += (*ps).d;
}

void AggregatorSumD::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_REAL);
	double d_val = 0.0;
	if(!v.IsEmpty()) {
		int r = RCNum::ParseReal(v, val, RC_REAL);
		if((r == BHRC_SUCCESS || r == BHRC_OUT_OF_RANGE_VALUE) && !val.IsNull()) {
			d_val = double(val);
		}
	}
	PutAggregatedValue(buf, *((_int64*)(&d_val)), factor);
}

bool AggregatorSumD::AggregatePack(unsigned char *buf)
{
	PutAggregatedValue(buf, pack_sum, 1);
	return true;
}

///////////////

void AggregatorAvg64::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
#ifndef PURE_LIBRARY
	stats_updated = false;
	*((double*)buf) += double(v) * factor;
	if(!warning_issued && (*((double*)buf) >  9.223372037e+18 || *((double*)buf)< -9.223372037e+18)) {
		IBPushWarning(&ConnectionInfoOnTLS.Get().Thd(), MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, "Values rounded in average() in Brighthouse");
		warning_issued = true;
	}
	*((_int64*)(buf + 8)) += factor;
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#endif
}

void AggregatorAvg64::Merge(unsigned char *buf, unsigned char *src_buf)
{
#ifndef PURE_LIBRARY
	if(*((_int64*)(src_buf + 8)) == 0)
		return;
	stats_updated = false;
	*((double*)buf) += *((double*)src_buf);
	if(!warning_issued && (*((double*)buf) >  9.223372037e+18 || *((double*)buf)< -9.223372037e+18)) {
		IBPushWarning(&ConnectionInfoOnTLS.Get().Thd(), MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, "Values rounded in average() in Brighthouse");
		warning_issued = true;
	}
	*((_int64*)(buf + 8)) += *((_int64*)(src_buf + 8));
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#endif
}

double AggregatorAvg64::GetValueD(unsigned char *buf)
{
	if(*((_int64*)(buf + 8)) == 0)
		return NULL_VALUE_D;
	return *((double*)buf) / *((_int64*)(buf + 8)) / prec_factor;
}

bool AggregatorAvg64::AggregatePack(unsigned char *buf)
{
	stats_updated = false;
	*((double*)buf) += pack_sum;
	*((_int64*)(buf + 8)) += pack_not_nulls;
	return true;
}

void AggregatorAvgD::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	stats_updated = false;
	*((double*)buf) += *((double*)(&v)) * factor;
	*((_int64*)(buf + 8)) += factor;
}

void AggregatorAvgD::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_REAL);
	if(!v.IsEmpty()) {
		int r = RCNum::ParseReal(v, val, RC_REAL);
		if((r == BHRC_SUCCESS || r == BHRC_OUT_OF_RANGE_VALUE) && !val.IsNull()) {
			double d_val = double(val);
			PutAggregatedValue(buf, *((_int64*)(&d_val)), factor);
		}
	}
}

void AggregatorAvgD::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((_int64*)(src_buf + 8)) == 0)
		return;
	stats_updated = false;
	*((double*)buf) += *((double*)src_buf);
	*((_int64*)(buf + 8)) += *((_int64*)(src_buf + 8));
}

double AggregatorAvgD::GetValueD(unsigned char *buf)
{
	if(*((_int64*)(buf + 8)) == 0)
		return NULL_VALUE_D;
	return *((double*)buf) / *((_int64*)(buf + 8));
}

bool AggregatorAvgD::AggregatePack(unsigned char *buf)
{
	stats_updated = false;
	*((double*)buf) += pack_sum;
	*((_int64*)(buf + 8)) += pack_not_nulls;
	return true;
}

void AggregatorAvgYear::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	stats_updated = false;
	*((double*)buf) += double(YearSortEncoding(v)) * factor;
	*((_int64*)(buf + 8)) += factor;
}

void AggregatorAvgYear::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((_int64*)(src_buf + 8)) == 0)
		return;
	stats_updated = false;
	*((double*)buf) += *((double*)src_buf);
	*((_int64*)(buf + 8)) += *((_int64*)(src_buf + 8));
}

void AggregatorAvgYear::PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)
{
	stats_updated = false;
	RCNum val(RC_INT);
	if(!v.IsEmpty() && RCNum::ParseNum(v, val, 0) == BHRC_SUCCESS && !val.IsNull()) {
		*((double*)buf) += double(val.GetValueInt64()) * factor;
		*((_int64*)(buf + 8)) += factor;
	}
}

/////////////////

void AggregatorMin32::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	if( *((int*)buf) == NULL_VALUE_32 ||
		*((int*)buf) > v) {
		stats_updated = false;
		*((int*)buf) = (int)v;
	}
}

void AggregatorMin32::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((int*)src_buf) == NULL_VALUE_32)
		return;
	if( *((int*)buf) == NULL_VALUE_32 ||
		*((int*)buf) > *((int*)src_buf)) {
			stats_updated = false;
			*((int*)buf) = *((int*)src_buf);
	}
}

_int64 AggregatorMin32::GetValue64(unsigned char *buf)
{
	if(*((int*)buf) == NULL_VALUE_32)
		return NULL_VALUE_64;
	return *((int*)buf);
}

void AggregatorMin64::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((_int64*)buf) > v) {
		stats_updated = false;
		*((_int64*)buf) = v;
	}
}

void AggregatorMin64::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((_int64*)src_buf) == NULL_VALUE_64)
		return;
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((_int64*)buf) > *((_int64*)src_buf)) {
			stats_updated = false;
			*((_int64*)buf) = *((_int64*)src_buf);
	}
}

bool AggregatorMin64::AggregatePack(unsigned char *buf)
{
  assert(pack_min != NULL_VALUE_64);
  PutAggregatedValue(buf, pack_min, 1);
  return true;
}

bool AggregatorMinD::AggregatePack(unsigned char *buf)
{
  assert(pack_min != NULL_VALUE_64);
  _int64 pack_min_64 = *(_int64*)&pack_min;			// pack_min is double, so it needs conversion into int64-encoded-double
  PutAggregatedValue(buf, pack_min_64, 1);
  return true;
}

bool AggregatorMax64::AggregatePack(unsigned char *buf)
{
  assert(pack_max != NULL_VALUE_64);
  PutAggregatedValue(buf, pack_max, 1);
  return true;
}

bool AggregatorMaxD::AggregatePack(unsigned char *buf)
{
  assert(pack_max != NULL_VALUE_D);
  _int64 pack_max_64 = *(_int64*)&pack_max;			// pack_max is double, so it needs conversion into int64-encoded-double
  PutAggregatedValue(buf, pack_max_64, 1);
  return true;
}

void AggregatorMinD::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((double*)buf) > *((double*)(&v))) {
		stats_updated = false;
		*((double*)buf) = *((double*)(&v));
	}
}

void AggregatorMinD::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((_int64*)src_buf) == NULL_VALUE_64)
		return;
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((double*)buf) > *((double*)src_buf)) {
			stats_updated = false;
			*((double*)buf) = *((double*)src_buf);
	}
}

void AggregatorMinT::PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor)
{
	assert((uint)val_len >= v.len);
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		memset(buf + 2, 0, val_len);
		*((unsigned short*)buf) = v.len;
		if(v.len > 0)
			memcpy(buf + 2, v.val, v.len);
		else
			buf[2] = 1;								// empty string indicator (non-null)
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		if(m > v) {
			stats_updated = false;
			memset(buf + 2, 0, val_len);
			*((unsigned short*)buf) = v.len;
			if(v.len > 0)
				memcpy(buf + 2, v.val, v.len);
			else
				buf[2] = 1;							// empty string indicator (non-null)
		}
	}
}

void AggregatorMinT::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((unsigned short*)src_buf) == 0 && src_buf[2] == 0)
		return;
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		memcpy(buf, src_buf, val_len + 2);
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		RCBString v((char*)src_buf + 2, *((unsigned short*)src_buf));
		if(m > v) {
			stats_updated = false;
			memcpy(buf, src_buf, val_len + 2);
		}
	}
}

RCBString AggregatorMinT::GetValueT(unsigned char *buf)
{
	int len = *((unsigned short*)(buf));
	char *p = (char*)(buf + 2);
	if(len == 0) {
		if( *p != 0 )						// empty string indicator: len==0 and nontrivial character
			return RCBString("",0,true);	// empty string
		return RCBString();					// null value
	}
	RCBString res(p , len);
	return res;
}

AggregatorMinT_UTF::AggregatorMinT_UTF(int max_len, DTCollation coll) : AggregatorMinT(max_len), collation(coll)
{
}

void AggregatorMinT_UTF::PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor)
{
	assert((uint)val_len >= v.len);
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		memset(buf + 2, 0, val_len);
		*((unsigned short*)buf) = v.len;
		if(v.len > 0)
			memcpy(buf + 2, v.val, v.len);
		else
			buf[2] = 1;								// empty string indicator (non-null)
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		if(CollationStrCmp(collation, m, v) > 0) {
			stats_updated = false;
			memset(buf + 2, 0, val_len);
			*((unsigned short*)buf) = v.len;
			if(v.len > 0)
				memcpy(buf + 2, v.val, v.len);
			else
				buf[2] = 1;							// empty string indicator (non-null)
		}
	}
}

void AggregatorMinT_UTF::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((unsigned short*)src_buf) == 0 && src_buf[2] == 0)
		return;
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		memcpy(buf, src_buf, val_len + 2);
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		RCBString v((char*)src_buf + 2, *((unsigned short*)src_buf));
		if(CollationStrCmp(collation, m, v) > 0) {
			stats_updated = false;
			memcpy(buf, src_buf, val_len + 2);
		}
	}
}

void AggregatorMax32::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	if( *((int*)buf) == NULL_VALUE_32 ||
		*((int*)buf) < v) {
		stats_updated = false;
		*((int*)buf) = (int)v;
	}
}

_int64 AggregatorMax32::GetValue64(unsigned char *buf)
{
	if(*((int*)buf) == NULL_VALUE_32)
		return NULL_VALUE_64;
	return *((int*)buf);
}

void AggregatorMax32::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((int*)src_buf) == NULL_VALUE_32)
		return;
	if( *((int*)buf) == NULL_VALUE_32 ||
		*((int*)buf) < *((int*)src_buf)) {
			stats_updated = false;
			*((int*)buf) = *((int*)src_buf);
	}
}

void AggregatorMax64::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((_int64*)buf) < v) {
		stats_updated = false;
		*((_int64*)buf) = v;
	}
}

void AggregatorMax64::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((_int64*)src_buf) == NULL_VALUE_64)
		return;
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((_int64*)buf) < *((_int64*)src_buf)) {
			stats_updated = false;
			*((_int64*)buf) = *((_int64*)src_buf);
	}
}

void AggregatorMaxD::PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
{
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((double*)buf) < *((double*)(&v))) {
		stats_updated = false;
		*((double*)buf) = *((double*)(&v));
	}
}

void AggregatorMaxD::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((_int64*)src_buf) == NULL_VALUE_64)
		return;
	if( *((_int64*)buf) == NULL_VALUE_64 ||
		*((double*)buf) < *((double*)src_buf)) {
			stats_updated = false;
			*((double*)buf) = *((double*)src_buf);
	}
}

AggregatorMaxT_UTF::AggregatorMaxT_UTF(int max_len, DTCollation coll) : AggregatorMaxT(max_len), collation(coll)
{
}

void AggregatorMaxT::PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor)
{
	stats_updated = false;
	assert((uint)val_len >= v.len);
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		memset(buf + 2, 0, val_len);
		*((unsigned short*)buf) = v.len;
		if(v.len > 0)
			memcpy(buf + 2, v.val, v.len);
		else
			buf[2] = 1;								// empty string indicator (non-null)
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		if(m < v) {
			memset(buf + 2, 0, val_len);
			*((unsigned short*)buf) = v.len;
			if(v.len > 0)
				memcpy(buf + 2, v.val, v.len);
			else
				buf[2] = 1;							// empty string indicator (non-null)
		}
	}
}

void AggregatorMaxT::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((unsigned short*)src_buf) == 0 && src_buf[2] == 0)
		return;
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		memcpy(buf, src_buf, val_len + 2);
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		RCBString v((char*)src_buf + 2, *((unsigned short*)src_buf));
		if(m < v) {
			stats_updated = false;
			memcpy(buf, src_buf, val_len + 2);
		}
	}
}

RCBString AggregatorMaxT::GetValueT(unsigned char *buf)
{
	int len = *((unsigned short*)(buf));
	char *p = (char*)(buf + 2);
	if(len == 0) {
		if( *p != 0 )						// empty string indicator: len==0 and nontrivial character
			return RCBString("",0,true);	// empty string
		return RCBString();					// null value
	}
	RCBString res(p , len);
	return res;
}

void AggregatorMaxT_UTF::PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor)
{
	stats_updated = false;
	assert((uint)val_len >= v.len);
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		memset(buf + 2, 0, val_len);
		*((unsigned short*)buf) = v.len;
		if(v.len > 0)
			memcpy(buf + 2, v.val, v.len);
		else
			buf[2] = 1;								// empty string indicator (non-null)
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		if(CollationStrCmp(collation, m, v) < 0) {
			memset(buf + 2, 0, val_len);
			*((unsigned short*)buf) = v.len;
			if(v.len > 0)
				memcpy(buf + 2, v.val, v.len);
			else
				buf[2] = 1;							// empty string indicator (non-null)
		}
	}
}

void AggregatorMaxT_UTF::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((unsigned short*)src_buf) == 0 && src_buf[2] == 0)
		return;
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		memcpy(buf, src_buf, val_len + 2);
	} else {
		RCBString m((char*)buf + 2, *((unsigned short*)buf));
		RCBString v((char*)src_buf + 2, *((unsigned short*)src_buf));
		if(CollationStrCmp(collation, m, v) < 0) {
			stats_updated = false;
			memcpy(buf, src_buf, val_len + 2);
		}
	}
}

_int64 AggregatorList32::GetValue64(unsigned char *buf)
{
	if(*((int*)buf) == NULL_VALUE_32)
		return NULL_VALUE_64;
	return *((int*)buf);
}

void AggregatorListT::PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor)
{
	assert((uint)val_len >= v.len);
	if(*((unsigned short*)buf) == 0 && buf[2] == 0) {			// still null
		stats_updated = false;
		*((unsigned short*)buf) = v.len;
		if(v.len > 0)
			memcpy(buf + 2, v.val, v.len);
		else
			buf[2] = 1;								// empty string indicator (non-null)
		value_set = true;
	}
}

void AggregatorListT::Merge(unsigned char *buf, unsigned char *src_buf)
{
	if(*((unsigned short*)buf) == 0 && buf[2] == 0 && (*((unsigned short*)src_buf) != 0 || src_buf[2] != 0)) {
		stats_updated = false;
		memcpy(buf, src_buf, val_len + 2);
		value_set = true;
	}
}

RCBString AggregatorListT::GetValueT(unsigned char *buf)
{
	int len = *((unsigned short*)(buf));
	char *p = (char*)(buf + 2);
	if(len == 0) {
		if( *p != 0 )						// empty string indicator: len==0 and nontrivial character
			return RCBString("",0,true);	// empty string
		return RCBString();					// null value
	}
	RCBString res(p , len);
	return res;
}

