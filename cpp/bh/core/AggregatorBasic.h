/* Copyright (C)  2005-2009 Infobright Inc.

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

#ifndef AGGREGATOR_BASIC_H_
#define AGGREGATOR_BASIC_H_

#include "Aggregator.h"

/*! \brief A generalization of aggregation algorithms (counters) to be used in GROUP BY etc.
 * See "Aggregator.h" for more details.
 *
 * \note The aim of this class hierarchy is to make aggregators more pluggable,
 * as well as to replace multiple ifs and switch/cases by virtual methods.
 * Therefore it is suggested to implement all variants as separate subclasses,
 * e.g. to distinguish 32- and 64-bit counters.
 */

///////////////////////////////////////////////////////////////////////////////////////////////
/*!
 * \brief An aggregator for SUM(...) of numerical (int64) values.
 *
 * The counter consists of just one 64-bit value:
 *     <cur_sum_64>
 * Start value: NULL_VALUE_64.
 * Throws an exception on overflow.
 */
class AggregatorSum64 : public Aggregator
{
public:
	AggregatorSum64() : Aggregator() { pack_sum = 0; pack_min = 0; pack_max = 0; null_group_found = false; }
	AggregatorSum64(AggregatorSum64 &sec) : Aggregator(sec), pack_sum(sec.pack_sum), pack_min(sec.pack_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorSum64(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }
	virtual void Reset(unsigned char *buf)									{ *((_int64*)buf) = NULL_VALUE_64; }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSum()					{ return true; }
	virtual void SetAggregatePackSum(_int64 par1, _int64 factor);
	virtual bool AggregatePack(unsigned char *buf);

	// if a data pack contains only 0, then no need to update
	virtual bool PackAggregationNeedsMin()									{ return true; }
	virtual bool PackAggregationNeedsMax()									{ return true; }
	virtual void SetAggregatePackMin(_int64 par1)				 			{ pack_min = par1; }
	virtual void SetAggregatePackMax(_int64 par1)				 			{ pack_max = par1; }
	virtual void ResetStatistics()							{ null_group_found = false; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((_int64*)buf) == NULL_VALUE_64)
			null_group_found = true;
		return null_group_found;	// if found, do not search any more
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && pack_min == 0 && pack_max == 0;		// uniform 0 pack - no change for sum
	}

private:
	_int64 pack_sum;
	_int64 pack_min;			// min and max are used to check whether a pack may update sum (i.e. both 0 means "no change")
	_int64 pack_max;
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for SUM(...) of double values.
 *
 * The counter consists of just one double value:
 *     <cur_sum_double>
 * Start value: NULL_VALUE_D.
 */
class AggregatorSumD : public Aggregator
{
public:
	AggregatorSumD() : Aggregator() { pack_sum = 0; }
	AggregatorSumD(AggregatorSumD &sec) : Aggregator(sec), pack_sum(sec.pack_sum) { }
	virtual Aggregator* Copy()								{ return new AggregatorSumD(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }	// double passed as 64-bit code
	virtual double GetValueD(unsigned char *buf)							{ return *((double*)buf); }

	virtual void Reset(unsigned char *buf)									{ *((double*)buf) = NULL_VALUE_D; }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSum()					{ return true; }
	virtual void SetAggregatePackSum(_int64 par1, _int64 factor)
	{ double d = *((double*)(&par1)) * factor; pack_sum = *((_int64*)(&d)); }
	virtual bool AggregatePack(unsigned char *buf);

private:
	_int64 pack_sum;
};

/*!
 * \brief An aggregator for AVG(...) of numerical (int64) values.
 *
 * The counter consists of a double sum and counter:
 *     <cur_sum_double><cur_count_64>
 * Start value: 0, but GetValueD will return NULL_VALUE_D.
 */
class AggregatorAvg64 : public Aggregator
{
public:
	AggregatorAvg64(int precision)
	{ prec_factor = PowOfTen(precision); warning_issued = false; pack_not_nulls = 0; pack_sum = 0; } 
	AggregatorAvg64(AggregatorAvg64 &sec) : Aggregator(sec), pack_sum(sec.pack_sum), pack_not_nulls(sec.pack_not_nulls), prec_factor(sec.prec_factor), warning_issued(sec.warning_issued)  { }
	virtual Aggregator* Copy()								{ return new AggregatorAvg64(*this); }

	virtual int BufferByteSize() 										{ return 16; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual double GetValueD(unsigned char *buf);
	virtual _int64 GetValue64(unsigned char *buf)				// double passed as 64-bit code
	{
		double res = GetValueD(buf);
		return *((_int64*)(&res));
	}

	virtual void Reset(unsigned char *buf)									{ *((double*)buf) = 0; *((_int64*)(buf + 8)) = 0; }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSum()					{ return true; }
	virtual bool PackAggregationNeedsNotNulls()				{ return true; }
	virtual void SetAggregatePackSum(_int64 par1, _int64 factor) { pack_sum = double(par1) * factor; }
	virtual void SetAggregatePackNotNulls(_int64 par1)		{ pack_not_nulls = par1; }
	virtual bool AggregatePack(unsigned char *buf);

private:
	double pack_sum;
	_int64 pack_not_nulls;

	double prec_factor;			// precision factor: the calculated avg must be divided by it
	bool warning_issued;
};

/*!
 * \brief An aggregator for AVG(...) of double values.
 *
 * The counter consists of a double sum and counter:
 *     <cur_sum_double><cur_count_64>
 * Start value: 0, but GetValueD will return NULL_VALUE_D.
 */
class AggregatorAvgD : public Aggregator
{
public:
	AggregatorAvgD() : Aggregator() { pack_not_nulls = 0; pack_sum = 0; }
	AggregatorAvgD(AggregatorAvgD &sec) : Aggregator(sec), pack_sum(sec.pack_sum), pack_not_nulls(sec.pack_not_nulls) { }
	virtual Aggregator* Copy()								{ return new AggregatorAvgD(*this); }

	virtual int BufferByteSize() 										{ return 16; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual double GetValueD(unsigned char *buf);
	virtual _int64 GetValue64(unsigned char *buf)				// double passed as 64-bit code
	{
		double res = GetValueD(buf);
		return *((_int64*)(&res));
	}

	virtual void Reset(unsigned char *buf)									{ *((double*)buf) = 0; *((_int64*)(buf + 8)) = 0; }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSum()					{ return true; }
	virtual bool PackAggregationNeedsNotNulls()				{ return true; }
	virtual void SetAggregatePackSum(_int64 par1, _int64 factor) { pack_sum = *((double*)(&par1)) * factor; }
	virtual void SetAggregatePackNotNulls(_int64 par1)		{ pack_not_nulls = par1; }
	virtual bool AggregatePack(unsigned char *buf);

private:
	double pack_sum;
	_int64 pack_not_nulls;
};

/*!
* \brief An aggregator for AVG(...) of year values.
*
* The counter consists of a double sum and counter:
*     <cur_sum_double><cur_count_64>
* Start value: 0, but GetValueD will return NULL_VALUE_D.
*/

class AggregatorAvgYear : public AggregatorAvgD
{
public:
	AggregatorAvgYear() : AggregatorAvgD() {}
	AggregatorAvgYear(AggregatorAvgYear &sec) : AggregatorAvgD(sec) { }
	virtual Aggregator* Copy()								{ return new AggregatorAvgYear(*this); }

	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSum()					{ return false; }
	virtual bool PackAggregationNeedsNotNulls()				{ return false; }
	virtual bool AggregatePack(unsigned char *buf)			{ return false; }	// no optimization
};

///////////////////////////////////////////////////////////////////////////////////////////////
//! Abstract class for all kinds of MIN(...)
class AggregatorMin : public Aggregator
{
public:
	AggregatorMin() : Aggregator()						{ }
	AggregatorMin(AggregatorMin &sec) : Aggregator(sec) { }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationDistinctIrrelevant()						{ return true; }
	virtual bool FactorNeeded()												{ return false; }
	virtual bool IgnoreDistinct()											{ return true; }
	virtual bool PackAggregationNeedsMin()									{ return true; }
};

//////////////////

/*!
 * \brief An aggregator for MIN(...) of int32 values.
 *
 * The counter consists of a current min:
 *     <cur_min_32>
 * Start value: NULL_VALUE (32 bit), will return NULL_VALUE_64.
 */
class AggregatorMin32 : public AggregatorMin
{
public:
	AggregatorMin32() : AggregatorMin() { stat_max = 0; pack_min = 0; null_group_found = false; }
	AggregatorMin32(AggregatorMin32 &sec) : AggregatorMin(sec), stat_max(sec.stat_max), pack_min(sec.pack_min), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorMin32(*this); }

	virtual int BufferByteSize() 										{ return 4; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual void SetAggregatePackMin(_int64 par1)				 			{ pack_min = par1; }
	virtual _int64 GetValue64(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ *((int*)buf) = NULL_VALUE_32; }

	virtual void ResetStatistics()							{ null_group_found = false; stat_max = INT_MIN; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((int*)buf) == NULL_VALUE_32)
			null_group_found = true;
		else if(*((int*)buf) > stat_max)
			stat_max = *((int*)buf);
		return false;
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && pack_min != NULL_VALUE_64 && pack_min >= (_int64)stat_max;
	}

private:
	int stat_max;				// maximal min found so far
	_int64 pack_min;			// min and max are used to check whether a pack may update sum (i.e. both 0 means "no change")
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for MIN(...) of int64 values.
 *
 * The counter consists of a current min:
 *     <cur_min_64>
 * Start value: NULL_VALUE_64.
 */
class AggregatorMin64 : public AggregatorMin
{
public:
	AggregatorMin64() : AggregatorMin() { stat_max = 0; pack_min = 0; null_group_found = false; }
	AggregatorMin64(AggregatorMin64 &sec) : AggregatorMin(sec), stat_max(sec.stat_max), pack_min(sec.pack_min), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorMin64(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }
	virtual void SetAggregatePackMin(_int64 par1)				 			{ pack_min = par1; }
	virtual bool AggregatePack(unsigned char *buf);
	virtual void Reset(unsigned char *buf)									{ *((_int64*)buf) = NULL_VALUE_64; }
	virtual void ResetStatistics()							{ null_group_found = false; stat_max = MINUS_INF_64; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((_int64*)buf) == NULL_VALUE_64)
			null_group_found = true;
		else if(*((_int64*)buf) > stat_max)
			stat_max = *((_int64*)buf);
		return false;
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && pack_min != NULL_VALUE_64 && pack_min >= stat_max;
	}
private:
	_int64 stat_max;
	_int64 pack_min;
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for MIN(...) of double values.
 *
 * The counter consists of a current min:
 *     <cur_min_double>
 * Start value: NULL_VALUE_D.
 */
class AggregatorMinD : public AggregatorMin
{
public:
	AggregatorMinD() : AggregatorMin() { stat_max = 0; pack_min = 0; null_group_found = false; }
	AggregatorMinD(AggregatorMinD &sec) : AggregatorMin(sec), stat_max(sec.stat_max), pack_min(sec.pack_min), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorMinD(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual double GetValueD(unsigned char *buf)							{ return *((double*)buf); }
	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }	// double passed as 64-bit code
	virtual void SetAggregatePackMin(_int64 par1){ 
		if(par1 == MINUS_INF_64)
			pack_min = MINUS_INF_DBL;
		else if(par1 == PLUS_INF_64)
			pack_min = PLUS_INF_DBL;
		else
			pack_min = *((double*)&par1); 
	}
	virtual bool AggregatePack(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ *((double*)buf) = NULL_VALUE_D; }
	virtual void ResetStatistics()							{ null_group_found = false; stat_max = NULL_VALUE_D; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((_int64*)buf) == NULL_VALUE_64)
			null_group_found = true;
		else if(*(_int64*)&stat_max == NULL_VALUE_64 || *((double*)buf) > stat_max)
			stat_max = *((double*)buf);
		return false;
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && !IsDoubleNull(pack_min) && !IsDoubleNull(stat_max) && pack_min >= stat_max;
	}
private:
	double stat_max;
	double pack_min;
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for MIN(...) of string values.
 *
 * The counter consists of a current min, together with its 16-bit length:
 *     <cur_min_len_16><cur_min_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by \0,\0,\1.
 */
class AggregatorMinT : public AggregatorMin
{
public:
	AggregatorMinT(int max_len) : val_len(max_len) 							{}
	AggregatorMinT(AggregatorMinT &sec) : AggregatorMin(sec), val_len(sec.val_len) { }
	virtual Aggregator* Copy()								{ return new AggregatorMinT(*this); }

	virtual int BufferByteSize() 										{ return val_len + 2; }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual RCBString GetValueT(unsigned char *buf);
	virtual bool PackAggregationNeedsMin()									{ return false; }

	virtual void Reset(unsigned char *buf)									{ memset( buf, 0, val_len + 2); }

protected:
	int	val_len;			// a maximal length of string stored (without \0 on the end, without len
};

/*!
 * \brief An aggregator for MIN(...) of string values utilizing CHARSET.
 *
 * The counter consists of a current min, together with its 16-bit length:
 *     <cur_min_len_16><cur_min_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by \0,\0,\1.
 */
class AggregatorMinT_UTF : public AggregatorMinT
{
public:
	AggregatorMinT_UTF(int max_len, DTCollation coll);
	AggregatorMinT_UTF(AggregatorMinT_UTF &sec) : AggregatorMinT(sec), collation(sec.collation) { }
	virtual Aggregator* Copy()								{ return new AggregatorMinT_UTF(*this); }

	virtual void PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
private:
	DTCollation collation;
};

///////////////////////////////////////////////////////////////////////////////////////////////
//! Abstract class for all kinds of MAX(...)
class AggregatorMax : public Aggregator
{
public:
	AggregatorMax() : Aggregator()						{ }
	AggregatorMax(AggregatorMax &sec) : Aggregator(sec) { }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationDistinctIrrelevant()						{ return true; }
	virtual bool FactorNeeded()												{ return false; }
	virtual bool IgnoreDistinct()											{ return true; }
	virtual bool PackAggregationNeedsMax()									{ return true; }
};

//////////////////

/*!
 * \brief An aggregator for MAX(...) of int32 values.
 *
 * The counter consists of a current max:
 *     <cur_max_32>
 * Start value: NULL_VALUE (32 bit), will return NULL_VALUE_64.
 */
class AggregatorMax32 : public AggregatorMax
{
public:
	AggregatorMax32() : AggregatorMax() { stat_min = 0; pack_max = 0; null_group_found = false; }
	AggregatorMax32(AggregatorMax32 &sec) : AggregatorMax(sec), stat_min(sec.stat_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorMax32(*this); }

	virtual int BufferByteSize() 										{ return 4; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual void SetAggregatePackMax(_int64 par1)			 				{ pack_max = par1; }
	virtual _int64 GetValue64(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ *((int*)buf) = NULL_VALUE_32; }
	virtual void ResetStatistics()							{ null_group_found = false; stat_min = INT_MAX; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((int*)buf) == NULL_VALUE_32)
			null_group_found = true;
		else if(*((int*)buf) < stat_min)
			stat_min = *((int*)buf);
		return false;
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && pack_max != NULL_VALUE_64 && pack_max <= (_int64)stat_min;
	}

private:
	int stat_min;				// maximal min found so far
	_int64 pack_max;			// min and max are used to check whether a pack may update sum (i.e. both 0 means "no change")
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for MAX(...) of int64 values.
 *
 * The counter consists of a current max:
 *     <cur_max_64>
 * Start value: NULL_VALUE_64.
 */
class AggregatorMax64 : public AggregatorMax
{
public:
	AggregatorMax64() : AggregatorMax() { stat_min = 0; pack_max = 0; null_group_found = false; }
	AggregatorMax64(AggregatorMax64 &sec) : AggregatorMax(sec), stat_min(sec.stat_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorMax64(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }
	virtual void SetAggregatePackMax(_int64 par1)			 				{ pack_max = par1; }
	virtual bool AggregatePack(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ *((_int64*)buf) = NULL_VALUE_64; }
	virtual void ResetStatistics()							{ null_group_found = false; stat_min = PLUS_INF_64; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((_int64*)buf) == NULL_VALUE_64)
			null_group_found = true;
		else if(*((_int64*)buf) < stat_min)
			stat_min = *((_int64*)buf);
		return false;
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && pack_max != NULL_VALUE_64 && pack_max <= stat_min;
	}
private:
	_int64 stat_min;
	_int64 pack_max;
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for MAX(...) of double values.
 *
 * The counter consists of a current max:
 *     <cur_max_double>
 * Start value: NULL_VALUE_D.
 */
class AggregatorMaxD : public AggregatorMax
{
public:
	AggregatorMaxD() : AggregatorMax() { stat_min = 0; pack_max = 0; null_group_found = false; }
	AggregatorMaxD(AggregatorMaxD &sec) : AggregatorMax(sec), stat_min(sec.stat_min), pack_max(sec.pack_max), null_group_found(sec.null_group_found) { }
	virtual Aggregator* Copy()								{ return new AggregatorMaxD(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual double GetValueD(unsigned char *buf)							{ return *((double*)buf); }
	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }	// double passed as 64-bit code
	virtual void SetAggregatePackMax(_int64 par1){ 
		if(par1 == MINUS_INF_64)
			pack_max = MINUS_INF_DBL;
		else if(par1 == PLUS_INF_64)
			pack_max = PLUS_INF_DBL;
		else
			pack_max = *((double*)&par1); 		
	}
	virtual bool AggregatePack(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ *((double*)buf) = NULL_VALUE_D; }
	virtual void ResetStatistics()							{ null_group_found = false; stat_min = NULL_VALUE_D; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((_int64*)buf) == NULL_VALUE_64)
			null_group_found = true;
		else if(*(_int64*)&stat_min == NULL_VALUE_64 || *((double*)buf) < stat_min)
			stat_min = *((double*)buf);
		return false;
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return !null_group_found && !IsDoubleNull(pack_max) && !IsDoubleNull(stat_min) && pack_max <= stat_min;
	}
private:
	double stat_min;
	double pack_max;
	bool null_group_found;		// true if SetStatistics found a null group (no optimization possible)
};

/*!
 * \brief An aggregator for MAX(...) of string values.
 *
 * The counter consists of a current max, together with its 16-bit length:
 *     <cur_max_len_16><cur_max_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by \0,\0,\1.
 */
class AggregatorMaxT : public AggregatorMax
{
public:
	AggregatorMaxT(int max_len) : val_len(max_len) 							{}
	AggregatorMaxT(AggregatorMaxT &sec) : AggregatorMax(sec), val_len(sec.val_len) { }
	virtual Aggregator* Copy()								{ return new AggregatorMaxT(*this); }

	virtual int BufferByteSize() 										{ return val_len + 2; }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual RCBString GetValueT(unsigned char *buf);
	virtual bool PackAggregationNeedsMax()									{ return false; }

	virtual void Reset(unsigned char *buf)									{ memset( buf, 0, val_len + 2); }
protected:
	int	val_len;			// a maximal length of string stored (without \0 on the end, without len
};

/*!
 * \brief An aggregator for MAX(...) of string values utilizing CHARSET.
 *
 * The counter consists of a current max, together with its 16-bit length:
 *     <cur_max_len_16><cur_max_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by \0,\0,\1.
 */
class AggregatorMaxT_UTF : public AggregatorMaxT
{
public:
	AggregatorMaxT_UTF(int max_len, DTCollation coll);
	AggregatorMaxT_UTF(AggregatorMaxT_UTF &sec) : AggregatorMaxT(sec), collation(sec.collation) { }
	virtual Aggregator* Copy()								{ return new AggregatorMaxT_UTF(*this); }

	virtual void PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
private:
	DTCollation collation;
};
///////////////////////////////////////////////////////////////////////////////////////////////

/*!
 * \brief An aggregator just to remember the first int32 values.
 *
 * The counter consists of a current value:
 *     <cur_val_32>
 * Start value: NULL_VALUE (32 bit), will return NULL_VALUE_64.
 */
class AggregatorList32 : public Aggregator
{
public:
	AggregatorList32() : Aggregator() { value_set = false; }
	AggregatorList32(AggregatorList32 &sec) : Aggregator(sec), value_set(sec.value_set) { }
	virtual Aggregator* Copy()								{ return new AggregatorList32(*this); }

	virtual int BufferByteSize() 										{ return 4; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
	{ 
		if(*((int*)buf) == NULL_VALUE_32)  { 
			stats_updated = false; 
			*((int*)buf) = (int)v; 
			value_set = true; 
		} 
	}
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{
		if(*((int*)buf) == NULL_VALUE_32 && *((int*)src_buf) != NULL_VALUE_32)  { 
			stats_updated = false; 
			*((int*)buf) = *((int*)src_buf); 
			value_set = true; 
		} 
	}

	virtual _int64 GetValue64(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ *((int*)buf) = NULL_VALUE_32; value_set = false; }
	///////////// Optimization part /////////////////
	virtual bool IgnoreDistinct()											{ return true; }
	virtual bool FactorNeeded()												{ return false; }
	virtual bool PackCannotChangeAggregation()								{ return value_set; }	// all values are unchangeable
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((int*)buf) == NULL_VALUE_32)
			value_set = false;
		return !value_set;	// if found, do not search any more
	}
private:
	bool value_set;			// true, if all values are not null
};

/*!
 * \brief An aggregator just to remember the first int64 or double values.
 *
 * The counter consists of a current value:
 *     <cur_val_64>
 * Start value: NULL_VALUE_64.
 */
class AggregatorList64 : public Aggregator
{
public:
	AggregatorList64() : Aggregator() { value_set = false; }
	AggregatorList64(AggregatorList64 &sec) : Aggregator(sec), value_set(sec.value_set) { }
	virtual Aggregator* Copy()								{ return new AggregatorList64(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)
	{ 
		if( *((_int64*)buf) == NULL_VALUE_64) { 
			stats_updated = false; 
			*((_int64*)buf) = v; 
			value_set = true;
		} 
	}
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{
		if( *((_int64*)buf) == NULL_VALUE_64 && *((_int64*)src_buf) != NULL_VALUE_64) { 
			stats_updated = false; 
			*((_int64*)buf) = *((_int64*)src_buf); 
			value_set = true;
		} 
	}

	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }

	virtual void Reset(unsigned char *buf)									{ *((_int64*)buf) = NULL_VALUE_64; value_set = false; }
	///////////// Optimization part /////////////////
	virtual bool IgnoreDistinct()											{ return true; }
	virtual bool FactorNeeded()												{ return false; }
	virtual bool PackCannotChangeAggregation()								{ return value_set; }	// all values are unchangeable
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((_int64*)buf) == NULL_VALUE_64)
			value_set = false;
		return !value_set;	// if found, do not search any more
	}
private:
	bool value_set;			// true, if all values are not null
};

/*!
 * \brief An aggregator just to remember the first string values.
 *
 * The counter consists of a current value, together with its 16-bit length:
 *     <cur_len_16><cur_val_txt>
 * Start value: all 0.
 * Null value is indicated by \0,\0,\0 and zero-length non-null string by \0,\0,\1.
 */
class AggregatorListT : public Aggregator
{
public:
	AggregatorListT(int max_len) : val_len(max_len)			{ value_set = false; }
	AggregatorListT(AggregatorListT &sec) : Aggregator(sec), value_set(sec.value_set), val_len(sec.val_len) { }
	virtual Aggregator* Copy()								{ return new AggregatorListT(*this); }

	virtual int BufferByteSize() 										{ return val_len + 2; }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString &v, _int64 factor);
	virtual void Merge(unsigned char *buf, unsigned char *src_buf);
	virtual RCBString GetValueT(unsigned char *buf);

	virtual void Reset(unsigned char *buf)									{ memset(buf, 0, val_len + 2); value_set = false; }
	///////////// Optimization part /////////////////
	virtual bool IgnoreDistinct()											{ return true; }
	virtual bool FactorNeeded()												{ return false; }
	virtual bool PackCannotChangeAggregation()								{ return value_set; }	// all values are unchangeable
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(*((unsigned short*)buf) == 0 && buf[2] == 0)
			value_set = false;
		return !value_set;	// if found, do not search any more
	}
private:
	bool value_set;			// true, if all values are not null
	int	val_len;			// a maximal length of string stored (without \0 on the end, without len
};

#endif
