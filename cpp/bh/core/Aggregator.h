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

#ifndef AGGREGATOR_H_
#define AGGREGATOR_H_

#include "Column.h"

/*! \brief A generalization of aggregation algorithms (counters) to be used in GROUP BY etc.
 * An object representing a concrete aggregation exists in one copy in the GROUP BY,
 * and it implements a method of adding new values into counters
 * (a counter is e.g. a current sum, a current min/max etc.).
 * The counter values themselves are stored outside the aggregator object, in a binary grouping table.
 * The aggregator will be given access to a buffer containing counters for a given group.
 * See the example Count(*) aggregator below.
 *
 * \note The aim of this class hierarchy is to make aggregators more pluggable,
 * as well as to replace multiple ifs and switch/cases by virtual methods.
 * Therefore it is suggested to implement all variants as separate subclasses,
 * e.g. to distinguish 32- and 64-bit counters.
 */
class Aggregator
{
public:
	Aggregator() : stats_updated(false)									{ }
	Aggregator(Aggregator &sec) : stats_updated(sec.stats_updated)		{ }
	virtual ~Aggregator() 												{ }
	///////////// Obligatory part ///////////////////

	/*!
	* \brief Get a new copy of the aggregator of the proper subtype.
	* Should be externally deleted after usage.
	*/
	virtual Aggregator* Copy() = 0;

	/*!
	 * \brief Get the number of bytes for one counter.
	 * Used by a grouping algorithm to calculate sizes/positions of counters
	 * in a global grouping table.
	 */
	virtual int BufferByteSize() = 0;

	/*!
	 * \brief Add the current value to counter pointed by the pointer.
	 * This version is used only for COUNT(*), when no value is needed
	 * May be left unimplemented if not applicable for a given aggregator.
	 */
	virtual void PutAggregatedValue(unsigned char *buf, _int64 factor)						{ assert(0); }
	/*!
	 * \brief Add the current value to counter pointed by the pointer.
	 * A version for all numerical values.
	 * May be left unimplemented if not applicable for a given aggregator.
	 */
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)			{ assert(0); }
	/*!
	 * \brief Add the current value to counter pointed by the pointer.
	 * A version for all text (not lookup) values.
	 * May be left unimplemented if not applicable for a given aggregator.
	 */
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)	{ assert(0); }

	/*!
	* \brief Add the counters value represented in src_buf into buf.
	* Common encoding is assumed.
	*/
	virtual void Merge(unsigned char *buf, unsigned char *src_buf) = 0;

	/*!
	 * \brief Get the current value of counter pointed by the pointer.
	 * May be left unimplemented if no numerical value is returned.
	 */
	virtual _int64 GetValue64(unsigned char *buf)			{ assert(0); return NULL_VALUE_64; }
	/*!
	 * \brief Get the current value of counter pointed by the pointer.
	 * May be left unimplemented if no text value is returned.
	 */
	virtual RCBString GetValueT(unsigned char *buf)			{ assert(0); return RCBString(); }
	/*!
	 * \brief Get the current value of counter pointed by the pointer.
	 * May be left unimplemented if no floating point value is returned.
	 */
	virtual double GetValueD(unsigned char *buf)			{ assert(0); return NULL_VALUE_D; }

	/*!
	 * \brief Reset the contents of the counter.
	 * This method should set a value which is valid for empty groups, e.g. 0 for COUNT(*).
	 */
	virtual void Reset(unsigned char *buf) = 0;

	///////////// Optimization part /////////////////

	/*!  If true, then the aggregation algorithm will not update counters for null value
	 *   (default valid for nearly all functions, even COUNT(*), where we have no column anyway).
	 */
	virtual bool IgnoreNulls()								{ return true; }

	/*!  If true, then DISTINCT modifier will be ignored
	 */
	virtual bool IgnoreDistinct()							{ return false; }

	/*!  If false, then the aggregation algorithm will ignore factor
	 */
	virtual bool FactorNeeded()								{ return true; }

	///////////// Pack aggregation part /////////////////

	/*!  If true, then the aggregation algorithm for a data pack needs no_obj as a parameter.
	 */
	virtual bool PackAggregationNeedsSize()					{ return false; }

	/*!  If true, then the aggregation algorithm for a data pack needs a number of not null objects as a parameter.
	 */
	virtual bool PackAggregationNeedsNotNulls()				{ return false; }

	/*!  If true, then the aggregation algorithm for a data pack needs a pack sum as a parameter.
	 */
	virtual bool PackAggregationNeedsSum()					{ return false; }

	/*!  If true, then the aggregation algorithm for a data pack needs a pack min as a parameter.
	 */
	virtual bool PackAggregationNeedsMin()					{ return false; }

	/*!  If true, then the aggregation algorithm for a data pack needs a pack max as a parameter.
	 */
	virtual bool PackAggregationNeedsMax()					{ return false; }

	/*!  If true, then the aggregation algorithm for a data pack works also if DISTINCT is used (e.g. MIN/MAX)
	 */
	virtual bool PackAggregationDistinctIrrelevant()			{ return false; }

	/*!  Set a parameter for the whole packrow aggregation.
	 */
	virtual void SetAggregatePackNoObj(_int64 par1)			{ assert(0); }

	/*!  Set a parameter for the whole packrow aggregation.
	 */
	virtual void SetAggregatePackNotNulls(_int64 par1)		{ assert(0); }

	/*!  Set a parameter for the whole packrow aggregation.
	 */
	virtual void SetAggregatePackSum(_int64 par1, _int64 factor) { assert(0); }

	/*!  Set a parameter for the whole packrow aggregation.
	 */
	virtual void SetAggregatePackMin(_int64 par1) { assert(0); }

	/*!  Set a parameter for the whole packrow aggregation.
	 */
	virtual void SetAggregatePackMax(_int64 par1) { assert(0); }

	/*!  Aggregate the whole packrow basing on parameters set previously.
	 *  Implementation depends on the actual aggregator.
	 *  \return True, if the packrow was successfully aggregated.
	 *  False if we must do it row by row.
	 */
	virtual bool AggregatePack(unsigned char *buf)			{ return false; }

	///////////// Pack omitting part /////////////////

	/*!  Return true if the statistics need to be updated before PackCannotChangeAggregation().
	 */
	bool StatisticsNeedsUpdate() const							{ return !stats_updated; }

	/*!  Set a flag after successful updating statistics.
	 */
	void SetStatisticsUpdated()								{ stats_updated = true; }

	/*!  Reset all statistics stored for the aggregator, prepare for new values.
	 */
	virtual void ResetStatistics()							{ stats_updated = false; }

	/*!  Add the given row to statistics.
	 *  \return True, if the statistics are updated and no more rows are needed
	 *  (e.g. when a limit value was already achieved).
	 */
	virtual bool UpdateStatistics(unsigned char *buf)		{ return true; }

	/*!  True only if we know that the pack will not change any of the currently existing counter value.
	 *   We know it basing on properly updated statistics stored in the aggregator
	 *   as well as on data pack statistics set by above functions.
	 *   \pre Aggregator statistics should be updated.
	 */
	virtual bool PackCannotChangeAggregation()				{ return false; }

protected:
	bool stats_updated;
};

/////////////////////////////////////////////////////////////////////////////
/////////////////// Examples: aggregators for COUNT /////////////////////////

/*!
 * \brief An aggregator for COUNT(...), for 64-bit counter.
 *
 * If we know that 32-bit counting is enough, it is better to use another class.
 * The counter consists of just one 64-bit value:
 *     <cur_count_64>
 * Start value: 0.
 */
class AggregatorCount64 : public Aggregator
{
public:
	AggregatorCount64(_int64 max_count) : max_counter(max_count), cur_min_counter(PLUS_INF_64), pack_count(0) { }
	AggregatorCount64(AggregatorCount64 &sec) : Aggregator(sec), max_counter(sec.max_counter), cur_min_counter(sec.cur_min_counter), pack_count(sec.pack_count) { }
	virtual Aggregator* Copy()								{ return new AggregatorCount64(*this); }

	virtual int BufferByteSize() 										{ return 8; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 factor)
	{
		stats_updated = false;
		*((_int64*)buf) += factor;
	}
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{
		stats_updated = false;
		*((_int64*)buf) += *((_int64*)src_buf);
	}

	virtual _int64 GetValue64(unsigned char *buf)							{ return *((_int64*)buf); }

	// Versions omitting the value:
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)			{ PutAggregatedValue(buf, factor); }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)	{ PutAggregatedValue(buf, factor); }

	virtual void Reset(unsigned char *buf)									{ *((_int64*)buf) = 0; }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSize()					{ return true; }	// we will need only one of these
	virtual bool PackAggregationNeedsNotNulls()				{ return true; }

	virtual void SetAggregatePackNoObj(_int64 par1)			{ pack_count = par1; }	// count(*) will set this one
	virtual void SetAggregatePackNotNulls(_int64 par1)		{ pack_count = par1; }	// count(a) will set this one (overwriting the above)

	virtual bool AggregatePack(unsigned char *buf)			{ PutAggregatedValue(buf, pack_count); return true; }
	virtual void ResetStatistics()							{ cur_min_counter = PLUS_INF_64; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(cur_min_counter > *((_int64*)buf))
			cur_min_counter = *((_int64*)buf);
		return (cur_min_counter == 1);						// minimal value, we will not find anything better
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return (cur_min_counter == max_counter);
	}

private:
	_int64 max_counter;				// upper limitation of counter (e.g. by no. of records, or dist. values)
	_int64 cur_min_counter;
	_int64 pack_count;
};

/*!
 * \brief An aggregator for COUNT(...), for 32-bit counter.
 * The counter consists of just one 32-bit value:
 *     <cur_count_32>
 * Start value: 0.
 */
class AggregatorCount32 : public Aggregator
{
public:
	AggregatorCount32(int max_count) : max_counter(max_count), cur_min_counter(0x7FFFFFFF), pack_count(0) { }
	AggregatorCount32(AggregatorCount32 &sec) : Aggregator(sec), max_counter(sec.max_counter), cur_min_counter(sec.cur_min_counter), pack_count(sec.pack_count) { }
	virtual Aggregator* Copy()								{ return new AggregatorCount32(*this); }

	virtual int BufferByteSize() 										{ return 4; }
	virtual void PutAggregatedValue(unsigned char *buf, _int64 factor)
	{
		stats_updated = false;
		*((int*)buf) += (int)factor;
	}
	virtual void Merge(unsigned char *buf, unsigned char *src_buf)
	{
		stats_updated = false;
		*((int*)buf) += *((int*)src_buf);
	}

	virtual _int64 GetValue64(unsigned char *buf)							{ return *((int*)buf); }

	// Versions omitting the value:
	virtual void PutAggregatedValue(unsigned char *buf, _int64 v, _int64 factor)			{ PutAggregatedValue(buf, factor); }
	virtual void PutAggregatedValue(unsigned char *buf, const RCBString& v, _int64 factor)	{ PutAggregatedValue(buf, factor); }

	virtual void Reset(unsigned char *buf)									{ *((int*)buf) = 0; }

	///////////// Optimization part /////////////////
	virtual bool PackAggregationNeedsSize()					{ return true; }	// we will need only one of these
	virtual bool PackAggregationNeedsNotNulls()				{ return true; }

	virtual void SetAggregatePackNoObj(_int64 par1)			{ pack_count = (int)par1; }	// count(*) will set this one
	virtual void SetAggregatePackNotNulls(_int64 par1)		{ pack_count = (int)par1; }	// count(a) will set this one (overwriting the above)

	virtual bool AggregatePack(unsigned char *buf)			{ PutAggregatedValue(buf, pack_count); return true; }
	virtual void ResetStatistics()							{ cur_min_counter = 0x7FFFFFFF; stats_updated = false; }
	virtual bool UpdateStatistics(unsigned char *buf)
	{
		if(cur_min_counter > *((int*)buf))
			cur_min_counter = *((int*)buf);
		return (cur_min_counter == 1);						// minimal value, we will not find anything better
	}
	virtual bool PackCannotChangeAggregation()
	{
		assert(stats_updated);
		return (cur_min_counter == max_counter);
	}

private:
	int max_counter;				// upper limitation of counter (e.g. by no. of records, or dist. values)
	int cur_min_counter;
	int pack_count;
};

#endif /*AGGREGATOR_H_*/
