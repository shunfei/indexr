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

#ifndef JOINER_H_
#define JOINER_H_

#include "Descriptor.h"
#include "MIIterator.h"
#include "Condition.h"

class RoughMultiIndex;
class TempTable;
class Query;

class JoinTips
{
public:
	JoinTips(MultiIndex& mind);
	JoinTips(const JoinTips& sec);
	_int64				limit;				// -1 - no limit, otherwise the max. number of tuples in result
	bool				count_only;
	std::vector<bool>	forget_now;			// do not materialize this dimension
	std::vector<bool>	distinct_only;		// ignore all repetitions of row numbers for this dimension
	std::vector<bool>	null_only;			// outer nulls only, e.g. "...t1 left join t2 on a=b where t2.c is null" (when c is not null by default)
};

enum JoinAlgType { 	JTYPE_NONE,	JTYPE_SORT,	JTYPE_HASH,	JTYPE_MIXED, JTYPE_MAP, JTYPE_GENERAL };
// MIXED   - for reporting: more than one algorithm was used

class TwoDimensionalJoiner		// abstract class for multiindex-based join algorithms
{
public:
	enum JoinFailure { NOT_FAILED, FAIL_COMPLEX, FAIL_SORTER_TOO_WIDE, FAIL_1N_TOO_HARD, FAIL_HASH, FAIL_WRONG_SIDES };

	////////////
	TwoDimensionalJoiner(	MultiIndex *_mind, // multi-index to be updated
							RoughMultiIndex *_rmind,
							TempTable *_table,
							JoinTips &_tips);
	virtual ~TwoDimensionalJoiner();

	virtual void ExecuteJoinConditions(Condition& cond) = 0;
								// join descriptors are concerned with the same pair of tables;
								// this method triggers the joining for the whole multiindex,
								// using all descriptors from the list
	virtual void ForceSwitchingSides()	{ }

	JoinFailure WhyFailed()		{ return why_failed; }
								// the reason of the last failure of join operation
public:
	static JoinAlgType ChooseJoinAlgorithm(MultiIndex& mind, Condition& desc);
	static JoinAlgType ChooseJoinAlgorithm(JoinFailure join_result, JoinAlgType prev_type, size_t desc_size);
	static std::auto_ptr<TwoDimensionalJoiner> CreateJoiner(JoinAlgType join_alg_type,
															MultiIndex& mind, RoughMultiIndex& rmind,
															JoinTips &_tips, TempTable *table);
protected:
	MultiIndex *mind;
	RoughMultiIndex *rmind;
	TempTable *table;
	JoinTips tips;
	ConnectionInfo& m_conn;
	JoinFailure why_failed;
};

typedef std::auto_ptr<TwoDimensionalJoiner> TwoDimsJoinerAutoPtr;

/////////////////////////// Specializations //////////////////////////

class JoinerGeneral : public TwoDimensionalJoiner
{
/*
 * Algorithm: use MIUpdatingIterator to iterate through all dimensions involved
 * and delete tuples not matching a list of (arbitrary) conditions.
 */
public:
	JoinerGeneral( MultiIndex *_mind, RoughMultiIndex *_rmind, TempTable *_table,
			JoinTips &_tips) : TwoDimensionalJoiner(_mind, _rmind, _table, _tips) {}
	~JoinerGeneral() {}

	void ExecuteJoinConditions(Condition& cond);

protected:
	void ExecuteOuterJoinLoop(Condition& cond, MINewContents &new_mind, DimensionVector &all_dims, 
								DimensionVector &outer_dims, _int64 &tuples_in_output, _int64 output_limit);
};

#endif /*JOINER_H_*/
