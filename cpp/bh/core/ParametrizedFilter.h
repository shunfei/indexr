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

#ifndef _CORE_PARAMZED_FILTER_H_
#define _CORE_PARAMZED_FILTER_H_

#include "MultiIndex.h"
#include "CQTerm.h"
#include "Condition.h"
#include "Joiner.h"

#include <set>

class TempTable;
class DescTree;
class RoughMultiIndex;

/*
A class defining multidimensional filter (by means of MultiIndex) on a set of tables. It can store descriptors defining
some restrictions on particular dimensions. It can be parametrized thus the multiindex is not
materialized.
It can also store tree of conditions
*/

class ParameterizedFilter
{
public:
    ParameterizedFilter(CondType filter_type = WHERE_COND);
    ParameterizedFilter(const ParameterizedFilter&, bool for_rough_query = false);
    virtual ~ParameterizedFilter(void);
    ParameterizedFilter& operator=(const ParameterizedFilter& pf);
    //ParameterizedFilter & operator =(const ParameterizedFilter & pf);
	void AddConditions(const Condition* conds);
	uint NoParameterizedDescs() { return parametrized_desc.Size(); }
    void ProcessParameters();
	void PrepareRoughMultiIndex();
	void RoughUpdateParamFilter();
	void UpdateMultiIndex(bool count_only, _int64 limit);
	bool RoughUpdateMultiIndex();
	void RoughUpdateJoins();
	bool PropagateRoughToMind();
	void SyntacticalDescriptorListPreprocessing(bool for_rough_query = false);
	void DescriptorListOrdering();
	void DescriptorJoinOrdering();
	void RoughMakeProjections();
	void RoughMakeProjections(int dim, bool update_reduced = true);
	void UpdateJoinCondition(Condition& cond, JoinTips& tips);
	void DisplayJoinResults(DimensionVector& all_involved_dims, JoinAlgType cur_join_type, bool is_outer, int conditions_used);
	void ApplyDescriptor(int desc_number, _int64 limit = -1);
	static bool TryToMerge(Descriptor& d1, Descriptor& d2);
	void PrepareJoiningStep(Condition& join_desc, Condition& desc, int desc_no, MultiIndex& mind);
	void RoughSimplifyCondition(Condition& desc);
	/*! \brief true if the desc vector contains at least 2 1-dimensional descriptors defined for different dimensions
	 *  e.g. true if contains T.a=1, U.b=7, false if T.a=1, T.b=7
	 *
	 */
	bool DimsWith1dimFilters();
	double EvaluateConditionNonJoinWeight(Descriptor& d, bool for_or = false);		// for_or: optimize for bigger result (not the smaller one, as in case of AND)
	double EvaluateConditionJoinWeight(Descriptor& d);
	Condition& GetConditions() {return descriptors;}

    TempTable* table;
    Condition descriptors;
    MultiIndex* mind;
	RoughMultiIndex* rough_mind;
    Condition parametrized_desc;
    CondType filter_type;
private:
	void AssignInternal(const ParameterizedFilter& pf);
	void ScanWorker(void*);
	bool is_mind_owner;
};
#endif //_CORE_PARAMZED_FILTER_H_

