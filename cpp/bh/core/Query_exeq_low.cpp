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

#include <boost/shared_ptr.hpp>

#include "edition/local.h"
#include "Query.h"
#include "ValueSet.h"
#include "edition/vc/VirtualColumn.h"
#include "ParametrizedFilter.h"
#include "TempTable.h"
#include "RoughMultiIndex.h"
#include "MIIterator.h"
#include "ConditionEncoder.h"
#include "Joiner.h"

using namespace std;

bool ParameterizedFilter::TryToMerge(Descriptor& d1, Descriptor& d2)   // true, if d2 is no longer needed
{										// Assumption: d1 is earlier than d2 (important for outer joins)
	MEASURE_FET("ParameterizedFilter::TryToMerge(...)");

	if(d1.IsType_OrTree() || d2.IsType_OrTree())
		return false;
	if(d1 == d2)
		return true;
	if(d1.attr.vc == d2.attr.vc && d1.IsInner() && d2.IsInner()) {
		// IS_NULL and anything based on the same column => FALSE
		// NOT_NULL and anything based on the same column => NOT_NULL is not needed
		// Exceptions:
		//		null NOT IN {empty set}
		//		null < ALL {empty set} etc.
		if( (d1.op == O_IS_NULL && d2.op != O_IS_NULL && !IsSetOperator(d2.op)) ||
			(d2.op == O_IS_NULL && d1.op != O_IS_NULL && !IsSetOperator(d1.op))) {
			d1.op = O_FALSE;
			d1.CalculateJoinType();
			return true;
		}
		if(d1.op == O_NOT_NULL && !IsSetOperator(d2.op)) {
			d1 = d2;
			return true;
		}
		if(d2.op == O_NOT_NULL && !IsSetOperator(d1.op))
			return true;
	}
	// If a condition is repeated both on outer join list and after WHERE, then the first one is not needed
	//    t1 LEFT JOIN t2 ON (a=b AND c=5) WHERE a=b   =>   t1 LEFT JOIN t2 ON c=5 WHERE a=b
	if(d1.EqualExceptOuter(d2)) {
		if(d1.IsInner() && d2.IsOuter())		// delete d2
			return true;
		if(d1.IsOuter() && d2.IsInner()) {		// delete d1
			d1 = d2;
			return true;
		}
	}
	// Content-based merging
	if(d1.attr.vc == d2.attr.vc) {
		if(d1.attr.vc->TryToMerge(d1, d2))
			return true;
	}
	return false;
}

void ParameterizedFilter::SyntacticalDescriptorListPreprocessing(bool for_rough_query)
{
	MEASURE_FET("ParameterizedFilter::SyntacticalDescriptorListPreprocessing(...)");
	// outer joins preprocessing (delaying conditions etc.)
	bool outer_join_found = false;
	uint no_desc = uint(descriptors.Size());
	DimensionVector all_outer(mind->NoDimensions());
	for(uint i = 0; i < no_desc; i++) {
		if(!descriptors[i].done && descriptors[i].IsOuter()) {
			outer_join_found = true;
			all_outer.Plus(descriptors[i].right_dims);
		}
	}
	if(outer_join_found) {
		for(uint i = 0; i < no_desc; i++)
			if(!descriptors[i].done && descriptors[i].IsInner()) {
				DimensionVector inner(mind->NoDimensions());
				descriptors[i].DimensionUsed(inner);
				if(all_outer.Intersects(inner)) {
					if(descriptors[i].NullMayBeTrue()) {
						descriptors[i].delayed = true;		// delayed, i.e. must be calculated after nulls occur
					} else {
						// e.g. t1 LEFT JOIN t2 ON a1=a2 WHERE t2.c = 5   - in such cases all nulls are excluded anyway,
						//                                                  so it is an equivalent of inner join
						all_outer.Minus(inner);		// set all involved dimensions as no longer outer
					}
				}
			}
			for(uint j = 0; j < no_desc; j++){
				if(!descriptors[j].done && descriptors[j].IsOuter() && !all_outer.Intersects(descriptors[j].right_dims))  /*[descriptors[i].outer_dim])*/ {
					descriptors[j].right_dims.Clean(); /*outer_dim = -1;*/			// change outer joins to inner (these identified above)
				}
			}
	}

	bool false_desc = false;

	// descriptor preparation (normalization etc.)
	for(uint i = 0; i < no_desc; i++) {		// Note that desc.size() may enlarge when joining with BETWEEN occur
		assert(descriptors[i].done == false);	// If not false, check it carefully.
		if(descriptors[i].IsTrue()) {
			if(descriptors[i].IsInner())
				descriptors[i].done = true;
			continue;
		} else if(descriptors[i].IsFalse())
			continue;

		if(descriptors[i].IsDelayed())
			continue;

		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(descriptors[i].lop == O_AND);
		//if(descriptors[i].lop != O_AND && descriptors[i].IsType_Join() && (descriptors[i].op == O_BETWEEN || descriptors[i].op == O_NOT_BETWEEN))
		//	throw NotImplementedRCException("This kind of join condition with OR is not implemented.");

		// normalization of descriptor of type 1 between a and b
		if(descriptors[i].op == O_BETWEEN) {
			if(descriptors[i].GetJoinType() != DT_COMPLEX_JOIN && !descriptors[i].IsType_Join() && descriptors[i].attr.vc && descriptors[i].attr.vc->IsConst()) {
				swap(descriptors[i].attr.vc, descriptors[i].val1.vc);
				descriptors[i].op = O_LESS_EQ;
				// now, the second part
				Descriptor dd(table, mind->NoDimensions());
				dd.attr = descriptors[i].val2;
				descriptors[i].val2.vc = NULL;
				dd.op = O_MORE_EQ;
				dd.val1 = descriptors[i].val1;
				dd.done = false;
				dd.left_dims = descriptors[i].left_dims;
				dd.right_dims = descriptors[i].right_dims;
				dd.CalculateJoinType();
				descriptors[i].CalculateJoinType();
				descriptors.AddDescriptor(dd);
				no_desc++;
			} else if(descriptors[i].IsType_JoinSimple()) {
				// normalization of descriptor of type a between 1 and b
				descriptors[i].op = O_MORE_EQ;
				Descriptor dd(table, mind->NoDimensions());
				dd.attr = descriptors[i].attr;
				dd.val1 = descriptors[i].val2;
				descriptors[i].val2.vc = NULL;
				dd.op = O_LESS_EQ;
				dd.done= false;
				dd.left_dims = descriptors[i].left_dims;
				dd.right_dims = descriptors[i].right_dims;
				dd.CalculateJoinType();
				descriptors[i].CalculateJoinType();
				descriptors.AddDescriptor(dd);
				no_desc++;
			}
		}
		descriptors[i].CoerceColumnTypes();
		descriptors[i].Simplify();
		if(descriptors[i].IsFalse()) {
			false_desc = true;
		}
	}

	// join descriptor merging (before normalization, because we may then add a condition for another column)
	vector<Descriptor> added_cond;
	for(uint i = 0; i < no_desc; i++) {			// t1.x == t2.y && t2.y == 5   =>   t1.x == 5
		if(!descriptors[i].done && descriptors[i].op == O_EQ &&	descriptors[i].IsType_JoinSimple() && descriptors[i].IsInner()) {
			// desc[i] is a joining (eq.) condition
			for(uint j = 0; j < descriptors.Size(); j++) {
				if(i != j && !descriptors[j].done && descriptors[j].IsType_AttrValOrAttrValVal() && descriptors[j].IsInner()) {
					// desc[j] is a second condition, non-join
					if(descriptors[j].attr.vc == descriptors[i].attr.vc) {
						// the same table and column
						if(descriptors[j].op == O_EQ) {	// t2.y == t1.x && t2.y == 5  change to  t1.x == 5 && t2.y == 5
							descriptors[i].attr = descriptors[i].val1;
							descriptors[i].val1 = descriptors[j].val1;
							descriptors[i].CalculateJoinType();
							descriptors[i].CoerceColumnTypes();
							break;
						} else {	// t2.y == t1.x && t2.y > 5  change to  t2.y == t1.x && t2.y > 5 && t1.x > 5
							Descriptor dd(table, mind->NoDimensions());
							dd.attr = descriptors[i].val1;
							dd.op = descriptors[j].op;
							dd.val1 = descriptors[j].val1;
							dd.val2 = descriptors[j].val2;
							dd.CalculateJoinType();
							dd.CoerceColumnTypes();
							added_cond.push_back(dd);
						}
					}
					if(descriptors[j].attr.vc == descriptors[i].val1.vc) { //the same as above for val1
						// the same table and column
						if(descriptors[j].op == O_EQ) {	// t1.x == t2.y && t2.y == 5  change to  t1.x == 5 && t2.y == 5
							descriptors[i].val1 = descriptors[j].val1;
							descriptors[i].CalculateJoinType();
							descriptors[i].CoerceColumnTypes();
							break;
						} else {	// t1.x == t2.y && t2.y > 5  change to  t1.x == t2.y && t2.y > 5 && t1.x > 5
							Descriptor dd(table, mind->NoDimensions());
							dd.attr = descriptors[i].attr;
							dd.op = descriptors[j].op;
							dd.val1 = descriptors[j].val1;
							dd.val2 = descriptors[j].val2;
							dd.CalculateJoinType();
							dd.CoerceColumnTypes();
							added_cond.push_back(dd);
						}
					}
				}
			}
		}
	}
	if(!added_cond.empty() && rccontrol.isOn())
		rccontrol.lock(mind->m_conn->GetThreadID()) << "Adding " << int(added_cond.size()) << " conditions..." << unlock;
	for(uint i = 0; i < added_cond.size(); i++) {
		descriptors.AddDescriptor(added_cond[i]);
		no_desc++;
	}
	// attribute-based transformation (normalization) of descriptors "attr-operator-value" and other, if possible
	if(!false_desc) {
		for(uint i = 0; i < no_desc; i++) {
			DimensionVector all_dims(mind->NoDimensions());
			descriptors[i].DimensionUsed(all_dims);
			bool additional_nulls_possible = false;
			for(int d = 0; d < mind->NoDimensions(); d++)
				if(all_dims[d] && mind->GetFilter(d) == NULL)
					additional_nulls_possible = true;
			if(descriptors[i].IsOuter())
				additional_nulls_possible = true;
			ConditionEncoder::EncodeIfPossible(descriptors[i], for_rough_query, additional_nulls_possible);

			if(descriptors[i].IsTrue()) {			// again, because something might be simplified
				if(descriptors[i].IsInner())
					descriptors[i].done = true;
				continue;
			}
		}

		// descriptor merging
		for(uint i = 0; i < no_desc; i++) {
			if(descriptors[i].done || descriptors[i].IsDelayed())
				continue;
			for(uint jj = i + 1; jj < no_desc; jj++) {
				if(descriptors[jj].right_dims != descriptors[i].right_dims || descriptors[jj].done || descriptors[jj].IsDelayed())
					continue;
				if(TryToMerge(descriptors[i], descriptors[jj])) {
					rccontrol.lock(mind->m_conn->GetThreadID()) << "Merging conditions..." << unlock;
					descriptors[jj].done = true;
				}
			}
		}
	}
}

void ParameterizedFilter::DescriptorListOrdering()
{
	MEASURE_FET("ParameterizedFilter::DescriptorListOrdering(...)");
	// descriptor evaluating
	// evaluation weight: higher value means potentially larger result (i.e. to be calculated as late as possible)
	if(descriptors.Size() > 1 || descriptors[0].IsType_OrTree()) {	// else all evaluations remain 0 (default)
		for(uint i = 0; i < descriptors.Size(); i++) {
			if(descriptors[i].done || descriptors[i].IsDelayed())
				descriptors[i].evaluation = 100000;				// to the end of queue
			else
				descriptors[i].evaluation = EvaluateConditionNonJoinWeight(descriptors[i]);	// joins are evaluated separately
		}
	}
	// descriptor ordering by evaluation weight
	for(uint k = 0; k < descriptors.Size(); k++) {
		for(uint i = 0; i < descriptors.Size() - 1; i++)	{
			int j = i + 1;			// bubble sort
			if( descriptors[i].evaluation > descriptors[j].evaluation &&
				descriptors[j].IsInner() && descriptors[i].IsInner())	// do not change outer join order
			{
				descriptors[i].swap(descriptors[j]);
			}
		}
	}
}

void ParameterizedFilter::UpdateMultiIndex(bool count_only,	_int64 limit)
{
	MEASURE_FET("ParameterizedFilter::UpdateMultiIndex(...)");

	/*		// Uncomment it to get an input descriptor list
	if(rccontrol.isOn()) {
		rccontrol.lock(mind->m_conn->GetThreadID()) << "TEMPORARY: raw descriptor list:" << unlock;
		for(uint i = 0; i < descriptors.Size(); i++) {
			char buf[1000];
			strcpy(buf, " ");
			descriptors[i].ToString(buf, 1000);
			if(descriptors[i].done)
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Done: " << buf << unlock;
			else if(descriptors[i].IsDelayed())
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Delayed: " << buf << unlock;
			else
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Cnd(" << i << "):  " << buf << unlock;
		}
	}
	*/

	////////////////////////////////////////////////////////////////////////

	if(descriptors.Size() < 1) {
		PrepareRoughMultiIndex();
		rough_mind->ClearLocalDescFilters();
		return;
	}
	SyntacticalDescriptorListPreprocessing();

	bool empty_cannot_grow = true;			// if false (e.g. outer joins), then do not optimize empty multiindex as empty result

	for(uint i = 0; i < descriptors.Size(); i++)
		if(descriptors[i].IsOuter())
			empty_cannot_grow = false;

	// special cases
	bool nonempty = true;

	DescriptorListOrdering();

	///// descriptor display
	if(rccontrol.isOn()) {
		rccontrol.lock(mind->m_conn->GetThreadID()) << "Initial execution plan (non-join):" << unlock;
		for(uint i = 0; i < descriptors.Size(); i++) 
			if(!descriptors[i].done && !descriptors[i].IsType_Join() && descriptors[i].IsInner()) {
			char buf[1000];
			strcpy(buf, " ");
			descriptors[i].ToString(buf, 1000);
			if(descriptors[i].IsDelayed())
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Delayed: " << buf << " \t(" << int(descriptors[i].evaluation*100)/100.0 << ")" << unlock;
			else
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Cnd(" << i << "):  " << buf << " \t(" << int(descriptors[i].evaluation*100)/100.0 << ")" << unlock;
		}
	}

	for(uint i = 0; i < table->NoVirtColumns(); i++) {
		if(table->GetVirtualColumn(i) && table->GetVirtualColumn(i)->NeedsPreparing())
				table->GetVirtualColumn(i)->Prepare();
	}

	///// end now if the multiindex is empty
	if(mind->ZeroTuples() && empty_cannot_grow) {
		mind->Empty();
		PrepareRoughMultiIndex();
		rough_mind->ClearLocalDescFilters();
		return;
	}

	///////////////// Prepare execution - rough set part
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done && descriptors[i].IsInner())	{
			if(descriptors[i].IsTrue()) {
				descriptors[i].done = true;

				continue;
			} else if(descriptors[i].IsFalse()) {
				if(descriptors[i].attr.vc) {
					descriptors[i].done = true;
					if(empty_cannot_grow) {
						mind->Empty();
						PrepareRoughMultiIndex();
						rough_mind->ClearLocalDescFilters();
						return;
					} else {
						DimensionVector dims(mind->NoDimensions());
						descriptors[i].attr.vc->MarkUsedDims(dims);
						mind->MakeCountOnly(0, dims);
					}
				}
			}
		}
	}
	if(rough_mind)
		rough_mind->ClearLocalDescFilters();
	PrepareRoughMultiIndex();
	nonempty = RoughUpdateMultiIndex();	// calculate all rough conditions,

	if(!nonempty && empty_cannot_grow) {
		mind->Empty();		// nonempty==false if the whole result is empty (outer joins considered)
		rough_mind->ClearLocalDescFilters();
		return;
	}
	PropagateRoughToMind(); 			//exclude RS_NONE from mind

	//////// count other types of conditions, e.g. joins (i.e. conditions using attributes from two dimensions)
	int  no_of_join_conditions = 0;		// count also one-dimensional outer join conditions
	int no_of_delayed_conditions = 0;
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done)
			if(descriptors[i].IsType_Join() || descriptors[i].IsDelayed() || descriptors[i].IsOuter()) {
				if(!descriptors[i].IsDelayed())
					no_of_join_conditions++;
				else
					no_of_delayed_conditions++;
			}
	}


	///////////////// Apply all one-dimensional filters (after where, i.e. without outer joins)
	int last_desc_dim = -1;
	int cur_dim = -1;

	int no_desc = 0;
	for(uint i = 0; i < descriptors.Size(); i++)
		if(!descriptors[i].done && descriptors[i].IsInner() && !descriptors[i].IsType_Join() && !descriptors[i].IsDelayed())
			++no_desc;

	int desc_no = 0;
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done && descriptors[i].IsInner() && !descriptors[i].IsType_Join() && !descriptors[i].IsDelayed()) {
			++desc_no;
			if(descriptors[i].attr.vc) {
				cur_dim = descriptors[i].attr.vc->GetDim();
			}
			if(last_desc_dim != -1 && cur_dim != -1 && last_desc_dim != cur_dim ) {
				// Make all possible projections to other dimensions
				RoughMakeProjections(cur_dim,false);
			}

			//limit should be applied only for the last descriptor
			ApplyDescriptor(i, (desc_no != no_desc || no_of_delayed_conditions > 0 || 
				no_of_join_conditions) ? -1 : limit);
			if (!descriptors[i].attr.vc)
				continue; //probably desc got simplified and is true or false
			if(cur_dim >= 0 && mind->GetFilter(cur_dim) && mind->GetFilter(cur_dim)->IsEmpty() && empty_cannot_grow) {
				mind->Empty();
				if(rccontrol.isOn()) {
					rccontrol.lock(mind->m_conn->GetThreadID()) << "Empty result set after non-join condition evaluation (WHERE)" << unlock;
				}
				rough_mind->ClearLocalDescFilters();
				return;
			}
			last_desc_dim = cur_dim;

		}
	}
	rough_mind->UpdateReducedDimension();
	mind->UpdateNoTuples();
	for(int i = 0; i < mind->NoDimensions(); i++)
		if(mind->GetFilter(i))
			table->SetVCDistinctVals(i, mind->GetFilter(i)->NoOnes());		// distinct values - not more than the number of rows after WHERE
	rough_mind->ClearLocalDescFilters();

	/////////////////////////////////////////////////////////////////////////////////////
	// Some displays

	if(rccontrol.isOn()) {
		int pack_full = 0, pack_some = 0, pack_all = 0;
		rccontrol.lock(mind->m_conn->GetThreadID()) << "Packrows after exact evaluation (WHERE):" << unlock;
		for(uint i = 0; i < (uint)mind->NoDimensions(); i++) if(mind->GetFilter(i)) {
			Filter *f = mind->GetFilter(i);
			pack_full = 0;
			pack_some = 0;
			pack_all = bh::common::NoObj2NoPacks(mind->OrigSize(i));
			for(int b = 0; b < pack_all; b++) {
				if(f->IsFull(b))
					pack_full++;
				else if(!f->IsEmpty(b))
					pack_some++;
			}
			rccontrol.lock(mind->m_conn->GetThreadID()) << "(t" << i << "): " << pack_all << " all packrows, " << pack_full+pack_some << " to open (including " << pack_full << " full)" << unlock;
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////
	DescriptorJoinOrdering();

	///// descriptor display for joins
	if(rccontrol.isOn()) {
		bool first_time = true;
		for(uint i = 0; i < descriptors.Size(); i++) if(!descriptors[i].done && (descriptors[i].IsType_Join() || descriptors[i].IsOuter())) {
			if(first_time) {
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Join execution plan:" << unlock;
				first_time = false;
			}
			char buf[1000];
			strcpy(buf," ");
			descriptors[i].ToString(buf, 1000);
			if(descriptors[i].IsDelayed())
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Delayed: " << buf << " \t(" << int(descriptors[i].evaluation*100)/100.0 << ")" << unlock;
			else
				rccontrol.lock(mind->m_conn->GetThreadID()) << "Cnd(" << i << "):  " << buf << " \t(" << int(descriptors[i].evaluation*100)/100.0 << ")" << unlock;
		}
	}

	bool join_or_delayed_present = false;
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(mind->ZeroTuples() && empty_cannot_grow) {
			// set all following descriptors to done
			for(uint j = i; j < descriptors.Size(); j++)
				descriptors[j].done = true;
			break;
		}
		if(!descriptors[i].done && !descriptors[i].IsDelayed()) {
			//////////////// Merging join conditions ///////////////////
			Condition join_desc;
			PrepareJoiningStep(join_desc, descriptors, i, *mind);		// group together all join conditions for one step
			no_of_join_conditions -= join_desc.Size();
			JoinTips join_tips(*mind);

			// Optimization: Check whether there exists "a is null" delayed condition for an outer join
			if(join_desc[0].IsOuter()) {
				for(uint i = 0; i < descriptors.Size(); i++) {
					if(descriptors[i].IsDelayed() && !descriptors[i].done && descriptors[i].op == O_IS_NULL
						&& join_desc[0].right_dims.Get(descriptors[i].attr.vc->GetDim())
						&& !descriptors[i].attr.vc->NullsPossible()) {
							for(int j = 0; j < join_desc[0].right_dims.Size(); j++) {
								if(join_desc[0].right_dims[j] == true)
									join_tips.null_only[j] = true;
							}
							descriptors[i].done = true;		// completed inside joining algorithms
							no_of_delayed_conditions--;
					}
				}
			}

			if(no_of_join_conditions == 0 && // optimizations used only for the last group of conditions
				no_of_delayed_conditions == 0 && parametrized_desc.Size() == 0) {
					// Optimization: count_only is true => do not materialize multiindex (just counts tuples). WARNING: in this case cannot use multiindex for any operations other than NoTuples().
					if(count_only)
						join_tips.count_only = true;
					join_tips.limit = limit;
					// only one dim used in distinct context?
					int distinct_dim = table->DimInDistinctContext();
					int dims_in_output = 0;
					for(int dim = 0; dim < mind->NoDimensions(); dim++) 
						if(mind->IsUsedInOutput(dim))
							dims_in_output++;
					if(distinct_dim != -1 && dims_in_output == 1)
						join_tips.distinct_only[distinct_dim] = true;
			}

			// Optimization: Check whether all dimensions are really used
			DimensionVector dims_used(mind->NoDimensions());
			for(uint jj = 0; jj < descriptors.Size(); jj++) {
				if(jj != i && !descriptors[jj].done)
					descriptors[jj].DimensionUsed(dims_used);
			}
			// can't utilize not_used_dims in case there are parameterized descs left
			if(parametrized_desc.Size() == 0) {
				for(int dim = 0; dim < mind->NoDimensions(); dim++) 
					if(!mind->IsUsedInOutput(dim) && dims_used[dim] == false)
						join_tips.forget_now[dim] = true;
			}

			// Joining itself
			UpdateJoinCondition(join_desc, join_tips);
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////
	// Execute all delayed conditions
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done) {
			rccontrol.lock(mind->m_conn->GetThreadID()) << "Executing delayed Cnd(" << i << ")" << unlock;
			descriptors[i].CoerceColumnTypes();
			descriptors[i].Simplify();
			ApplyDescriptor(i);
			join_or_delayed_present = true;
		}
	}
	if(join_or_delayed_present)
		rough_mind->MakeDimensionSuspect();			// no RS_ALL packs
	mind->UpdateNoTuples();
}

void ParameterizedFilter::RoughUpdateParamFilter()
{
	MEASURE_FET("ParameterizedFilter::RoughUpdateParamFilter(...)");
	//Prepare();
	PrepareRoughMultiIndex();
	if(descriptors.Size() < 1)
		return;
	SyntacticalDescriptorListPreprocessing(true);
	bool non_empty = RoughUpdateMultiIndex();
	if(!non_empty)
		rough_mind->MakeDimensionEmpty();
	RoughUpdateJoins();
}
