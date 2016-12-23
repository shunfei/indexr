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

#include "JoinerMapped.h"
#include "edition/vc/VirtualColumn.h"

using namespace std;

JoinerMapped::JoinerMapped( MultiIndex *_mind, RoughMultiIndex *_rmind, TempTable *_table,
							JoinTips &_tips) : TwoDimensionalJoiner( _mind, _rmind, _table, _tips)
{
	traversed_dims = DimensionVector(_mind->NoDimensions());
	matched_dims = DimensionVector(_mind->NoDimensions());
}

void JoinerMapped::ExecuteJoinConditions(Condition& cond)
{
	MEASURE_FET("JoinerMapped::ExecuteJoinConditions(...)");

	why_failed = FAIL_1N_TOO_HARD;
	Descriptor *desc = &(cond[0]);
	if(cond.Size() > 1 || !desc->IsType_JoinSimple() || desc->op != O_EQ)
		return;

	VirtualColumn* vc1 = desc->attr.vc;
	VirtualColumn* vc2 = desc->val1.vc;

	if(!vc1->Type().IsFixed() || !vc2->Type().IsFixed() || 
		vc1->Type().GetScale() != vc2->Type().GetScale() || 
		vc1->Type().IsLookup() || vc2->Type().IsLookup())
		return;

	vc1->MarkUsedDims(traversed_dims);				// "traversed" is unique now (a "dimension" table)
	vc2->MarkUsedDims(matched_dims);				// a "fact" table for 1:n relation
	mind->MarkInvolvedDimGroups(traversed_dims);
	mind->MarkInvolvedDimGroups(matched_dims);
	if(traversed_dims.Intersects(matched_dims))		// both materialized - we should rather use a simple loop
		return;					
	
	MIIterator mit_m(mind, matched_dims);
	_uint64 dim_m_size = mit_m.NoTuples();
	MIIterator mit_t(mind, traversed_dims);
	_uint64 dim_t_size = mit_t.NoTuples();

	if((vc2->IsDistinct() && !vc1->IsDistinct()) ||
		traversed_dims.NoDimsUsed() > 1 ||
		dim_m_size < dim_t_size) {
		desc->SwitchSides();
		vc1 = desc->attr.vc;
		vc2 = desc->val1.vc;
		DimensionVector temp_dims(traversed_dims);
		traversed_dims = matched_dims;
		matched_dims = temp_dims;
	}
	if(traversed_dims.NoDimsUsed() > 1 ||			// more than just a number
		matched_dims.Intersects(desc->right_dims))	// only traversed dimension may be outer
		return;					
	int traversed_dim = traversed_dims.GetOneDim();
	assert(traversed_dim > -1);

	///////////////////////////////////////////////////////////////////////////
	// Prepare mapping function
	JoinerMapFunction* map_function = GenerateFunction(vc1);
	if(map_function == NULL) 
		return;

	///////////////////////////////////////////////////////////////////////////
	bool outer_join = !(desc->right_dims.IsEmpty());	// traversed is outer: scan facts, add nulls for all non-matching dimension values
	bool outer_nulls_only = outer_join;
	if(tips.null_only[traversed_dim] == false)
		outer_nulls_only = false;					// leave only those tuples which obtain outer nulls

	MIIterator mit(mind, matched_dims);
	_uint64 dim2_size = mit.NoTuples();

	mind->LockAllForUse();
	MINewContents new_mind(mind, tips);
	new_mind.SetDimensions(traversed_dims);
	new_mind.SetDimensions(matched_dims);
	if(!tips.count_only)
		new_mind.Init(dim2_size);
	else if(outer_join) {						// outer join on keys verified as unique - a trivial answer
		new_mind.CommitCountOnly(dim2_size);
		why_failed = NOT_FAILED;
		mind->UnlockAllFromUse();
		delete map_function;
		return;
	}

	// Matching loop itself
	_int64 joined_tuples = 0;
	_int64 packrows_omitted = 0;
	_int64 packrows_matched = 0;
	vc2->InitPrefetching(mit);
	int single_filter_dim = new_mind.OptimizedCaseDimension();		// indicates a special case: the fact table remains a filter and the dimension table is forgotten
	if(single_filter_dim != -1 && !matched_dims[single_filter_dim])
		single_filter_dim = -1;
	while(mit.IsValid()) {
		bool omit_this_packrow = false;
		if(mit.PackrowStarted()) {
			if(m_conn.killed()) {
				delete map_function;
				throw KilledRCException();
			}
			_int64 local_min = MINUS_INF_64;
			_int64 local_max = PLUS_INF_64;
			if(vc2->GetNoNulls(mit) == mit.GetPackSizeLeft()) {
				omit_this_packrow = true;
			} else {
				local_min = vc2->GetMinInt64(mit);
				local_max = vc2->GetMaxInt64(mit);
				if(map_function->ImpossibleValues(local_min, local_max)) {
					omit_this_packrow = true;
				}
			}
			packrows_matched++;
			bool roughly_all = (vc2->GetNoNulls(mit) == 0 && map_function->CertainValues(local_min, local_max));
			if(single_filter_dim != -1 && roughly_all) {
				if(new_mind.CommitPack(mit.GetCurPackrow(single_filter_dim))) {	// processed as a pack
					joined_tuples += mit.GetPackSizeLeft();
					mit.NextPackrow();
					packrows_omitted++;
					continue;				// here we are jumping out for processed packrow
				}
			} else if(omit_this_packrow && !outer_join) {
				mit.NextPackrow();
				packrows_omitted++;
				continue;				// here we are jumping out for impossible packrow
			}
			if(new_mind.NoMoreTuplesPossible())
				break;					// stop the join if nothing new may be obtained in some optimized cases

			vc2->LockSourcePacks(mit);
		}

		// Exact part
		_int64 row = NULL_VALUE_64;
		if(!vc2->IsNull(mit))
			row = map_function->F(vc2->GetNotNullValueInt64(mit));
		if((row != NULL_VALUE_64 || outer_join) && !(outer_nulls_only && row != NULL_VALUE_64)) {
			joined_tuples++;
			if(!tips.count_only) {
				for(int i = 0; i < mind->NoDimensions(); i++)
					if(matched_dims[i])
						new_mind.SetNewTableValue(i, mit[i]);
				new_mind.SetNewTableValue(traversed_dim, row);
				new_mind.CommitNewTableValues();
			}
		}
		if(tips.limit != -1 && tips.limit <= joined_tuples)
			break;
		++mit;
	}

	vc2->UnlockSourcePacks();
	vc2->StopPrefetching();

	/////////////////////////////////////////////////////////////////////////////////////////////
	// Cleaning up
	rccontrol.lock(m_conn.GetThreadID()) << "Produced " << joined_tuples << " tuples." << unlock;
	if(packrows_omitted > 0)
		rccontrol.lock(m_conn.GetThreadID()) << "Roughly omitted " << int(packrows_omitted / double(packrows_matched) * 10000.0) / 100.0 << "% packrows." << unlock;
	if(tips.count_only)
		new_mind.CommitCountOnly(joined_tuples);
	else
		new_mind.Commit(joined_tuples);

	// update local statistics - wherever possible
	_int64 dist_vals_found = map_function->DistinctVals();
	if(dist_vals_found != NULL_VALUE_64) {
		vc1->SetLocalDistVals(dist_vals_found);
		if(!outer_join)
			vc2->SetLocalDistVals(dist_vals_found);		// matched values: not more than the traversed ones in case of equality
	}

	mind->UnlockAllFromUse();
	delete map_function;
	why_failed = NOT_FAILED;
}

JoinerMapFunction* JoinerMapped::GenerateFunction(VirtualColumn *vc)
{
	MIIterator mit(mind, traversed_dims);
	_int64 traversed_rows = mit.NoTuples();
	OffsetMapFunction *map_function = new OffsetMapFunction;
	bool success = map_function->Init(vc, mit);
	if(!success) {
		delete map_function;
		return NULL;
	}
	rccontrol.lock(m_conn.GetThreadID()) << "Join mapping (offset map) created on " << traversed_rows << " rows." << unlock;
	return map_function;
}


//////////////////////////////////////////////////////////////////////////////////////////////////////

_int64 OffsetMapFunction::F(_int64 key_val)
{
	if(key_val < key_min || key_val > key_max)
		return NULL_VALUE_64;
	unsigned char s = key_status[key_val - key_table_min];
	if(s == 255)
		return NULL_VALUE_64;
	return key_val + offset_table[s];
}

OffsetMapFunction::~OffsetMapFunction()
{
	delete [] key_status;
}

bool OffsetMapFunction::Init(VirtualColumn *vc, MIIterator &mit)
{
	int dim = vc->GetDim();
	if(dim == -1)
		return false;
	key_table_min =  vc->RoughMin();
	_int64 key_table_max = vc->RoughMax();
	if(key_table_min == NULL_VALUE_64 || key_table_min == MINUS_INF_64 || 
		key_table_max == PLUS_INF_64 || key_table_max == NULL_VALUE_64)
		return false;
	_int64 span = key_table_max - key_table_min + 1;
	if(span < 0 || span > 32 * MBYTE || (span < mit.NoTuples() && !vc->NullsPossible()))
		return false;
	assert(key_status == NULL);
	key_status = new unsigned char [span];
	memset(key_status, 255, span);
	int offsets_used = 0;
	vc->InitPrefetching(mit);
	while(mit.IsValid()) {
		if(mit.PackrowStarted())
			vc->LockSourcePacks(mit);
		_int64 val = vc->GetValueInt64(mit);
		if(val != NULL_VALUE_64) {
			if(val < key_min)
				key_min = val;
			if(val > key_max)
				key_max = val;
			_int64 row_offset = mit[dim] - val;
			int s;
			for(s = 0; s < offsets_used; s++) {
				if(offset_table[s] == row_offset)
					break;
			}
			if(s == offsets_used) {		// new offset found
				if(offsets_used == 255) {
					vc->UnlockSourcePacks();
					vc->StopPrefetching();
					return false;		// too many offsets
				}
				offset_table[s] = row_offset;
				offsets_used++;
			}
			val = val - key_table_min;
			if(key_status[val] != 255) {
				vc->UnlockSourcePacks();
				vc->StopPrefetching();
				return false;			// key repetition found
			}
			key_status[val] = s;
			dist_vals_found++;
		}
		++mit;
	}
	vc->UnlockSourcePacks();
	vc->StopPrefetching();
	key_continuous_max = key_min;
	while(key_continuous_max < key_max && key_status[key_continuous_max - key_table_min] != 255)
		key_continuous_max++;
	return true;
}

