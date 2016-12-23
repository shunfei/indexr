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

#include "JoinerHash.h"
#include "TempTable.h"
#include "edition/vc/VirtualColumn.h"
#include "common/bhassert.h"
#include "system/ConnectionInfo.h"
#include "core/RoughMultiIndex.h"
#include "system/fet.h"

using namespace std;

JoinerHash::JoinerHash( MultiIndex *_mind, RoughMultiIndex *_rmind, TempTable *_table,
			JoinTips &_tips) : TwoDimensionalJoiner( _mind, _rmind, _table, _tips)
{
	traversed_dims = DimensionVector(_mind->NoDimensions());
	matched_dims = DimensionVector(_mind->NoDimensions());
	for(int i = 0; i < _mind->NoDimensions(); i++)
		traversed_hash_column.push_back(-1);
	no_of_traversed_dims = 0;
	vc1 = NULL;
	vc2 = NULL;
	cond_hashed = 0;
	other_cond_exist = false;
	packrows_omitted = 0;
	packrows_matched = 0;
	actually_traversed_rows = 0;
	watch_traversed = false;
	watch_matched = false;
	outer_filter = NULL;
	force_switching_sides = false;
	too_many_conflicts = false;
	outer_nulls_only = false;
}

JoinerHash::~JoinerHash()
{
	delete [] vc1;
	delete [] vc2;
	delete outer_filter;
}

void JoinerHash::ExecuteJoinConditions(Condition& cond)
{
	MEASURE_FET("JoinerHash::ExecuteJoinConditions(...)");
	why_failed = FAIL_HASH;

	vector<int> hash_descriptors;
	/////////////////// Prepare all descriptor information /////////////
	bool first_found = true;
	DimensionVector dims1(mind->NoDimensions());		// Initial dimension descriptions
	DimensionVector dims2(mind->NoDimensions());
	DimensionVector dims_other(mind->NoDimensions());	// dimensions for other conditions, if needed
	for(uint i = 0; i < cond.Size(); i++) {
		bool added = false;
		if(cond[i].IsType_JoinSimple() && cond[i].op == O_EQ) {
			if(first_found) {
				hash_descriptors.push_back(i);
				added = true;
				cond[i].attr.vc->MarkUsedDims(dims1);
				cond[i].val1.vc->MarkUsedDims(dims2);
				mind->MarkInvolvedDimGroups(dims1);
				mind->MarkInvolvedDimGroups(dims2);
				// Add dimensions for nested outer joins
				if(dims1.Intersects(cond[i].right_dims))
					dims1.Plus(cond[i].right_dims);
				if(dims2.Intersects(cond[i].right_dims))
					dims2.Plus(cond[i].right_dims);
				first_found = false;
			} else {
				DimensionVector sec_dims1(mind->NoDimensions());		// Make sure the local descriptions are compatible
				DimensionVector sec_dims2(mind->NoDimensions());
				cond[i].attr.vc->MarkUsedDims(sec_dims1);
				cond[i].val1.vc->MarkUsedDims(sec_dims2);
				if(dims1.Includes(sec_dims1) && dims2.Includes(sec_dims2)) {
					hash_descriptors.push_back(i);
					added = true;
				} else if(dims1.Includes(sec_dims2) && dims2.Includes(sec_dims1)) {
					cond[i].SwitchSides();
					hash_descriptors.push_back(i);
					added = true;
				}
			}
		}
		if(!added) {
			other_cond_exist = true;
			cond[i].DimensionUsed(dims_other);
			other_cond.push_back(cond[i]);
		}
	}
	cond_hashed = int(hash_descriptors.size());
	if(cond_hashed == 0) {
		why_failed = FAIL_HASH;
		return;
	}
	//////////////////////////////////////////////////////////////////////////////////
	// Check the proper direction: the (much) smaller dimension should be traversed, the larger one should be matched
	// but some special cases may change the direction. Rules:
	// - traverse dim1 if it is 3 times smaller than dim2,
	// - if neither is 3 times smaller than other, traverse the one which is less repeatable
	bool switch_sides = false;
	_int64 dim1_size = mind->NoTuples(dims1);
	_int64 dim2_size = mind->NoTuples(dims2);
	bool easy_roughable = false;
	if(min(dim1_size, dim2_size) > 100000) {			// approximate criteria for large tables (many packs)
		if(dim1_size > 2 * dim2_size)
			switch_sides = true;
		else if(2 * dim1_size > dim2_size) {				// within reasonable range - check again whether to change sides
			_int64 dim1_distinct = cond[hash_descriptors[0]].attr.vc->GetApproxDistVals(false);	// exclude nulls
			_int64 dim2_distinct = cond[hash_descriptors[0]].val1.vc->GetApproxDistVals(false);
			// check whether there are any nontrivial differencies in distinct value distributions
			if(dim1_distinct > dim1_size * 0.9 && dim2_distinct > dim2_size * 0.9) {
				if(dim1_size > dim2_size)		// no difference - just check table sizes
					switch_sides = true;
			} else if(double(dim1_distinct) / dim1_size < double(dim2_distinct) / dim2_size) // switch if dim1 has more repeating values
				switch_sides = true;
		}
		// Check whether the join may be easily optimized on rough level
		/*	// NOTE: verified negatively (no example of positive influence found)
		double dpn_density1 = cond[hash_descriptors[0]].attr.vc->RoughSelectivity();
		double dpn_density2 = cond[hash_descriptors[0]].val1.vc->RoughSelectivity();
		if(dpn_density1 < 0.2 && dpn_density2 < 0.2)
			easy_roughable = true;
		*/
	} else
		if(dim1_size > dim2_size)
			switch_sides = true;

	if(force_switching_sides)
		switch_sides = !switch_sides;
	if(switch_sides) {
		for(int i = 0; i < cond_hashed; i++)			// switch sides of joining conditions
			cond[hash_descriptors[i]].SwitchSides();
		traversed_dims = dims2;
		matched_dims = dims1;
	} else {
		traversed_dims = dims1;
		matched_dims = dims2;
	}

	//////////////////////////////////////////////////////////////////////////////////
	// jhash is a class field, initialized as empty
	vc1 = new VirtualColumn * [cond_hashed];
	vc2 = new VirtualColumn * [cond_hashed];
	bool compatible = true;
	for(int i = 0; i < cond_hashed; i++)		// add all key columns
	{
		vc1[i] = cond[hash_descriptors[i]].attr.vc;
		vc2[i] = cond[hash_descriptors[i]].val1.vc;
		compatible = jhash.AddKeyColumn(vc1[i], vc2[i]) && compatible;
	}
	// enlarge matched or traversed dimension lists by adding non-hashed conditions, if any
	if(other_cond_exist) {
		if(matched_dims.Intersects(cond[0].right_dims) && !cond[0].right_dims.Includes(dims_other)) {
			// special case: do not add non-right-side other dimensions to right-side matched
			dims_other.Minus(matched_dims);
			traversed_dims.Plus(dims_other);
		} else {
			// default: other dims as matched
			dims_other.Minus(traversed_dims);
			matched_dims.Plus(dims_other);
		}
	}
	// check whether we should take into account more dims
	mind->MarkInvolvedDimGroups(traversed_dims);
	mind->MarkInvolvedDimGroups(matched_dims);

	if(traversed_dims.Intersects(matched_dims) ||	// both materialized - we should rather use a simple loop
		!compatible) {								// could not prepare common encoding
		why_failed = FAIL_HASH;
		return;
	}
	// prepare columns for traversed dimension numbers in hash table
	no_of_traversed_dims = 0;
	for(int i = 0; i < mind->NoDimensions(); i++) {
		if(traversed_dims[i] && !(tips.count_only && !other_cond_exist)) {	// storing tuples may be omitted if (count_only_now && !other_cond_exist)
			traversed_hash_column[i] = cond_hashed + no_of_traversed_dims;	// jump over the joining key columns
			no_of_traversed_dims++;
			int bin_index_size = 4;
			if(mind->OrigSize(i) > 0x000000007FFFFF00)
				bin_index_size = 8;
			jhash.AddTupleColumn(bin_index_size);
		}
	}
	// calculate the size of hash table
	jhash.Initialize(_int64(mind->NoTuples(traversed_dims) * 1.5), easy_roughable);

	// jhash prepared, perform the join
	InitOuter(cond);
	ExecuteJoin();
	if(too_many_conflicts)
		why_failed = FAIL_WRONG_SIDES;
	else
		why_failed = NOT_FAILED;
}

void JoinerHash::ExecuteJoin()
{
	MEASURE_FET("JoinerHash::ExecuteJoin(...)");
	_int64 joined_tuples = 0;

	// preparing the new multiindex tables
	MIIterator tr_mit(mind, traversed_dims);
	MIIterator match_mit(mind, matched_dims);
	_uint64 dim1_size = tr_mit.NoTuples();
	_uint64 dim2_size = match_mit.NoTuples();

	_uint64 approx_join_size = dim1_size;
	if(dim2_size > approx_join_size)
		approx_join_size = dim2_size;

	vector<bool> traverse_keys_unique;
	for(int i = 0; i < cond_hashed; i++)
		traverse_keys_unique.push_back(vc1[i]->IsDistinctInTable());

	mind->LockAllForUse();
	MINewContents new_mind(mind, tips);
	new_mind.SetDimensions(traversed_dims);
	new_mind.SetDimensions(matched_dims);
	if(!tips.count_only)
		new_mind.Init(approx_join_size);
	// joining itself

	_int64 traversed_tuples = 0;
	actually_traversed_rows = 0;
	_int64 outer_tuples = 0;

	if(dim1_size > 0 && dim2_size > 0)
		while(tr_mit.IsValid()) {
			traversed_tuples += TraverseDim(new_mind, tr_mit, outer_tuples);
			if(tr_mit.IsValid())
				rccontrol.lock(m_conn.GetThreadID()) << "Traversed " << traversed_tuples << "/" << dim1_size << " rows." << unlock;
			else
				rccontrol.lock(m_conn.GetThreadID()) << "Traversed all " << dim1_size << " rows." << unlock;

			if(too_many_conflicts) {
				rccontrol.lock(m_conn.GetThreadID()) << "Too many hash conflicts: restarting join." << unlock;
				return;	// without committing new_mind
			}
			joined_tuples += MatchDim(new_mind, match_mit);
			if(watch_traversed)
				outer_tuples +=  SubmitOuterTraversed(new_mind);

			rccontrol.lock(m_conn.GetThreadID()) << "Produced " << joined_tuples << " tuples." << unlock;
			if(tips.limit != -1 && tips.limit <= joined_tuples)
				break;
		}
	// outer join part
	_int64 outer_tuples_matched = 0;
	if(watch_matched && !outer_filter->IsEmpty())
		outer_tuples_matched = SubmitOuterMatched(match_mit, new_mind);
	outer_tuples += outer_tuples_matched;
	if(outer_tuples > 0)
		rccontrol.lock(m_conn.GetThreadID()) << "Added " << outer_tuples << " null tuples by outer join." << unlock;
	joined_tuples += outer_tuples;
	// revert multiindex to the updated tables
	if(packrows_omitted > 0)
		rccontrol.lock(m_conn.GetThreadID()) << "Roughly omitted " << int(packrows_omitted / double(packrows_matched) * 10000.0) / 100.0 << "% packrows." << unlock;
	if(tips.count_only)
		new_mind.CommitCountOnly(joined_tuples);
	else
		new_mind.Commit(joined_tuples);

	// update local statistics - wherever possible
	_int64 traversed_dist_limit = actually_traversed_rows;
	for(int i = 0; i < cond_hashed; i++)
		if(!vc2[i]->Type().IsString()) {		// otherwise different collations may be a counterexample
			vc2[i]->SetLocalDistVals(actually_traversed_rows + outer_tuples_matched);		// matched values: not more than the traversed ones in case of equality
			if(traverse_keys_unique[i])			// unique => there is no more rows than distinct values
				traversed_dist_limit = std::min(traversed_dist_limit, vc2[i]->GetApproxDistVals(false) + (outer_tuples - outer_tuples_matched));
		}
	for(int i = 0; i < mind->NoDimensions(); i++)
		if(traversed_dims[i])
			table->SetVCDistinctVals(i, traversed_dist_limit);		// all dimensions involved in traversed side
	mind->UnlockAllFromUse();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

_int64 JoinerHash::TraverseDim(MINewContents &new_mind, MIIterator &mit, _int64 &outer_tuples)		
// new_mind and outer_tuples (the counter) are used only for outer joins
{
	MEASURE_FET("JoinerHash::TraverseMaterialDim(...)");
	jhash.ClearAll();

	_int64 hash_row = 0;				// hash_row = 0, otherwise deadlock for null on the first position
	_int64 traversed_rows = 0;

	for(int i = 0; i < cond_hashed; i++)
		vc1[i]->InitPrefetching(mit);
	if(watch_traversed)
		outer_filter->Reset();			// set only occupied hash positions
	bool first_run = true;				// to prevent opening unlocked packs on the next traverse

	while(mit.IsValid()) {
		if(m_conn.killed())
			throw KilledRCException();
		if(mit.PackrowStarted() || first_run) {
			for(int i = 0; i < cond_hashed; i++)
				vc1[i]->LockSourcePacks(mit);
			first_run = false;
		}
		bool omit_row = false;
		for(int i = 0; i < cond_hashed; i++) {
			if(vc1[i]->IsNull(mit)) {
				omit_row = true;
				break;
			}
			jhash.PutKeyValue(i, mit);
		}
		if(!omit_row) {  // else go to the next row - equality cannot be fulfilled
			hash_row = jhash.FindAndAddCurrentRow();
			if(hash_row == NULL_VALUE_64)
				break;			// no space left - stop for now and then restart from the current row
			if(!force_switching_sides && jhash.TooManyConflicts()) {
				too_many_conflicts = true;
				break;		// and exit the function
			}
			if(watch_traversed)
				outer_filter->Set(hash_row);
			actually_traversed_rows++;
			// Put the tuple column. Note: needed also for count_only_now, because another conditions may need it.
			if(!tips.count_only || other_cond_exist) {
				for(int i = 0; i < mind->NoDimensions(); i++)
					if(traversed_dims[i])
						jhash.PutTupleValue(traversed_hash_column[i], hash_row, mit[i]);
			}
		} else if(watch_traversed) {
			// Add outer tuples for omitted hash positions
			for(int i = 0; i < mind->NoDimensions(); i++) {
				if(traversed_dims[i])
					new_mind.SetNewTableValue(i, mit[i]);
				else if(matched_dims[i])
					new_mind.SetNewTableValue(i, NULL_VALUE_64);
			}
			new_mind.CommitNewTableValues();
			actually_traversed_rows++;
			outer_tuples++;
		}
		++mit;
		traversed_rows++;
	}

	for(int i = 0; i < cond_hashed; i++) {
		vc1[i]->UnlockSourcePacks();
		vc1[i]->StopPrefetching();
	}

	return traversed_rows;
}

_int64 JoinerHash::MatchDim(MINewContents &new_mind, MIIterator &mit)
{
	MEASURE_FET("JoinerHash::MatchMaterialDim(...)");
	_int64 joined_tuples = 0;
	_int64 hash_row;
	_int64 no_of_matching_rows;
	mit.Rewind();
	_int64 matching_row = 0;
	MIDummyIterator combined_mit(mind);		// a combined iterator for checking non-hashed conditions, if any

	for(int i = 0; i < cond_hashed; i++)
		vc2[i]->InitPrefetching(mit);

	while(mit.IsValid()) {
		if(m_conn.killed())
			throw KilledRCException();
		// Rough and locking part
		bool omit_this_packrow = false;
		bool packrow_uniform = false;				// if the packrow is uniform, process it massively
		if(mit.PackrowStarted()) {
			packrow_uniform = true;
			for(int i = 0; i < cond_hashed; i++) {
				if(jhash.StringEncoder(i)) {
					if(!vc2[i]->Type().IsLookup()) {	// lookup treated as string, when the dictionaries aren't convertible
						RCBString local_min = vc2[i]->GetMinString(mit);
						RCBString local_max = vc2[i]->GetMaxString(mit);
						if(!local_min.IsNull() && !local_max.IsNull() && jhash.ImpossibleValues(i, local_min, local_max)) {
								omit_this_packrow = true;
								break;
						}
					}
					packrow_uniform = false;
				} else {
					_int64 local_min = vc2[i]->GetMinInt64(mit);
					_int64 local_max = vc2[i]->GetMaxInt64(mit);
					if(local_min == NULL_VALUE_64 || local_max == NULL_VALUE_64 ||		// NULL_VALUE_64 only for nulls only
						jhash.ImpossibleValues(i, local_min, local_max)) {
						omit_this_packrow = true;
						break;
					}
					if(other_cond_exist || local_min != local_max ||
							vc2[i]->NullsPossible()) {
						packrow_uniform = false;
					}
				}
			}
			packrows_matched++;
			if(packrow_uniform && !omit_this_packrow) {
				for(int i = 0; i < cond_hashed; i++) {
					_int64 local_min = vc2[i]->GetMinInt64(mit);
					jhash.PutMatchedValue(i, local_min);
				}
				no_of_matching_rows = jhash.InitCurrentRowToGet() * mit.GetPackSizeLeft();
				if(!tips.count_only)
					while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64) {
						MIIterator mit_this_pack(mit);
						_int64 matching_this_pack = matching_row;
						do {
							SubmitJoinedTuple(hash_row, mit_this_pack, new_mind);
							if(watch_matched)
								outer_filter->ResetDelayed(matching_this_pack);
							++mit_this_pack;
							matching_this_pack++;
						} while(mit_this_pack.IsValid() && !mit_this_pack.PackrowStarted());
					}
				else if(watch_traversed) {
					while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64)
						outer_filter->Reset(hash_row);
				}

				joined_tuples += no_of_matching_rows;
				omit_this_packrow = true;
			}
			if(omit_this_packrow) {
				matching_row += mit.GetPackSizeLeft();
				mit.NextPackrow();
				packrows_omitted++;
				continue;				// here we are jumping out for impossible or uniform packrow
			}
			if(new_mind.NoMoreTuplesPossible())
				break;					// stop the join if nothing new may be obtained in some optimized cases

			for(int i = 0; i < cond_hashed; i++)
				vc2[i]->LockSourcePacks(mit);
		}
		// Exact part - make the key row ready for comparison
		bool null_found = false;
		bool non_matching_sizes = false;
		for(int i = 0; i < cond_hashed; i++) {
			if(vc2[i]->IsNull(mit)) {
				null_found = true;
				break;
			}
			jhash.PutMatchedValue(i, vc2[i], mit);
		}
		if(!null_found && !non_matching_sizes) {	// else go to the next row - equality cannot be fulfilled
			no_of_matching_rows = jhash.InitCurrentRowToGet();
			// Find all matching rows
			if(!other_cond_exist) {
				// Basic case - just equalities
				if(!tips.count_only)
					while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64)
						SubmitJoinedTuple(hash_row, mit, new_mind);	// use the multiindex iterator position
				else if(watch_traversed) {
					while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64)
						outer_filter->Reset(hash_row);
				}
				if(watch_matched && no_of_matching_rows > 0)
					outer_filter->ResetDelayed(matching_row);
				joined_tuples += no_of_matching_rows;
			} else {
				// Complex case: different types of join conditions mixed together
				combined_mit.Combine(mit);
				while((hash_row = jhash.GetNextRow()) != NULL_VALUE_64) {
					bool other_cond_true = true;
					for(int i = 0; i < mind->NoDimensions(); i++) {
						if(traversed_dims[i])
							combined_mit.Set(i, jhash.GetTupleValue(traversed_hash_column[i], hash_row));
					}
					for(int j = 0; j < other_cond.size(); j++) {
						other_cond[j].LockSourcePacks(combined_mit);
						if(other_cond[j].CheckCondition(combined_mit) == false) {
							other_cond_true = false;
							break;
						}
					}
					if(other_cond_true) {
						if(!tips.count_only)
							SubmitJoinedTuple(hash_row, mit, new_mind);	// use the multiindex iterator position
						else if(watch_traversed)
							outer_filter->Reset(hash_row);
						joined_tuples++;
						if(watch_matched)
							outer_filter->ResetDelayed(matching_row);
					}
				}
			}
			if(tips.limit != -1 && tips.limit <= joined_tuples)
				break;
		}
		++mit;
		matching_row++;
	}
	if(watch_matched)
		outer_filter->Commit();				// commit the delayed resets

	for(int i = 0; i < cond_hashed; i++) {
		vc2[i]->UnlockSourcePacks();
		vc2[i]->StopPrefetching();
	}
	for(int j = 0; j < other_cond.size(); j++)
		other_cond[j].UnlockSourcePacks();

	if(outer_nulls_only)
		joined_tuples = 0;					// outer tuples added later
	return joined_tuples;
}

void JoinerHash::SubmitJoinedTuple(_int64 hash_row, MIIterator &mit, MINewContents &new_mind)
{
	MEASURE_FET("JoinerHash::SubmitJoinedTuple(...)");
	if(watch_traversed)
		outer_filter->Reset(hash_row);
	// assumption: SetNewTableValue is called once for each dimension involved (no integrity checking)
	if(!outer_nulls_only) {
		for(int i = 0; i < mind->NoDimensions(); i++) {
			if(matched_dims[i])
				new_mind.SetNewTableValue(i, mit[i]);
			else if(traversed_dims[i])
				new_mind.SetNewTableValue(i, jhash.GetTupleValue(traversed_hash_column[i], hash_row));
		}
		new_mind.CommitNewTableValues();
	}
}

//////////////////////////////// outer part /////////////////////////////////////

void JoinerHash::InitOuter(Condition& cond)
{
	DimensionVector outer_dims(cond[0].right_dims);	// outer_dims will be filled with nulls for non-matching tuples
	if(!outer_dims.IsEmpty()) {
		if(traversed_dims.Includes(outer_dims)) {
			watch_matched = true;			// watch the non-outer dim for unmatched tuples and add them with nulls on outer dim
			outer_filter = new Filter(mind->NoTuples(matched_dims), true);	// the filter updated for each matching
		}
		else if(matched_dims.Includes(outer_dims)) {
			watch_traversed = true;
			outer_filter = new Filter(jhash.NoRows());			// the filter reused for each traverse
		}
		outer_nulls_only = true;
		for(int j = 0; j < outer_dims.Size(); j++)
			if(outer_dims[j] && tips.null_only[j] == false)
				outer_nulls_only = false;
	}
}

_int64 JoinerHash::SubmitOuterMatched(MIIterator &mit, MINewContents &new_mind)
{
	MEASURE_FET("JoinerHash::SubmitOuterMatched(...)");
	// mit - an iterator through the matched dimensions
	assert(outer_filter != NULL && watch_matched);
	if(tips.count_only)
		return outer_filter->NoOnes();
	mit.Rewind();	
	_int64 matched_row = 0;
	_int64 outer_added = 0;
	while(mit.IsValid()) {
		if(outer_filter->Get(matched_row)) {					// if the matched tuple is still unused, add it with nulls
			for(int i = 0; i < mind->NoDimensions(); i++) {
				if(matched_dims[i])
					new_mind.SetNewTableValue(i, mit[i]);
				else if(traversed_dims[i])
					new_mind.SetNewTableValue(i, NULL_VALUE_64);
			}
			new_mind.CommitNewTableValues();
			outer_added++;
		}
		++mit;
		matched_row++;
	}
	return outer_added;
}

_int64 JoinerHash::SubmitOuterTraversed(MINewContents &new_mind)
{
	MEASURE_FET("JoinerHash::SubmitOuterTraversed(...)");
	assert(outer_filter != NULL && watch_traversed);
	if(tips.count_only)
		return outer_filter->NoOnes();
	_int64 outer_added = 0;
	for(_int64 hash_row = 0; hash_row < jhash.NoRows(); hash_row++) {
		if(outer_filter->Get(hash_row)) {
			for(int i = 0; i < mind->NoDimensions(); i++) {
				if(matched_dims[i])
					new_mind.SetNewTableValue(i, NULL_VALUE_64);
				else if(traversed_dims[i])
					new_mind.SetNewTableValue(i, jhash.GetTupleValue(traversed_hash_column[i], hash_row));
			}
			new_mind.CommitNewTableValues();
			outer_added++;
			actually_traversed_rows++;
		}
	}
	return outer_added;
}
