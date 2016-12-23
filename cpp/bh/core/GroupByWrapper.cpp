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

#include "edition/local.h"
#include "GroupByWrapper.h"
#include "RCAttr.h"

using namespace std;

GroupByWrapper::GroupByWrapper(int a_size, bool just_distinct, ConnectionInfo& conn)
	:	distinct_watch(), m_conn(conn)

{
	attrs_size =	a_size;
	virt_col = 		new VirtualColumn * [attrs_size];

	input_mode = 	new GBInputMode [attrs_size];
	is_lookup =		new bool [attrs_size];
	attr_mapping =	new int [attrs_size];			// output attr[j] <-> gt group[attr_mapping[j]]
	dist_vals = 	new _int64 [attrs_size];

	for(int i = 0; i < attrs_size; i++)
		attr_mapping[i] = -1;

	pack_not_omitted  = NULL;
	no_grouping_attr = 0;
	no_aggregated_attr = 0;
	no_attr = 0;
	gt = new GroupTable();
	no_more_groups = false;
	no_groups = 0;
	packrows_omitted = 0;
	packrows_part_omitted = 0;
	th_no = 0;
	tuple_left = NULL;
}

GroupByWrapper::~GroupByWrapper()
{
	for(int i = 0; i < no_attr; i++) {
		if(virt_col[i])
			virt_col[i]->UnlockSourcePacks();
	}
	delete [] input_mode;
	delete [] is_lookup;
	delete [] attr_mapping;
	delete [] pack_not_omitted;
	delete [] dist_vals;
	delete [] virt_col;
	delete gt;
	delete tuple_left;
}

GroupByWrapper::GroupByWrapper(GroupByWrapper &sec) : distinct_watch(), m_conn(sec.m_conn)
{
	attrs_size = sec.attrs_size;
	virt_col = 		new VirtualColumn * [attrs_size];
	input_mode = 	new GBInputMode [attrs_size];
	is_lookup =		new bool [attrs_size];
	attr_mapping =	new int [attrs_size];			// output attr[j] <-> gt group[attr_mapping[j]]
	dist_vals = 	new _int64 [attrs_size];

	for(int i = 0; i < attrs_size; i++) {
		attr_mapping[i] = sec.attr_mapping[i];
		virt_col[i] = sec.virt_col[i];
		input_mode[i] = sec.input_mode[i];
		is_lookup[i] = sec.is_lookup[i];
		dist_vals[i] = sec.dist_vals[i];
	}
	no_grouping_attr = sec.no_grouping_attr;
	no_aggregated_attr = sec.no_aggregated_attr;
	no_more_groups = sec.no_more_groups;
	no_groups = sec.no_groups;
	no_attr = sec.no_attr;
	pack_not_omitted = new bool [no_attr];
	packrows_omitted = 0;
	packrows_part_omitted = 0;
	th_no = sec.th_no;
	for(int i = 0; i < no_attr; i++)
		pack_not_omitted[i] = sec.pack_not_omitted[i];

	gt = new GroupTable(*sec.gt);
	tuple_left = NULL;
	if(sec.tuple_left)
		tuple_left = new Filter(*sec.tuple_left);		// a copy of filter
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////

void GroupByWrapper::AddGroupingColumn( int attr_no, int orig_attr_no, TempTable::Attr &a, ushort max_size)
										// "a" must be a reference, because a.term will be taken as a pointer
{
	virt_col[attr_no]		= a.term.vc;

	assert(virt_col[attr_no]);

	// Not used for grouping columns:
	is_lookup[attr_no]		= false;
	dist_vals[attr_no]		= NULL_VALUE_64;
	input_mode[attr_no]		= GBIMODE_NOT_SET;

	attr_mapping[orig_attr_no] = attr_no;
	gt->AddGroupingColumn(virt_col[attr_no]);
	no_grouping_attr++;
	no_attr++;
}

void GroupByWrapper::AddAggregatedColumn( int orig_attr_no, TempTable::Attr &a,
										// "a" must be a reference, because a.term will be taken as a pointer
										_int64 max_no_vals,
										_int64 min_v, _int64 max_v, ushort max_size)

{
	//MEASURE_FET("GroupByWrapper::AddAggregatedColumn(...)");
	GT_Aggregation	ag_oper;
	AttributeType	ag_type = a.TypeName();			// original type, not the output one (it is important e.g. for AVG)
	int 			ag_size = max_size;
	int				ag_prec = a.Type().GetScale();
	bool			ag_distinct = a.distinct;
	int 			attr_no	= no_attr;			// i.e. add at the end (after all grouping cols and previous aggr.cols)
	DTCollation		ag_collation = a.GetCollation();

	virt_col[attr_no]		= a.term.vc;

	assert(virt_col[attr_no] || a.term.IsNull());		// the second possibility is count(*)

	is_lookup[attr_no]		= false;
	dist_vals[attr_no]		= max_no_vals;

	switch(a.mode) {
		case SUM:
			ag_oper = GT_SUM;
			ag_type = virt_col[attr_no]->TypeName();
			ag_prec = virt_col[attr_no]->Type().GetScale();
			break;
		case AVG:
			ag_oper = GT_AVG;
			ag_type = virt_col[attr_no]->TypeName();
			ag_prec = virt_col[attr_no]->Type().GetScale();
			break;
		case MIN:
			ag_oper = GT_MIN;
			break;
		case MAX:
			ag_oper = GT_MAX;
			break;
		case COUNT:
			if(a.term.IsNull() || (!ag_distinct && virt_col[attr_no]->IsConst())) {
				if(virt_col[attr_no] && virt_col[attr_no]->IsConst()) {
					MIIterator dummy(NULL);
					if(virt_col[attr_no]->IsNull(dummy)) {
						ag_oper = GT_COUNT_NOT_NULL;
						ag_type = virt_col[attr_no]->TypeName();
						ag_size = max_size;
					} else {
						virt_col[attr_no] = NULL;		// forget about constant in count(...), except null
						ag_oper = GT_COUNT;
					}
				} else {
					virt_col[attr_no] = NULL;		// forget about constant in count(...), except null
					ag_oper = GT_COUNT;
				}
			} else {
				ag_oper = GT_COUNT_NOT_NULL;
				ag_type = virt_col[attr_no]->TypeName();
				ag_size = max_size;
			}
			break;
		case LISTING:
			ag_oper = GT_LIST;
			break;
		case VAR_POP:
			ag_oper = GT_VAR_POP;
			ag_type = virt_col[attr_no]->TypeName();
			ag_prec = virt_col[attr_no]->Type().GetScale();
			break;
		case VAR_SAMP:
			ag_oper = GT_VAR_SAMP;
			ag_type = virt_col[attr_no]->TypeName();
			ag_prec = virt_col[attr_no]->Type().GetScale();
			break;
		case STD_POP:
			ag_oper = GT_STD_POP;
			ag_type = virt_col[attr_no]->TypeName();
			ag_prec = virt_col[attr_no]->Type().GetScale();
			break;
		case STD_SAMP:
			ag_oper = GT_STD_SAMP;
			ag_type = virt_col[attr_no]->TypeName();
			ag_prec = virt_col[attr_no]->Type().GetScale();
			break;
		case BIT_AND:
			ag_oper = GT_BIT_AND;
			break;
		case BIT_OR:
			ag_oper = GT_BIT_OR;
			break;
		case BIT_XOR:
			ag_oper = GT_BIT_XOR;
			break;
		default:
			throw NotImplementedRCException("Aggregation not implemented");
	}

	if(virt_col[attr_no] && virt_col[attr_no]->Type().IsLookup() && 
		!RequiresUTFConversions(virt_col[attr_no]->GetCollation()) &&
  		(ag_oper == GT_COUNT || ag_oper == GT_COUNT_NOT_NULL || ag_oper == GT_LIST)) {
 		// lookup for these operations may use codes
		ag_size = 4;						// integer
		ag_prec = 0;
		ag_type = RC_INT;
		is_lookup[attr_no] = true;
	}
	if(ag_oper == GT_COUNT)
		input_mode[attr_no] = GBIMODE_NO_VALUE;
	else
		input_mode[attr_no] = (ATI::IsStringType(virt_col[attr_no]->TypeName()) && 
								(!is_lookup[attr_no] || RequiresUTFConversions(virt_col[attr_no]->GetCollation()))
									? GBIMODE_AS_TEXT
									: GBIMODE_AS_INT64);

	gt->AddAggregatedColumn(virt_col[attr_no], ag_oper, ag_distinct, ag_type, ag_size, ag_prec, ag_collation);		// note: size will be automatically calculated for all numericals
	gt->AggregatedColumnStatistics(no_aggregated_attr, max_no_vals, min_v, max_v);
	attr_mapping[orig_attr_no] = attr_no;
	no_aggregated_attr++;
	no_attr++;
}

void GroupByWrapper::Initialize(_int64 upper_approx_of_groups, bool parallel_allowed)
{
	//MEASURE_FET("GroupByWrapper::Initialize(...)");
	gt->Initialize(upper_approx_of_groups, parallel_allowed);
	distinct_watch.Initialize(no_attr);
	for (int gr_a = 0; gr_a < no_attr; gr_a++) {
		if(gt->AttrDistinct(gr_a)) {
			distinct_watch.DeclareAsDistinct(gr_a);
		}
	}
	pack_not_omitted = new bool [no_attr];
}

void GroupByWrapper::Merge(GroupByWrapper &sec)
{
	_int64 old_groups = gt->GetNoOfGroups();
	gt->Merge(*(sec.gt), m_conn);
	if(tuple_left)
		tuple_left->And(*(sec.tuple_left));
	packrows_omitted += sec.packrows_omitted;
	packrows_part_omitted += sec.packrows_part_omitted;
	no_groups += gt->GetNoOfGroups() - old_groups;		// note that no_groups may be different than gt->..., because it is global
}

bool GroupByWrapper::AggregatePackInOneGroup( int attr_no, MIIterator &mit, _int64 uniform_pos, _int64 rows_in_pack, _int64 factor)
{
	bool no_omitted = (rows_in_pack == mit.GetPackSizeLeft());
	bool aggregated_roughly = false;
	if(virt_col[attr_no] && virt_col[attr_no]->GetNoNulls(mit) == mit.GetPackSizeLeft() && gt->AttrAggregator(attr_no)->IgnoreNulls())
		return true;			// no operation needed - the pack is ignored
	if(!gt->AttrAggregator(attr_no)->PackAggregationDistinctIrrelevant() && gt->AttrDistinct(attr_no) && virt_col[attr_no]) {
		// Aggregated values for distinct cases (if possible)
		vector<_int64> val_list = virt_col[attr_no]->GetListOfDistinctValues(mit);
		if(val_list.size() == 0 || (val_list.size() > 1 && !no_omitted))
			return false;
		vector<_int64>::iterator val_it = val_list.begin();
		while(val_it != val_list.end()) {
			GDTResult res = gt->FindDistinctValue(attr_no, uniform_pos, *val_it);
			if(res == GDT_FULL)
				return false;			// no chance to optimize
			if(res == GDT_EXISTS)
				val_it = val_list.erase(val_it);
			else
				++val_it;
		}
		if(val_list.size() == 0)
			return true;				// no need to analyze pack - all values already found
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsSize())
			gt->AttrAggregator(attr_no)->SetAggregatePackNoObj(val_list.size());
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsNotNulls())
			gt->AttrAggregator(attr_no)->SetAggregatePackNotNulls(val_list.size());
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsSum()) {
			_int64 i_sum = 0;
			val_it = val_list.begin();
			while(val_it != val_list.end()) {
				i_sum += *val_it;			// not working for more than one double value
				++val_it;
			}
			gt->AttrAggregator(attr_no)->SetAggregatePackSum(i_sum, 1);		// no factor for distinct
		}
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsMin() && virt_col[attr_no]) {
			_int64 i_min = virt_col[attr_no]->GetMinInt64Exact(mit);
			if(i_min == NULL_VALUE_64)
				return false;				// aggregation no longer possible
			else
				gt->AttrAggregator(attr_no)->SetAggregatePackMin(i_min);
		}
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsMax() && virt_col[attr_no]) {
			_int64 i_max = virt_col[attr_no]->GetMaxInt64Exact(mit);
			if(i_max == NULL_VALUE_64)
				return false;				// aggregation no longer possible
			else
				gt->AttrAggregator(attr_no)->SetAggregatePackMax(i_max);
		}
		aggregated_roughly = gt->AggregatePack(attr_no, uniform_pos);		// check the aggregation basing on the above parameters
		if(aggregated_roughly) {
			// commit the added values
			val_it = val_list.begin();
			while(val_it != val_list.end()) {
				gt->AddDistinctValue(attr_no, uniform_pos, *val_it);
				++val_it;
			}
		}
	} else {
		// Aggregated values for non-distinct cases

		if(gt->AttrAggregator(attr_no)->FactorNeeded() && factor == NULL_VALUE_64)
			throw NotImplementedRCException("Aggregation overflow.");
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsSize())
			gt->AttrAggregator(attr_no)->SetAggregatePackNoObj(SafeMultiplication(rows_in_pack, factor));
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsNotNulls() && virt_col[attr_no]) {
			_int64 no_nulls	= virt_col[attr_no]->GetNoNulls(mit);
			if(no_nulls == NULL_VALUE_64)
				return false;				// aggregation no longer possible
			else
				gt->AttrAggregator(attr_no)->SetAggregatePackNotNulls(SafeMultiplication(rows_in_pack - no_nulls, factor));
		}
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsSum() && virt_col[attr_no]) {
			bool nonnegative = false;		// not used anyway
			_int64 i_sum = virt_col[attr_no]->GetSum(mit, nonnegative);
			if(i_sum == NULL_VALUE_64)
				return false;				// aggregation no longer possible
			else
				gt->AttrAggregator(attr_no)->SetAggregatePackSum(i_sum, factor);
		}
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsMin() && virt_col[attr_no]) {
			_int64 i_min = virt_col[attr_no]->GetMinInt64Exact(mit);
			if(i_min == NULL_VALUE_64)
				return false;				// aggregation no longer possible
			else
				gt->AttrAggregator(attr_no)->SetAggregatePackMin(i_min);
		}
		if(gt->AttrAggregator(attr_no)->PackAggregationNeedsMax() && virt_col[attr_no]) {
			_int64 i_max = virt_col[attr_no]->GetMaxInt64Exact(mit);
			if(i_max == NULL_VALUE_64)
				return false;				// aggregation no longer possible
			else
				gt->AttrAggregator(attr_no)->SetAggregatePackMax(i_max);
		}
		aggregated_roughly = gt->AggregatePack(attr_no, uniform_pos);		// check the aggregation basing on the above parameters
	}
	return aggregated_roughly;
}

bool GroupByWrapper::AddPackIfUniform(int attr_no, MIIterator &mit)
{
	if(virt_col[attr_no] && virt_col[attr_no]->GetPackOntologicalStatus(mit) == UNIFORM && !mit.NullsPossibleInPack()) {
		// Put constant values for the grouping vector (will not be changed for this pack)
		gt->PutUniformGroupingValue(attr_no, mit, th_no);
		return true;
	}
	return false;
}


void GroupByWrapper::AddAllGroupingConstants(MIIterator &mit)
{
	for(int attr_no = 0; attr_no < no_grouping_attr; attr_no++)
		if(virt_col[attr_no] && virt_col[attr_no]->IsConst()) {
			PutGroupingValue(attr_no, mit);
		}
}

void GroupByWrapper::AddAllAggregatedConstants(MIIterator &mit)
{
	for(int attr_no = no_grouping_attr; attr_no < no_attr; attr_no++)
		if(virt_col[attr_no] && virt_col[attr_no]->IsConst()) {
			if(mit.NoTuples() > 0 || gt->AttrOper(attr_no) == GT_LIST) {
				if(!(gt->AttrOper(attr_no) == GT_COUNT && virt_col[attr_no]->IsNull(mit)))	// else left as 0
					PutAggregatedValue(attr_no, 0, mit);
			} else
				PutAggregatedNull(attr_no, 0);
		}
}

void GroupByWrapper::AddAllCountStar(_int64 row, MIIterator &mit, _int64 val)			// set all count(*) values
{
	for(int gr_a = no_grouping_attr; gr_a < no_attr; gr_a++) {
		if((virt_col[gr_a] == NULL || virt_col[gr_a]->IsConst()) &&
			gt->AttrOper(gr_a) == GT_COUNT && !gt->AttrDistinct(gr_a)) {
				if(virt_col[gr_a] && virt_col[gr_a]->IsNull(mit))
					PutAggregatedValueForCount(gr_a, row, 0);
				else
					PutAggregatedValueForCount(gr_a, row, val);		// note: mit.NoTuples() may be 0 in some not-used cases
		}
	}
}

bool GroupByWrapper::AttrMayBeUpdatedByPack(int i, MIIterator &mit)		// false, if pack is out of (grouping) scope
{
	return gt->AttrMayBeUpdatedByPack(i, mit);
}


bool GroupByWrapper::PackWillNotUpdateAggregation(int i, MIIterator &mit)	// false, if counters can be changed
{
	//MEASURE_FET("GroupByWrapper::PackWillNotUpdateAggregation(...)");
	assert(input_mode[i] != GBIMODE_NOT_SET);
	if(((is_lookup[i] || input_mode[i] == GBIMODE_AS_TEXT) && 
		(gt->AttrOper(i) == GT_MIN || gt->AttrOper(i) == GT_MAX))
		|| virt_col[i] == NULL)
		return false;

	// Optimization: do not recalculate statistics if there is too much groups
	if(gt->GetNoOfGroups() > 1024)
		return false;

	// Statistics of aggregator:
	gt->UpdateAttrStats(i);			// Warning: slow if there is a lot of groups involved

	// Statistics of data pack:
	if(virt_col[i]->GetNoNulls(mit) == mit.GetPackSizeLeft())
		return true;				// nulls only - omit pack

	if(gt->AttrAggregator(i)->PackAggregationNeedsMin()) {
		_int64 i_min = virt_col[i]->GetMinInt64(mit);
		gt->AttrAggregator(i)->SetAggregatePackMin(i_min);
	}
	if(gt->AttrAggregator(i)->PackAggregationNeedsMax()) {
		_int64 i_max = virt_col[i]->GetMaxInt64(mit);
		gt->AttrAggregator(i)->SetAggregatePackMax(i_max);
	}

	// Check the possibility of omitting pack
	return gt->AttrAggregator(i)->PackCannotChangeAggregation();		// true => omit
}

bool GroupByWrapper::DataWillNotUpdateAggregation(int i)	// false, if counters can be changed
{
	// Identical with PackWillNot...(), but calculated for global statistics
	assert(input_mode[i] != GBIMODE_NOT_SET);
	if(((is_lookup[i] || input_mode[i] == GBIMODE_AS_TEXT) && 
		(gt->AttrOper(i) == GT_MIN || gt->AttrOper(i) == GT_MAX))
		|| virt_col[i] == NULL)
		return false;

	// Optimization: do not recalculate statistics if there is too much groups
	if(gt->GetNoOfGroups() > 1024)
		return false;

	// Statistics of aggregator:
	gt->UpdateAttrStats(i);			// Warning: slow if there is a lot of groups involved

	// Statistics of the whole data:
	if(virt_col[i]->RoughNullsOnly())
		return true;				// nulls only - omit pack

	if(gt->AttrAggregator(i)->PackAggregationNeedsMin()) {
		_int64 i_min = virt_col[i]->RoughMin();
		gt->AttrAggregator(i)->SetAggregatePackMin(i_min);
	}
	if(gt->AttrAggregator(i)->PackAggregationNeedsMax()) {
		_int64 i_max = virt_col[i]->RoughMax();
		gt->AttrAggregator(i)->SetAggregatePackMax(i_max);
	}

	// Check the possibility of omitting the rest of packs
	return gt->AttrAggregator(i)->PackCannotChangeAggregation();		// true => omit
}

bool GroupByWrapper::PutAggregatedValueForCount(int gr_a, _int64 pos, _int64 factor)
{
	assert(gt->AttrOper(gr_a) == GT_COUNT || gt->AttrOper(gr_a) == GT_COUNT_NOT_NULL);
	return gt->PutAggregatedValue(gr_a, pos, factor);
}

bool GroupByWrapper::PutAggregatedNull(int gr_a, _int64 pos)
{
	assert(input_mode[gr_a] != GBIMODE_NOT_SET);
	return gt->PutAggregatedNull(gr_a, pos, (input_mode[gr_a] == GBIMODE_AS_TEXT));
	return false;
}

bool GroupByWrapper::PutAggregatedValue(int gr_a, _int64 pos, MIIterator &mit, _int64 factor)
{
	assert(input_mode[gr_a] != GBIMODE_NOT_SET);
	if(input_mode[gr_a] == GBIMODE_NO_VALUE)
		return gt->PutAggregatedValue(gr_a, pos, factor);
	return gt->PutAggregatedValue(gr_a, pos, mit, factor, (input_mode[gr_a] == GBIMODE_AS_TEXT));
}

RCBString GroupByWrapper::GetValueT(int col, _int64 row)
{
	if(is_lookup[col]) {
		_int64 v = GetValue64(col, row);	// lookup code
		return virt_col[col]->DecodeValue_S(v);
	}
	return gt->GetValueT(col, row);
}

void GroupByWrapper::FillDimsUsed(DimensionVector &dims)		// set true on all dimensions used
{
	for(int i = 0; i < no_attr; i++) {
		if(virt_col[i])
			virt_col[i]->MarkUsedDims(dims);
	}
}

_int64 GroupByWrapper::ApproxDistinctVals(int gr_a, MultiIndex *mind)
{
	if(dist_vals[gr_a] == NULL_VALUE_64)
		dist_vals[gr_a] = virt_col[gr_a]->GetApproxDistVals(true);		// incl. nulls, because they may define a group
	return dist_vals[gr_a];
}

////////////////////////////////////////////////////////////////////

void GroupByWrapper::DistinctlyOmitted(int attr, _int64 obj)
{
	distinct_watch.SetAsOmitted(attr, obj);
	gt->AddCurrentValueToCache(attr, distinct_watch.gd_cache[attr]);
	if(distinct_watch.gd_cache[attr].NextWrite() == false)
		throw OutOfMemoryRCException();										// check limitations in GroupDistinctCache::Initialize
}

void GroupByWrapper::RewindDistinctBuffers()
{
	gt->ClearDistinct();
	for (int i = 0; i < no_attr; i++)
		if(DistinctAggr(i))
			distinct_watch.gd_cache[i].Rewind();
}

void GroupByWrapper::Clear()								// reset all contents of the grouping table and statistics
{
	packrows_omitted = 0;
	packrows_part_omitted = 0;
	no_groups = 0;
	gt->ClearAll();
}

void GroupByWrapper::ClearDistinctBuffers()
{
	gt->ClearDistinct();
	for (int i = 0; i < no_attr; i++)
		if(DistinctAggr(i))
			distinct_watch.gd_cache[i].Reset();
}

bool GroupByWrapper::PutCachedValue(int gr_a)							// current value from distinct cache
{
	assert(input_mode[gr_a] != GBIMODE_NOT_SET);
	bool added = gt->PutCachedValue(gr_a, distinct_watch.gd_cache[gr_a], (input_mode[gr_a] == GBIMODE_AS_TEXT));
	if(!added)
		distinct_watch.gd_cache[gr_a].MarkCurrentAsPreserved();
	return added;
}

bool GroupByWrapper::CacheValid(int gr_a)								// true if there is a value cached for current row
{
	return (distinct_watch.gd_cache[gr_a].GetCurrentValue() != NULL);
}

void GroupByWrapper::OmitInCache(int gr_a, _int64 obj_to_omit)
{
	distinct_watch.gd_cache[gr_a].Omit(obj_to_omit);
}

void GroupByWrapper::UpdateDistinctCaches()								// take into account which values are already counted
{
	for (int i = 0; i < no_attr; i++)
		if(DistinctAggr(i))
			distinct_watch.gd_cache[i].SwitchToPreserved();				// ignored if n/a
}

bool GroupByWrapper::IsCountOnly(int gr_a)				// true, if an attribute is count(*)/count(const), or if there is no columns except constants and count (when no attr specified)
{
	if(gr_a != -1) {
		return 	(virt_col[gr_a] == NULL ||	virt_col[gr_a]->IsConst()) &&
				gt->AttrOper(gr_a) == GT_COUNT && !gt->AttrDistinct(gr_a);
	}
	bool count_found = false;
	for(int i = 0; i < no_attr; i++) {			// function should return true for e.g.: "SELECT 1, 2, 'ala', COUNT(*), 5, COUNT(4) FROM ..."
		if(gt->AttrOper(i) == GT_COUNT)
			count_found = true;
		if(!((virt_col[i] == NULL || virt_col[i]->IsConst()) 
				&&	gt->AttrOper(i) == GT_COUNT && !gt->AttrDistinct(i))	// count(*) or count(const)
			&&
			(gt->AttrOper(i) != GT_LIST || !virt_col[i]->IsConst()))		// a constant
		  return false;

	}
	return count_found;	
}

void GroupByWrapper::InitTupleLeft(_int64 n)
{ 
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT (tuple_left == NULL); 
	tuple_left = new Filter(n);
	tuple_left->Set();
}

bool GroupByWrapper::AnyTuplesLeft(_int64 from, _int64 to)
{
	if(tuple_left == NULL)
		return true;
	return !tuple_left->IsEmptyBetween(from, to);
}

_int64 GroupByWrapper::TuplesLeftBetween(_int64 from, _int64 to)
{
	if(tuple_left == NULL)
		return to - from + 1;
	return tuple_left->NoOnesBetween(from, to);
}


/////////////////////////////////////////////////////////////////////

void GroupByWrapper::LockPack(int i, MIIterator &mit)
{
	if(ColumnNotOmitted(i) && virt_col[i])
#ifndef __BH_COMMUNITY__
		virt_col[i]->LockSourcePacks(mit, th_no);
#else
		virt_col[i]->LockSourcePacks(mit);
#endif
}

void GroupByWrapper::ResetPackrow()
{
	for(int i = 0; i < no_attr; i++) {
		pack_not_omitted[i] = true;			// reset information about packrow
	}
}

/////////////////////////////////////////////////////////////////////
DistinctWrapper::DistinctWrapper()
{
	f = NULL;
	is_dist = NULL;
	no_attr = 0;
	no_obj = 0;
	gd_cache = NULL;
}

void DistinctWrapper::Initialize(int n_attr)
{
	no_attr = n_attr;
	f = new Filter * [no_attr];
	is_dist = new bool [no_attr];
	gd_cache = new GroupDistinctCache [no_attr];
	for(int i = 0; i < no_attr; i++ ) {
		f[i] = NULL;
		is_dist[i] = false;
	}
}

void DistinctWrapper::InitTuples(_int64 n_obj, GroupTable *gt)
{
	no_obj = n_obj;
	for(int i = 0; i < no_attr; i++ )
		if(is_dist[i]) {
			// for now - init cache with a number of objects decreased (will cache on disk if more is needed)
			_int64 limit_for_small_distincters = 200000;
			if(no_obj > limit_for_small_distincters)	// more objects - prepare for caching omitted objects
				gd_cache[i].SetNoObj(limit_for_small_distincters/3 + (no_obj - limit_for_small_distincters));
			else										// less objects - rarely omitted
				gd_cache[i].SetNoObj(no_obj/3 + 1);
			assert(f[i] == NULL);
			f[i] = new Filter(no_obj);
			gd_cache[i].SetWidth(gt->GetCachedWidth(i));
		}
}

void DistinctWrapper::DeclareAsDistinct(int attr)
{
	is_dist[attr] = true;
}

DistinctWrapper::~DistinctWrapper()
{
	for(int i = 0; i < no_attr; i++ ) {
		if(f[i])
			delete f[i];
	}
	delete [] gd_cache;
	delete [] f;
	delete [] is_dist;
}

bool DistinctWrapper::AnyOmitted()
{
	for(int i = 0; i < no_attr; i++ )
		if(f[i] && !(f[i]->IsEmpty()))
			return true;
	return false;
}
