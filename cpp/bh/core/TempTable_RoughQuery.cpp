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

/////////////////////////////////////////////////////////////////////////////////////////////////////
// This is a part of TempTable implementation concerned with the rough query execution
/////////////////////////////////////////////////////////////////////////////////////////////////////

#include "TempTable.h"
#include "MIIterator.h"
#include "PackOrderer.h"
#include "RCEngine.h"
#include "vc/SingleColumn.h"

using namespace std;

////////////////////////////////////////////////////////////////////////////////////////////////////

void TempTable::RoughMaterialize(bool in_subq, ResultSender* sender, bool lazy)
{
	MEASURE_FET("Descriptor::RoughMaterialize(...)");
	if(materialized)
		return;
	filter.PrepareRoughMultiIndex();
	RoughAggregate(sender);		// Always use RoughAggregate (table distinct, order by, having, etc. are ignored)
}

////////////////////////////////////////////////////////////////////////////////////////////////////

_int64 UpdateMin(_int64 old_min, _int64 v, bool double_val)
{
	if(old_min == NULL_VALUE_64)
		return v;
	if(v == NULL_VALUE_64)
		return old_min;
	if(double_val) {
		if(*(double*)&old_min > *(double*)&v)
			return v;
	} else {
		if(old_min > v)
			return v;
	}
	return old_min;
}

_int64 UpdateMax(_int64 old_max, _int64 v, bool double_val)
{
	if(old_max == NULL_VALUE_64)
		return v;
	if(v == NULL_VALUE_64)
		return old_max;
	if(double_val) {
		if(*(double*)&old_max < *(double*)&v)
			return v;
	} else {
		if(old_max < v)
			return v;
	}
	return old_max;
}

void TempTable::RoughAggregateMinMax(VirtualColumn *vc, _int64 &min_val, _int64 &max_val)
{
	int dim = vc->GetDim();
	if(dim == -1) {
		min_val = vc->RoughMin();
		max_val = vc->RoughMax();
		return;
	}
	bool double_vals = vc->Type().IsFloat();
	MIIterator mit(filter.mind, dim);
	while(mit.IsValid()) {
		if(filter.rough_mind->GetPackStatus(dim, mit.GetCurPackrow(dim)) != RS_NONE && 
			vc->GetPackOntologicalStatus(mit) != NULLS_ONLY) {
				_int64 v = vc->GetMinInt64(mit);
				if(v == NULL_VALUE_64)
					min_val = MINUS_INF_64;
				else
					min_val = UpdateMin(min_val, v, double_vals);
				v = vc->GetMaxInt64(mit);
				if(v == NULL_VALUE_64)
					max_val = PLUS_INF_64;
				else
					max_val = UpdateMax(max_val, v, double_vals);
		}
		mit.NextPackrow();
	}
	if(max_val < min_val)
		min_val = max_val = NULL_VALUE_64;
}

void TempTable::RoughAggregateCount(DimensionVector &dims, _int64 &min_val, _int64 &max_val, bool group_by_present)
{
	for(int dim = 0; dim < dims.Size(); dim++) if(dims[dim]) {
		MIIterator mit(filter.mind, dim);
		_int64 loc_min = 0;
		_int64 loc_max = 0;
		while(mit.IsValid()) {
			RSValue res = filter.rough_mind->GetPackStatus(dim, mit.GetCurPackrow(dim));
			if(res != RS_NONE) {
				loc_max += mit.GetPackSizeLeft();
				if(!group_by_present && res == RS_ALL)
					loc_min += mit.GetPackSizeLeft();
			}
			mit.NextPackrow();
		}
		if(min_val == NULL_VALUE_64)
			min_val = loc_min;
		else {
			min_val = SafeMultiplication(min_val, loc_min);
			if(min_val == NULL_VALUE_64)
				min_val = 0;
		}
		if(max_val == NULL_VALUE_64)
			max_val = loc_max;
		else {
			max_val = SafeMultiplication(max_val, loc_max);
			if(max_val == NULL_VALUE_64)
				max_val = PLUS_INF_64;
		}
	}
}

void TempTable::RoughAggregateSum(VirtualColumn *vc, _int64 &min_val, _int64 &max_val, vector<Attr*> &group_by_attrs, bool nulls_only, bool distinct_present)
{
	int dim = vc->GetDim();
	bool double_vals = vc->Type().IsFloat();
	bool is_const = vc->IsConst();
	double min_val_d = (is_const ? 1 : 0);
	double max_val_d = (is_const ? 1 : 0);
	bool success = false;	// left as false for empty set
	bool empty_set = true;
	bool group_by_present = (group_by_attrs.size() > 0);
	if(!nulls_only && !is_const) {
		MIIterator mit(filter.mind, dim);
		mit.Rewind();
		while(mit.IsValid()) {
			RSValue res = filter.rough_mind->GetPackStatus(dim, mit.GetCurPackrow(dim));
			bool no_groups_or_uniform = true;		// false if there is a nontrivial grouping (more than one group possible)
			for(int j = 0; j < group_by_attrs.size(); j++) {
				VirtualColumn *vc_gb = group_by_attrs[j]->term.vc;
				if( vc_gb == NULL || vc_gb->GetNoNulls(mit) != 0 || 
					vc_gb->GetMinInt64(mit) == NULL_VALUE_64 || vc_gb->GetMinInt64(mit) != vc_gb->GetMaxInt64(mit))
					no_groups_or_uniform = false;			// leave it true only when we are sure the grouping columns are uniform for this packrow
			}
			if(res != RS_NONE && vc->GetPackOntologicalStatus(mit) != NULLS_ONLY) {
				empty_set = false;
				success = true;
				bool nonnegative = false;
				_int64 v = vc->GetSum(mit, nonnegative);
				if(no_groups_or_uniform && res == RS_ALL && !distinct_present) {
					if(v == NULL_VALUE_64) {	// unknown sum
						success = false;
						break;
					}
					if(!group_by_present) {
						if(double_vals) {
							min_val_d += *(double*)&v;
							max_val_d += *(double*)&v;
						} else {
							min_val_d += v;
							max_val_d += v;
						}
					} else {					// grouping by on uniform pack
						// cannot approximate minimum (unknown distribution of groups in other packs)
						if(double_vals) {
							max_val_d += *(double*)&v;
						} else {
							max_val_d += v;
						}
					}
				} else {
					if(nonnegative) {
						// nonnegative: minimum not changed, maximum limited by sum
						if(v == NULL_VALUE_64)
							v = vc->GetApproxSum(mit, nonnegative);
						if(double_vals)
							max_val_d += *(double*)&v;
						else
							max_val_d += v;
						if(v == NULL_VALUE_64) {
							success = false;
							break;
						}
					} else {
						// negative values possible: approximation by min/max
						v = vc->GetMinInt64(mit);
						if(v == MINUS_INF_64) {
							success = false;
							break;
						}
						if(v < 0) {
							if(double_vals)
								min_val_d += (*(double*)&v) * mit.GetPackSizeLeft();
							else
								min_val_d += double(v) * mit.GetPackSizeLeft();
						} // else it is actually nonnegative
						v = vc->GetMaxInt64(mit);
						if(v == PLUS_INF_64) {
							success = false;
							break;
						}
						if(double_vals)
							max_val_d += (*(double*)&v) * mit.GetPackSizeLeft();
						else
							max_val_d += double(v) * mit.GetPackSizeLeft();
					}
				}
			}
			mit.NextPackrow();
		}
	}
	if(is_const) {
		_int64 min_count = NULL_VALUE_64;
		_int64 max_count = NULL_VALUE_64;
		DimensionVector other_dims(filter.mind->NoDimensions());
		other_dims.SetAll();
		RoughAggregateCount(other_dims, min_count, max_count, group_by_present);
		MIIterator mit(filter.mind, dim);
		mit.Rewind();
		_int64 val = vc->GetValueInt64(mit);
		if(double_vals) {
			min_val_d = *(double*)&val * min_count;
			max_val_d = *(double*)&val * max_count;
			min_val = *(_int64*)&min_val_d;
			max_val = *(_int64*)&max_val_d;
		} else {
			min_val = val * min_count;
			max_val = val * max_count;
		}
		if((max_count == 0 && min_count == 0) || val == NULL_VALUE_64) {
			min_val = NULL_VALUE_64;
			max_val = NULL_VALUE_64;
		}
	} else if(success) {
		empty_set = false;
		if(filter.mind->NoDimensions() > 1) {
			_int64 min_count = NULL_VALUE_64;
			_int64 max_count = NULL_VALUE_64;
			DimensionVector other_dims(filter.mind->NoDimensions());
			other_dims.SetAll();
			other_dims[dim] = false;
			RoughAggregateCount(other_dims, min_count, max_count, group_by_present);
			min_val_d *= (min_val_d < 0 ? max_count : min_count);
			max_val_d *= (max_val_d < 0 ? min_count : max_count);
		}
		if(double_vals || (min_val_d > -9.223372037e+18 && max_val_d < 9.223372037e+18)) { // overflow check
			if(double_vals) {
				min_val = *(_int64*)&min_val_d;
				max_val = *(_int64*)&max_val_d;
			} else {
				min_val = _int64(min_val_d);
				max_val = _int64(max_val_d);
			}
		}
	} else if(empty_set) {
		min_val = NULL_VALUE_64;
		max_val = NULL_VALUE_64;
	} else {
		min_val = MINUS_INF_64;	// +-INF
		max_val = PLUS_INF_64;
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////

bool IsTempTableColumn(VirtualColumn* vc) 
{
	SingleColumn* sc = ((vc && vc->IsSingleColumn()) ? static_cast<SingleColumn*>(vc) : NULL); 
	return (sc && sc->IsTempTableColumn());
}

void TempTable::RoughAggregate(ResultSender* sender)
{
	MEASURE_FET("TempTable::RoughAggregate(...)");
	// Assumptions:
	// filter.mind			- multiindex with nontrivial contents, although not necessarily updated by conditions
	// filter.rough_mind	- rough multiindex with more up-to-date contents than mind, i.e. a packrow may exist in mind, but be marked as RS_NONE in rough_mind
	// To check a rough status of a packrow, use both mind and rough_mind.
	// The method does not change mind / rough_mind.

	// Interpretation of the result:
	// Minimal and maximal possible value for a given column, if executed as exact. NULL if not known (unable to determine).

	//filter.Prepare();
	bool group_by_present = false;
	bool aggregation_present = false;
	for(uint i = 0; i < attrs.size(); i++) {
		if(attrs[i]->mode != LISTING && attrs[i]->mode != GROUP_BY && attrs[i]->mode != DELAYED)
			aggregation_present = true;			// changing interpretation of result: statistics of possible value in any group
		if(attrs[i]->mode == GROUP_BY)
			group_by_present = true;			// changing interpretation of result: statistics of possible value in any group
	}

	///////////////////////////////////////////// Rough values for EXIST
	rough_is_empty = BHTRIBOOL_UNKNOWN;		// no_obj > 0 ? false - non-empty for sure, true - empty for sure
	if(!aggregation_present || group_by_present) {					// otherwise even empty multiindex may produce nonempty result - checked later
		rough_is_empty = false;
		for(int dim = 0; dim < filter.mind->NoDimensions(); dim++) {
			bool local_empty = true;
			bool local_some = true;					// true if no pack is full
			for(int pack = 0; pack < filter.rough_mind->NoPacks(dim); pack++) {
				RSValue res = filter.rough_mind->GetPackStatus(dim, pack);
				if(res != RS_NONE) {
					local_empty = false;
					if(rough_is_empty != false)
						break;
				}
				if(res == RS_ALL) {
					local_some = false;
					break;
				}
			}
			if(local_empty) {
				rough_is_empty = true;
				break;
			}
			if(local_some) 
				rough_is_empty = BHTRIBOOL_UNKNOWN;		// cannot be false any more
		}
	}
	if(!group_by_present && aggregation_present)
		rough_is_empty = false;

	CalculatePageSize(2);						// 2 rows in result

	if(rough_is_empty == true || (mode.top && mode.param2 == 0)) {		// empty or "limit 0"
		no_obj = 0;
		materialized = true;
		if(sender)
			sender->Send(this);
		return;
	}

	///////////////////////////////////////////// Rough sorting / limit
	if(!aggregation_present && !group_by_present && !mode.distinct && 
		mode.top && mode.param2 > -1 && filter.mind->NoDimensions() == 1) {
		_int64 local_limit = mode.param1 + mode.param2;
		if(order_by.size() > 0) {
			VirtualColumn *vc;
			vc = order_by[0].vc;
			bool asc = (order_by[0].dir == 0);			// ascending sorting, if needed
			if(!vc->Type().IsString() && !vc->Type().IsLookup() && vc->GetDim() == 0) {
				vector<PackOrderer> po(1);
				po[0].Init(vc, (asc ? PackOrderer::MaxAsc : PackOrderer::MinDesc), 	// start with best packs to possibly roughly exclude others
								filter.rough_mind->GetRSValueTable(0));
				DimensionVector loc_dims(1);
				loc_dims[0] = true;
				MIIterator mit(filter.mind, loc_dims, po);

				bool double_vals = vc->Type().IsFloat();
				_int64 cutoff_value = NULL_VALUE_64;
				_int64 certain_rows = 0;
				bool cutoff_is_null = false;		// true if all values up to limit are NULL for ascending
				while(mit.IsValid()) {
					RSValue res = filter.rough_mind->GetPackStatus(0, mit.GetCurPackrow(0));
					if(res == RS_ALL) {
						// Algorithm for ascending:
						// - cutoff value is the maximum of the first full data pack which hit the limit
						certain_rows += mit.GetPackSizeLeft();
						if(certain_rows >= local_limit) {
							cutoff_value = (asc ? vc->GetMaxInt64Exact(mit) : vc->GetMinInt64Exact(mit));
							if(asc && vc->GetNoNulls(mit) == mit.GetPackSizeLeft())
								cutoff_is_null = true;
							break;
						}
					}
					mit.NextPackrow();
				}
				if(cutoff_value != NULL_VALUE_64 || cutoff_is_null) {
					mit.Rewind();
					_int64 local_stat = NULL_VALUE_64;
					while(mit.IsValid()) {
						RSValue res = filter.rough_mind->GetPackStatus(0, mit.GetCurPackrow(0));
						if(res != RS_NONE) {
							bool omit = false;
							if(asc) {
								local_stat = vc->GetMinInt64(mit);		// omit if pack minimum is larger than cutoff
								if(!cutoff_is_null && local_stat != NULL_VALUE_64 &&
									(( double_vals && *(double*)&local_stat > *(double*)&cutoff_value) ||
									 (!double_vals && local_stat > cutoff_value)))
									 omit = true;
								if(cutoff_is_null && vc->GetNoNulls(mit) == 0)
									omit = true;
							} else {
								local_stat = vc->GetMaxInt64(mit);	
								if(local_stat != NULL_VALUE_64 &&
									(( double_vals && *(double*)&local_stat < *(double*)&cutoff_value) ||
									(!double_vals && local_stat < cutoff_value)))
									omit = true;
							}
							if(omit)
								filter.rough_mind->SetPackStatus(0, mit.GetCurPackrow(0), RS_NONE);
						}
						mit.NextPackrow();
					}
				}
			}
		} else {
			_int64 certain_rows = 0;
			bool omit_the_rest = false;
			MIIterator mit(filter.mind);
			while(mit.IsValid()) {
				if(omit_the_rest) {
					filter.rough_mind->SetPackStatus(0, mit.GetCurPackrow(0), RS_NONE);
				} else {
					RSValue res = filter.rough_mind->GetPackStatus(0, mit.GetCurPackrow(0));
					if(res == RS_ALL) {
						certain_rows += mit.GetPackSizeLeft();
						if(certain_rows >= local_limit)
							omit_the_rest = true;
					}
				}
				mit.NextPackrow();
			}
		}
	}

	///////////////////////////////////////////// Rough values for columns
	for(uint i = 0; i < attrs.size(); i++) {
		bool value_set = false;
		attrs[i]->CreateBuffer(2);

		VirtualColumn* vc = attrs[i]->term.vc;
		bool nulls_only = vc ? (vc->GetLocalNullsOnly() || vc->RoughNullsOnly()) : false;
		RCBString vals;
		bool double_vals = (vc != NULL && vc->Type().IsFloat());
		if(vc && vc->IsConst() && !vc->IsSubSelect() && attrs[i]->mode != SUM && attrs[i]->mode != COUNT && attrs[i]->mode != BIT_XOR) {
			if(attrs[i]->mode == STD_POP || attrs[i]->mode == VAR_POP || attrs[i]->mode == STD_SAMP || attrs[i]->mode == VAR_SAMP) {
				attrs[i]->SetValueInt64(0, 0);	// deviations for constants = 0
				attrs[i]->SetValueInt64(1, 0);
			} else {							// other rough values for constants: usually just these constants
				MIIterator mit(filter.mind);
				if(vc->IsNull(mit)) {
					attrs[i]->SetNull(0);
					attrs[i]->SetNull(1);
				} else if(attrs[i]->mode == AVG) {
					_int64 val = vc->GetValueInt64(mit);
					val = vc->DecodeValueAsDouble(val);
					attrs[i]->SetValueInt64(0, val);
					attrs[i]->SetValueInt64(1, val);
				} else {
					switch(attrs[i]->TypeName()) {
						case RC_STRING:
						case RC_VARCHAR:
						case RC_BIN:
						case RC_BYTE:
						case RC_VARBYTE:
							vc->GetValueString(vals, mit);
							attrs[i]->SetValueString(0, vals);
							attrs[i]->SetValueString(1, vals);
							break;
						default:
							attrs[i]->SetValueInt64(0, vc->GetValueInt64(mit));
							attrs[i]->SetValueInt64(1, vc->GetValueInt64(mit));
							break;
					}
				}
			}
			value_set = true;				
		} else {
			switch(attrs[i]->mode) {
				case LISTING:
				case GROUP_BY:			// Rough values of listed rows: min and max of possible packs
				case AVG:				// easy implementation of AVG: between min and max
					if(!vc->Type().IsString() && !vc->Type().IsLookup()) {
						_int64 min_val = NULL_VALUE_64;
						_int64 max_val = NULL_VALUE_64;
						if(!nulls_only)
							RoughAggregateMinMax(vc, min_val, max_val);
						if(!double_vals && ATI::IsRealType(attrs[i]->TypeName()) && !nulls_only && min_val != NULL_VALUE_64) { // decimal column, double result (e.g. for AVG)
							min_val = vc->DecodeValueAsDouble(min_val);
							max_val = vc->DecodeValueAsDouble(max_val);
						}
						attrs[i]->SetValueInt64(0, min_val);
						attrs[i]->SetValueInt64(1, max_val);
						value_set = true;				
					}
					break;
				case MIN:		// Rough min of MIN: minimum of all possible packs 
				case MAX:		// Rough min of MAX: maximum of actual_max(relevant) and minimum of min(suspect)
					// Rough max of MIN: minimum of actual_min(relevant) and maximum of max(suspect)
					if(!vc->Type().IsString() && !vc->Type().IsLookup() && vc->GetDim() != -1) {
						int dim = vc->GetDim();
						_int64 min_val = NULL_VALUE_64;
						_int64 max_val = NULL_VALUE_64;
						_int64 relevant_val = NULL_VALUE_64;
						bool is_min = (attrs[i]->mode == MIN);
						bool skip_counting = (IsTempTableColumn(vc) || SubqueryInFrom());
						if(!nulls_only) {
							MIIterator mit(filter.mind, dim);
							while(mit.IsValid()) {
								RSValue res = filter.rough_mind->GetPackStatus(dim, mit.GetCurPackrow(dim));
								if(res != RS_NONE && vc->GetPackOntologicalStatus(mit) != NULLS_ONLY) {
									min_val = UpdateMin(min_val, vc->GetMinInt64(mit), double_vals);
									max_val = UpdateMax(max_val, vc->GetMaxInt64(mit), double_vals);									
									if(!skip_counting && !group_by_present && res == RS_ALL) {									
										_int64 exact_val = is_min ? vc->GetMinInt64Exact(mit) 
											: vc->GetMaxInt64Exact(mit);
										if(exact_val != NULL_VALUE_64)
											relevant_val = is_min ? UpdateMin(relevant_val, exact_val, double_vals) 
											: UpdateMax(relevant_val, exact_val, double_vals);
									} 								
								}
								mit.NextPackrow();
							}
							if(relevant_val != NULL_VALUE_64) {
								if(is_min)
									max_val = UpdateMin(max_val, relevant_val, double_vals);	// take relevant_val, if smaller
								else
									min_val = UpdateMax(min_val, relevant_val, double_vals);	// take relevant_val, if larger
							}
							if(max_val < min_val)
								min_val = max_val = NULL_VALUE_64;

						}
						attrs[i]->SetValueInt64(0, min_val);
						attrs[i]->SetValueInt64(1, max_val);
						value_set = true;				
					}
					break;
				case COUNT: {
					_int64 min_val = NULL_VALUE_64;
					_int64 max_val = NULL_VALUE_64;
					DimensionVector dims(filter.mind->NoDimensions());	// initialized as empty
					bool skip_counting = (IsTempTableColumn(vc) || SubqueryInFrom());
					if(vc && !attrs[i]->distinct && !skip_counting) {		// COUNT(a)
						int dim = vc->GetDim();
						if(dim != -1) {
							dims[dim] = true;
							MIIterator mit(filter.mind, dim);
							min_val = 0;
							max_val = 0;
							if(!nulls_only) {
								while(mit.IsValid()) {
									RSValue res = filter.rough_mind->GetPackStatus(dim, mit.GetCurPackrow(dim));
									if(res != RS_NONE) {
										max_val += mit.GetPackSizeLeft();
										if(!group_by_present && res == RS_ALL) {
											_int64 no_nulls = vc->GetNoNulls(mit);
											if(no_nulls != NULL_VALUE_64) {
												min_val += mit.GetPackSizeLeft() - no_nulls;
												max_val -= no_nulls;
											}
										}
									}
									mit.NextPackrow();
								}
							}
						} else
							min_val = 0;
					} else if(vc && attrs[i]->distinct && !skip_counting) {	// COUNT(DISTINCT a)
						vc->MarkUsedDims(dims);
						MIIterator mit(filter.mind, dims);
						min_val = 0;
						max_val = vc->GetApproxDistVals(false, filter.rough_mind);	// Warning: mind used inside - may be more exact if rmind is also used

						// compare it with a rough COUNT(*)
						if(!nulls_only) {
							bool all_only = true;
							_int64 max_count_star = NULL_VALUE_64;
							_int64 min_count_star = 0;
							for(int dim = 0; dim < dims.Size(); dim++) if(dims[dim]) {
								MIIterator mit(filter.mind, dim);
								_int64 loc_max = 0;
								while(mit.IsValid()) {
									RSValue res = filter.rough_mind->GetPackStatus(dim, mit.GetCurPackrow(dim));
									if(res != RS_NONE)
										loc_max += mit.GetPackSizeLeft();
									if(res == RS_ALL && !group_by_present)
										min_count_star += mit.GetPackSizeLeft();
									if(res != RS_ALL)
										all_only = false;
									mit.NextPackrow();
								}
								max_count_star = (max_count_star == NULL_VALUE_64 ? loc_max : SafeMultiplication(max_count_star, loc_max));
								if(max_count_star == NULL_VALUE_64)
									max_count_star = PLUS_INF_64;
								if(max_count_star > max_val)		// approx. distinct vals reached - stop execution
									break;
							}
							if(max_count_star < max_val)
								max_val = max_count_star;
							if(vc->IsDistinct() && dims.Size() == 1)
								min_val = min_count_star;
							if(all_only) {
								_int64 exact_dist = vc->GetExactDistVals();
								if(exact_dist != NULL_VALUE_64)
									min_val = max_val = exact_dist;
							}
						} else
							max_val = 0;
					}

					// COUNT(*) or dimensions not covered by the above
					dims.Complement();		// all unused dimensions
					if(!skip_counting)
						RoughAggregateCount(dims, min_val, max_val, group_by_present);
					else {
						min_val = 0;
						max_val = PLUS_INF_64;
					}

					attrs[i]->SetValueInt64(0, min_val);
					attrs[i]->SetValueInt64(1, max_val);
					value_set = true;				
					break;
				}
				case SUM:
					// Rough min of SUM: positive only: sum of sums of relevant
					// Rough max of SUM: positive only: sum of sums of suspect
					if((!vc->Type().IsString() && !vc->Type().IsLookup() && vc->GetDim() != -1) || vc->IsConst()) {
						if(IsTempTableColumn(vc) || SubqueryInFrom()) {
							bool nonnegative = false;
							MIIterator mit(filter.mind, vc->GetDim());
							vc->GetSum(mit, nonnegative);
							attrs[i]->SetValueInt64(0, nonnegative ? 0 : MINUS_INF_64);	// +-INF
							attrs[i]->SetValueInt64(1, PLUS_INF_64);
							value_set = true;
							break;
						}
						_int64 min_val = NULL_VALUE_64;
						_int64 max_val = NULL_VALUE_64;
						vector<Attr*> group_by_attrs;
						for(int j = 0; j < attrs.size(); j++)
							if(attrs[j]->mode == GROUP_BY)
								group_by_attrs.push_back(attrs[j]);
						RoughAggregateSum(vc, min_val, max_val, group_by_attrs, nulls_only, attrs[i]->distinct);
						if(min_val == NULL_VALUE_64 || max_val == NULL_VALUE_64) {
							attrs[i]->SetNull(0);		// NULL as a result of empty set or non-computable sum
							attrs[i]->SetNull(1);
						} else {
							attrs[i]->SetValueInt64(0, min_val);
							attrs[i]->SetValueInt64(1, max_val);
						}
						value_set = true;
					}
					break;
				case BIT_AND:
					if(nulls_only) {
						attrs[i]->SetValueInt64(0, -1);				// unsigned 64-bit result
						attrs[i]->SetValueInt64(1, -1);
					} else {
						attrs[i]->SetValueInt64(0, 0);				// unsigned 64-bit result
						attrs[i]->SetValueInt64(1, -1);
					}
					value_set = true;				
					break;
				case BIT_OR:
				case BIT_XOR:
					if(nulls_only){
						attrs[i]->SetValueInt64(0, 0);				// unsigned 64-bit result
						attrs[i]->SetValueInt64(1, 0);
					} else {
						attrs[i]->SetValueInt64(0, 0);				// unsigned 64-bit result
						attrs[i]->SetValueInt64(1, -1);
					}
					value_set = true;				
					break;
				case STD_POP:
				case STD_SAMP:
				case VAR_POP:
				case VAR_SAMP:
					if(!vc->Type().IsString() && !vc->Type().IsLookup() && vc->GetDim() != -1) {
						_int64 min_val = NULL_VALUE_64;
						_int64 max_val = NULL_VALUE_64;
						if(!nulls_only) {
							RoughAggregateMinMax(vc, min_val, max_val);
							if(min_val != NULL_VALUE_64 && max_val != NULL_VALUE_64) {
								min_val = vc->DecodeValueAsDouble(min_val);		// decode to double
								max_val = vc->DecodeValueAsDouble(max_val);
								double diff = *(double*)(&max_val) - *(double*)(&min_val);
								if(attrs[i]->mode == VAR_POP || attrs[i]->mode == VAR_SAMP)
									diff *= diff;
								attrs[i]->SetValueInt64(0, 0);
								attrs[i]->SetValueInt64(1, *(_int64*)(&diff));	// May be approximated better. For now Var <= difference^2
								value_set = true;				
							} else {
								attrs[i]->SetValueInt64(0, NULL_VALUE_64);
								attrs[i]->SetValueInt64(1, NULL_VALUE_64);	
								value_set = true;				
							}
						} else {
							attrs[i]->SetValueInt64(0, NULL_VALUE_64);
							attrs[i]->SetValueInt64(1, NULL_VALUE_64);
							value_set = true;				
						}
					}
					break;
			} 
		}

		// Fill output buffers, if not filled yet
		if(!value_set) {
			if(ATI::IsStringType(attrs[i]->TypeName())) {
				attrs[i]->SetValueString(0, RCBString("*"));
				attrs[i]->SetValueString(1, RCBString("*"));
			} else {
				attrs[i]->SetMinusInf(0);
				attrs[i]->SetPlusInf(1);
			}
		}
	}
	no_obj = 2;
	materialized = true;
	if(sender)
		sender->Send(this);
}

void TempTable::RoughUnion(TempTable* t, ResultSender* sender)
{
	if(!t) {
		this->RoughMaterialize(false);
		if(sender && !this->IsSent())
			sender->Send(this);
		return;
	}
	assert( NoDisplaybleAttrs() == t->NoDisplaybleAttrs() );
	if(NoDisplaybleAttrs() != t->NoDisplaybleAttrs())
		throw NotImplementedRCException("UNION of tables with different number of columns.");
	if(this->IsParametrized() || t->IsParametrized())
		throw NotImplementedRCException("Materialize: not implemented union of parameterized queries.");
	this->RoughMaterialize(false);
	t->RoughMaterialize(false);
	if(sender) {
		if(!this->IsSent())
			sender->Send(this);
		if(!t->IsSent())
			sender->Send(t);
		return;
	}
	for(uint i = 0; i < attrs.size(); i++)
		if(!attrs[i]->buffer)
			attrs[i]->CreateBuffer(2);

	_int64 pos = NoObj();
	no_obj += 2;
	for(int i = 0; i < attrs.size(); i++) {
		if(IsDisplayAttr(i) && !ATI::IsStringType(attrs[i]->TypeName())) {
			_int64 v;
			if(GetColumnType(i).IsFloat()) {
				RCNum rc = (RCNum)t->GetTable(0, i);
				v = rc.ToReal().Value();
			} else if(t->GetColumnType(i).IsFloat()) {
				RCNum rc = (RCNum)t->GetTable(0, i);
				v =  (_int64)((double)rc * PowOfTen(GetAttrScale(i)));
			} else {
				double multiplier = PowOfTen(GetAttrScale(i) - t->GetAttrScale(i));
				v = (_int64)(t->GetTable64(0, i) * multiplier);
			}
			attrs[i]->SetValueInt64(pos, v); 
			if(GetColumnType(i).IsFloat()) {
				RCNum rc = (RCNum)t->GetTable(1, i);
				v = rc.ToReal().Value();
			} else if(t->GetColumnType(i).IsFloat()) {
				RCNum rc = (RCNum)t->GetTable(1, i);
				v =  (_int64)((double)rc * PowOfTen(GetAttrScale(i)));
			} else {
				double multiplier = PowOfTen(GetAttrScale(i) - t->GetAttrScale(i));
				v = (_int64)(t->GetTable64(1, i) * multiplier);
			}
			attrs[i]->SetValueInt64(pos + 1, v); 
		}
	}
}

