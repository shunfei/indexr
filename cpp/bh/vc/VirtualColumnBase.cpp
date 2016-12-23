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

#include <assert.h>
#include "core/CompiledQuery.h"
#include "core/MysqlExpression.h"
#include "vc/VirtualColumnBase.h"
#include "InSetColumn.h"
#include "SubSelectColumn.h"
#include "core/RCAttr.h"

using namespace std;

VirtualColumnBase::VirtualColumnBase(ColumnType const& ct, MultiIndex* mind)
	: 	Column(ct), mind(mind), conn_info(ConnectionInfoOnTLS.Get()), first_eval(true), dim(-1)
{
	ResetLocalStatistics();
}

VirtualColumnBase::VirtualColumnBase(VirtualColumn const& vc)
	:	 Column(vc.ct), mind(vc.mind), conn_info(vc.conn_info), var_map(vc.var_map), params(vc.params), first_eval(true), dim(vc.dim)
{
	ResetLocalStatistics();
}

void VirtualColumnBase::AssignFromACopy(const VirtualColumn* vc)
{
	assert(this!=vc);
	mind = vc->mind;
	// conn_info = vc->conn_info; not copyable and anyway must stay the same- this session
	var_map = vc->var_map;
	params = vc->params;
	first_eval = true;
	dim = vc->dim;
	ct = vc->ct;
	ResetLocalStatistics();
}

void  VirtualColumnBase::MarkUsedDims(DimensionVector& dims_usage)
{
	for(var_maps_t::const_iterator it = var_map.begin(), end = var_map.end(); it != end; ++it)
		dims_usage[it->dim] = true;
}

VirtualColumnBase::dimensions_t VirtualColumnBase::GetDimensions()
{
	std::set<int> d;
	for(var_maps_t::const_iterator it = var_map.begin(), end = var_map.end(); it != end;++ it)
		d.insert(it->dim);
	return d;
}

_int64 VirtualColumnBase::NoTuples(void)
{
	if(mind == NULL)							// constant
		return 1;
	DimensionVector dims(mind->NoDimensions());
	MarkUsedDims(dims);
	return mind->NoTuples(dims);
}

bool VirtualColumnBase::IsConstExpression(MysqlExpression* expr, int temp_table_alias, const vector<int>* aliases)
{
	MysqlExpression::SetOfVars& vars = expr->GetVars(); 		// get all variables from complex term

	for(MysqlExpression::SetOfVars::iterator iter = vars.begin(); iter != vars.end(); iter++) {
		vector<int>::const_iterator ndx_it = find(aliases->begin(), aliases->end(), iter->tab);
		if(ndx_it != aliases->end())
			return false;
		else
			if(iter->tab == temp_table_alias)
				return false;
	}
	return true;
}

void VirtualColumnBase::SetMultiIndex(MultiIndex* m, JustATablePtr t)
{
	mind = m;
	if(t)
		for(vector<VarMap>::iterator iter = var_map.begin(); iter != var_map.end(); iter++) {
			(*iter).tab = t;
		}
}

void VirtualColumnBase::ResetLocalStatistics()
{
	//if(Type().IsFloat()) {
	//	vc_min_val = MINUS_INF_64;
	//	vc_max_val = PLUS_INF_64;
	//} else {
	//	vc_min_val = MINUS_INF_64;
	//	vc_max_val = PLUS_INF_64;
	//}
	vc_min_val = NULL_VALUE_64;
	vc_max_val = NULL_VALUE_64;
	vc_nulls_possible = true;
	vc_dist_vals = NULL_VALUE_64;
	vc_max_len = 65536;
	nulls_only = false;
}

void VirtualColumnBase::SetLocalMinMax(_int64 loc_min, _int64 loc_max)
{
	if(loc_min == NULL_VALUE_64)
		loc_min = MINUS_INF_64;
	if(loc_max == NULL_VALUE_64)
		loc_max = PLUS_INF_64;
	if(Type().IsFloat()) {
		if(vc_min_val == NULL_VALUE_64 || (loc_min != MINUS_INF_64 && (*(double*)&loc_min > *(double*)&vc_min_val || vc_min_val == MINUS_INF_64)))
			vc_min_val = loc_min;
		if(vc_max_val == NULL_VALUE_64 || (loc_max != PLUS_INF_64 && (*(double*)&loc_max < *(double*)&vc_max_val || vc_max_val == PLUS_INF_64)))
			vc_max_val = loc_max;
	} else {
		if(vc_min_val == NULL_VALUE_64 || loc_min > vc_min_val)
			vc_min_val = loc_min;
		if(vc_max_val == NULL_VALUE_64 || loc_max < vc_max_val)
			vc_max_val = loc_max;
	}
}

_int64 VirtualColumnBase::RoughMax()
{
	_int64 res = DoRoughMax();
	assert(res != NULL_VALUE_64);
	if(Type().IsFloat()) {
		if(*(double*)&res > *(double*)&vc_max_val && vc_max_val != PLUS_INF_64 && vc_max_val != NULL_VALUE_64)
			return vc_max_val;
	} else if(res > vc_max_val && vc_max_val != NULL_VALUE_64)
		return vc_max_val;
	return res;
}

_int64 VirtualColumnBase::RoughMin()
{
	_int64 res = DoRoughMin();
	assert(res != NULL_VALUE_64);
	if(Type().IsFloat()) {
		if(*(double*)&res < *(double*)&vc_min_val && vc_min_val != MINUS_INF_64 && vc_min_val != NULL_VALUE_64)
			return vc_min_val;
	} else if(res < vc_min_val && vc_min_val != NULL_VALUE_64)
		return vc_min_val;
	return res;
}

_int64 VirtualColumnBase::GetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind)
{
	_int64 res = DoGetApproxDistVals(incl_nulls, rough_mind);
	if(vc_dist_vals != NULL_VALUE_64) {
		_int64 local_res = vc_dist_vals;
		if(incl_nulls && NullsPossible())
			local_res++;
		if(res == NULL_VALUE_64 || res > local_res)
			res = local_res;
	}
	if(!Type().IsFloat() && vc_min_val > (MINUS_INF_64/3) && vc_max_val < (PLUS_INF_64/3)) {
		_int64 local_res = vc_max_val - vc_min_val + 1;
		if(incl_nulls && NullsPossible())
			local_res++;
		if(res == NULL_VALUE_64 || res > local_res)
			return local_res;
	}
	if(Type().IsFloat() && vc_min_val != NULL_VALUE_64 && vc_min_val == vc_max_val) {
		_int64 local_res = 1;
		if(incl_nulls && NullsPossible())
			local_res++;
		if(res == NULL_VALUE_64 || res > local_res)
			return local_res;
	}
	return res;
}

_int64 VirtualColumnBase::GetMaxInt64(const MIIterator& mit)
{
	_int64 res = DoGetMaxInt64(mit);
	assert(res != NULL_VALUE_64);
	if(Type().IsFloat()) {
		if(*(double*)&res > *(double*)&vc_max_val && vc_max_val != PLUS_INF_64 && vc_max_val != NULL_VALUE_64)
			return vc_max_val;
	} else if((vc_max_val != NULL_VALUE_64 && res > vc_max_val))
		return vc_max_val;
	return res;
}

_int64 VirtualColumnBase::GetMinInt64(const MIIterator& mit)
{
	_int64 res = DoGetMinInt64(mit);
	assert(res != NULL_VALUE_64);
	if(Type().IsFloat()) {
		if(*(double*)&res < *(double*)&vc_min_val && vc_min_val != MINUS_INF_64 && vc_min_val != NULL_VALUE_64)
			return vc_min_val;
	} else if((vc_min_val != NULL_VALUE_64 && res < vc_min_val))
		return vc_min_val;
	return res;
}

_int64 VirtualColumnBase::DoGetApproxSum(const MIIterator& mit, bool &nonnegative) 
{ 
	_int64 res = DoGetSum(mit, nonnegative);
	if(res != NULL_VALUE_64)
		return res;
	res = DoGetMaxInt64(mit);
	_int64 n = mit.GetPackSizeLeft();
	if(res == PLUS_INF_64 || n == NULL_VALUE_64)
		return NULL_VALUE_64;
	if(Type().IsFloat()) {
		double d = *(double*)&res;
		if(d <= 0)
			return 0;
		d *= n;
		if(d > -9.223372037e+18 && d < 9.223372037e+18)
			return *(_int64*)(&d);
	} else {
		if(res <= 0)
			return 0;
		if(res < 0x00007FFFFFFFFFFFll && n <= 65536)
			return n * res;
	}
	return NULL_VALUE_64;
}


_int64 VirtualColumnBase::DecodeValueAsDouble(_int64 code)
{
	if(Type().IsFloat())
		return code;				// no conversion
	double res = double(code) / PowOfTen(ct.GetScale());
	return *(_int64*)(&res);
}

RSValue VirtualColumnBase::DoRoughCheck(const MIIterator &mit, Descriptor &d)
{
	// default implementation
	if(d.op == O_FALSE)
		return RS_NONE;
	if(d.op == O_TRUE)
		return RS_ALL;
	bool nulls_possible = NullsPossible();
	if(d.op == O_IS_NULL || d.op == O_NOT_NULL) {
		if(GetNoNulls(mit) == mit.GetPackSizeLeft()) {		// nulls only
			if(d.op == O_IS_NULL)
				return RS_ALL;
			else
				return RS_NONE;
		}
		if(!nulls_possible) {
			if(d.op == O_IS_NULL)
				return RS_NONE;
			else
				return RS_ALL;
		}
		return RS_SOME;
	}
	RSValue res = RS_SOME;
	if(d.val1.vc == NULL || ((d.op == O_BETWEEN || d.op == O_NOT_BETWEEN) && d.val2.vc == NULL))
		return RS_SOME;			// irregular descriptor - cannot use VirtualColumn rough statistics
	// In all other situations: RS_NONE for nulls only
	if(GetNoNulls(mit) == mit.GetPackSizeLeft() ||
			(!d.val1.vc->IsMultival() && (d.val1.vc->GetNoNulls(mit) == mit.GetPackSizeLeft() ||
			(d.val2.vc && !d.val2.vc->IsMultival() && d.val2.vc->GetNoNulls(mit) == mit.GetPackSizeLeft()))))
		return RS_NONE;
	if(d.op == O_LIKE || d.op == O_NOT_LIKE || d.val1.vc->IsMultival() || (d.val2.vc && d.val2.vc->IsMultival()) )
		return RS_SOME;

	if(Type().IsString()) {
		if(RequiresUTFConversions(d.GetCollation()))
			return RS_SOME;
		RCBString vamin = GetMinString(mit);
		RCBString vamax = GetMaxString(mit);
		RCBString v1min = d.val1.vc->GetMinString(mit);
		RCBString v1max = d.val1.vc->GetMaxString(mit);
		RCBString v2min = ( d.val2.vc ? d.val2.vc->GetMinString(mit) : RCBString() );
		RCBString v2max = ( d.val2.vc ? d.val2.vc->GetMaxString(mit) : RCBString() );
		if(vamin.IsNull() || vamax.IsNull() || v1min.IsNull() || v1max.IsNull() ||
			(d.val2.vc && (v2min.IsNull() || v2max.IsNull())))
			return RS_SOME;
		// Note: rough string values are not suitable to ensuring equality. Only inequalities are processed.
		if(d.op == O_BETWEEN || d.op == O_NOT_BETWEEN) { // the second case will be negated soon
			if(vamin > v1max && vamax < v2min)
				res = RS_ALL;
			else if(vamin > v2max || vamax < v1min)
				res = RS_NONE;
		} else {
			if(vamin > v1max) {	// NOTE: only O_MORE, O_LESS and O_EQ are analyzed here, the rest of operators will be taken into account later (treated as negations)
				if(d.op == O_EQ  || d.op == O_NOT_EQ || d.op == O_LESS || d.op == O_MORE_EQ)
					res = RS_NONE;
				if(d.op == O_MORE || d.op == O_LESS_EQ)
					res = RS_ALL;
			}
			if(vamax < v1min) {
				if(d.op == O_EQ  || d.op == O_NOT_EQ || d.op == O_MORE || d.op == O_LESS_EQ) 
					res = RS_NONE;
				if(d.op == O_LESS || d.op == O_MORE_EQ) 
					res = RS_ALL;
			}
		}
	} else {
		if(!Type().IsNumComparable(d.val1.vc->Type()) || 
			((d.op == O_BETWEEN || d.op == O_NOT_BETWEEN) && !Type().IsNumComparable(d.val2.vc->Type())))
			return RS_SOME;			// non-numerical or non-comparable

		_int64 vamin = GetMinInt64(mit);
		_int64 vamax = GetMaxInt64(mit);
		_int64 v1min = d.val1.vc->GetMinInt64(mit);
		_int64 v1max = d.val1.vc->GetMaxInt64(mit);
		_int64 v2min = ( d.val2.vc ? d.val2.vc->GetMinInt64(mit) : NULL_VALUE_64 );
		_int64 v2max = ( d.val2.vc ? d.val2.vc->GetMaxInt64(mit) : NULL_VALUE_64 );
		if(vamin == NULL_VALUE_64 || vamax == NULL_VALUE_64 || v1min == NULL_VALUE_64 || v1max == NULL_VALUE_64 ||
			(d.val2.vc && (v2min == NULL_VALUE_64 || v2max == NULL_VALUE_64)))
			return RS_SOME;

		if(!Type().IsFloat()) {
			if(d.op == O_BETWEEN || d.op == O_NOT_BETWEEN) { // the second case will be negated soon
				if(vamin >= v1max && vamax <= v2min)
					res = RS_ALL;
				else if(vamin > v2max || vamax < v1min)
					res = RS_NONE;
			} else {
				if(vamin >= v1max) {	// NOTE: only O_MORE, O_LESS and O_EQ are analyzed here, the rest of operators will be taken into account later (treated as negations)
					if(d.op == O_LESS || d.op == O_MORE_EQ) // the second case will be negated soon
						res = RS_NONE;
					if(vamin > v1max) {
						if(d.op == O_EQ  || d.op == O_NOT_EQ)
							res = RS_NONE;
						if(d.op == O_MORE || d.op == O_LESS_EQ)
							res = RS_ALL;
					}
				}
				if(vamax <= v1min) {
					if(d.op == O_MORE || d.op == O_LESS_EQ)	// the second case will be negated soon
						res = RS_NONE;
					if(vamax < v1min) {
						if(d.op == O_EQ  || d.op == O_NOT_EQ) 
							res = RS_NONE;
						if(d.op == O_LESS || d.op == O_MORE_EQ) 
							res = RS_ALL;
					}
				}
				if(res == RS_SOME && (d.op == O_EQ  || d.op == O_NOT_EQ) && vamin == v1max && vamax == v1min)
					res = RS_ALL;
			}
		} else {
			double vamind = (vamin == MINUS_INF_64 ? MINUS_INF_DBL : *(double*)&vamin);
			double vamaxd = (vamax == PLUS_INF_64 ? PLUS_INF_DBL : *(double*)&vamax);
			double v1mind = (v1min == MINUS_INF_64 ? MINUS_INF_DBL : *(double*)&v1min);
			double v1maxd = (v1max == PLUS_INF_64 ? PLUS_INF_DBL : *(double*)&v1max);
			double v2mind = (v2min == MINUS_INF_64 ? MINUS_INF_DBL : *(double*)&v2min);
			double v2maxd = (v2max == PLUS_INF_64 ? PLUS_INF_DBL : *(double*)&v2max);

			if(d.op == O_BETWEEN || d.op == O_NOT_BETWEEN) { // the second case will be negated soon
				if(vamind >= v1maxd && vamaxd <= v2mind)
					res = RS_ALL;
				else if(vamind > v2maxd || vamaxd < v1mind)
					res = RS_NONE;
			} else {
				if(vamind >= v1maxd) {	// NOTE: only O_MORE, O_LESS and O_EQ are analyzed here, the rest of operators will be taken into account later (treated as negations)
					if(d.op == O_LESS || d.op == O_MORE_EQ) // the second case will be negated soon
						res = RS_NONE;
					if(vamind > v1maxd) {
						if(d.op == O_EQ  || d.op == O_NOT_EQ)
							res = RS_NONE;
						if(d.op == O_MORE || d.op == O_LESS_EQ)
							res = RS_ALL;
					}
				}
				if(vamaxd <= v1mind) {
					if(d.op == O_MORE || d.op == O_LESS_EQ)	// the second case will be negated soon
						res = RS_NONE;
					if(vamaxd < v1mind) {
						if(d.op == O_EQ  || d.op == O_NOT_EQ) 
							res = RS_NONE;
						if(d.op == O_LESS || d.op == O_MORE_EQ) 
							res = RS_ALL;
					}
				}
				if(res == RS_SOME && (d.op == O_EQ  || d.op == O_NOT_EQ) && vamind == v1maxd && vamaxd == v1mind)
					res = RS_ALL;
			}
		}
	}
	// reverse negations
	if(d.op == O_NOT_EQ || d.op == O_LESS_EQ || d.op == O_MORE_EQ || d.op == O_NOT_BETWEEN) {
		if(res == RS_ALL)
			res = RS_NONE;
		else if(res == RS_NONE)	
			res = RS_ALL;
	}
	// check nulls
	if(res == RS_ALL && (nulls_possible || d.val1.vc->NullsPossible() || (d.val2.vc && d.val2.vc->NullsPossible())))
		res = RS_SOME;
	return res;
}
