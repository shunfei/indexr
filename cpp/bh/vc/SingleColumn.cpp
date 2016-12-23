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
#include "SingleColumn.h"
#include "core/MysqlExpression.h"
#include "core/RCAttr.h"

using namespace std;

SingleColumn::SingleColumn(PhysicalColumn* col, MultiIndex* mind, int alias, int col_no, JustATable* source_table, int d)
	: VirtualColumn(col->Type(), mind), col(col)
{
	dim = d;
	VarMap vm(VarID(alias,col_no), source_table, dim);
	var_map.push_back(vm);
}

SingleColumn::SingleColumn(const SingleColumn& sc) : VirtualColumn(sc), col(sc.col) 
{
	dim = sc.dim;
	var_map = sc.var_map;
}

SingleColumn::~SingleColumn()
{
}

void SingleColumn::TranslateSourceColumns(map<PhysicalColumn *, PhysicalColumn *> &attr_translation)
{
	if(attr_translation.find(col) != attr_translation.end())
		col = attr_translation[col];
}

double SingleColumn::DoGetValueDouble(const MIIterator& mit) {
	double val = 0;
	if(col->IsNull(mit[dim])) {
		return NULL_VALUE_D;
	} else if(ATI::IsIntegerType(TypeName())){
		val = double(col->GetValueInt64(mit[dim]));
	} else if(ATI::IsFixedNumericType(TypeName())) {
		_int64 v = col->GetValueInt64(mit[dim]);
		val = ((double) v) / PowOfTen(ct.GetScale());
	} else if(ATI::IsRealType(TypeName())) {
		union { double d; _int64 i;} u;
		u.i = col->GetValueInt64(mit[dim]);
		val = u.d;
	} else if(ATI::IsDateTimeType(TypeName())) {
		_int64 v = col->GetValueInt64(mit[dim]);
		RCDateTime vd(v, TypeName());	// 274886765314048  ->  2000-01-01
		_int64 vd_conv = 0;
		vd.ToInt64(vd_conv);			// 2000-01-01  ->  20000101
		val = (double)vd_conv;
	} else if(ATI::IsStringType(TypeName())) {
		RCBString vrcbs;
		col->GetValueString(mit[dim], vrcbs);
		char *vs = new char [vrcbs.len + 1];
		memcpy(vs, vrcbs.Value(), vrcbs.len);
		vs[vrcbs.len] = '\0';
		val = atof(vs);
		delete [] vs;
	} else
		assert(0 && "conversion to double not implemented");
	return val;
}

RCValueObject SingleColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num) {
	return col->GetValue(mit[dim], lookup_to_num);
}

_int64 SingleColumn::DoGetSum(const MIIterator &mit, bool &nonnegative)
{
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!ATI::IsStringType(TypeName()));
	if(mit.WholePack(dim) && mit.GetCurPackrow(dim) >= 0)
		return col->GetSum(mit.GetCurPackrow(dim), nonnegative);
	nonnegative = false;
	return NULL_VALUE_64;
}

_int64 SingleColumn::DoGetApproxSum(const MIIterator &mit, bool &nonnegative)
{
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!ATI::IsStringType(TypeName()));
	if(mit.GetCurPackrow(dim) >= 0)
		return col->GetSum(mit.GetCurPackrow(dim), nonnegative);
	nonnegative = false;
	return NULL_VALUE_64;
}

_int64 SingleColumn::DoGetMinInt64(const MIIterator &mit) 
{
	if(mit.GetCurPackrow(dim) >= 0 && !(col->Type().IsString() && !col->Type().IsLookup()))
		return col->GetMinInt64(mit.GetCurPackrow(dim));
	else
		return MINUS_INF_64;
}

_int64 SingleColumn::DoGetMaxInt64(const MIIterator &mit) 
{
	if(mit.GetCurPackrow(dim) >= 0 && !(col->Type().IsString() && !col->Type().IsLookup()))
		return col->GetMaxInt64(mit.GetCurPackrow(dim));
	else
		return PLUS_INF_64;
}

_int64 SingleColumn::DoGetMinInt64Exact(const MIIterator &mit) 
{
	if(mit.GetCurPackrow(dim) >= 0 && mit.WholePack(dim)  && !(col->Type().IsString() && !col->Type().IsLookup())) {
		_int64 val = col->GetMinInt64(mit.GetCurPackrow(dim));
		if(val != MINUS_INF_64)
			return val;
	} 
	return NULL_VALUE_64;
}

_int64 SingleColumn::DoGetMaxInt64Exact(const MIIterator &mit) 
{
	if(mit.GetCurPackrow(dim) >= 0 && mit.WholePack(dim)  && !(col->Type().IsString() && !col->Type().IsLookup())) {
		_int64 val = col->GetMaxInt64(mit.GetCurPackrow(dim));
		if(val != PLUS_INF_64)
			return val;
	}
	return NULL_VALUE_64;
}


RCBString SingleColumn::DoGetMinString(const MIIterator &mit) 
{
	return col->GetMinString(mit.GetCurPackrow(dim));
}

RCBString SingleColumn::DoGetMaxString(const MIIterator &mit) 
{
	return col->GetMaxString(mit.GetCurPackrow(dim));
}

_int64 SingleColumn::DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind)
{
	if(rough_mind)
		return col->ApproxDistinctVals(incl_nulls, mind->GetFilter(dim), rough_mind->GetRSValueTable(dim), true);
	return col->ApproxDistinctVals(incl_nulls, mind->GetFilter(dim), NULL, mind->NullsExist(dim));
}

_int64 SingleColumn::GetExactDistVals()
{
	return col->ExactDistinctVals(mind->GetFilter(dim));
}

double SingleColumn::RoughSelectivity()
{
	return col->RoughSelectivity();
}

vector<_int64> SingleColumn::GetListOfDistinctValues(MIIterator const& mit)
{
	int pack = mit.GetCurPackrow(dim);
	if(pack < 0 ||
		(!mit.WholePack(dim) && col->GetPackOntologicalStatus(pack) != UNIFORM)) {
		vector<_int64> empty;
		return empty;
	}
	return col->GetListOfDistinctValuesInPack(pack);
}

bool SingleColumn::TryToMerge(Descriptor &d1, Descriptor &d2)
{
	return col->TryToMerge(d1, d2);
}


ushort SingleColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return col->MaxStringSize(mind->NoDimensions() == 0 ? NULL : mind->GetFilter(dim));
}

PackOntologicalStatus SingleColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	return col->GetPackOntologicalStatus(mit.GetCurPackrow(dim));
}

void SingleColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc)
{
	col->EvaluatePack(mit, dim, desc);
}

RSValue SingleColumn::DoRoughCheck(const MIIterator& mit, Descriptor& d)
{
	if(mit.GetCurPackrow(dim) >= 0) {
		// check whether isn't it a join
		SingleColumn* sc = NULL;
		if(d.val1.vc && d.val1.vc->IsSingleColumn())
			sc = static_cast<SingleColumn*>(d.val1.vc);
		if(sc && sc->dim != dim)	// Pack2Pack rough check
			return col->RoughCheck(mit.GetCurPackrow(dim), mit.GetCurPackrow(sc->dim), d);
		else						// One-dim rough check
			return col->RoughCheck(mit.GetCurPackrow(dim), d, mit.NullsPossibleInPack(dim));
	} else
		return RS_SOME;
}

void SingleColumn::DisplayAttrStats()
{
	col->DisplayAttrStats(mind->GetFilter(dim));
}

char *SingleColumn::ToString(char p_buf[], size_t buf_ct) const
{
	int attr_no = col->AttrNo();
	if(attr_no > -1)
		snprintf(p_buf, buf_ct, "t%da%d", dim, attr_no);
	else
		snprintf(p_buf, buf_ct, "t%da*", dim);
	return p_buf;
}
