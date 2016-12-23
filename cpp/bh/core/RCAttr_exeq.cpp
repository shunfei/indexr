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

////////////////////////////////////////////////////////////////////////////////////////
// This is a part of RCAttr implementation concerned with the query execution mechanisms
////////////////////////////////////////////////////////////////////////////////////////

#include "RCAttr.h"
#include "RCAttrPack.h"
#include "CQTerm.h"
#include "RCAttrTypeInfo.h"
#include "edition/local.h"
#include "PackGuardian.h"
#include "tools.h"
#include "ValueSet.h"
#include "vc/ConstColumn.h"
#include "vc/InSetColumn.h"
#include "vc/SingleColumn.h"
#include "common/bhassert.h"

using namespace std;

/////////////////////////////////////////////////////////////////////////////////////////////////////

void RCAttr::EvaluatePack(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack(...)");
	BHASSERT(d.encoded, "Descriptor is not encoded!");
	if(d.op == O_FALSE) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
	} else if(d.op == O_TRUE)
		mit.NextPackrow();
	else if(d.op == O_NOT_NULL)
		EvaluatePack_NotNull(mit, dim);
	else if(d.op == O_IS_NULL)
		EvaluatePack_IsNull(mit, dim);
	else if(d.val1.vc && !d.val1.vc->IsConst()) {
		if(PackType() == PackN) {
			if(ATI::IsRealType(TypeName()))
				EvaluatePack_AttrAttrReal(mit, dim, d);
			else
				EvaluatePack_AttrAttr(mit, dim, d);
		} else
			assert(0);		// case not implemented
	} else if(PackType() == PackN && (d.op == O_BETWEEN || d.op == O_NOT_BETWEEN)) {
		if(!ATI::IsRealType(TypeName()))
			EvaluatePack_BetweenInt(mit, dim, d);
		else
			EvaluatePack_BetweenReal(mit, dim, d);
	} else if(PackType() == PackS && (d.op == O_BETWEEN || d.op == O_NOT_BETWEEN)) {
		if(RequiresUTFConversions(d.GetCollation()))
			EvaluatePack_BetweenString_UTF(mit, dim, d);
		else
			EvaluatePack_BetweenString(mit, dim, d);
	} else if(d.op == O_LIKE || d.op == O_NOT_LIKE) {
		if(RequiresUTFConversions(d.GetCollation()))
			EvaluatePack_Like_UTF(mit, dim, d);
		else
			EvaluatePack_Like(mit, dim, d);
	} else if(PackType() == PackS && (d.op == O_IN || d.op == O_NOT_IN)) {
		if(RequiresUTFConversions(d.GetCollation()))
			EvaluatePack_InString_UTF(mit, dim, d);
		else
			EvaluatePack_InString(mit, dim, d);
	} else if(PackType() == PackN && (d.op == O_IN || d.op == O_NOT_IN))
		EvaluatePack_InNum(mit, dim, d);
	else
		assert(0);	// case not implemented!
}

void RCAttr::EvaluatePack_IsNull(MIUpdatingIterator &mit, int dim)
{
	MEASURE_FET("RCAttr::EvaluatePack_IsNull(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.NextPackrow();
		return;
	}
	if(dpns[pack].pack_mode != PACK_MODE_TRIVIAL && dpns[pack].no_nulls != 0) {		// nontrivial pack exists
		do {
			if(mit[dim] != NULL_VALUE_64 && !dpns[pack].pack->IsNull(mit.GetCurInpack(dim)))
				mit.ResetCurrent();
			++mit;
		} while(mit.IsValid() && !mit.PackrowStarted());
	} else {													// pack is trivial - uniform or null only
		if(GetPackOntologicalStatus(pack) != NULLS_ONLY) {
			if(mit.NullsPossibleInPack(dim)) {
				do {
					if(mit[dim] != NULL_VALUE_64)
						mit.ResetCurrent();
					++mit;
				} while(mit.IsValid() && !mit.PackrowStarted());
			} else
				mit.ResetCurrentPack();
		}
		mit.NextPackrow();
	}
}

void RCAttr::EvaluatePack_NotNull(MIUpdatingIterator &mit, int dim)
{
	MEASURE_FET("RCAttr::EvaluatePack_NotNull(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {			// nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	if(dpns[pack].pack_mode != PACK_MODE_TRIVIAL && dpns[pack].no_nulls != 0) {
		do {
			if(mit[dim] == NULL_VALUE_64  || dpns[pack].pack->IsNull(mit.GetCurInpack(dim)))
				mit.ResetCurrent();
			++mit;
		} while(mit.IsValid() && !mit.PackrowStarted());
	} else {							// pack is trivial - uniform or null only
		if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
			mit.ResetCurrentPack();
		else if(mit.NullsPossibleInPack(dim)) {
			do {
				if(mit[dim] == NULL_VALUE_64)
					mit.ResetCurrent();
				++mit;
			} while(mit.IsValid() && !mit.PackrowStarted());
		}
		mit.NextPackrow();
	}
}

void RCAttr::EvaluatePack_Like(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_Like(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	AttrPackS *p = (AttrPackS*)dpns[pack].pack.get();
	if(p == NULL) {			// => nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	RCBString pattern;
	d.val1.vc->GetValueString(pattern, mit);
	int min_len = 0; // the number of fixed characters
	for(uint i = 0; i < pattern.len; i++) {
		if(pattern[i] != '%')
			min_len++;
		if(pattern[i] == d.like_esc) {		// disable optimization, escape character may do a lot of mess
			min_len = 0;
			break;
		}
	}
	int inpack;
	do {
		inpack = mit.GetCurInpack(dim);
		if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
			mit.ResetCurrent();
		else {
			int len = p->GetSize(inpack);
			bool res;
			if(len < min_len)
				res = false;
			else {
				RCBString v(p->GetVal(inpack), len, true);
				res = v.Like(pattern, d.like_esc);
			}
			if(d.op == O_NOT_LIKE)
				res = !res;
			if(!res)
				mit.ResetCurrent();
		}
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_Like_UTF(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_Like_UTF(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	AttrPackS *p = (AttrPackS*)dpns[pack].pack.get();
	if(p == NULL) {			// => nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	RCBString pattern;
	d.val1.vc->GetValueString(pattern, mit);
	int min_len = 0; // the number of fixed characters 
	for(uint i = 0; i < pattern.len; i++)
		if(pattern[i] != '%')
			min_len++;
	int inpack;
	do {
		inpack = mit.GetCurInpack(dim);
		if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
			mit.ResetCurrent();
		else {
			int len = p->GetSize(inpack);
			bool res;
			if(len < min_len)
				res = false;
			else {
				RCBString v(p->GetVal(inpack), len, true);
				int x = wildcmp(d.GetCollation(), v.val, v.val + v.len,  pattern.val, pattern.val + pattern.len, '\\', '_', '%');
				res = (x == 0 ? true : false);
			}
			if(d.op == O_NOT_LIKE)
				res = !res;
			if(!res)
				mit.ResetCurrent();
		}
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_InString(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_InString(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	AttrPackS *p = (AttrPackS*)dpns[pack].pack.get();
	if(p == NULL) {			// => nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<MultiValColumn*>(d.val1.vc) != NULL);
	MultiValColumn *multival_column = static_cast<MultiValColumn*>(d.val1.vc);
	bool encoded_set = multival_column->IsSetEncoded(TypeName(), ct.GetScale());
	int inpack;
	do {
		inpack = mit.GetCurInpack(dim);
		if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
			mit.ResetCurrent();
		else {
			BHTribool res;
			if(encoded_set)	{	// fast path for numerics vs. encoded constant set
				RCBString s(p->GetVal(inpack), p->GetSize(inpack), true);
				res = multival_column->ContainsString(mit, s);
			} else
				res = multival_column->Contains(mit, RCBString(p->GetVal(inpack), p->GetSize(inpack), true));
			if(d.op == O_NOT_IN)
				res = !res;
			if(res != true)
				mit.ResetCurrent();
		}
		if(ConnectionInfoOnTLS.Get().killed())
			throw KilledRCException();
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_InString_UTF(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_InString_UTF(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	AttrPackS *p = (AttrPackS*)dpns[pack].pack.get();
	if(p == NULL) {			// => nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	int v_len = CollationBufLen(d.GetCollation(), GetActualSize(mit.GetCurPackrow(dim)));
	RCBString v(NULL, v_len, true);

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<MultiValColumn*>(d.val1.vc) != NULL);
	MultiValColumn *multival_column = static_cast<MultiValColumn*>(d.val1.vc);
	int inpack;
	do {
		inpack = mit.GetCurInpack(dim);
		if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
			mit.ResetCurrent();
		else {
			BHTribool res;
			RCBString vt(p->GetVal(inpack), p->GetSize(inpack), true);
			res = multival_column->Contains(mit, vt);
			if(d.op == O_NOT_IN)
				res = !res;
			if(res != true)
				mit.ResetCurrent();
		}
		if(ConnectionInfoOnTLS.Get().killed())
			throw KilledRCException();
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_InNum(MIUpdatingIterator& mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_InNum(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	// added trivial case due to OR tree
	if(dpns[pack].pack_file == PF_NULLS_ONLY) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	DPN& dpn = dpns[pack];
	AttrPackN* p = (AttrPackN*)dpn.pack.get();
	_int64 local_min = dpn.local_min;
	_int64 local_max = dpn.local_max;

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<MultiValColumn*>(d.val1.vc) != NULL);
	MultiValColumn* multival_column = static_cast<MultiValColumn*>(d.val1.vc);
	bool lookup_to_num = ATI::IsStringType(TypeName());
	bool encoded_set = (lookup_to_num ?	multival_column->IsSetEncoded(RC_NUM, 0) :
										multival_column->IsSetEncoded(TypeName(), ct.GetScale()));
	BHTribool res;
	boost::scoped_ptr<RCDataType> value(ValuePrototype(lookup_to_num).Clone());
	bool not_in = (d.op == O_NOT_IN);
	if(local_min == local_max) {
		if(GetPackOntologicalStatus(pack) == NULLS_ONLY) {
			mit.ResetCurrentPack();
			mit.NextPackrow();
		} else {
			// only nulls and single value
			// pack does not exist
			do {
				if(IsNull(mit[dim]))
					mit.ResetCurrent();
				else {
					// find the first non-null and set the rest basing on it.
					//const RCValueObject& val = GetValue(mit[dim], lookup_to_num);
					// note: res may be UNKNOWN for NOT IN (...null...)
					res = multival_column->Contains(mit, GetValue(mit[dim], *value, lookup_to_num));
					if(not_in)
						res = !res;
					if(res == true) {
						if(dpn.no_nulls != 0)
							EvaluatePack_NotNull(mit, dim);
						else
							mit.NextPackrow();
					} else {
						mit.ResetCurrentPack();
						mit.NextPackrow();
					}
					break;
				}
				++mit;
			} while(mit.IsValid() && !mit.PackrowStarted());
		}
	} else {
		do {
			if(mit[dim] == NULL_VALUE_64 || p->IsNull(mit.GetCurInpack(dim)))
				mit.ResetCurrent();
			else {
				// note: res may be UNKNOWN for NOT IN (...null...)
				if(encoded_set)		// fast path for numerics vs. encoded constant set
					res = multival_column->Contains64(mit, GetNotNullValueInt64(mit[dim]));
				else
					res = multival_column->Contains(mit, GetValue(mit[dim], *value, lookup_to_num));
				if(not_in)
					res = !res;
				if(res != true)
					mit.ResetCurrent();
			}
			++mit;
		} while(mit.IsValid() && !mit.PackrowStarted());
		if(ConnectionInfoOnTLS.Get().killed())
			throw KilledRCException();
	}
}

void RCAttr::EvaluatePack_BetweenString(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_BetweenString(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	// added trivial case due to OR tree
	if(dpns[pack].pack_file == PF_NULLS_ONLY) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	AttrPackS* p = (AttrPackS*)dpns[pack].pack.get();
	if(p == NULL) {			// => nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	RCBString v1, v2;
	d.val1.vc->GetValueString(v1, mit);
	d.val2.vc->GetValueString(v2, mit);
	int inpack;
	bool res;
	do {
		inpack = mit.GetCurInpack(dim);		// row number inside the pack
		if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
			mit.ResetCurrent();
		else {
			RCBString v(p->GetVal(inpack), p->GetSize(inpack));			// change to materialized in case of problems, but the pack should be locked and unchanged here
			// IsNull() below means +/-inf
			res = ( d.sharp && ((v1.IsNull() || v > v1)  && (v2.IsNull() || v < v2) )) ||
				  (!d.sharp && ((v1.IsNull() || v >= v1) && (v2.IsNull() || v <= v2))) ;
			if(d.op == O_NOT_BETWEEN)
				res = !res;
			if(!res)
				mit.ResetCurrent();
		}
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_BetweenString_UTF(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_BetweenString_UTF(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	// added trivial case due to OR tree
	if(dpns[pack].pack_file == PF_NULLS_ONLY) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	AttrPackS* p = (AttrPackS*)dpns[pack].pack.get();
	if(p == NULL) {			// => nulls only
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	RCBString v1, v2;
	DTCollation coll = d.GetCollation();
	d.val1.vc->GetValueString(v1, mit);
	d.val2.vc->GetValueString(v2, mit);
	int inpack;
	do {
		inpack = mit.GetCurInpack(dim);		// row number inside the pack
		if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
			mit.ResetCurrent();
		else {
			RCBString v(p->GetVal(inpack), p->GetSize(inpack));			// change to materialized in case of problems, but the pack should be locked and unchanged here
			// IsNull() below means +/-inf
			bool res = ( d.sharp && ((v1.IsNull() || CollationStrCmp(coll, v, v1) > 0) && (v2.IsNull() || CollationStrCmp(coll, v, v2) < 0) )) ||
				  (!d.sharp && ((v1.IsNull() || CollationStrCmp(coll, v, v1) >= 0) && (v2.IsNull() || CollationStrCmp(coll, v, v2) <= 0)));
			if(d.op == O_NOT_BETWEEN)
				res = !res;
			if(!res)
				mit.ResetCurrent();
		}
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_BetweenInt(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_BetweenInt(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	DPN& dpn = dpns[pack];

	// added trivial case due to OR tree
	if(dpn.pack_file == PF_NULLS_ONLY) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	AttrPackN *p = (AttrPackN*)dpn.pack.get();
	_int64 pv1 = d.val1.vc->GetValueInt64(mit);
	_int64 pv2 = d.val2.vc->GetValueInt64(mit);
	_int64 local_min = dpn.local_min;
	_int64 local_max = dpn.local_max;
	if(pv1 != MINUS_INF_64)
		pv1 = pv1 - local_min;
	else
		pv1 = 0;

	if(pv2 != PLUS_INF_64) // encode from 0-level to 2-level
		pv2 = pv2 - local_min;
	else
		pv2 = local_max - local_min;
	if(local_min != local_max) {
		if(d.op == O_BETWEEN && !mit.NullsPossibleInPack(dim) && dpn.no_nulls == 0) {
			// easy and fast case - no "if"s
			_int64 v;
			do {
				v = p->GetVal64(mit.GetCurInpack(dim));
				if(pv1 > v || v > pv2)
					mit.ResetCurrent();
				++mit;
			} while(mit.IsValid() && !mit.PackrowStarted());
		} else {
			// more general case
			bool res;
			_int64 v;
			int inpack;
			do {
				inpack = mit.GetCurInpack(dim);
				if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
					mit.ResetCurrent();
				else {
					v = p->GetVal64(inpack);
					res = (pv1 <= v && v <= pv2);
					if(d.op == O_NOT_BETWEEN)
						res = !res;
					if(!res)
						mit.ResetCurrent();
				}
				++mit;
			} while(mit.IsValid() && !mit.PackrowStarted());
		}
	} else {
		// local_min==local_max, and in 2-level encoding both are 0
		if(	((pv1 > 0 || pv2 < 0)	&& d.op == O_BETWEEN) ||
			( pv1 <= 0 && pv2 >= 0	&& d.op == O_NOT_BETWEEN) ) {
			mit.ResetCurrentPack();
			mit.NextPackrow();
		} else
			EvaluatePack_NotNull(mit, dim);
	}
}

void RCAttr::EvaluatePack_BetweenReal(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_BetweenReal(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	// added trivial case due to OR tree
	if(dpns[pack].pack_file == PF_NULLS_ONLY) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}

	AttrPackN *p = (AttrPackN*)dpns[pack].pack.get();
	_int64 pv1 = d.val1.vc->GetValueInt64(mit);
	_int64 pv2 = d.val2.vc->GetValueInt64(mit);
	double dv1 = *((double*)&pv1);
	double dv2 = *((double*)&pv2);
	if(dpns[pack].local_min != dpns[pack].local_max) {
		bool res;
		double v;
		int inpack;
		do {
			inpack = mit.GetCurInpack(dim);
			if(mit[dim] == NULL_VALUE_64 || p->IsNull(inpack))
				mit.ResetCurrent();
			else {
				v = p->GetValD(inpack);
				res = (dv1 <= v && v <= dv2);
				if(d.op == O_NOT_BETWEEN)
					res = !res;
				if(!res)
					mit.ResetCurrent();
			}
			++mit;
		} while(mit.IsValid() && !mit.PackrowStarted());
	} else {
		// local_min==local_max, for float
		double uni_val = *((double*)(&dpns[pack].local_min));
		if(	((dv1 > uni_val  || dv2 < uni_val) && d.op == O_BETWEEN) ||
			( dv1 <= uni_val && dv2 >= uni_val && d.op == O_NOT_BETWEEN) ) {
			mit.ResetCurrentPack();
			mit.NextPackrow();
		} else
			EvaluatePack_NotNull(mit, dim);
	}
}

void RCAttr::EvaluatePack_AttrAttr(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_AttrAttr(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	RCAttr* a2 = (RCAttr*)(((SingleColumn*)d.val1.vc)->GetPhysical());
	if(dpns[pack].no_nulls == dpns[pack].GetNoObj() || a2->dpns[pack].no_nulls == a2->dpns[pack].GetNoObj()) {
		mit.ResetCurrentPack();		// nulls only
		mit.NextPackrow();
		return;
	}
	AttrPackN *p1 = (AttrPackN*)dpns[pack].pack.get();
	AttrPackN *p2 = (AttrPackN*)(a2->dpns[pack].pack.get());
	_int64 min1 = dpns[pack].local_min;
	_int64 min2 = a2->dpns[pack].local_min;
	_int64 max1 = dpns[pack].local_max;
	_int64 max2 = a2->dpns[pack].local_max;
	bool pack1_uniform = (min1 == max1);
	bool pack2_uniform = (min2 == max2);
	_int64 val1_offset = min1 - min2;				// GetVal_1 + val_offset = GetVal_2
	_int64 v1, v2;
	int obj_in_pack;
	bool res;
	do {
		obj_in_pack = mit.GetCurInpack(dim);
		if(mit[dim] == NULL_VALUE_64 || 
			(p1 && p1->IsNull(obj_in_pack)) || (p2 && p2->IsNull(obj_in_pack)))	// p1, p2 may be null for uniform
			mit.ResetCurrent();
		else {
			v1 = (pack1_uniform ? 0 : p1->GetVal64(obj_in_pack)) + val1_offset;
			v2 = (pack2_uniform ? 0 : p2->GetVal64(obj_in_pack));
			switch(d.op) {
				case O_EQ:		res = (v1 == v2); break;
				case O_NOT_EQ:	res = (v1 != v2); break;
				case O_LESS:	res = (v1 <  v2); break;
				case O_LESS_EQ:	res = (v1 <= v2); break;
				case O_MORE:	res = (v1 >  v2); break;
				case O_MORE_EQ:	res = (v1 >= v2); break;
				default:		assert(0);
			}
			if(!res)
				mit.ResetCurrent();
		}
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

void RCAttr::EvaluatePack_AttrAttrReal(MIUpdatingIterator &mit, int dim, Descriptor& d)
{
	MEASURE_FET("RCAttr::EvaluatePack_AttrAttrReal(...)");
	int pack = mit.GetCurPackrow(dim);
	if(pack == -1) {
		mit.ResetCurrentPack();
		mit.NextPackrow();
		return;
	}
	RCAttr* a2 = (RCAttr*)(((SingleColumn*)d.val1.vc)->GetPhysical());
	if(dpns[pack].no_nulls == dpns[pack].no_objs || a2->dpns[pack].no_nulls == a2->dpns[pack].no_objs) {
		mit.ResetCurrentPack();		// nulls only
		mit.NextPackrow();
		return;
	}
	AttrPackN *p1 = (AttrPackN*)dpns[pack].pack.get();
	AttrPackN *p2 = (AttrPackN*)(a2->dpns[pack].pack.get());
	_int64 min1 = dpns[pack].local_min;
	_int64 min2 = a2->dpns[pack].local_min;
	_int64 max1 = dpns[pack].local_max;
	_int64 max2 = a2->dpns[pack].local_max;
	bool pack1_uniform = (min1 == max1);
	bool pack2_uniform = (min2 == max2);
	_int64 pv1, pv2;
	int obj_in_pack;
	bool res;
	do {
		obj_in_pack = mit.GetCurInpack(dim);
		if(mit[dim] == NULL_VALUE_64 || 
			(p1 && p1->IsNull(obj_in_pack)) || (p2 && p2->IsNull(obj_in_pack)))	// p1, p2 may be null for uniform
			mit.ResetCurrent();
		else {
			pv1 = (pack1_uniform ? min1 : p1->GetVal64(obj_in_pack));
			pv2 = (pack2_uniform ? min2 : p2->GetVal64(obj_in_pack));
			double v1 = *((double*)&pv1);
			double v2 = *((double*)&pv2);
			switch(d.op) {
				case O_EQ:		res = (v1 == v2); break;
				case O_NOT_EQ:	res = (v1 != v2); break;
				case O_LESS:	res = (v1 <  v2); break;
				case O_LESS_EQ:	res = (v1 <= v2); break;
				case O_MORE:	res = (v1 >  v2); break;
				case O_MORE_EQ:	res = (v1 >= v2); break;
				default:		assert(0);
			}
			if(!res)
				mit.ResetCurrent();
		}
		++mit;
	} while(mit.IsValid() && !mit.PackrowStarted());
}

bool RCAttr::IsDistinct(Filter* f)
{
	MEASURE_FET("RCAttr::IsDistinct(...)");
	if(ct.IsLookup() && RequiresUTFConversions(GetCollation()))
		return false;
	if(PhysicalColumn::IsDistinct() == RS_ALL) {		// = is_unique_updated && is_unique
		if(f == NULL)
			return (NoNulls() == 0);		// no nulls at all, and is_unique  => distinct
		LoadPackInfo();
		for(int b = 0; b < NoPack(); b++)
			if(!f->IsEmpty(b) && dpns[b].no_nulls > 0)		// any null in nonempty pack?
				return false;
		return true;
	}
	return false;
}


_uint64 RCAttr::ApproxAnswerSize(Descriptor& d)
{
	MEASURE_FET("RCAttr::ApproxAnswerSize(...)");
	BHASSERT(d.encoded, "The descriptor is not encoded!");
	static MIIterator const mit(NULL);
	LoadPackInfo();
	if(d.op == O_NOT_NULL)
		return NoObj() - NoNulls();
	if(d.op == O_IS_NULL)
		return NoNulls();
	if(d.val1.vc && !d.val1.vc->IsConst()) {
		_uint64 no_distinct = ApproxDistinctVals(false, NULL, NULL, false);
		if(no_distinct == 0)
			no_distinct = 1;
		if(d.op == O_EQ)
			return	NoObj() / no_distinct;
		if(d.op == O_NOT_EQ)
			return	NoObj() - (NoObj() / no_distinct);
		return (NoObj() - NoNulls()) / 2;		// default
	}
	if(d.op == O_BETWEEN && d.val1.vc->IsConst() && d.val2.vc->IsConst() && PackType()==PackN) {
		double res = 0;
		_int64 val1 = d.val1.vc->GetValueInt64(mit);
		_int64 val2 = d.val2.vc->GetValueInt64(mit);
		if(!ATI::IsRealType(TypeName())) {
			_int64 span1, span2;				// numerical case: approximate number of rows in each pack
			for(int b = 0; b < NoPack(); b++) {
				if(dpns[b].local_min > val2 || dpns[b].local_max < val1 || dpns[b].no_nulls == dpns[b].no_objs)
					continue;	// pack irrelevant
				span1 = dpns[b].local_max - dpns[b].local_min + 1;
				if(span1 <= 0)					// out of _int64 range
					span1 = 1;
				if(val2 < dpns[b].local_max)			// calculate the size of intersection
					span2 = val2;
				else
					span2 = dpns[b].local_max;
				if(val1 > dpns[b].local_min)
					span2 -= val1;
				else
					span2 -= dpns[b].local_min;
				span2 += 1;
				res += (dpns[b].no_objs - dpns[b].no_nulls) * double(span2) / span1;		// supposing uniform distribution of values
			}
		} else {		// double
			double span1, span2;				// numerical case: approximate number of rows in each pack
			double v_min = *(double*)&val1;
			double v_max = *(double*)&val2;
			for(int b = 0; b < NoPack(); b++) {
				double d_min = *((double*)(&dpns[b].local_min));
				double d_max = *((double*)(&dpns[b].local_max));
				if(d_min > v_max || d_max < v_min || dpns[b].no_nulls == dpns[b].no_objs)
					continue;	// pack irrelevant
				span1 = d_max - d_min;
				span2 = min(v_max, d_max) - max(v_min, d_min);
				if(span1 == 0)
					res += dpns[b].no_objs - dpns[b].no_nulls;
				else if(span2 == 0)					// just one value
					res += 1;
				else
					res += (dpns[b].no_objs - dpns[b].no_nulls) * (span2 / span1);		// supposing uniform distribution of values
			}
		}
		return _int64(res);
	}
	return (NoObj() - NoNulls()) / 2;		// default
}

ushort RCAttr::MaxStringSize(Filter* f)		// maximal byte string length in column
{
	LoadPackInfo();
	ushort max_size = 1;
	if(Type().IsLookup()) {
		_int64 cur_min = PLUS_INF_64;
		_int64 cur_max = MINUS_INF_64;
		for(int b = 0; b < NoPack(); b++) {
			if((f && f->IsEmpty(b)) || GetPackOntologicalStatus(b) == NULLS_ONLY)
				continue;
			DPN& d = dpns[b];
			if(d.local_min < cur_min)
				cur_min = d.local_min;
			if(d.local_max > cur_max)
				cur_max = d.local_max;
		}
		if(cur_min != PLUS_INF_64)
			max_size = dic->MaxValueSize(int(cur_min), int(cur_max));
	} else {
		for(int b = 0; b < NoPack(); b++) {
			if(f && f->IsEmpty(b))
				continue;
			ushort cur_size = GetActualSize(b);
			if(max_size < cur_size)
				max_size = cur_size;
			if(max_size == Type().GetPrecision())
				break;
		}
	}
	return max_size;
}

///////////////////////////////////////////////////////////////////////////////////

bool RCAttr::TryToMerge(Descriptor &d1, Descriptor &d2)
{
	MEASURE_FET("RCAttr::TryToMerge(...)");
	if((d1.op != O_BETWEEN && d1.op != O_NOT_BETWEEN) || (d2.op != O_BETWEEN && d2.op != O_NOT_BETWEEN))
		return false;
	if(PackType() == PackN &&
		d1.val1.vc && d1.val2.vc && d2.val1.vc && d2.val2.vc &&
		d1.val1.vc->IsConst() && d1.val2.vc->IsConst() && d2.val1.vc->IsConst() && d2.val2.vc->IsConst()) {

		static MIIterator const mit(NULL);
		_int64 d1min = d1.val1.vc->GetValueInt64(mit);
		_int64 d1max = d1.val2.vc->GetValueInt64(mit);
		_int64 d2min = d2.val1.vc->GetValueInt64(mit);
		_int64 d2max = d2.val2.vc->GetValueInt64(mit);
		if(!ATI::IsRealType(TypeName())) {
			if(d1.op == O_BETWEEN && d2.op == O_BETWEEN) {
				if(d2min > d1min) {
					swap(d1.val1, d2.val1);
					swap(d1min, d2min);
				}
				if(d2max < d1max) {
					swap(d1.val2, d2.val2);
					swap(d1max, d2max);
				}
				if(d1min > d1max)
					d1.op = O_FALSE;	// disjoint?
				return true;
			}
			if(d1.op == O_NOT_BETWEEN && d2.op == O_NOT_BETWEEN) {
				if(d1min < d2max && d2min < d1max) {
					if(d2min < d1min)
						swap(d1.val1, d2.val1);
					if(d2max > d1max)
						swap(d1.val2, d2.val2);
					return true;
				}
			}
		} else {		// double
			if(d1.sharp != d2.sharp)
				return false;
			double dv1min = *((double*)&d1min);
			double dv1max = *((double*)&d1max);
			double dv2min = *((double*)&d2min);
			double dv2max = *((double*)&d2max);
			if(d1.op == O_BETWEEN && d2.op == O_BETWEEN) {
				if(dv2min > dv1min) {
					swap(d1.val1, d2.val1);
					swap(dv2min, dv1min);
				}
				if(dv2max < dv1max) {
					swap(d1.val2, d2.val2);
					swap(dv2max, dv1max);
				}
				if(dv1min > dv1max)
					d1.op = O_FALSE;	// disjoint?
				return true;
			}
			if(d1.op == O_NOT_BETWEEN && d2.op == O_NOT_BETWEEN) {
				if(dv1min < dv2max && dv2min < dv1max) {
					if(dv2min < dv1min)
						swap(d1.val1, d2.val1);
					if(dv2max > dv1max)
						swap(d1.val2, d2.val2);
					return true;
				}
			}
		}
	} else if(PackType() == PackS &&
		d1.val1.vc && d1.val2.vc && d2.val1.vc && d2.val2.vc &&
		d1.val1.vc->IsConst() && d1.val2.vc->IsConst() && d2.val1.vc->IsConst() && d2.val2.vc->IsConst() &&
		d1.sharp == d2.sharp &&
		d1.GetCollation().collation == d2.GetCollation().collation) {
			static MIIterator const mit(NULL);
			RCBString d1min, d1max, d2min, d2max; 
			d1.val1.vc->GetValueString(d1min, mit);
			d1.val2.vc->GetValueString(d1max, mit);
			d2.val1.vc->GetValueString(d2min, mit);
			d2.val2.vc->GetValueString(d2max, mit);
			DTCollation my_coll = d1.GetCollation();
			if(d1.op == O_BETWEEN && d2.op == O_BETWEEN) {
				if(RequiresUTFConversions(my_coll)) {
					if(d1min.IsNull() || CollationStrCmp(my_coll, d2min, d1min, O_MORE)) {
						swap(d1.val1, d2.val1);
						swap(d1min, d2min);
					}
					if(d1max.IsNull() || (!d2max.IsNull() && CollationStrCmp(my_coll, d2max, d1max, O_LESS))) {
						swap(d1.val2, d2.val2);
						swap(d1max, d2max);
					}
					if(CollationStrCmp(my_coll, d1min, d1max, O_MORE))
						d1.op = O_FALSE;	// disjoint?
				} else {
					if(d1min.IsNull() || d2min > d1min) {		// IsNull() means infinity here
						swap(d1.val1, d2.val1);
						swap(d1min, d2min);
					}
					if(d1max.IsNull() || d2max < d1max) {
						swap(d1.val2, d2.val2);
						swap(d1max, d2max);
					}
					if(d1min > d1max)
						d1.op = O_FALSE;	// disjoint?
				}
				return true;
			}
			if(d1.op == O_NOT_BETWEEN && d2.op == O_NOT_BETWEEN) {
				if(d1min.IsNull() || d1max.IsNull() || d2min.IsNull() || d2max.IsNull())
					return false;	// should not appear in normal circumstances
				if(RequiresUTFConversions(my_coll)) {
					if(CollationStrCmp(my_coll, d1min, d2max, O_LESS) && 
						CollationStrCmp(my_coll, d2min, d1max, O_LESS)) {
						if(CollationStrCmp(my_coll, d2min, d1min, O_LESS))
							swap(d1.val1, d2.val1);
						if(CollationStrCmp(my_coll, d2max, d1max, O_MORE))
							swap(d1.val2, d2.val2);
						return true;
					}
				} else {
					if(d1min < d2max && d2min < d1max) {
						if(d2min < d1min)
							swap(d1.val1, d2.val1);
						if(d2max > d1max)
							swap(d1.val2, d2.val2);
						return true;
					}
				}
			}
	}
	return false;
}
