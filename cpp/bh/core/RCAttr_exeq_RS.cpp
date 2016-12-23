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
// This is a part of RCAttr implementation concerned with the KNs and its usage
////////////////////////////////////////////////////////////////////////////////////////

#include "RCAttr.h"
#include "RCAttrPack.h"
#include "CQTerm.h"
#include "RCAttrTypeInfo.h"
#include "edition/local.h"
#include "PackGuardian.h"
#include "ValueSet.h"
#include "vc/MultiValColumn.h"
#include "vc/SingleColumn.h"

using namespace std;

/////////////////////////////////////////////////////////////////////////////////
/////// Rough queries and indexes

uint Int64StrLen(_int64 x) 
{
	uint min_len = 8;
	uchar* s = (uchar*)(&x);
	while(min_len > 0 && s[min_len - 1] == '\0') 
		min_len--;
	return min_len;
}

RSValue RCAttr::RoughCheck(int pack, Descriptor& d, bool additional_nulls_possible)		// NOTE: similar code is in VirtualColumnBase::DoRoughCheck
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RS_SOME;
#else
	if(d.op == O_FALSE)
		return RS_NONE;
	else if(d.op == O_TRUE)
		return RS_ALL;
	// TODO: implement RoughCheck CMAP for utf8
	if(PackType() == PackS && RequiresUTFConversions(d.GetCollation()) && d.GetCollation().collation != Type().GetCollation().collation) {
		if(d.encoded) {
			LoadPackInfo();		// just in case, although the condition was encoded and this function should be executed earlier
			DPN const& dpn( dpns[pack] );
			if(dpn.pack_file == PF_NULLS_ONLY || dpn.no_nulls - 1 == dpn.no_objs) { // all objects are null
				if(d.op == O_IS_NULL)
					return RS_ALL;
				else
					return RS_NONE;
			}
		}
		return RS_SOME;
	}
	if(d.IsType_AttrValOrAttrValVal()) {
		if(!d.encoded)
			return RS_SOME;
		VirtualColumn* vc1 = d.val1.vc;
		static MIIterator const mit(NULL);
		LoadPackInfo();		// just in case, although the condition was encoded and this function should be executed earlier
		DPN const& dpn( dpns[pack] );
		if(dpn.pack_file == PF_NULLS_ONLY || dpn.no_nulls - 1 == dpn.no_objs) { // all objects are null
			if(d.op == O_IS_NULL)
				return RS_ALL;
			else
				return RS_NONE;
		}
		if(d.op == O_IS_NULL || d.op == O_NOT_NULL) {
			if(dpn.no_nulls == 0 && !additional_nulls_possible) {
				if(d.op == O_IS_NULL)
					return RS_NONE;
				else
					return RS_ALL;
			}
			return RS_SOME;
		} else if((d.op == O_LIKE || d.op == O_NOT_LIKE) && PackType() == PackS) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(vc1->IsConst());
			RCBString pat;
			vc1->GetValueString(pat, mit);
			RSValue res = RS_SOME;
			/////////////////////////////////////////////
			// here: check min, max
			uint pattern_prefix = 0;				// e.g. "ab_cd_e%f"  -> 7
			uint pattern_fixed_prefix = 0;		// e.g. "ab_cd_e%f"  -> 2
			uint pack_prefix;
			if(RequiresUTFConversions(d.GetCollation())) {
				my_match_t mm;
				if(d.GetCollation().collation->coll->instr(d.GetCollation().collation, pat.val,pat.len,"%",1,&mm, 1) ==2)
					pattern_prefix = pattern_fixed_prefix = mm.end;

				if(d.GetCollation().collation->coll->instr(d.GetCollation().collation, pat.val,pat.len,"_",1,&mm, 1) ==2)
					if(mm.end < pattern_fixed_prefix)
						pattern_fixed_prefix = mm.end;

				if((pattern_fixed_prefix > 0) &&
						RCBString(pat.val,pattern_fixed_prefix).LessEqThanMaxUTF(dpn.local_max, Type().GetCollation() )== false)
					res = RS_NONE;

				if(pattern_fixed_prefix > GetActualSize(pack))
					res = RS_NONE;
				pack_prefix = GetPrefix(pack).len;
				if(res == RS_SOME && pack_prefix > 0
					&& pattern_fixed_prefix <= pack_prefix		// special case: "xyz%" and the pack prefix is at least 3
					&& pattern_fixed_prefix + 1 == pat.len && pat[pattern_fixed_prefix] == '%') {
						if( d.GetCollation().collation->coll->strnncoll(d.GetCollation().collation, (const uchar*)pat.val,pattern_fixed_prefix, (const uchar*)(&dpn.local_min), pattern_fixed_prefix, 0) == 0)
							res = RS_ALL;
						else
							res = RS_NONE;							// prefix and pattern are different
				}

			} else {

				while(pattern_prefix < pat.len && pat[pattern_prefix] != '%')
					pattern_prefix++;
				while(pattern_fixed_prefix < pat.len && pat[pattern_fixed_prefix] != '%' && pat[pattern_fixed_prefix] != '_')
					pattern_fixed_prefix++;

				if((pattern_fixed_prefix > 0)
						&& RCBString(pat.val,pattern_fixed_prefix).LessEqThanMax(dpn.local_max) == false)		// val_t==NULL means +/-infty
					res = RS_NONE;
				if(pattern_fixed_prefix > GetActualSize(pack))
					res = RS_NONE;
				pack_prefix = GetPrefix(pack).len;
				if(res == RS_SOME && pack_prefix > 0
					&& pattern_fixed_prefix <= pack_prefix		// special case: "xyz%" and the pack prefix is at least 3
					&& pattern_fixed_prefix + 1 == pat.len && pat[pattern_fixed_prefix] == '%') {
					if(memcmp(pat.val, (char*)(&dpn.local_min), pattern_fixed_prefix) == 0)		// pattern is equal to the prefix
						res = RS_ALL;
					else
						res = RS_NONE;							// prefix and pattern are different
				}
			}

			/////////////////////////////////////////////
			if(res == RS_SOME && min(pattern_prefix, pack_prefix) < pat.len && !RequiresUTFConversions(d.GetCollation())) {
				RCBString pattern_for_cmap;												// note that cmap is shifted by a common prefix!
				if(pattern_prefix > pack_prefix)
					pattern_for_cmap = RCBString(pat.val + pack_prefix, pat.len - pack_prefix);			// "xyz%abc" -> "z%abc"
				else
					pattern_for_cmap = RCBString(pat.val + pattern_prefix, pat.len - pattern_prefix);		// "xyz%abc" -> "%abc"

				if( !(pattern_for_cmap.len == 1 && pattern_for_cmap[0] == '%') ) {		// i.e. "%" => all is matching
					RSIndex_CMap *rsi_cmap = LoadRSI_CMap();
					if(rsi_cmap && rsi_cmap->UpToDate(NoObj(), pack))
						res = rsi_cmap->IsLike(pattern_for_cmap, pack, d.like_esc);
					ReleaseRSI(rsi_cmap);
				} else
					res = RS_ALL;
			}
			if(d.op == O_NOT_LIKE) {
				if(res == RS_ALL)
					res = RS_NONE;
				else if(res == RS_NONE)
					res = RS_ALL;
			}
			if((dpn.no_nulls != 0 || additional_nulls_possible) && res == RS_ALL)
				res = RS_SOME;
			return res;
		} else if((d.op == O_IN || d.op == O_NOT_IN) && PackType() == PackS) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT( dynamic_cast<MultiValColumn*>(vc1) );
			MultiValColumn* mvc( static_cast<MultiValColumn*>(vc1));
			uint pack_prefix = GetPrefix(pack).len;
			RSValue res = RS_SOME;
			if((mvc->IsConst()) && (mvc->NoValues(mit) > 0 && mvc->NoValues(mit) < 64) && !RequiresUTFConversions(d.GetCollation())) {
				RSIndex_CMap *rsi_cmap = LoadRSI_CMap();
				if(rsi_cmap && rsi_cmap->UpToDate(NoObj(), pack)) {
					res = RS_NONE;					// TODO: get rid with the iterator below
					
					for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); (it != end) && (res == RS_NONE); ++it) {
						RCBString v1 = it->GetString();
						if(pack_prefix <= v1.len) {
							if(pack_prefix == 0 || memcmp(v1.val, (char*)(&dpn.local_min), pack_prefix) == 0) {
								size_t len = v1.len - pack_prefix;
								RCBString v(len <= 0 ? "" : v1.val + pack_prefix, (int)len);
								if(v1.len == pack_prefix ||
									rsi_cmap->IsValue(v, v, pack) != RS_NONE)	// suspected, if any value is possible (due to the prefix or CMAP)
									res = RS_SOME;
							}
						}
					}
				}
				ReleaseRSI(rsi_cmap);
			}
			if(d.op == O_NOT_IN) {
				if(res == RS_ALL)
					res = RS_NONE;
				else if(res == RS_NONE)
					res = RS_ALL;
			}
			if(res == RS_ALL && (dpn.no_nulls > 0 || additional_nulls_possible))
				res = RS_SOME;
			return res;
		} else if((d.op == O_IN || d.op == O_NOT_IN) && (PackType() == PackN)) {
			if(vc1->IsConst()) {
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT( dynamic_cast<MultiValColumn*>(vc1) );
				MultiValColumn* mvc( static_cast<MultiValColumn*>(vc1));
				_int64 v1, v2;
				RCNum rcn_min = mvc->GetSetMin(mit);
				RCNum rcn_max = mvc->GetSetMax(mit);
				if(rcn_min.IsNull() || rcn_max.IsNull())		// cannot determine min/max
					return RS_SOME;
				if(Type().IsLookup()) {
					v1 = rcn_min.GetValueInt64();
					v2 = rcn_max.GetValueInt64();
				} else {
					bool v1_rounded, v2_rounded;
					v1 = EncodeValue64(&rcn_min, v1_rounded);
					v2 = EncodeValue64(&rcn_max, v2_rounded);
				}

				RSValue res = RS_SOME;
				if(ATI::IsRealType(TypeName()))
					return res;	// real values not implemented yet
				if(v1 > dpn.local_max || v2 < dpn.local_min) {
					res = RS_NONE;								// calculate as for O_IN and then take O_NOT_IN into account
				} else if(dpn.local_min == dpn.local_max) {
					RCValueObject rcvo(ATI::IsDateTimeType(TypeName()) ? RCValueObject(RCDateTime(dpn.local_min, TypeName())) : RCValueObject(RCNum(dpn.local_min, Type().GetScale())));
					res = (mvc->Contains(mit, *rcvo) != false) ? RS_ALL : RS_NONE;
				} else {
					RSIndex_Hist *rsi_hist = LoadRSI_Hist();
					if(rsi_hist && rsi_hist->UpToDate(NoObj(), pack))
						res = rsi_hist->IsValue(v1, v2, pack, dpn.local_min, dpn.local_max);
					ReleaseRSI(rsi_hist);
					if(res == RS_ALL)	// v1, v2 are just a boundary, not continuous interval
						res = RS_SOME;
				}
				///////////////////////////////////////////////
				if(res == RS_SOME && (mvc->NoValues(mit) > 0 && mvc->NoValues(mit) < 64)) {
					RSIndex_Hist *rsi_hist = LoadRSI_Hist();
					if(rsi_hist && !rsi_hist->UpToDate(NoObj(), pack)) {
						ReleaseRSI(rsi_hist);
						rsi_hist = NULL;
					}
					bool v_rounded = false;
					res = RS_NONE;
					_int64 v;					// TODO: get rid with the iterator below
					for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); (it != end) && (res == RS_NONE); ++it) {
						if(!Type().IsLookup()) {		// otherwise it will be decoded to text
							v = EncodeValue64(it->GetValue().Get(), v_rounded);
						} else
							v = it->GetInt64();
						if(!v_rounded) {
							if((rsi_hist == NULL && v <= dpn.local_max && v >= dpn.local_min) ||
								(rsi_hist && rsi_hist->IsValue(v, v, pack, dpn.local_min, dpn.local_max) != RS_NONE)) {	// suspected, if any value is possible
								res = RS_SOME;		// note: v_rounded means that this real value could not match this pack
								break;
							}
						}
					}
					ReleaseRSI(rsi_hist);
				}
				///////////////////////////////////////////////
				if(d.op == O_NOT_IN) {
					if(res == RS_ALL)
						res = RS_NONE;
					else if(res == RS_NONE)
						res = RS_ALL;
				}
				if(res == RS_ALL && (dpn.no_nulls > 0  || additional_nulls_possible))
					res = RS_SOME;
				return res;
			}
		} else if(PackType() == PackS) {		// Note: text operations as PackN calculated as IN or below
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(d.op==O_BETWEEN || d.op==O_NOT_BETWEEN);
			VirtualColumn* vc2 = d.val2.vc;
			RSValue res = RS_SOME;
			uint pack_prefix = GetPrefix(pack).len;
			uint val_prefix = 0;
			RCBString vmin;
			RCBString vmax;
			vc1->GetValueString( vmin, mit );
			vc2->GetValueString( vmax, mit );
			if(vmin.IsNull() && vmax.IsNull())			// comparing with null - always false
				return RS_NONE;
			while(  vmin.val && vmax.val &&
					val_prefix < vmin.len && val_prefix < vmax.len &&
					vmin[val_prefix] == vmax[val_prefix])
						val_prefix++;	// Common prefix for values. It is a value length in case of equality.
			/////////////////////////////////////////////
			// check min, max
			// TODO UTF8: check PREFIX handling
			if(val_prefix > GetActualSize(pack)) {		// value to be found is longer than texts in the pack
				res = RS_NONE;
			} else if((vmax.val && vmax.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation()) == false) ||
					  (vmin.val && vmin.LessEqThanMaxUTF(dpn.local_max, Type().GetCollation() )== false))		// val_t==NULL means +/-infty
				res = RS_NONE;
			else if((vmin.val==NULL || vmin.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation()) == false)
					&& (vmax.val==NULL || vmax.LessEqThanMaxUTF(dpn.local_max, Type().GetCollation()) == false) )		// val_t==NULL means +/-infty
				res = RS_ALL;
			else if(pack_prefix == GetActualSize(pack) && vmin==vmax) {	// exact case for short texts
					if(vmin.GreaterEqThanMinUTF(dpn.local_min, Type().GetCollation()) && vmin.LessEqThanMaxUTF(dpn.local_min, Type().GetCollation()))
						res = RS_ALL;
					else
						res = RS_NONE;
			}
			/////////////////////////////////////////////
			if(res == RS_SOME && vmin.len >= pack_prefix && vmax.len >= pack_prefix && !RequiresUTFConversions(d.GetCollation())) {
				vmin += pack_prefix;			// redefine - shift by a common prefix
				vmax += pack_prefix;
				RSIndex_CMap *rsi_cmap = LoadRSI_CMap();
				if(rsi_cmap && rsi_cmap->UpToDate(NoObj(),pack)) {
					res = rsi_cmap->IsValue(vmin, vmax, pack);
					if(d.sharp && res == RS_ALL)
						res = RS_SOME;					// simplified version
				}
				ReleaseRSI(rsi_cmap);
			}
			if(d.op == O_NOT_BETWEEN) {
				if(res == RS_ALL)
					res = RS_NONE;
				else if(res == RS_NONE)
					res = RS_ALL;
			}
			if((dpn.no_nulls != 0  || additional_nulls_possible) && res == RS_ALL) {
				res = RS_SOME;
			}
			return res;
		} else if(PackType() == PackN) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(d.op==O_BETWEEN || d.op==O_NOT_BETWEEN);
			// O_BETWEEN or O_NOT_BETWEEN
			_int64 v1=d.val1.vc->GetValueInt64(mit);			// 1-level values; note that these values were already transformed in EncodeCondition
			_int64 v2=d.val2.vc->GetValueInt64(mit);
			if(!ATI::IsRealType(TypeName())) {
				if(v1 == MINUS_INF_64)
					v1 = dpn.local_min;
				if(v2 == PLUS_INF_64)
					v2 = dpn.local_max;
			} else {
				if(v1 == *(_int64*) &MINUS_INF_DBL)
					v1 = dpn.local_min;
				if(v2 == *(_int64*) &PLUS_INF_DBL)
					v2 = dpn.local_max;
			}

			RSValue res = RoughCheckBetween(pack, v1, v2);		// calculate as for O_BETWEEN and then consider negation
			if(d.op == O_NOT_BETWEEN) {
				if(res == RS_ALL)
					res = RS_NONE;
				else if(res == RS_NONE) {
					res = RS_ALL;
				}
			}
			if(res == RS_ALL && (dpn.no_nulls != 0 || additional_nulls_possible)) {
				res = RS_SOME;
			}
			return res;
		}
	} else {
		if(!d.encoded)
			return RS_SOME;
		SingleColumn* sc = (d.val1.vc->IsSingleColumn() ? static_cast<SingleColumn*>(d.val1.vc) : NULL);
		RCAttr* sec = NULL;
		if(sc)
			sec = dynamic_cast<RCAttr*>(sc->GetPhysical());
		if(d.IsType_AttrAttr() && d.op != O_BETWEEN && d.op != O_NOT_BETWEEN && sec) {
			RSValue res = RS_SOME;
			// special cases, not implemented yet:
			if( (TypeName()!=sec->TypeName() && 	// Exceptions:
					!(ATI::IsDateTimeType(TypeName()) && ATI::IsDateTimeType(sec->TypeName())) &&
					!(ATI::IsIntegerType(TypeName()) && ATI::IsIntegerType(sec->TypeName()))
				)
				|| Type().GetScale()!=sec->Type().GetScale() || Type().IsLookup() || sec->Type().IsLookup())
				return RS_SOME;
			LoadPackInfo();
			sec->LoadPackInfo();

			DPN const& dpn( dpns[pack] );
			DPN const& secDpn( sec->dpns[pack] );
			if(d.op != O_IN && d.op != O_NOT_IN) {
				if(PackType() == PackN && !ATI::IsRealType(TypeName())) {
					_int64 v1min = dpn.local_min;
					_int64 v1max = dpn.local_max;
					_int64 v2min = secDpn.local_min;
					_int64 v2max = secDpn.local_max;
					if(v1min >= v2max) {	// NOTE: only O_MORE, O_LESS and O_EQ are analyzed here, the rest of operators will be taken into account later (treated as negations)
						if(d.op == O_LESS || d.op == O_MORE_EQ) // the second case will be negated soon
							res = RS_NONE;
						if(v1min > v2max) {
							if(d.op == O_EQ  || d.op == O_NOT_EQ)
								res = RS_NONE;
							if(d.op == O_MORE || d.op == O_LESS_EQ)
								res = RS_ALL;
						}
					}
					if(v1max <= v2min) {
						if(d.op == O_MORE || d.op == O_LESS_EQ)	// the second case will be negated soon
							res = RS_NONE;
						if(v1max < v2min) {
							if(d.op == O_EQ  || d.op == O_NOT_EQ) 
								res = RS_NONE;
							if(d.op == O_LESS || d.op == O_MORE_EQ) 
								res = RS_ALL;
						}
					}
					if(res == RS_SOME && (d.op == O_EQ || d.op == O_NOT_EQ)) { // the second case will be negated soon
						RSIndex_Hist *rsi_hist1=LoadRSI_Hist();
						RSIndex_Hist *rsi_hist2=sec->LoadRSI_Hist();

						// check intersection possibility on histograms
						if(	rsi_hist1 && rsi_hist2
								&& rsi_hist1->UpToDate(NoObj(),pack)
								&& rsi_hist2->UpToDate(sec->NoObj(),pack)
								&& (rsi_hist1->Intersection( pack, v1min,	v1max, rsi_hist2,	pack,	v2min, v2max) == false))
							res = RS_NONE;
						ReleaseRSI(rsi_hist1);
						ReleaseRSI(rsi_hist2);
					}
					// Now take into account all negations
					if(d.op == O_NOT_EQ || d.op == O_LESS_EQ || d.op == O_MORE_EQ) {
						if(res == RS_ALL)
							res=RS_NONE;
						else if(res == RS_NONE)	
							res=RS_ALL;
					}
				} else if(PackType() == PackS) {
					RCBString v1min((char*)(&dpn.local_min), Int64StrLen(dpn.local_min), false);
					RCBString v1max((char*)(&dpn.local_max), Int64StrLen(dpn.local_max), false);
					RCBString v2min((char*)(&dpn.local_min), Int64StrLen(dpn.local_min), false);
					RCBString v2max((char*)(&dpn.local_max), Int64StrLen(dpn.local_max), false);
					if(CollationStrCmp(d.GetCollation(), v1min, v2max) >= 0) {	// NOTE: only O_MORE, O_LESS and O_EQ are analyzed here, the rest of operators will be taken into account later (treated as negations)
						if(d.op == O_LESS || d.op == O_MORE_EQ) // the second case will be negated soon
							res = RS_NONE;
						if(CollationStrCmp(d.GetCollation(), v1min, v2max) > 0) {
							if(d.op == O_EQ  || d.op == O_NOT_EQ)
								res = RS_NONE;
							if(d.op == O_MORE || d.op == O_LESS_EQ)
								res = RS_ALL;
						}
					}
					if(CollationStrCmp(d.GetCollation(), v1max, v2min) <= 0) {
						if(d.op == O_MORE || d.op == O_LESS_EQ)	// the second case will be negated soon
							res = RS_NONE;
						if(CollationStrCmp(d.GetCollation(), v1max, v2min) < 0) {
							if(d.op == O_EQ  || d.op == O_NOT_EQ) 
								res = RS_NONE;
							if(d.op == O_LESS || d.op == O_MORE_EQ) 
								res = RS_ALL;
						}
					}				
					if(d.op == O_NOT_EQ || d.op == O_LESS_EQ || d.op == O_MORE_EQ) {
						if(res == RS_ALL)
							res = RS_NONE;
						else if(res == RS_NONE)	
							res = RS_ALL;
					}
				}
			}
			// take nulls into account
			if((dpn.no_nulls != 0 || secDpn.no_nulls != 0 || additional_nulls_possible) && res == RS_ALL)
				res = RS_SOME;
			return res;
		}
	}
	return RS_SOME;
#endif
}

RSValue RCAttr::RoughCheck(int pack1, int pack2, Descriptor& d)
{
	VirtualColumn* vc1 = d.val1.vc;
	VirtualColumn* vc2 = d.val2.vc;

	// Limitations for now: only the easiest numerical cases
	if(vc1 == NULL || vc2 != NULL || d.op != O_EQ || pack1 == -1 || pack2 == -1)
		return RS_SOME;
	SingleColumn* sc = NULL;
	if(vc1->IsSingleColumn())
		sc = static_cast<SingleColumn*>(vc1);
	if(sc == NULL)
		return RS_SOME;
	RCAttr* sec = dynamic_cast<RCAttr*>(sc->GetPhysical());
	if(sec == NULL || !Type().IsNumComparable(sec->Type()))
		return RS_SOME;

	LoadPackInfo();
	sec->LoadPackInfo();
	DPN const& secDpn( sec->dpns[pack2] );
//	if(sec->Type().IsFloat())
//		return RoughCheckBetween(pack1, *(double*)(&secDpn.local_min), *(double*)(&secDpn.local_max));
	RSValue r = RoughCheckBetween(pack1, secDpn.local_min, secDpn.local_max);
	return r == RS_ALL ? RS_SOME : r;

	/*
	// Check whether two packs from two different attributes/tables may or must meet the condition
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(d.attr.tab_p->TableType()==RC_TABLE && d.val1.tab_p->TableType()==RC_TABLE);
	LoadPackInfo();
	sec->LoadPackInfo();
	RCTable *cur_t1 = (RCTable *)d.attr.tab_p;
	RCTable *cur_t2 = (RCTable *)d.val1.tab_p;
	RSIndex_PP *rsi_pp = NULL;
	bool rsi_pp_flipped = false;

	//////////////////////////////////////////////////////
	// TODO: consider O_NOT_EQ more carefully!
	if(	  d.op==O_IS_NULL || d.op==O_NOT_NULL || d.op==O_BETWEEN || d.op==O_NOT_BETWEEN || d.op==O_NOT_EQ ||
		( PackType()!=sec->PackType() && Type().IsLookup()==false && sec->Type().IsLookup()==false))
	{
		return RS_SOME;
	}
	if(	dpn.pack_file==PF_NULLS_ONLY || dpn.no_nulls-1==dpn.no_objs ||		// all objects are null
		secDpn.pack_file==PF_NULLS_ONLY || secDpn.no_nulls-1==secDpn.no_objs)
	{
		return RS_NONE;				// for all unary operators
	}
	if(d.op==O_EQ || d.op==O_NOT_EQ)	// if rsi_pp exists, then this is the most accurate information about equality joins
	{
		bool res;
		rsi_pp = cur_t1->LoadRSI_PP(cur_t2,d.attr.attr_id.n,d.val1.attr_id.n,rsi_pp_flipped);
		bool up_to_date;
		if(rsi_pp)
		{
			if(rsi_pp_flipped)
			{
				res = rsi_pp->GetValue(pack2,pack1);
				up_to_date = rsi_pp->IsUpToDate(pack2,pack1);
			}
			else
			{
				res = rsi_pp->GetValue(pack1,pack2);
				up_to_date = rsi_pp->IsUpToDate(pack1,pack2);
			}
			rsi_manager->ReleaseIndex(rsi_pp);			// release the join index
			if(up_to_date)
			{
				if((!res && d.op==O_EQ) || (res && d.op==O_NOT_EQ))
				{
					return RS_NONE;
				}
				return RS_SOME;
			}	// not up to date - try standard path (DPN etc.)
		}
	}
	if(d.op==O_LIKE || d.op==O_IN || d.op == O_NOT_LIKE || d.op==O_NOT_IN)
	{
		// ...
	}
	else if(PackType() == PackS && sec->PackType() == PackS)
	{
		if(d.op==O_EQ)
		{
			int pack1_prefix = GetPrefix(pack1).len;
			int pack2_prefix = sec->GetPrefix(pack2).len;
			int common_prefix = (pack1_prefix<=pack2_prefix ? pack1_prefix : pack2_prefix);
			if(common_prefix>0)
			{
				if(RCBString((char*)(local_min+pack1),common_prefix)!=RCBString((char*)(sec->local_min+pack2),common_prefix))
				{
					return RS_NONE;						// common prefixes does not match
				}
			}
			if(pack1_prefix==pack2_prefix)			// else cmaps must be shifted, which is too complicated as for now
			{
				RSIndex_CMap *rsi_cmap1=LoadRSI_CMap();
				RSIndex_CMap *rsi_cmap2=sec->LoadRSI_CMap();
				// check intersection possibility on histograms
				if(	rsi_cmap1 && rsi_cmap2 &&
					rsi_cmap1->UpToDate(NoObj(),pack1) &&
					rsi_cmap2->UpToDate(sec->NoObj(),pack2) )
				{
					if(rsi_cmap1->Intersection(pack1, rsi_cmap2, pack2) == false)
					{
						ReleaseRSI(rsi_cmap1);
						ReleaseRSI(rsi_cmap2);
						return RS_NONE;
					}
				}
				ReleaseRSI(rsi_cmap1);
				ReleaseRSI(rsi_cmap2);
			}
		}
		return RS_SOME;
	}
	else if(PackType() == PackS || sec->PackType() == PackS || Type().IsLookup() || sec->Type().IsLookup())
	{
		// ...
	}
	else		// both numerical
	{
		if(Type().GetScale()!=sec->Type().GetScale() || ATI::IsRealType(TypeName()) || ATI::IsRealType(sec->TypeName()))
			return RS_SOME;					// TODO: take precision/floats into account

		_int64 v1min = dpn.local_min;
		_int64 v1max = dpn.local_max;
		_int64 v2min = secDpn.local_min;
		_int64 v2max = secDpn.local_max;
		bool maybe_all=false;
		if(v1min>=v2max)
		{
			if(d.op==O_LESS) return RS_NONE;
			if(d.op==O_MORE_EQ) maybe_all=true;
			if(v1min>v2max)
			{
				if(d.op==O_LESS_EQ || d.op==O_EQ) return RS_NONE;
				if(d.op==O_MORE) maybe_all=true;
			}
		}
		if(v1max<=v2min)
		{
			if(d.op==O_MORE) return RS_NONE;
			if(d.op==O_LESS_EQ) maybe_all=true;
			if(v1max<v2min)
			{
				if(d.op==O_MORE_EQ || d.op==O_EQ) return RS_NONE;
				if(d.op==O_LESS) maybe_all=true;
			}
		}
		if(maybe_all && dpn.no_nulls==0 && secDpn.no_nulls==0)
			return RS_ALL;
		if( d.op==O_EQ)
		{
			RSIndex_Hist *rsi_hist1=LoadRSI_Hist();
			RSIndex_Hist *rsi_hist2=sec->LoadRSI_Hist();

			// check intersection possibility on histograms
			if(	rsi_hist1 && rsi_hist2 &&
				rsi_hist1->UpToDate(NoObj(),pack1) &&
				rsi_hist2->UpToDate(sec->NoObj(),pack2) )
			{
				if( rsi_hist1->Intersection(			pack1,	     dpn.local_min,		 dpn.local_max,
					rsi_hist2,	pack2,	secDpn.local_min,	secDpn.local_max) == false)
				{
					ReleaseRSI(rsi_hist1);
					ReleaseRSI(rsi_hist2);
					return RS_NONE;
				}
			}
			ReleaseRSI(rsi_hist1);
			ReleaseRSI(rsi_hist2);
		}
		return RS_SOME;
	}
	return RS_SOME;
	*/
}

RSValue RCAttr::RoughCheckBetween(int pack, _int64 v1, _int64 v2)	// check whether any value from the pack may meet the condition "... BETWEEN min AND max"
{
	RSValue res = RS_SOME;		// calculate as for O_BETWEEN and then consider negation
	bool is_float = Type().IsFloat();
	DPN const& dpn( dpns[pack] );
	if(!is_float && (v1 == PLUS_INF_64 || v2 == MINUS_INF_64)) {
		res = RS_NONE;
	} else if(is_float && (v1 == *(_int64*) &PLUS_INF_DBL || v2 == *(_int64*) &MINUS_INF_DBL)) {
		res = RS_NONE;
	} else if(!is_float && (v1 > dpn.local_max || v2 < dpn.local_min)) {
		res = RS_NONE;
	} else if(is_float && (*(double*) &v1 > *(double*) &dpn.local_max ||
			*(double*) &v2 < *(double*) &dpn.local_min)) {
		res = RS_NONE;
	} else if(!is_float && (v1 <= dpn.local_min && v2 >= dpn.local_max)) {
		res = RS_ALL;
	} else if(is_float && (*(double*) &v1 <= *(double*) &dpn.local_min &&
			*(double*) &v2 >= *(double*) &dpn.local_max)) {
		res = RS_ALL;
	} else if((!is_float && v1 > v2) || (is_float && (*(double*) &v1	> *(double*) &v2))) {
		res = RS_NONE;
	} else {
		RSIndex_Hist *rsi_hist = LoadRSI_Hist();
		if(rsi_hist && rsi_hist->UpToDate(NoObj(), pack))
			res = rsi_hist->IsValue(v1, v2, pack, dpn.local_min, dpn.local_max);
		ReleaseRSI(rsi_hist);
	}
	if(dpn.no_nulls != 0 && res == RS_ALL) {
		res = RS_SOME;
	}
	return res;
}

_int64 RCAttr::RoughMin(Filter *f, RSValue* rf)		// f == NULL is treated as full filter
{
	LoadPackInfo();
	if(PackType() == PackS)
		return MINUS_INF_64;
	if(f && f->IsEmpty() && rf == NULL)
		return 0;
	_int64 res;
	if(ATI::IsRealType(TypeName())) {
		union {double d; _int64 i;} di;
		di.d = DBL_MAX;
		for(int p=0; p < NoPack(); p++) {			// minimum of nonempty packs
			DPN const& dpn( dpns[p] );
			if((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != RS_NONE)) {
				if(di.d > *(double*)(&dpn.local_min))
					di.i = dpn.local_min;
			}
		}
		if(di.d == DBL_MAX)
			di.d = -(DBL_MAX);
		res = di.i;
	} else {
		res = PLUS_INF_64;
		for(int p=0; p < NoPack(); p++)	{		// minimum of nonempty packs
			DPN const& dpn( dpns[p] );
			if((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != RS_NONE) && res > dpn.local_min)
				res = dpn.local_min;
		}
		if(res == PLUS_INF_64)
			res = MINUS_INF_64;	//NULLS only
	}
	return res;
}

_int64 RCAttr::RoughMax(Filter *f, RSValue* rf)		// f == NULL is treated as full filter
{
	LoadPackInfo();
	if(PackType() == PackS)
		return PLUS_INF_64;
	if(f && f->IsEmpty() && rf == NULL)
		return 0;
	_int64 res;
	if(ATI::IsRealType(TypeName())) {
		union {double d; _int64 i;} di;
		di.d = -(DBL_MAX);
		for(int p=0; p < NoPack(); p++) {		// minimum of nonempty packs
			DPN const& dpn( dpns[p] );
			if((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != RS_NONE)) {
				if(di.d < *(double*)(&dpn.local_max))
					di.i = dpn.local_max;
			}
		}
		if(di.d == -(DBL_MAX))
			di.d = DBL_MAX;
		res = di.i;
	} else {
		res = MINUS_INF_64;
		for(int p=0; p < NoPack(); p++) {			// maximum of nonempty packs
			DPN const& dpn( dpns[p] );
			if((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != RS_NONE) && res < dpn.local_max)
				res = dpn.local_max;
		}
		if(res == MINUS_INF_64)
			res = PLUS_INF_64;	//NULLS only
	}
	return res;
}

vector<_int64> RCAttr::GetListOfDistinctValuesInPack(int pack)
{
	vector<_int64> list_vals;
	if(PackType() != PackN || pack == -1 || (Type().IsLookup() && RequiresUTFConversions(GetCollation())))
		return list_vals;
	DPN const& dpn( dpns[pack] );
	if(dpn.local_min == dpn.local_max) {
		list_vals.push_back(dpn.local_min);
		if(NoNulls()>0)
			list_vals.push_back(NULL_VALUE_64);
		return list_vals;
	} else if(GetPackOntologicalStatus(pack) == NULLS_ONLY) {
		list_vals.push_back(NULL_VALUE_64);
		return list_vals;
	} else if(TypeName() == RC_REAL || TypeName() == RC_FLOAT) {
		return list_vals;
	} else if(dpn.local_max - dpn.local_min > 0 && dpn.local_max - dpn.local_min < 1024) {
		RSIndex_Hist *rsi_hist = LoadRSI_Hist();
		if(rsi_hist == NULL || !rsi_hist->UpToDate(NoObj(), pack) || !rsi_hist->ExactMode(dpn.local_min, dpn.local_max)) {
			ReleaseRSI(rsi_hist);
			return list_vals;
		}
		list_vals.push_back(dpn.local_min);
		list_vals.push_back(dpn.local_max);
		for(_int64 v = dpn.local_min + 1; v < dpn.local_max; v++) {
			if(rsi_hist->IsValue(v, v, pack, dpn.local_min, dpn.local_max) != RS_NONE)
				list_vals.push_back(v);
		}
		ReleaseRSI(rsi_hist);
		if(NoNulls() > 0)
			list_vals.push_back(NULL_VALUE_64);
		return list_vals;
	}
	return list_vals;
}


_uint64 RCAttr::ApproxDistinctVals(bool incl_nulls, Filter *f, RSValue* rf, bool outer_nulls_possible)
{
	LoadPackInfo();
	_uint64 no_dist = 0;
	_int64 max_obj = NoObj();			// no more values than objects
	if(NoNulls()>0 || outer_nulls_possible) {
		if(incl_nulls)
			no_dist++;		// one value for null
		max_obj = max_obj - NoNulls() + (incl_nulls ? 1 : 0);
		if(f && max_obj > f->NoOnes())
			max_obj = f->NoOnes();
	} else if(f)
		max_obj = f->NoOnes();
	if(TypeName()==RC_DATE) {
		try {
			RCDateTime date_min(RoughMin(f, rf), RC_DATE);
			RCDateTime date_max(RoughMax(f, rf), RC_DATE);
			no_dist += (date_max - date_min) + 1;			// overloaded minus - a number of days between dates
		} catch(...) {	// in case of any problems with conversion of dates - just numerical approximation
			no_dist += RoughMax(f, rf) - RoughMin(f, rf) + 1;
		}
	} else if(TypeName()==RC_YEAR) {
		try {
			RCDateTime date_min(RoughMin(f, rf), RC_YEAR);
			RCDateTime date_max(RoughMax(f, rf), RC_YEAR);
			no_dist += ((int)(date_max.Year()) - date_min.Year()) + 1;
		} catch(...) {	// in case of any problems with conversion of dates - just numerical approximation
			no_dist += RoughMax(f, rf) - RoughMin(f, rf) + 1;
		}
	} else if(PackType()==PackN && TypeName()!=RC_REAL && TypeName()!=RC_FLOAT) {
		_int64 cur_min = RoughMin(f, rf);		// extrema of nonempty packs
		_int64 cur_max = RoughMax(f, rf);
		_uint64 span = cur_max - cur_min + 1;
		if(span < 100000) {				// use Histograms to calculate exact number of distinct values
			RSIndex_Hist *rsi_hist=LoadRSI_Hist();
			Filter values_present(span);
			values_present.Reset();
			for(int p = 0; p < NoPack(); p++) {
				DPN const& dpn( dpns[p] );
				if((f == NULL || !f->IsEmpty(p)) && (rf == NULL || rf[p] != RS_NONE) && dpn.local_min <= dpn.local_max) {
					// dpn.local_min <= dpn.local_max is not true e.g. when the pack contains nulls only
					if(rsi_hist && rsi_hist->UpToDate(NoObj(), p) && rsi_hist->ExactMode(dpn.local_min, dpn.local_max)) {
						values_present.Set(dpn.local_min - cur_min);
						values_present.Set(dpn.local_max - cur_min);
						for(_int64 v = dpn.local_min + 1; v < dpn.local_max; v++)
							if(rsi_hist->IsValue(v, v, p, dpn.local_min, dpn.local_max) != RS_NONE) {
								values_present.Set(v - cur_min);
							}
					} else {		// no Histogram or not exact: mark the whole interval
						values_present.SetBetween(dpn.local_min - cur_min, dpn.local_max - cur_min);
					}
				}
				if(values_present.IsFull())
					break;
			}
			ReleaseRSI(rsi_hist);
			no_dist += values_present.NoOnes();
		} else
			no_dist += span;		// span between min and max
	} else if(TypeName() == RC_REAL || TypeName() == RC_FLOAT) {
		_int64 cur_min = RoughMin(f, rf);		// extrema of nonempty packs
		_int64 cur_max = RoughMax(f, rf);
		if(cur_min == cur_max && cur_min != NULL_VALUE_64)		// the only case we can do anything
			no_dist += 1;
		else
			no_dist = max_obj;
	} else if(TypeName() == RC_STRING || TypeName() == RC_VARCHAR) {
		int max_len = 0;
		for(int p = 0; p < NoPack(); p++) {			// max len of nonempty packs
			if(f == NULL || !f->IsEmpty(p))
				max_len = max(max_len, int(GetActualSize(p)));
		}
		if(max_len > 0 && max_len < 6)
			no_dist += _int64(256) << ((max_len - 1) * 8);
		else if(max_len > 0)
			no_dist = max_obj;			// default
	} else
		no_dist = max_obj;			// default

	if(no_dist > (_uint64)max_obj)
		return max_obj;
	return no_dist;
}

_uint64 RCAttr::ExactDistinctVals(Filter* f)			// provide the exact number of diff. non-null values, if possible, or NULL_VALUE_64
{
	if(f == NULL)				// no exact information about tuples => nothing can be determined for sure
		return NULL_VALUE_64;
	LoadPackInfo();
	if(Type().IsLookup() && !RequiresUTFConversions(GetCollation()) && f->IsFull())
		return RoughMax(NULL) + 1;
	bool nulls_only = true;
	for(int p = 0; p < NoPack(); p++)
		if(!f->IsEmpty(p) && GetPackOntologicalStatus(p) != NULLS_ONLY) {
			nulls_only = false;
			break;
		}
	if(nulls_only)
		return 0;
	if(PackType() == PackN && !RequiresUTFConversions(GetCollation()) && TypeName() != RC_REAL && TypeName() != RC_FLOAT) {
		_int64 cur_min = RoughMin(f);		// extrema of nonempty packs
		_int64 cur_max = RoughMax(f);
		_uint64 span = cur_max - cur_min + 1;
		if(span < 100000) {				// use Histograms to calculate exact number of distinct values
			RSIndex_Hist *rsi_hist = LoadRSI_Hist();
			Filter values_present(span);
			values_present.Reset();
			// Phase 1: mark all values, which are present for sure
			for(int p = 0; p < NoPack(); p++) if(f->IsFull(p)) {
				DPN const& dpn( dpns[p] );
				if(dpn.local_min < dpn.local_max) {
					// dpn.local_min <= dpn.local_max is not true when the pack contains nulls only
					if(rsi_hist && rsi_hist->UpToDate(NoObj(), p) && rsi_hist->ExactMode(dpn.local_min, dpn.local_max)) {
						values_present.Set(dpn.local_min - cur_min);
						values_present.Set(dpn.local_max - cur_min);
						for(_int64 v = dpn.local_min + 1; v < dpn.local_max; v++)
							if(rsi_hist->IsValue(v, v, p, dpn.local_min, dpn.local_max) != RS_NONE) {
								values_present.Set(v - cur_min);
							}
					} else { 		// no Histogram or not exact: cannot calculate exact number
						ReleaseRSI(rsi_hist);
						return NULL_VALUE_64;
					}
				} else if(dpn.local_min == dpn.local_max) {	// only one value
					values_present.Set(dpn.local_min - cur_min);
				}
				if(values_present.IsFull())
					break;
			}
			// Phase 2: check whether there are any other values possible in suspected packs
			for(int p = 0; p < NoPack(); p++) {
				if(!f->IsEmpty(p) && !f->IsFull(p)) {		// suspected pack
					DPN const& dpn( dpns[p] );
					if(!values_present.IsFullBetween(dpn.local_min - cur_min, dpn.local_max - cur_min)) {
						ReleaseRSI(rsi_hist);	// not full between => there is a new value possible
						return NULL_VALUE_64;
					}
				}
			}
			ReleaseRSI(rsi_hist);
			return values_present.NoOnes();
		}
	}
	return NULL_VALUE_64;
}

double RCAttr::RoughSelectivity()
{
	if(rough_selectivity == -1) {
		LoadPackInfo();
		if(PackType() == PackN && TypeName() != RC_REAL && TypeName() != RC_FLOAT && NoPack() > 0) {
			_int64 global_min = PLUS_INF_64;
			_int64 global_max = MINUS_INF_64;
			double width_sum = 0;
			for(int p = 0; p < NoPack(); p++) {			// minimum of nonempty packs
				DPN const& dpn( dpns[p] );
				if(dpn.no_nulls == uint(dpn.no_objs) + 1)
					continue;
				if(dpn.local_min < global_min)
					global_min = dpn.local_min;
				if(dpn.local_max > global_max)
					global_max = dpn.local_max;
				width_sum += double(dpn.local_max) - double(dpn.local_min) + 1;
			}
			rough_selectivity =  (width_sum / NoPack()) / (double(global_max) - double(global_min) + 1);
		} else
			rough_selectivity = 1;
	}
	return rough_selectivity;
}

void RCAttr::GetTextStat(TextStat &s, Filter *f)
{
	bool success = false;
	LoadPackInfo();
	if(PackType() == PackS && !RequiresUTFConversions(GetCollation())) {
		RSIndex_CMap *rsi_cmap = LoadRSI_CMap();
		if(rsi_cmap) {
			success = true;
			for(int p = 0; p < NoPack(); p++) if(f == NULL || !f->IsEmpty(p)) {
				if(!rsi_cmap->UpToDate(NoObj(), p)) {
					success = false;
					break;
				}
				DPN const& dpn(dpns[p]);
				if(dpn.pack_file==PF_NULLS_ONLY || dpn.no_objs == dpn.no_nulls - 1)
					continue;
				int len = GetActualSize(p);
				if(len > 48) {
					success = false;
					break;
				}
				int pack_prefix = GetPrefix(p).len;
				int i = 0;
				uchar *prefix = (uchar *)(&(dpn.local_min));
				for(i = 0; i < pack_prefix; i++)
					s.AddChar(prefix[i], i);
				for(i = pack_prefix; i < len; i++) {
					s.AddLen(i);		// end of value is always possible (except a prefix)
					for(int c = 0; c < 256; c++)
						if(rsi_cmap->IsSet(p, c, i - pack_prefix))
							s.AddChar(c, i);
				}
				s.AddLen(len);
				if(p % 16 == 15)		// check it from time to time
					s.CheckIfCreatePossible();
				if(!s.IsValid())
					break;
			}
		}
		ReleaseRSI(rsi_cmap);
	}
	if(success == false)
		s.Invalidate();
}


//////////////////////////// RSI maintenance //////////////////////////////////////////////////

void RCAttr::ReleaseRSI(RSIndex *rsi)			// reading version only
{
	if(rsi_manager && rsi)	rsi_manager->ReleaseIndex(rsi);
}

RSIndex_Hist *RCAttr::LoadRSI_Hist()					// remember to use ReleaseRSI() after use
{
	if(rsi_manager==NULL) return NULL;
	return (RSIndex_Hist*)rsi_manager->GetIndex(RSIndexID(RSI_HIST,table_number,attr_number),GetCurReadLocation());
}

RSIndex_CMap *RCAttr::LoadRSI_CMap()					// remember to use ReleaseRSI() after use
{
	if(rsi_manager==NULL) return NULL;
	return (RSIndex_CMap*)rsi_manager->GetIndex(RSIndexID(RSI_CMAP,table_number,attr_number),GetCurReadLocation());
}


void RCAttr::SaveRSI()			// make all RSI up to date
{
	try {
		if(rsi_hist_update)
			rsi_manager->UpdateIndex(rsi_hist_update, GetCurSaveLocation());		// save updates and unlock
		if(rsi_cmap_update)
			rsi_manager->UpdateIndex(rsi_cmap_update, GetCurSaveLocation());
	} catch ( ... ) {
		rsi_hist_update=NULL;
		rsi_cmap_update=NULL;
		throw;
	}
	rsi_hist_update=NULL;
	rsi_cmap_update=NULL;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void RCAttr::RoughStats(double& hist_density, int& trivial_packs, double& span)		// calculate the number of 1's in histograms and other KN stats
{
	int npack = NoPack();
	hist_density = -1;
	trivial_packs = 0;
	span = -1;
	LoadPackInfo();
	for(int pack = 0; pack < npack; pack++) {
		DPN const& dpn( dpns[pack] );
		if(dpn.pack_file == PF_NULLS_ONLY || dpn.no_objs == dpn.no_nulls - 1 || (PackType() == PackN && dpn.no_nulls == 0 && dpn.local_min == dpn.local_max))
			trivial_packs++;
		else {
			if(PackType()==PackN && !ATI::IsRealType(TypeName())) {
				if(span == -1)
					span = 0;
				span += dpn.local_max - dpn.local_min;
			}
			if(PackType()==PackN && ATI::IsRealType(TypeName())) {
				if(span == -1) 
					span = 0;
				span += *(double*)(&(dpn.local_max)) - *(double*)(&(dpn.local_min));
			}
		}
	}
	if(span != -1) {
		_int64 tmp_min = GetMinInt64();			// always a value - i_min from RCAttr
		_int64 tmp_max = GetMaxInt64();
		if(ATI::IsRealType(TypeName()))
			span = (span / (npack - trivial_packs)) / (*(double*)(&tmp_max) - *(double*)(&tmp_min));
		else
			span = (span / (npack - trivial_packs)) / double(GetMaxInt64() - GetMinInt64());
	}
	RSIndex_Hist *rsi_hist=LoadRSI_Hist();
	if(rsi_hist && PackType() == PackN) {
		int ones_found = 0, ones_needed = 0;
		for(int pack = 0; pack < npack; pack++) {
			DPN const& dpn( dpns[pack] );
			if(dpn.no_objs + 1 != dpn.no_nulls && dpn.local_min + 1 < dpn.local_max) {
				int loc_no_ones;
				if(dpn.local_max - dpn.local_min > 1024)
					loc_no_ones = 1024;
				else
					loc_no_ones = int(dpn.local_max - dpn.local_min - 1);
				ones_needed += loc_no_ones;
				ones_found += rsi_hist->NoOnes(pack, loc_no_ones);
			}
		}
		if(ones_needed > 0)
			hist_density = ones_found / double(ones_needed);
		ReleaseRSI(rsi_hist);
	}
}

void RCAttr::DisplayAttrStats(Filter *f)		// filter is for # of objects
{
	int npack = NoPack();
	LoadPackInfo();
	rccontrol.lock() << "Column " << attr_number << ", table " << table_number
					 << ( IsUnique() ? ", unique" : " ") << unlock;
	if(PackType()==PackN)
		rccontrol.lock() << "Pack    Rows  Nulls              Min              Max             Sum  Hist. " << unlock;
	else
		rccontrol.lock() << "Pack    Rows  Nulls       Min       Max  Size CMap " << unlock;
	// This line is 79 char. width:
	rccontrol.lock() <<     "-------------------------------------------------------------------------------" << unlock;
	_int64 cur_obj;
	char line_buf[150];
	RSIndex_Hist *rsi_hist = NULL;
	RSIndex_CMap *rsi_cmap = NULL;
	if(PackType()==PackN)
		rsi_hist = LoadRSI_Hist();
	if(PackType()==PackS)
		rsi_cmap = LoadRSI_CMap();
	for(int pack = 0; pack < npack; pack++)
	{
		cur_obj = 65536;
		DPN const& dpn( dpns[pack] );
		if(f)
			cur_obj = f->NoOnes(pack);
		else if(pack == npack - 1)
			cur_obj = dpn.no_objs + 1;

		sprintf(line_buf, "%-7d %5lld %5d"
						, pack, cur_obj, dpn.no_nulls);

		if(dpn.no_objs+1 != dpn.no_nulls) {
			if(PackType()==PackN && !ATI::IsRealType(TypeName())) {
				int rsi_span = -1;
				int rsi_ones = 0;
				if(rsi_hist) {
					rsi_span = 0;
					rsi_ones = 0;
					if(dpn.local_max > dpn.local_min + 1) {
						if(rsi_hist->ExactMode(dpn.local_min, dpn.local_max))
							rsi_span = int(dpn.local_max-dpn.local_min-1);
						else
							rsi_span = 1024;
						rsi_ones = rsi_hist->NoOnes(pack, rsi_span);
					}
				}
				sprintf(line_buf + strlen(line_buf), " %16lld %16lld"
								, GetMinInt64(pack), GetMaxInt64(pack));
				bool nonnegative = false;	// not used anyway
				_int64 loc_sum = GetSum(pack, nonnegative);
				if(loc_sum != NULL_VALUE_64)
					sprintf(line_buf + strlen(line_buf), " %15lld", loc_sum);
				else
					sprintf(line_buf + strlen(line_buf), "             n/a");
				if(rsi_span >= 0)
					sprintf(line_buf + strlen(line_buf), "  %d/%d", rsi_ones, rsi_span);
				if(!rsi_hist)
					sprintf(line_buf + strlen(line_buf), "   n/a");
			} else if(PackType()==PackN && ATI::IsRealType(TypeName())) {
				bool dummy;
				_int64 sum_d = GetSum(pack, dummy);
				sprintf(line_buf + strlen(line_buf), " %16g %16g %15g  -", 
					*((double*)(&dpn.local_min)), *((double*)(&dpn.local_max)), *((double*)(&sum_d)));
			} else {		// PackType = PackS
				char smin[10];
				char smax[10];
				memcpy(smin, &dpn.local_min, 8);
				memcpy(smax, &dpn.local_max, 8);
				smin[8]='\0';
				smax[8]='\0';
				sprintf(line_buf + strlen(line_buf), "  %8s  %8s %5d ", smin, smax, GetActualSize(pack));
				if(rsi_cmap && !RequiresUTFConversions(Type().GetCollation())) {
					int len = rsi_cmap->NoPositions();
					int i;
					for(i = 0; i < len && i < 8; i++) {	// display at most 8 positions (limited by line width)
						if((i == len - 1) || (i == 7))
							sprintf(line_buf + strlen(line_buf), "%d", rsi_cmap->NoOnes(pack, i));
						else
							sprintf(line_buf + strlen(line_buf), "%d-", rsi_cmap->NoOnes(pack, i));
					}
					if(i < len)
						sprintf(line_buf + strlen(line_buf), "...");
				}
				else
					sprintf(line_buf + strlen(line_buf), "n/a");
			}
		}
		rccontrol.lock() << line_buf << unlock;
	}
	rccontrol.lock() <<     "-------------------------------------------------------------------------------" << unlock;
	if(rsi_hist)
		ReleaseRSI(rsi_hist);
	if(rsi_cmap)
		ReleaseRSI(rsi_cmap);
}
