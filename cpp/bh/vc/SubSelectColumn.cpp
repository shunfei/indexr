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
#include "SubSelectColumn.h"
#include "core/MysqlExpression.h"
#include "ConstColumn.h"
#include "core/RCAttr.h"
#include "core/ValueSet.h"
#include "boost/make_shared.hpp"

using namespace std;
using namespace boost;

SubSelectColumn::SubSelectColumn(TempTable* subq, MultiIndex* mind, TempTable* temp_table, int temp_table_alias)
	:	MultiValColumn(subq->GetColumnType(0), mind), subq(static_pointer_cast<TempTableForSubquery>(subq->shared_from_this())),
		parent_tt_alias(temp_table_alias), min_max_uptodate(false), no_cached_values(0)
{
	const vector<JustATable*>* tables = &temp_table->GetTables();
	const vector<int>* aliases = &temp_table->GetAliases();

	col_idx = 0;
	for(uint i = 0; i < subq->NoAttrs(); i++)
		if(subq->IsDisplayAttr(i)) {
			col_idx = i;
			break;
		}
	ct = subq->GetColumnType(col_idx);
	MysqlExpression::SetOfVars all_params;

	for(uint i = 0; i < subq->NoVirtColumns(); i++) {
		MysqlExpression::SetOfVars params = subq->GetVirtualColumn(i)->GetParams();
		all_params.insert(params.begin(), params.end());
		MysqlExpression::bhfields_cache_t bhfields = subq->GetVirtualColumn(i)->GetBHItems();
		if(!bhfields.empty()) {
			for(MysqlExpression::bhfields_cache_t::const_iterator bhfield = bhfields.begin(); bhfield != bhfields.end(); ++bhfield) {
				MysqlExpression::bhfields_cache_t::iterator bhitem = bhitems.find(bhfield->first) ;
				if(bhitem == bhitems.end())
					bhitems.insert(*bhfield);
				else {
					bhitem->second.insert(bhfield->second.begin(), bhfield->second.end());
				}
			}
		}
	}

	for(MysqlExpression::SetOfVars::iterator iter = all_params.begin(); iter != all_params.end(); ++iter) {
		vector<int>::const_iterator ndx_it = find(aliases->begin(), aliases->end(), iter->tab);
		if(ndx_it != aliases->end()) {
			int ndx = (int)distance(aliases->begin(), ndx_it);

			var_map.push_back(VarMap(*iter,(*tables)[ndx], ndx));
			var_types[*iter] = (*tables)[ndx]->GetColumnType(var_map[var_map.size()-1].col_ndx);
			var_buf_for_exact[*iter] = std::vector<MysqlExpression::value_or_null_info_t>(); //now empty, pointers inserted by SetBufs()
		} else if(iter->tab == temp_table_alias) {
			var_map.push_back(VarMap(*iter, temp_table, 0));
			var_types[*iter] = temp_table->GetColumnType(var_map[var_map.size()-1].col_ndx);
			var_buf_for_exact[*iter] = std::vector<MysqlExpression::value_or_null_info_t>(); //now empty, pointers inserted by SetBufs()
		} else {
			//parameter
			params.insert(*iter);
		}
	}
	SetBufs(&var_buf_for_exact);
	var_buf_for_rough = var_buf_for_exact;

	if(var_map.size() == 1)
		dim = var_map[0].dim;
	else
		dim = -1;
	first_eval_for_rough = true;
	out_of_date_rough = true;
}

SubSelectColumn::SubSelectColumn(const SubSelectColumn& c) : MultiValColumn(c),
		col_idx(c.col_idx), subq(c.subq), min(c.min), max(c.max), min_max_uptodate(c.min_max_uptodate),
		expected_type(c.expected_type), var_buf_for_exact(), 
		no_cached_values(c.no_cached_values),
		out_of_date_rough(c.out_of_date_rough)
{
	var_types.empty();
	if(c.cache.get()) {
		cache = make_shared<ValueSet>(*c.cache.get());
		//ValueSet* vs = new ValueSet(*c.cache.get());
		//cache = shared_ptr<ValueSet>(vs);
	}

}


SubSelectColumn::~SubSelectColumn( void )
{
//	if(!!table)
//			table->TranslateBackVCs();
//
//	if(!!table_for_rough)
//		table_for_rough->TranslateBackVCs();
//	if(table != subq) {
//		table.reset();
//		table_for_rough.reset();
//	}
}

void SubSelectColumn::SetBufs(MysqlExpression::var_buf_t* bufs)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	assert(bufs);
	for( MysqlExpression::bhfields_cache_t::iterator it = bhitems.begin(); it != bhitems.end(); ++ it ) {
		MysqlExpression::var_buf_t::iterator buf_set = bufs->find(it->first);
		if(buf_set != bufs->end()) {
			//for each bhitem* in the set it->second put its buffer to buf_set.second
			for(std::set<Item_bhfield*>::iterator bhfield = it->second.begin(); bhfield != it->second.end(); ++bhfield) {
				ValueOrNull* von;
				(*bhfield)->SetBuf(von);
				buf_set->second.push_back(MysqlExpression::value_or_null_info_t(ValueOrNull(), von));
			}
		}
	}
#endif
}

RCBString SubSelectColumn::DoGetMinString(const MIIterator &mit)
{
	RCBString s;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return s;
}

RCBString SubSelectColumn::DoGetMaxString(const MIIterator &mit)
{
	RCBString s;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return s;
}

ushort SubSelectColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return ct.GetPrecision();
}

PackOntologicalStatus SubSelectColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	return NORMAL;
}

void SubSelectColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	assert(0);
}

BHTribool SubSelectColumn::DoContains(MIIterator const& mit, RCDataType const& v)
{
	PrepareSubqResult(mit, false);
	BHTribool res = false;
	if(!cache) {
		cache = shared_ptr<ValueSet>(new ValueSet());
		cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
	}

	if(cache->Contains(v, GetCollation()))
		return true;
	else if(cache->ContainsNulls())
		res = BHTRIBOOL_UNKNOWN;

	bool added_new_value = false;
	if(RequiresUTFConversions(GetCollation()) && Type().IsString()) {
		for(_int64 i = no_cached_values; i < subq->NoObj(); i++) {
			no_cached_values++;
			added_new_value = true;
			if(subq->IsNull(i, col_idx)) {
				cache->AddNull();
				res = BHTRIBOOL_UNKNOWN;
			} else {
				RCBString val;
				subq->GetTableString(val, i, col_idx);
				val.MakePersistent();
				cache->Add(val);
				if(CollationStrCmp(GetCollation(), val, v.ToRCString()) == 0) {
					res = true;
					break;
				}
			}
		}
	} else {
		for(_int64 i = no_cached_values; i < subq->NoObj(); i++) {
			no_cached_values++;
			added_new_value = true;
			if(subq->IsNull(i, col_idx)) {
				cache->AddNull();
				res = BHTRIBOOL_UNKNOWN;
			} else {
				RCValueObject value;
				if(Type().IsString()) {
					RCBString s ;
					subq->GetTableString(s, i, col_idx);
					value = s;
					static_cast<RCBString*>(value.Get())->MakePersistent();
				} else
					value = subq->GetTable(i, col_idx);
				cache->Add(value);
				if(value == v) {
					res = true;
					break;
				}
			}
		}
	}
	if(added_new_value && no_cached_values == subq->NoObj())
		cache->ForcePreparation();				// completed for the first time => recalculate cache
	return res;
}

void SubSelectColumn::PrepareAndFillCache()
{
	if(!cache) {
		cache = shared_ptr<ValueSet>(new ValueSet());
		cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
	}
	for(_int64 i = no_cached_values; i < subq->NoObj(); i++) {
		no_cached_values++;
		if(subq->IsNull(i, col_idx)) {
			cache->AddNull();
		} else {
			RCValueObject value;
			if(Type().IsString()) {
				RCBString s ;
				subq->GetTableString(s, i, col_idx);
				value = s;
				static_cast<RCBString*>(value.Get())->MakePersistent();
			} else
				value = subq->GetTable(i, col_idx);
			cache->Add(value);
		}
	}
	cache->ForcePreparation();
	cache->Prepare(Type().GetTypeName(), Type().GetScale(), GetCollation());
}

bool SubSelectColumn::IsSetEncoded(AttributeType at, int scale)	// checks whether the set is constant and fixed size equal to the given one
{
	if(!cache || !cache->EasyMode() || !subq->IsMaterialized() || no_cached_values < subq->NoObj())
		return false;
	return (scale == ct.GetScale() &&
			(at == expected_type.GetTypeName() ||
				(ATI::IsFixedNumericType(at) && ATI::IsFixedNumericType(expected_type.GetTypeName()))));
}

BHTribool SubSelectColumn::DoContains64(const MIIterator& mit, _int64 val)			// easy case for integers
{
	if(cache && cache->EasyMode()) {
		BHTribool contains = false;
		if(val == NULL_VALUE_64)
			return BHTRIBOOL_UNKNOWN;
		if(cache->Contains(val))
			contains = true;
		else if(cache->ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
		return contains;
	}
	return DoContains(mit, RCNum(val, ct.GetScale()));
}

BHTribool SubSelectColumn::DoContainsString(const MIIterator& mit, RCBString &val)			// easy case for strings
{
	if(cache && cache->EasyMode()) {
		BHTribool contains = false;
		if(val.IsNull())
			return BHTRIBOOL_UNKNOWN;
		if(cache->Contains(val))
			contains = true;
		else if(cache->ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
		return contains;
	}
	return DoContains(mit, val);
}

_int64 SubSelectColumn::DoNoValues(MIIterator const& mit)
{
	PrepareSubqResult(mit, false);
	if(!subq->IsMaterialized())
		subq->Materialize();
	return subq->NoObj();
}

_int64 SubSelectColumn::DoAtLeastNoDistinctValues(MIIterator const& mit, _int64 const at_least)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(at_least > 0);
	PrepareSubqResult(mit, false);
	ValueSet vals;
	vals.Prepare(expected_type.GetTypeName(), expected_type.GetScale(), expected_type.GetCollation());
	if(RequiresUTFConversions(GetCollation()) && Type().IsString()) {
		RCBString buf(NULL, CollationBufLen(GetCollation(), subq->MaxStringSize(col_idx)), true);
		for(_int64 i = 0; vals.NoVals() < at_least && i < subq->NoObj(); i++) {
			if(!subq->IsNull(i, col_idx)) {
				RCBString s;
				subq->GetTable_S(s, i, col_idx);
				ConvertToBinaryForm(s, buf, GetCollation());
				vals.Add(buf);
			}
		}
	} else {	
		for(_int64 i = 0; vals.NoVals() < at_least && i < subq->NoObj(); i++) {
			if(!subq->IsNull(i, col_idx)) {
				RCValueObject val = subq->GetTable(i, col_idx);
				vals.Add(val);
			}
		}
	}
	return vals.NoVals();
}

bool SubSelectColumn::DoContainsNull(const MIIterator& mit)
{
	PrepareSubqResult(mit, false);
	for(_int64 i = 0; i < subq->NoObj(); ++i)
		if(subq->IsNull(i, col_idx))
			return true;
	return false;
}

MultiValColumn::Iterator::impl_t SubSelectColumn::DoBegin(MIIterator const& mit)
{
	PrepareSubqResult(mit, false);
	return MultiValColumn::Iterator::impl_t(new IteratorImpl(subq->begin(), expected_type));
}

MultiValColumn::Iterator::impl_t SubSelectColumn::DoEnd(MIIterator const& mit)
{
	PrepareSubqResult(mit, false);
	return MultiValColumn::Iterator::impl_t(new IteratorImpl(subq->end(), expected_type));
}

void SubSelectColumn::RequestEval(const MIIterator& mit, const int tta)
{
	first_eval = true;
	first_eval_for_rough = true;
	for(uint i = 0; i < subq->NoVirtColumns(); i++)
		subq->GetVirtualColumn(i)->RequestEval(mit, tta);
}


void SubSelectColumn::PrepareSubqResult(const MIIterator& mit, bool exists_only)
{
	MEASURE_FET("SubSelectColumn::PrepareSubqCopy(...)");
	bool cor = IsCorrelated();
	if(!cor) {
		if(subq->IsFullyMaterialized())
			return;
	}

	subq->CreateTemplateIfNotExists();

	if(cor && (FeedArguments(mit, false) )) {
		cache.reset();
		no_cached_values = 0;
		subq->ResetToTemplate(false);
		subq->ResetVCStatistics();
		subq->SuspendDisplay();
		try {
			subq->ProcessParameters(mit, parent_tt_alias);		// exists_only is not needed here (limit already set)
		} catch(...) {
			subq->ResumeDisplay();
			throw;
		}
		subq->ResumeDisplay();
	}
	subq->SuspendDisplay();
	try {
		if(exists_only)
			subq->SetMode(TM_EXISTS);
		subq->Materialize(cor);
	} catch(...) {
		subq->ResumeDisplay();
		//subq->ResetForSubq();
		throw;
	}
	subq->ResumeDisplay();
}

void SubSelectColumn::RoughPrepareSubqCopy(const MIIterator& mit, SubSelectOptimizationType sot)
{
	MEASURE_FET("SubSelectColumn::RoughPrepareSubqCopy(...)");
	subq->CreateTemplateIfNotExists();
	if((!IsCorrelated() && first_eval_for_rough) ||
		(IsCorrelated() && (FeedArguments(mit, true) ))) {
		out_of_date_rough = false;
//		cache.reset();
//		no_cached_values = 0;
		subq->ResetToTemplate(true);
		subq->ResetVCStatistics();
		subq->SuspendDisplay();
		try {
			subq->RoughProcessParameters(mit, parent_tt_alias);
		} catch(...) {
			subq->ResumeDisplay();
		}
		subq->ResumeDisplay();
	}
	subq->SuspendDisplay();
	try {
		subq->RoughMaterialize(true);
	} catch(...) {}
	subq->ResumeDisplay();
}

bool SubSelectColumn::IsCorrelated() const
{
	if(var_map.size() || params.size())
		return true;
	return false;
}

bool SubSelectColumn::DoIsNull(const MIIterator& mit)
{
	PrepareSubqResult(mit, false);
	return subq->IsNull(0, col_idx);
}

RCValueObject SubSelectColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num)
{
	PrepareSubqResult(mit, false);
	RCValueObject val = subq->GetTable(0, col_idx);
	if(expected_type.IsString())
		return val.ToRCString();
	if(expected_type.IsNumeric() && ATI::IsStringType(val.Type())) {
		RCNum rc;
		RCNum::Parse(*static_cast<RCBString*>(val.Get()), rc, expected_type.GetTypeName());
		val = rc;	
	}
	return val;
}

_int64 SubSelectColumn::DoGetValueInt64(const MIIterator& mit)
{
	PrepareSubqResult(mit, false);
	return subq->GetTable64(0, col_idx);
}

RoughValue SubSelectColumn::RoughGetValue(const MIIterator& mit, SubSelectOptimizationType sot)
{
	RoughPrepareSubqCopy(mit, sot);
	return RoughValue(subq->GetTable64(0, col_idx), subq->GetTable64(1, col_idx));
}

double SubSelectColumn::DoGetValueDouble(const MIIterator& mit)
{
	PrepareSubqResult(mit, false);
	_int64 v = subq->GetTable64(0, col_idx);
	return *((double*)(&v));
}

void SubSelectColumn::DoGetValueString(RCBString& s, MIIterator const& mit)
{
	PrepareSubqResult(mit, false);
	subq->GetTable_S(s, 0, col_idx);
}

RCValueObject SubSelectColumn::DoGetSetMin(MIIterator const& mit)
{
	// assert: this->params are all set
	PrepareSubqResult(mit, false);
	if(!min_max_uptodate)
		CalculateMinMax();
	return min;
}

RCValueObject SubSelectColumn::DoGetSetMax(MIIterator const& mit)
{
	PrepareSubqResult(mit, false);
	if(!min_max_uptodate)
		CalculateMinMax();
	return max;
}
bool SubSelectColumn::CheckExists(MIIterator const& mit)
{
	PrepareSubqResult(mit, true);		// true: exists_only
	return subq->NoObj() > 0;
}

bool SubSelectColumn::DoIsEmpty(MIIterator const& mit)
{
	PrepareSubqResult(mit, false);
	return subq->NoObj() == 0;
}

BHTribool SubSelectColumn::RoughIsEmpty(MIIterator const& mit, SubSelectOptimizationType sot)
{
	RoughPrepareSubqCopy(mit, sot);
	return subq->RoughIsEmpty();
}

void SubSelectColumn::CalculateMinMax()
{
	if(!subq->IsMaterialized())
		subq->Materialize();
	if(subq->NoObj() == 0) {
		min = max = RCValueObject();
		min_max_uptodate = true;
		return;
	}

	min = max = RCValueObject();
	bool found_not_null = false;
	if(RequiresUTFConversions(GetCollation()) && Type().IsString() && expected_type.IsString()) {
		RCBString val_s, min_s, max_s;
		for(_int64 i = 0; i < subq->NoObj(); i++) {
			subq->GetTable_S(val_s, i, col_idx);
			if(val_s.IsNull())
				continue;
			if(!found_not_null && !val_s.IsNull()) {
				found_not_null = true;
				min_s.PersistentCopy(val_s);
				max_s.PersistentCopy(val_s);
				continue;
			}
			if(CollationStrCmp(GetCollation(), val_s, max_s) > 0) {
				max_s.PersistentCopy(val_s);
			} else if(CollationStrCmp(GetCollation(), val_s, min_s) < 0) {
				min_s.PersistentCopy(val_s);
			}
		}
		min = min_s;
		max = max_s;
	} else {
		RCValueObject val;
		for(_int64 i = 0; i < subq->NoObj(); i++) {
			val = subq->GetTable(i, col_idx);
			if(expected_type.IsString()) {
				val = val.ToRCString();
				static_cast<RCBString*>(val.Get())->MakePersistent();
			}
			else if(expected_type.IsNumeric() && ATI::IsStringType(val.Type())) {
				RCNum rc;
				RCNum::Parse(*static_cast<RCBString*>(val.Get()), rc, expected_type.GetTypeName());
				val = rc;
			}

			if(!found_not_null && !val.IsNull()) {
				found_not_null = true;
				min = max = val;
				continue;
			}
			if(val > max) {
				max = val;
			} else if(val < min) {
				min = val;
			}
		}
	}
	min_max_uptodate = true;
}

bool SubSelectColumn::FeedArguments(const MIIterator& mit, bool for_rough)
{
	MEASURE_FET("SubSelectColumn::FeedArguments(...)");

	bool diff;
	if(for_rough) {
		diff = first_eval_for_rough;
		for(vector<VarMap>::const_iterator iter = var_map.begin(); iter != var_map.end(); ++iter) {
			ValueOrNull v = iter->GetTabPtr()->GetComplexValue(mit[iter->dim], iter->col_ndx);
			MysqlExpression::var_buf_t::iterator cache = var_buf_for_rough.find(iter->var);
			v.MakeStringOwner();
			if(cache->second.empty()) {  //empty if IBexpression - feeding unnecessary
				bool ldiff = v != param_cache_for_rough[iter->var];
				diff = diff || ldiff;
				if(ldiff)
					param_cache_for_rough[iter->var] = v;
			} else {
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT( cache != var_buf_for_rough.end() );
				diff = diff || (v != cache->second.begin()->first);
				if(diff)
					for(std::vector<MysqlExpression::value_or_null_info_t>::iterator val_it = cache->second.begin(); val_it  != cache->second.end(); ++val_it)
						*((*val_it ).second) = (*val_it ).first = v;
			}
		}
		first_eval_for_rough = false;
	} else {
		diff = first_eval;
		for(vector<VarMap>::const_iterator iter = var_map.begin(); iter != var_map.end(); ++iter) {
			ValueOrNull v = iter->GetTabPtr()->GetComplexValue(mit[iter->dim], iter->col_ndx);
			MysqlExpression::var_buf_t::iterator cache = var_buf_for_exact.find(iter->var);
			v.MakeStringOwner();
			if(cache->second.empty()) {  //empty if IBexpression - feeding unnecessary
				bool ldiff = v != param_cache_for_exact[iter->var];
				diff = diff || ldiff;
				if(ldiff)
					param_cache_for_exact[iter->var] = v;
			} else {
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT( cache != var_buf_for_exact.end() );
				diff = diff || (v != cache->second.begin()->first);
				if(diff)
					for(std::vector<MysqlExpression::value_or_null_info_t>::iterator val_it = cache->second.begin(); val_it  != cache->second.end(); ++val_it)
						*((*val_it ).second) = (*val_it ).first = v;
			}
		}
		first_eval = false;
	}

	if(diff) {
		min_max_uptodate = false;
		out_of_date_rough = true;
	}
	return diff;
}

void SubSelectColumn::DoSetExpectedType(ColumnType const& ct)
{
	expected_type = ct;
}

bool SubSelectColumn::MakeParallelReady()
{
	MIDummyIterator mit(mind);
	PrepareSubqResult(mit, false);
	if(!subq->IsMaterialized())
		subq->Materialize();
	if(subq->NoObj() > subq->GetPageSize())
		return false; //multipage Attrs - not thread safe
	// below assert doesn't take into account lazy field
	// NoMaterialized() tells how many rows in lazy mode are materialized 
	assert(subq->NoObj() == subq->NoMaterialized());
	PrepareAndFillCache();
	return true;
}
