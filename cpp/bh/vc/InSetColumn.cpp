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

#include <iterator>
#include <boost/bind.hpp>

#include "core/CompiledQuery.h"
#include "InSetColumn.h"
#include "core/MysqlExpression.h"
#include "ConstColumn.h"
#include "ExpressionColumn.h"
#include "core/RCAttr.h"

using namespace std;
using namespace boost;

InSetColumn::InSetColumn(ColumnType const& ct, MultiIndex* mind, columns_t const& columns_)
	: MultiValColumn( ct, mind ), columns(columns_), full_cache(false), expected_type(ct), 
	last_mit(NULL), last_mit_size(0)
{
	typedef set<VarMap> unique_varmaps_t;
	unique_varmaps_t uvms;
	for(columns_t::iterator it = columns.begin(), end = columns.end(); it != end; ++ it) {
		var_maps_t const& vm = (*it)->GetVarMap();
		uvms.insert(vm.begin(), vm.end());
		MysqlExpression::bhfields_cache_t const& bhfields_cache = (*it)->GetBHItems();
		bhitems.insert(bhfields_cache.begin(), bhfields_cache.end());
	}
	var_maps_t(uvms.begin(), uvms.end()).swap(var_map);
	set<int> dims;
	for(var_maps_t::iterator it = var_map.begin(), end = var_map.end(); it != end; ++it)
		dims.insert((*it).dim);
	if(dims.size() == 1)
		dim = *(dims.begin());
	else
		dim = -1;

	is_const = find_if(columns.begin(), columns.end(), !bind(&VirtualColumn::IsConst, _1)) == columns.end();
}

InSetColumn::InSetColumn(ColumnType const& ct, MultiIndex* mind, ValueSet& external_valset)
: MultiValColumn( ct, mind ), cache(external_valset), expected_type(ct), last_mit(NULL), last_mit_size(0)
{
	dim = -1;
	is_const = true;
	full_cache = true;
	cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
}

InSetColumn::InSetColumn(const InSetColumn& c) : MultiValColumn(c), columns(c.columns), full_cache(c.full_cache), cache(c.cache),
		expected_type(c.expected_type), is_const(c.is_const), last_mit(c.last_mit), last_mit_size(c.last_mit_size)
{}

InSetColumn::~InSetColumn(void)
{
}

bool InSetColumn::IsConst() const
{
	return is_const;
}

void InSetColumn::RequestEval(const MIIterator& mit, const int tta){
	full_cache = false;
	cache.Clear();
	for(int i = 0; i< columns.size(); i++)
		columns[i]->RequestEval(mit, tta);
}

RCBString InSetColumn::DoGetMinString(const MIIterator &mit)
{
	RCBString s;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return s;
}

RCBString InSetColumn::DoGetMaxString(const MIIterator &mit)
{
	RCBString s;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return s;
}


ushort InSetColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return ct.GetPrecision();
}

PackOntologicalStatus InSetColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	return NORMAL;
}

void InSetColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
	assert(0); 	// comparison of a const with a const should be simplified earlier
}

bool InSetColumn::IsSetEncoded(AttributeType at, int scale)
{
	return (cache.EasyMode() &&
		scale == ct.GetScale() &&
		(at == expected_type.GetTypeName() ||
			(ATI::IsFixedNumericType(at) && ATI::IsFixedNumericType(expected_type.GetTypeName()))));
}

BHTribool InSetColumn::DoContains64(const MIIterator& mit, _int64 val)			// easy case for numerics
{
	if(cache.EasyMode()) {
		BHTribool contains = false;
		if(val == NULL_VALUE_64)
			return BHTRIBOOL_UNKNOWN;
		if(cache.Contains(val))
			contains = true;
		else if(cache.ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
		return contains;
	}
	return DoContains(mit, RCNum(val, ct.GetScale()));
}

BHTribool InSetColumn::DoContainsString(const MIIterator& mit, RCBString &val)	// easy case for numerics
{
	if(cache.EasyMode()) {
		BHTribool contains = false;
		if(val.IsNull())
			return BHTRIBOOL_UNKNOWN;
		if(cache.Contains(val))
			contains = true;
		else if(cache.ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
		return contains;
	}
	return DoContains(mit, val);
}

BHTribool InSetColumn::DoContains(const MIIterator& mit, const RCDataType& val)
{
	BHTribool contains = false;
	if(val.IsNull())
		return BHTRIBOOL_UNKNOWN;
	if(IsConst()) {
		if(!full_cache)
			PrepareCache(mit);
		cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());

		if(cache.Contains(val, GetCollation()))
			contains = true;
		else if(cache.ContainsNulls())
			contains = BHTRIBOOL_UNKNOWN;
	} else {
		//TODO: remember mit value for the last cache creation and reuse cache if possible.
		if(RequiresUTFConversions(GetCollation()) && Type().IsString()) {
			for(columns_t::const_iterator it = columns.begin(), end = columns.end(); it != end; ++it) {
				RCBString s;
				(*it)->GetValueString(s, mit);
				if(s.IsNull())
					contains = BHTRIBOOL_UNKNOWN;
				else {
					//ConvertToBinaryForm(s, buf, 0, GetCollation().collation, true);
					if(CollationStrCmp(GetCollation(), val.ToRCString(), s) == 0) {
						contains = true;
						break;
					}
				}
			}
		} else {
			for(columns_t::const_iterator it = columns.begin(), end = columns.end(); it != end; ++it) {
				if((*it)->IsNull(mit))
					contains = BHTRIBOOL_UNKNOWN;
				else if(val == *((*it)->GetValue( mit, false ).Get())) {
					contains = true;
					break;
				}
			}
		}
	}
	return contains;
}

_int64 InSetColumn::DoNoValues(MIIterator const& mit)
{
	if(full_cache && is_const)
		return cache.NoVals();
	return columns.size();
}

bool InSetColumn::DoIsEmpty(MIIterator const& mit)
{
	return DoAtLeastNoDistinctValues(mit, 1) == 0;
}

void InSetColumn::PrepareCache(const MIIterator& mit, const _int64& at_least) 
{
	//MEASURE_FET("InSetColumn::PrepareCache(...)");
	full_cache = false;
    cache.Clear();
    columns_t::const_iterator it(columns.begin());
    columns_t::const_iterator end(columns.end());
	if(RequiresUTFConversions(GetCollation()) && Type().IsString()) {
		ValueSet bin_cache;
		bin_cache.Prepare(expected_type.GetTypeName(), expected_type.GetScale(), expected_type.GetCollation());
		int bin_size = 0;
		int max_str_size = 0;
		for(; (it != end) && (bin_cache.NoVals() <= at_least); ++it)
			max_str_size += (*it)->MaxStringSize();
		it = columns.begin();
		RCBString buf(NULL, CollationBufLen(GetCollation(), max_str_size), true);
		for(; (it != end) && (bin_cache.NoVals() <= at_least); ++it){
			RCBString s;
			(*it)->GetValueString(s, mit);
			ConvertToBinaryForm(s, buf, GetCollation());
			bin_size = bin_cache.NoVals();
			bin_cache.Add(buf);
			if(bin_size < bin_cache.NoVals())
				cache.Add(s);
		}
	} else {
		for(; (it != end) && (cache.NoVals() <= at_least); ++it)
			cache.Add((*it)->GetValue(mit));
	}
	if((at_least == PLUS_INF_64) || (it == end))
    	full_cache = true;
}

_int64 InSetColumn::DoAtLeastNoDistinctValues(const MIIterator& mit, _int64 const at_least)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(at_least > 0);
	if(!full_cache  || !is_const)
		PrepareCache(mit, at_least);
    return cache.NoVals();
}

bool InSetColumn::DoContainsNull(const MIIterator& mit)
{
	if(!full_cache  || !is_const)
		PrepareCache(mit);
	cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
	return cache.ContainsNulls();
}

MultiValColumn::Iterator::impl_t InSetColumn::DoBegin(MIIterator const& mit)
{
	if(!full_cache  || !is_const)
		PrepareCache(mit);
	cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
	return MultiValColumn::Iterator::impl_t(new IteratorImpl(cache.begin()));
}

MultiValColumn::Iterator::impl_t InSetColumn::DoEnd(MIIterator const& mit)
{
	if(!full_cache  || !is_const)
		PrepareCache(mit);
	cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
	return MultiValColumn::Iterator::impl_t(new IteratorImpl(cache.end()));
}

RCValueObject InSetColumn::DoGetSetMin(MIIterator const& mit)
{
	if(!full_cache  || !is_const)
		PrepareCache(mit);
	cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
	return RCValueObject(*cache.Min());
}

RCValueObject InSetColumn::DoGetSetMax(MIIterator const& mit)
{
	if(!full_cache  || !is_const)
		PrepareCache(mit);
	cache.Prepare(expected_type.GetTypeName(), ct.GetScale(), expected_type.GetCollation());
	return RCValueObject(*cache.Max());
}

void InSetColumn::DoSetExpectedType(ColumnType const& ct)
{
	expected_type = ct;
}

char *InSetColumn::ToString(char p_buf[], size_t buf_ct) const
{
	if(full_cache && is_const) {
		_int64 no_vals = cache.NoVals();
		snprintf(p_buf, buf_ct, "%lld vals", no_vals);
	}
	return p_buf;
}

void InSetColumn::LockSourcePacks(const MIIterator& mit)
{
	for(int i = 0; i < columns.size(); i++) {
		columns[i]->LockSourcePacks(mit);
	}
}

bool InSetColumn::CanCopy() const
{
	for(int i = 0; i < columns.size(); i++) {
		if(!columns[i]->CanCopy())
			return false;
	}
	return true;
}
