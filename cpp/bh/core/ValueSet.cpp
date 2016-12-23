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

#include "ValueSet.h"
#include "common/CommonDefinitions.h"
#include "types/RCDataTypes.h"
#include "Filter.h"
#include "RCAttrTypeInfo.h"

using namespace std;

ValueSet::ValueSet()
{
	prepared = false;
	contains_nulls = false;
	prep_type = RC_UNKNOWN;
	prep_scale = 0;
	prep_collation = DTCollation();
	min = max = 0;
	values = new RCDTptrHashSet;
	no_obj = 0;
	easy_min = 0;
	easy_max = 0;
	easy_vals = NULL;
	easy_hash = NULL;
	easy_text = NULL;
	use_easy_table = false;
}

ValueSet::ValueSet(const ValueSet& sec) :
	prepared(sec.prepared), prep_type(sec.prep_type), prep_scale(sec.prep_scale), prep_collation(sec.prep_collation)
{
	values = new RCDTptrHashSet;
	no_obj = sec.NoVals();
	contains_nulls = sec.contains_nulls;
	min = max = 0;
	if(sec.values) {
		for( RCDTptrHashSet::iterator it = sec.values->begin(), end = sec.values->end(); it != end; it++)
			values->insert((*it)->Clone().release());
	}

	if(sec.min)
		min = sec.min->Clone().release();

	if(sec.max)
		max = sec.max->Clone().release();

	easy_min = sec.easy_min;
	easy_max = sec.easy_max;
	use_easy_table = sec.use_easy_table;
	if(use_easy_table)
		for(int i = 0; i < no_obj; i++)
			easy_table[i] = sec.easy_table[i];
	if(sec.easy_vals)
		easy_vals = new Filter(*sec.easy_vals);
	else
		easy_vals = NULL;
	if(sec.easy_hash)
		easy_hash = new Hash64(*sec.easy_hash);
	else
		easy_hash = NULL;
	if(sec.easy_text)
		easy_text = new TextStat(*sec.easy_text);
	else
		easy_text = NULL;
}

ValueSet::~ValueSet()
{
	MEASURE_FET("ValueSet::~ValueSet(...)");
	Clear();
	delete values;
	delete easy_vals;
	delete easy_hash;
	delete easy_text;
}

void ValueSet::Clear( void )
{
	RCDTptrHashSet::iterator iter = values->begin();
	RCDTptrHashSet::iterator tmp;
	RCDTptrHashSet::iterator end = values->end();
	while(iter != end) {
		tmp = iter++;
		delete *tmp;
	}
	values->clear();
	delete min;
	min = 0;
	delete max;
	max = 0;
	prepared = false;
	prep_type = RC_UNKNOWN;
	prep_scale = 0;
	prep_collation = DTCollation();
	delete easy_vals;
	easy_vals = NULL;
	delete easy_hash;
	easy_hash = NULL;
	delete easy_text;
	easy_text = NULL;
	use_easy_table = false;
}

void ValueSet::Add64(_int64 v)		// only for integers
{
	if(v == NULL_VALUE_64) {
		contains_nulls = true;
		return;
	}
	RCDataType* rcv = new RCNum(v);
	if(prep_type != RC_NUM || prep_scale != 0)
		prepared = false;

	if(!values->insert(rcv).second)
		delete rcv;
	else if(prepared) {
		if(RequiresUTFConversions(prep_collation)) {
			if(min->IsNull() || CollationStrCmp(prep_collation, rcv->ToRCString(), min->ToRCString()) < 0)
				*min = *rcv;
			if(max->IsNull() || CollationStrCmp(prep_collation, rcv->ToRCString(), max->ToRCString()) > 0)
				*max = *rcv;
		} else {
			if(min->IsNull() || *rcv < *min)
				*min = *rcv;
			if(max->IsNull() || *rcv > *max)
				*max = *rcv;
		}
	}

	no_obj = (int)values->size();
}

void ValueSet::Add(auto_ptr<RCDataType> rcdt)
{
	if(rcdt->IsNull()) {
		contains_nulls = true;
		return;
	}
	RCDataType* rcv = rcdt.release();
	if(prep_type != rcv->Type() || (rcv->Type() == RC_NUM && static_cast<RCNum&>(*rcv).Scale() != prep_scale))
		prepared = false;

	if(!values->insert(rcv).second)
		delete rcv;
	else if(prepared) {
		if(RequiresUTFConversions(prep_collation)) {
			if(min->IsNull() || CollationStrCmp(prep_collation, rcv->ToRCString(), min->ToRCString()) < 0)
				*min = *rcv;
			if(max->IsNull() || CollationStrCmp(prep_collation, rcv->ToRCString(), max->ToRCString()) > 0)
				*max = *rcv;
		} else {
			if(min->IsNull() || *rcv < *min)
				*min = *rcv;
			if(max->IsNull() || *rcv > *max)
				*max = *rcv;
		}
	}

	no_obj = (int)values->size();
}

void ValueSet::Add(const RCValueObject& rcv)
{
	if(rcv.IsNull())
		contains_nulls = true;
	else
		Add(rcv.Get()->Clone());
}

bool ValueSet::Contains(_int64 v)
{
	assert(prepared);
	assert(v != NULL_VALUE_64);

	if(use_easy_table) {
		for(int i = 0; i < no_obj; i++)
			if(v == easy_table[i])
				return true;
		return false;
	}
	if(easy_vals) {
		if(v < easy_min || v > easy_max)
			return false;
		return easy_vals->Get(v - easy_min);
	}
	if(easy_hash)
		return easy_hash->Find(v);
	return Contains(RCNum(v, prep_scale), prep_collation);
}

bool ValueSet::Contains(RCBString &v)
{
	assert(prepared);	// it implies a trivial collation

	if(v.IsNull())
		return false;
	if(easy_hash && easy_text) {
		_int64 vcode = easy_text->Encode(v);
		if(vcode == NULL_VALUE_64)
			return false;
		return easy_hash->Find(vcode);
	}
	return (values->size() > 0 && values->find(&v/*const_cast<RCDataType*>(&v)*/) != values->end());
}

bool ValueSet::Contains(const RCDataType& v, DTCollation coll)
{
	// no need to check if it is prepared, as it is checked at the beginning of Prepare:
	Prepare(v.Type(), v.Type() == RC_NUM ? ((RCNum*)&v)->Scale() : 0, coll);

	if(v.IsNull())
		return false;
	if(RequiresUTFConversions(coll)) {
		RCDTptrHashSet::iterator it = values->begin();
		RCDTptrHashSet::iterator it_end = values->end();
		while(it != it_end) {
			if(CollationStrCmp(coll, (*(*it)).ToRCString(), v.ToRCString()) == 0) 
				return true;
			it++;
		}
		return false;
	} else
		return (values->size() > 0 && values->find(const_cast<RCDataType*>(&v)) != values->end());
}

bool ValueSet::Contains(const RCValueObject& v, DTCollation coll)
{
	if(v.IsNull())
		return false;
	return Contains(*v.Get(), coll);
}

string ValueSet::ToText()
{
	string res = boost::lexical_cast<string>(NoVals());
	res += " item";
	if(NoVals()!=1)
		res+="s";
	return res;
}

inline bool ValueSet::IsPrepared(AttributeType at, int scale, DTCollation coll)
{
	return (prepared && (prep_type == at || (ATI::IsStringType(prep_type) && ATI::IsStringType(at)))	// CHAR = VARCHAR
		&& prep_scale == scale && 
		(!ATI::IsStringType(at) || prep_collation.collation == coll.collation || 
		(!RequiresUTFConversions(prep_collation) && !RequiresUTFConversions(coll))
		));
}

void ValueSet::Prepare(AttributeType at, int scale, DTCollation coll)
{
	//MEASURE_FET("ValueSet::Prepare(...)");
	if(!IsPrepared(at, scale, coll)) {
		mx.Lock();
		if(!IsPrepared(at, scale, coll)) {
			auto_ptr<RCDTptrHashSet> new_values(new RCDTptrHashSet());
			RCDTptrHashSet::iterator it = values->begin();
			RCDTptrHashSet::iterator it_end = values->end();
			RCDTptrHashSet::iterator tmp;
			RCDataType* rcdt_it = 0;
			if(ATI::IsStringType(at)) {
				delete min;
				min = new RCBString();
				delete max;
				max = new RCBString();
				it = values->begin();
				bool min_set = false, max_set = false;
				if(RequiresUTFConversions(coll)) {
					while(it != it_end) {
						RCBString* v = new RCBString((*(*it)).ToRCString());
						if(!min_set || CollationStrCmp(coll, v->ToRCString(), min->ToRCString()) < 0) {
							*min = *v;
							min_set = true;
						}
						if(!max_set || CollationStrCmp(coll, v->ToRCString(), max->ToRCString()) > 0) {
							*max = *v;
							max_set = true;
						}
						new_values->insert(v);
						tmp = it++;
						delete *tmp;
					}
				} else {
					while(it != it_end) {
						RCBString* v = new RCBString((*(*it)).ToRCString());
						if(!min_set || *v < *min) {
							*min = *v;
							min_set = true;
						}
						if(!max_set || *v > *max) {
							*max = *v;
							max_set = true;
						}
						new_values->insert(v);
						tmp = it++;
						delete *tmp;
					}
				}
				delete values;
				values = new_values.release();
			} else if(ATI::IsIntegerType(at)) {
				delete min;
				min = new RCNum();
				delete max;
				max = new RCNum();
				while(it != it_end) {
					rcdt_it = (*it);
					if(RCNum* rcn = dynamic_cast<RCNum*>(rcdt_it)) {
						if(rcn->IsInt() || rcn->IsReal()) {
							RCNum* rcn_new = new RCNum();
							if(rcn->IsInt())
								RCDataType::ToInt(*rcn, *rcn_new);
							else
								RCDataType::ToReal(*rcn, *rcn_new);

							if(!(*rcn_new > *min))
								*min = *rcn_new;
							if(!(*rcn_new < *max))
								*max = *rcn_new;
							if(!new_values->insert(rcn_new).second)
								delete rcn_new;
						}
						tmp = it++;
						delete *tmp;
					} else if(rcdt_it->Type() == RC_STRING) {
						RCNum* rcn = new RCNum();
						if(RCNum::Parse(rcdt_it->ToRCString(), *rcn, at) == BHRC_SUCCESS) {
							if(!(*rcn > *min))
								*min = *rcn;
							if(!(*rcn < *max))
								*max = *rcn;
							if(!new_values->insert(rcn).second)
								delete rcn;
						} else {
							delete rcn;
							rcn = new RCNum(0, scale, false, at);
							if(!new_values->insert(rcn).second)
								delete rcn;
						}
						tmp = it++;
						delete *tmp;
					} else {
						tmp = it++;
						delete *tmp;
					}
				}
				delete values;
				values = new_values.release();
			} else if(at == RC_NUM) {
				delete min;
				min = new RCNum();
				delete max;
				max = new RCNum();

				while(it != it_end) {
					rcdt_it = (*it);
					if(RCNum* rcn = dynamic_cast<RCNum*>(rcdt_it)) {
						if(rcn->IsDecimal(scale)) {
							RCNum* rcn_new = new RCNum();
							RCDataType::ToDecimal(*rcn, scale, *rcn_new);
							if(!(*rcn_new > *min))
								*min = *rcn_new;
							if(!(*rcn_new < *max))
								*max = *rcn_new;
							if(!new_values->insert(rcn_new).second)
								delete rcn_new;
						}
					} else if(rcdt_it->Type() == RC_STRING) {
						RCNum* rcn = new RCNum();
						if(RCNum::Parse(rcdt_it->ToRCString(), *rcn, RC_NUM) == BHRC_SUCCESS && rcn->IsDecimal(scale)) {
							if(!(*rcn > *min))
								*min = *rcn;
							if(!(*rcn < *max))
								*max = *rcn;
							if(!new_values->insert(rcn).second)
								delete rcn;
						} else {
							delete rcn;
							rcn = new RCNum(0, scale, false, at);
							if(!new_values->insert(rcn).second)
								delete rcn;
						}
					}
					tmp = it++;
					delete *tmp;
				}

				delete values;
				values = new_values.release();
			} else if(ATI::IsRealType(at)) {
				delete min;
				min = new RCNum(at);
				delete max;
				max = new RCNum(at);

				while(it != it_end) {
					rcdt_it = (*it);
					if(RCNum* rcn = dynamic_cast<RCNum*>(rcdt_it)) {
						if(ATI::IsRealType(rcn->Type())) {
							if(!(*rcn > *min))
								*min = (*rcn);
							if(!(*rcn < *max))
								*max = (*rcn);
							if(!new_values->insert(rcn).second) {
								tmp = it++;
								delete *tmp;
							} else
								it++;
						} else {
							RCNum* rcn_new = new RCNum(rcn->ToReal());
							if(!(*rcn_new > *min))
								*min = (*rcn_new);
							if(!(*rcn < *max))
								*max = (*rcn_new);
							if(!new_values->insert(rcn_new).second)
								delete rcn_new;
							tmp = it++;
							delete *tmp;
						}
					} else if(rcdt_it->Type() == RC_STRING) {
						RCNum *rcn = new RCNum();
						if(RCNum::ParseReal(*(RCBString*)rcdt_it, *rcn, at) == BHRC_SUCCESS) {
							if(!(*rcn > *min))
								*min = *rcn;
							if(!(*rcn < *max))
								*max = *rcn;
							if(!new_values->insert(rcn).second)
								delete rcn;
						} else {
							delete rcn;
							rcn = new RCNum(0, scale, true, at);
							if(!new_values->insert(rcn).second)
								delete rcn;
						}
						tmp = it++;
						delete *tmp;
					} else {
						tmp = it++;
						delete *tmp;
					}
				}
				delete values;
				values = new_values.release();
			} else if(ATI::IsDateTimeType(at)) {
				delete min;
				min = new RCDateTime();
				delete max;
				max = new RCDateTime();

				while(it != it_end) {
					rcdt_it = (*it);
					if(RCDateTime* rcdt = dynamic_cast<RCDateTime*>(rcdt_it)) {
						if(!(*rcdt > *min))
							*min = *rcdt;
						if(!(*rcdt < *max))
							*max = *rcdt;
						if(!new_values->insert(rcdt_it).second) {
							tmp = it++;
							delete *tmp;
						} else
							it++;
					} else if(rcdt_it->Type() == RC_STRING) {
						RCDateTime* rcdt = new RCDateTime();
						if(!BHReturn::IsError(RCDateTime::Parse(rcdt_it->ToRCString(), *rcdt, at))) {
							if(!(*rcdt > *min))
								*min = *rcdt;
							if(!(*rcdt < *max))
								*max = *rcdt;
							if(!new_values->insert(rcdt).second)
								delete rcdt;
						} else {
							delete rcdt;
							rcdt = static_cast<RCDateTime*>(RCDateTime::GetSpecialValue(at).Clone().release());
							if(!new_values->insert(rcdt).second)
								delete rcdt;
						}
						tmp = it++;
						delete *tmp;
					} else if(RCNum* rcn = dynamic_cast<RCNum*>(rcdt_it)) {
						try {
							RCDateTime* rcdt = new RCDateTime(*rcn, at);
							if(!(*rcdt> *min))
								*min = *rcdt;
							if(!(*rcdt < *max))
								*max = *rcdt;
							if(!new_values->insert(rcdt).second)
								delete rcdt;
						} catch (DataTypeConversionRCException&) {
							delete rcdt;
							rcdt = static_cast<RCDateTime*>(RCDateTime::GetSpecialValue(at).Clone().release());
							if(!new_values->insert(rcn).second)
								delete rcdt;
						}
						tmp = it++;
						delete *tmp;
					} else {
						tmp = it++;
						delete *tmp;
					}
				}
				delete values;
				values = new_values.release();
			}

			no_obj = (int)values->size();
			prepared = true;
			prep_type = at;
			prep_scale = scale;
			prep_collation = coll;
			if(no_obj > 0) {
				if(no_obj <= VS_EASY_TABLE_SIZE && !ATI::IsStringType(prep_type)) {
					it = values->begin();
					it_end = values->end();
					int i = 0;
					while(it != it_end) {
						rcdt_it = (*it);
						RCNum* rcn = static_cast<RCNum*>(rcdt_it);
						easy_table[i] = rcn->Value();
						i++;
						it++;
					}
					use_easy_table = true;
				}
				if(!use_easy_table && ATI::IsFixedNumericType(prep_type)) {
					easy_min = ((RCNum*)min)->Value();
					easy_max = ((RCNum*)max)->Value();
					delete easy_vals;
					easy_vals = NULL;
					if(easy_max < PLUS_INF_64 / 2 && easy_min > MINUS_INF_64 / 2 
						&& (easy_max - easy_min) < 8 * 64 * MBYTE 			// upper limit: 64 MB for a filter
						&& (easy_max - easy_min) / 8 < no_obj * 16) {		// otherwise easy_hash will be smaller
							easy_vals = new Filter(easy_max - easy_min + 1);
							it = values->begin();
							it_end = values->end();
							while(it != it_end) {
								rcdt_it = (*it);
								RCNum* rcn = static_cast<RCNum*>(rcdt_it);
								easy_vals->Set(rcn->Value() - easy_min);
								it++;
							}
					}
				}
				if(!use_easy_table && easy_vals == NULL && !ATI::IsStringType(prep_type)) {
					delete easy_hash; 
					easy_hash = new Hash64(no_obj);
					it = values->begin();
					it_end = values->end();
					while(it != it_end) {
						rcdt_it = (*it);
						RCNum* rcn = static_cast<RCNum*>(rcdt_it);
						easy_hash->Insert(rcn->Value());
						it++;
					}
				}
				if(ATI::IsStringType(prep_type) && !RequiresUTFConversions(coll)) {
					delete easy_text;
					easy_text = new TextStat;
					bool still_valid = true;
					it = values->begin();
					it_end = values->end();
					while(it != it_end && still_valid) {
						rcdt_it = (*it);
						RCBString* v = static_cast<RCBString*>(rcdt_it);
						still_valid = easy_text->AddString(*v);
						it++;
					}
					if(still_valid) 
						still_valid = easy_text->CreateEncoding();
					if(still_valid) {
						delete easy_hash;
						easy_hash = new Hash64(no_obj);
						it = values->begin();
						while(it != it_end) {
							rcdt_it = (*it);
							RCBString* v = static_cast<RCBString*>(rcdt_it);
							_int64 vcode = easy_text->Encode(*v);
							assert(vcode != NULL_VALUE_64);		// should not occur, as we put this value in the previous loop
							easy_hash->Insert(vcode);
							it++;
						}
					} else {
						delete easy_text;
						easy_text = NULL;
					}
				}
			}
		}
		mx.Unlock();
	}
}

int ValueSet::NoVals() const
{
	if(values)
		return (int)values->size();
	return no_obj;
}

/////////////////////

ValueSet::const_iterator ValueSet::begin() const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( values );
	if(!prepared)
		return values->end();
	return values->begin();
}

ValueSet::const_iterator ValueSet::end() const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( values );
	return values->end();
}
