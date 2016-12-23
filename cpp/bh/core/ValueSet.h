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

#ifndef _VALUESET_H_
#define _VALUESET_H_

#include "common/CommonDefinitions.h"
#include "types/RCDataTypes.h"
#include "system/ib_system.h"

#ifdef __GNUC__
#ifndef stdext
#define stdext __gnu_cxx
#endif
#endif

class RCDataType;
class RCValueObject;
class Filter;

#define VS_EASY_TABLE_SIZE 12
// Results of tuning VS_EASY_TABLE_SIZE: 
// - for up to 12 values easy table is faster, 
// - for more than 16 values hash64 is faster,
// - between 12 and 16 the results are comparable, so 12 is chosen to minimize memory (static) footprint of ValueSet.

class ValueSet
{
	// ValueSet might be considered as thread safe object only in case it is used for one 
	// type of values and various threads would not try to prepare it to different types
public:

#ifdef __GNUC__
	typedef stdext::hash_set<RCDataType*, rc_hash_compare<RCDataType*>, rc_hash_compare<RCDataType*> > RCDTptrHashSet;
#else
	typedef stdext::hash_set<RCDataType*, rc_hash_compare<RCDataType*> > RCDTptrHashSet;
#endif
	typedef RCDTptrHashSet::const_iterator const_iterator;
	ValueSet();
	ValueSet(const ValueSet& sec);
	~ValueSet();

	void Add64(_int64 v);							// add as RCNum, scale 0
	void Add(std::auto_ptr<RCDataType> rcdt);
	void Add(const RCValueObject& rcv);
	void AddNull()				{ contains_nulls = true; }
	void ForcePreparation()		{ prepared = false; }
	void Prepare(AttributeType at, int scale, DTCollation);
	bool Contains(_int64 v);						// easy case, for non-null integers
	bool Contains(RCBString &v);					// easy case, for strings
	bool EasyMode()				{ return (use_easy_table || easy_vals != NULL || easy_hash != NULL) && prepared; }

	bool Contains(const RCDataType& v, DTCollation);
	bool Contains(const RCValueObject& v, DTCollation);

	void Clear( void );
	int 		NoVals() const;
	bool 		IsEmpty() const		{ return (NoVals() == 0 && !contains_nulls); }
	bool 		ContainsNulls() const 	{ return contains_nulls; }
	RCDataType* Min() const				{ return min; }
	RCDataType* Max() const				{ return max; }
	std::string ToText();
	const_iterator begin() const;
	const_iterator end() const;
	/////////////////////////////////////////////////////////////////////////////////////////////////
protected:
	bool contains_nulls;
	int no_obj;					// number of values added
	bool prepared;
	AttributeType prep_type;	// type to which it is prepared
	int prep_scale;				// scale to which it is prepared
	DTCollation prep_collation;
	bool IsPrepared(AttributeType at, int scale, DTCollation);

	IBMutex mx;

	RCDTptrHashSet* values;
	RCDataType* min;
	RCDataType* max;

	// easy implementation for numerical values
	_int64	easy_min;
	_int64	easy_max;
	_int64	easy_table[VS_EASY_TABLE_SIZE];
	bool	use_easy_table;
	Filter*	easy_vals;		// if not null, then it contains a characteristic set of values between easy_min and easy_max, including
	Hash64* easy_hash;
	TextStat* easy_text;	// if not null, then this is an alternative way of storing text values (together with easy_hash)
};

#endif

