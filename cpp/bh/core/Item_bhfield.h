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

#ifndef PURE_LIBRARY
#ifndef _ITEM_BHFIELD_H_
#define _ITEM_BHFIELD_H_

#include <set>
#include <map>

#include <boost/static_assert.hpp>
#include "common/CommonDefinitions.h"
#include "ValueOrNull.h"

extern int my_decimal_shift(uint mask, my_decimal *res, int shift);

class Item_bhfield : public Item_field
{
public:
	std::vector<VarID>	varID;		// BH identifiers of the variable/field/column represented by 'ifield' for each bhfield usage
	short 	curr_varID;				// when building IBExpression use varID[curr_varID]
	Item_bhfield(Item_field* ifield, VarID varID);
	virtual ~Item_bhfield();

	Item_field* OriginalItem()			{ return ifield; }

	void	SetBuf(ValueOrNull*& b);
	void 	ClearBuf();
	void	SetType(DataType t);

	/////  Implementation of MySQL Item interface  /////

	BOOST_STATIC_ASSERT(sizeof(Type) >=2);
	enum { BHFIELD_ITEM = 12345 };			// WARNING: it is risky. We are assuming sizeof(enum) >= 2 in a superclass (Item)
	static Type get_bhitem_type() {return (Type) BHFIELD_ITEM;}

	virtual Type type() const   			{ return (Type) BHFIELD_ITEM; }

	virtual mysql_bool is_null()					{ return buf->null; }
	virtual mysql_bool is_null_result()
	{ 
		return buf->null; 
	}

	virtual double val_real();
	virtual double val_result()				{ return val_real(); }
	virtual longlong val_int();
	virtual longlong val_int_result()		{ return val_int(); }
	virtual bool val_bool_result()			{ return (val_int() ? true : false); }
	virtual String* val_str(String* s);
	virtual String* str_result(String* s)	{ return val_str(s); }
	virtual my_decimal *val_decimal(my_decimal *d);
	virtual my_decimal *val_decimal_result(my_decimal *d) { return val_decimal(d); }
	virtual mysql_bool get_date(TIME *ltime,uint fuzzydate);
	virtual mysql_bool get_date_result(TIME *ltime,uint fuzzydate) { return get_date(ltime,fuzzydate); }
	virtual mysql_bool get_time(TIME *ltime);
	table_map used_tables() const { return ifield->used_tables(); }
	enum Item_result result_type () const {
		return was_aggregation ? aggregation_result : Item_field::result_type();
	}
	enum_field_types field_type() const { return ifield->field_type(); 	}
	virtual mysql_bool const_item() const			{ return false; }

	virtual const char* full_name() const	{ return fullname; }
	mysql_bool operator == ( Item_bhfield const& ) const;
	bool IsAggregation() {return was_aggregation;}
	const ValueOrNull GetCurrentValue();

	//possibly more functions of Item_field should be redifined to redirect them to ifield
  bool result_as_longlong()
  {
	return ifield->result_as_longlong();
  }

private:

	// Translate BH value stored in 'buf' into MySQL value stored in 'ivalue'
	void FeedValue();

	Item_field* ifield;		// for recovery of original item tree structure

	// Type of values coming in from BH
	DataType	bhtype;

	// BH buffer where current field value will be searched for
	ValueOrNull*	buf;

	//! Indicates whether 'buf' field is owned (was allocated) by this object
	bool	isBufOwner;

	// MySQL buffer (constant value) keeping the current value of this field.
	// Actual Item subclass of this object depends on what type comes in from BH
	Item*	ivalue;
	bool was_aggregation;	//this bhfield replaces an aggregation
	Item_result aggregation_result;
	char fullname[32];
	Item_bhfield(Item_bhfield const&);
	Item_bhfield& operator = (Item_bhfield const&);
};

// This subclass is created only to get access to protected field
// 'decimal_value' of Item_decimal.
class Item_bhdecimal : public Item_decimal
{
public:
	Item_bhdecimal(DataType t);

	void Set(_int64 val);

private:
	int		scale;
	_int64	scaleCoef;
};


//! Base class for IB's Item classes to store date/time values of columns
//! occuring in a complex expression
class Item_bhdatetime_base : public Item
{
protected:
	RCDateTime			dt;

public:
	void Set(_int64 x, AttributeType at)	{ dt = RCDateTime(x, at); }

	virtual Type type() const		{ return (Type) -1; }

	virtual double val_real()		{ return (double) val_int(); }
	virtual my_decimal *val_decimal(my_decimal *d);
	virtual mysql_bool get_time(TIME *ltime);
};

class Item_bhdatetime : public Item_bhdatetime_base
{
	virtual longlong val_int();
	virtual String* val_str(String* s);
	virtual mysql_bool get_date(TIME *ltime,uint fuzzydate);
};

class Item_bhtimestamp : public Item_bhdatetime
{};

class Item_bhdate : public Item_bhdatetime_base
{
	virtual longlong val_int();
	virtual String* val_str(String* s);
	virtual mysql_bool get_date(TIME *ltime,uint fuzzydate);
};

class Item_bhtime : public Item_bhdatetime_base
{
	virtual longlong val_int();
	virtual String* val_str(String* s);
	virtual mysql_bool get_date(TIME *ltime,uint fuzzydate);
};

class Item_bhyear : public Item_bhdatetime_base
{
	//! Is it 4- or 2-digit year (2009 or 09).
	//! This info is taken from MySQL.
	int		length;

public:
	Item_bhyear(int len)	:length(len) { assert(length == 2 || length == 4); }

	virtual longlong val_int();
	virtual String* val_str(String* s);
	virtual mysql_bool get_date(TIME *ltime,uint fuzzydate);
};

#endif
#endif
