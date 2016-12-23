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
#include "common/bhassert.h"
#include "compilation_tools.h"
#include "QuickMath.h"
#include "Item_bhfield.h"

using namespace std;

int my_decimal_shift(uint mask, my_decimal *res, int shift)
{
  return check_result_and_overflow(mask,
                                   decimal_shift((decimal_t*)res, shift),
                                   res);
}

Item_bhfield::Item_bhfield(Item_field* ifield, VarID varID)
	: Item_field(& ConnectionInfoOnTLS.Get().Thd(), ifield),
	ifield(ifield),
	buf(NULL),
	ivalue(NULL)
{
	this->varID.push_back(varID);
	curr_varID=-1;
	if(ifield->type() == Item::SUM_FUNC_ITEM) {
		was_aggregation = true;
		aggregation_result = ifield->result_type();
	}
	else
		was_aggregation = false;
	sprintf(fullname, "bhfield of %p", (void*)ifield);
	isBufOwner = false;
}

Item_bhfield::~Item_bhfield()
{
	//if(ivalue != NULL) delete ivalue;	// done by MySQL not IB, for each Item subclass
	ClearBuf();
}

void Item_bhfield::ClearBuf() {
	if(isBufOwner) {
		delete buf;
		buf = NULL;
	}
}

void Item_bhfield::SetBuf(ValueOrNull*& b)
{
	if (buf == NULL) {
		isBufOwner = true;
		buf = new ValueOrNull;
	}
	b = buf;
}

void Item_bhfield::SetType(DataType t)
{
	if(ivalue != NULL) {
		//delete ivalue;		// done by MySQL not IB, for each Item subclass
		ivalue = NULL;
	}

	bhtype = t;
	switch(bhtype.valtype) {
		case VT_FIXED:
			if(bhtype.IsInt()) 
				ivalue = new Item_int(static_cast<longlong>(0));
			else			 
				ivalue = new Item_bhdecimal(bhtype);
			ivalue->unsigned_flag = ifield->unsigned_flag;
			break;

		case VT_FLOAT:
			ivalue = new Item_float(0.0, NOT_FIXED_DEC);
			break;

		case VT_STRING:
			ivalue = new Item_string("", 0, ifield->collation.collation, ifield->collation.derivation);
			break;

		case VT_DATETIME:
			switch(bhtype.attrtype) {
				case RC_DATETIME:	ivalue = new Item_bhdatetime(); break;
				case RC_TIMESTAMP:	ivalue = new Item_bhtimestamp(); break;
				case RC_DATE:		ivalue = new Item_bhdate(); break;
				case RC_TIME:		ivalue = new Item_bhtime(); break;
				case RC_YEAR:		ivalue = new Item_bhyear(bhtype.precision); break;
				default:
					BHERROR("Incorrect date/time data type passed to Item_bhfield::SetType()");
					break;
			}
			break;

		default:
			BHERROR("Incorrect data type passed to Item_bhfield::SetType()");
			break;
	}
}

const ValueOrNull Item_bhfield::GetCurrentValue() {
	return *buf;
}

void Item_bhfield::FeedValue()
{
	switch(bhtype.valtype) {
		case VT_FIXED:
			if(bhtype.IsInt())	
				((Item_int*)ivalue)->value = buf->GetFixed();
			else				
				((Item_bhdecimal*)ivalue)->Set(buf->GetFixed());
			break;

		case VT_FLOAT:
			((Item_float*)ivalue)->value = buf->GetDouble();
			break;

		case VT_STRING:
			((Item_string*)ivalue)->str_value.copy(buf->sp, buf->len, ifield->collation.collation); 
			break;

		case VT_DATETIME:
			((Item_bhdatetime_base*)ivalue)->Set(buf->GetDateTime64(), bhtype.attrtype);
			break;

		default:
			BHERROR("Unrecognized type in Item_bhfield::FeedValue()");
	}
}

double Item_bhfield::val_real()
{
	//DBUG_ASSERT(fixed == 1);
	if((null_value = buf->null))
		return 0.0;
	FeedValue();
	return ivalue->val_real();
}

longlong Item_bhfield::val_int()
{
	//DBUG_ASSERT(fixed == 1);
	if((null_value = buf->null))
		return 0;
	FeedValue();
	return ivalue->val_int();
}

my_decimal* Item_bhfield::val_decimal(my_decimal *decimal_value)
{
	if((null_value = buf->null))
		return 0;
	FeedValue();
	return ivalue->val_decimal(decimal_value);
}

String* Item_bhfield::val_str(String *str)
{
	//DBUG_ASSERT(fixed == 1);
	if((null_value = buf->null))
		return 0;
	FeedValue();
	//str->set_charset(str_value.charset());
	//return field->val_str(str,&str_value);
	str->copy(*(ivalue->val_str(str)));
	//fprintf(stderr, "'%s'\n", s->c_ptr());
	return str;
}

mysql_bool Item_bhfield::get_date(TIME *ltime, uint fuzzydate)
{
	if(null_value = buf->null || ((!(fuzzydate & TIME_FUZZY_DATE) &&
			(bhtype.attrtype == RC_DATETIME || bhtype.attrtype == RC_DATE) && buf->x == 0)))
		return 1;	// like in Item_field::get_date - return 1 on null value.
	FeedValue();
	return ivalue->get_date(ltime, fuzzydate);
}

mysql_bool Item_bhfield::get_time(TIME *ltime)
{
	if(null_value = buf->null || ((bhtype.attrtype == RC_DATETIME || bhtype.attrtype == RC_DATE) && buf->x == 0)) //zero date is illegal
		return 1;	// like in Item_field::get_time - return 1 on null value.
	FeedValue();
	return ivalue->get_time(ltime);
}

mysql_bool Item_bhfield::operator == ( Item_bhfield const& o ) const
{
	return ( varID == o.varID );
}

Item_bhdecimal::Item_bhdecimal(DataType t)
	:Item_decimal(0, false)
{
	scale = t.fixscale;
	scaleCoef = QuickMath::power10i(scale);
}

void Item_bhdecimal::Set(_int64 val)
{
	std::fill( decimal_value.buf, decimal_value.buf + decimal_value.len, 0 );
	if ( val ) {
		int2my_decimal((uint)-1, val, 0, &decimal_value);
		my_decimal_shift((uint)-1, &decimal_value, -scale);
	} else {
		my_decimal_set_zero( &decimal_value);
	}
	decimal_value.frac = scale;
}


my_decimal* Item_bhdatetime_base::val_decimal(my_decimal *d)
{
	int2my_decimal((uint)-1, val_int(), 0, d);
	return d;
}
mysql_bool Item_bhdatetime_base::get_time(TIME *ltime)
{
	return get_date(ltime, 0);
}

longlong Item_bhdatetime::val_int()
{
	return dt.Year() * 10000000000LL + dt.Month() * 100000000 + dt.Day() * 1000000
	       + dt.Hour() * 10000 + dt.Minute() * 100 + dt.Second();
}
String* Item_bhdatetime::val_str(String *s)
{
	TIME ltime;
	get_date(&ltime, 0);
	s->alloc(19);
	make_datetime((DATE_TIME_FORMAT*) 0, &ltime, s);
	return s;
}
mysql_bool Item_bhdatetime::get_date(TIME *ltime, uint fuzzydate)
{
    // Maybe we should check against zero date?
	// Then 'fuzzydate' would be used.
	// See Field_timestamp::get_date(...)

	memset((char*) ltime, 0, sizeof(*ltime));		// safety

	ltime->year =   dt.Year();
    ltime->month =  dt.Month();
    ltime->day =    dt.Day();
    ltime->hour =   dt.Hour();
    ltime->minute = dt.Minute();
    ltime->second = dt.Second();
    ltime->second_part = 0;

    ltime->neg = (dt.IsNegative() ? 1 : 0);
    ltime->time_type = MYSQL_TIMESTAMP_DATETIME;

	return 0;
}

longlong Item_bhdate::val_int()
{
	return dt.Year() * 10000 + dt.Month() * 100 + dt.Day();
}
String* Item_bhdate::val_str(String *s)
{
	TIME ltime;
	get_date(&ltime, 0);
	s->alloc(19);
	make_date((DATE_TIME_FORMAT*) 0, &ltime, s);
	return s;
}
mysql_bool Item_bhdate::get_date(TIME *ltime, uint fuzzydate)
{
    // Maybe we should check against zero date?
	// Then 'fuzzydate' would be used.
	// See Field_timestamp::get_date(...)

	memset((char*) ltime, 0, sizeof(*ltime));

	ltime->year =   dt.Year();
    ltime->month =  dt.Month();
    ltime->day =    dt.Day();

    ltime->neg = (dt.IsNegative() ? 1 : 0);
    ltime->time_type = MYSQL_TIMESTAMP_DATE;

	return 0;
}

longlong Item_bhtime::val_int()
{
	return dt.Hour() * 10000 + dt.Minute() * 100 + dt.Second();
}
String* Item_bhtime::val_str(String *s)
{
	TIME ltime;
	get_time(&ltime);
	s->alloc(19);
	make_time((DATE_TIME_FORMAT*) 0, &ltime, s);
	return s;
}
namespace ib { namespace util {
template<typename T>
inline T abs( T const& a )
	{ return a >= 0 ? a : -a; }
} }
mysql_bool Item_bhtime::get_date(TIME *ltime, uint fuzzydate)
{
    // Maybe we should check against zero date?
	// Then 'fuzzydate' would be used.
	// See Field_timestamp::get_date(...)

	memset((char*) ltime, 0, sizeof(*ltime));

    ltime->hour =   ib::util::abs( dt.Hour() );
    ltime->minute = ib::util::abs( dt.Minute() );
    ltime->second = ib::util::abs( dt.Second() );
    ltime->neg = (dt.IsNegative() ? 1 : 0);
    ltime->time_type = MYSQL_TIMESTAMP_TIME;	// necessary for get_time(), not sure if OK in get_date()

	return 0;
}

longlong Item_bhyear::val_int()
{
	return length == 4 ? dt.Year() : dt.Year() % 100;
}
String* Item_bhyear::val_str(String *s)
{
	s->alloc(length+1);
	s->length(length);
	s->set_charset(&my_charset_bin);	// Safety
	sprintf((char*) s->ptr(), length == 2 ? "%02d" : "%04d", (int)val_int());
	return s;
}
mysql_bool Item_bhyear::get_date(TIME *ltime, uint fuzzydate)
{
	memset((char*) ltime, 0, sizeof(*ltime));
	return 1;
}

#endif
