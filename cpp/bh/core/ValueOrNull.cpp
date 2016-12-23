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

#include <cstdio>
#include "MysqlExpression.h"
#include "CQTerm.h"
#include "system/RCException.h"
#include "QuickMath.h"
#include "common/bhassert.h"
#include "ColumnType.h"

using namespace std;

#define MAX(a,b) ((a)>(b) ? (a) : (b))

DataType::DataType(AttributeType atype, int prec, int scale, DTCollation collation)
	:	precision(prec)
{
	valtype = VT_NOTKNOWN;
	attrtype = atype;
	fixscale = scale;
	fixmax = -1;
	this->collation = collation;

	switch(attrtype) {
		case RC_INT:			valtype = VT_FIXED; fixmax = MAX(BH_INT_MAX, -BH_INT_MIN); break;
		case RC_BIGINT:			valtype = VT_FIXED; fixmax = MAX(BH_BIGINT_MAX, -BH_BIGINT_MIN); break;
		case RC_MEDIUMINT:		valtype = VT_FIXED; fixmax = MAX(BH_MEDIUMINT_MAX, -BH_MEDIUMINT_MIN); break;
		case RC_SMALLINT:		valtype = VT_FIXED; fixmax = MAX(BH_SMALLINT_MAX, -BH_SMALLINT_MIN); break;
		case RC_BYTEINT:		valtype = VT_FIXED; fixmax = MAX(BH_TINYINT_MAX, -BH_TINYINT_MIN); break;

		case RC_NUM:
			assert((prec > 0) && (prec <= 19) && (fixscale >= 0));
			if(prec == 19)
				fixmax = PLUS_INF_64;
			else
				fixmax = QuickMath::power10i(prec) - 1;
			valtype = VT_FIXED;
			break;

		case RC_REAL:
		case RC_FLOAT:			valtype = VT_FLOAT;
			break;

		case RC_STRING:
		case RC_VARCHAR:
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
			valtype = VT_STRING;
			break;

		case RC_DATETIME:
		case RC_TIMESTAMP:
		case RC_TIME:
		case RC_DATE:
		case RC_YEAR:
			valtype = VT_DATETIME;
			break;

		case RC_DATETIME_N:
		case RC_TIMESTAMP_N:
		case RC_TIME_N:
			// NOT IMPLEMENTED YET
			;
	}
}

char* ValueOrNull::GetStringCopy()
{
	char* p = new char[len + 1];
	memcpy(p, sp, len);
	p[len] = 0;
	return p;
}

DataType& DataType::operator=(const ColumnType& ct)
{
	*this = DataType();
	if(!ct.IsKnown())
		return *this;

	*this = DataType(ct.GetTypeName(), ct.GetPrecision(), ct.GetScale(), ct.GetCollation());

	if(valtype == VT_NOTKNOWN) {
		char s[128];
		sprintf(s, "ColumnType (AttributeType #%d) is not convertible to a DataType", (int)ct.GetTypeName());
		throw RCException(s);
	}

	return *this;
}

void ValueOrNull::SetString(const RCBString& rcs)
{
	Clear();
	if(!rcs.IsNull()) {
		null = false;
		if(rcs.IsPersistent()) {
			string_owner = true;
			sp = new char[rcs.len + 1];
			memcpy(sp, rcs.val, rcs.len);
			sp[rcs.len] = 0;
		} else {
			sp = rcs.val;
			string_owner = false;
		}
		len = rcs.len;
	}
}

void ValueOrNull::MakeStringOwner()
{
	if(!sp || string_owner)
		return;
	char* tmp = new char[len + 1];
	memcpy(tmp, sp, len);
	tmp[len] = 0;
	sp = tmp;
	string_owner = true;
}

void ValueOrNull::GetString(RCBString& rcs) const
{
	if(null) {
		RCBString rcs_null;
		rcs = rcs_null;
	} else {
		// copy either from sp or x
		if(sp)
			rcs = RCBString(sp, len, true, false);
		else
			rcs = RCNum(x).ToRCString();
		rcs.MakePersistent();
	}
}

ValueOrNull::ValueOrNull(ValueOrNull const& von)
	: x(von.x), len(von.len), sp(von.string_owner ? new char[von.len + 1] : von.sp),
	string_owner(von.string_owner), null(von.null) 
{
	if(string_owner) {
		memcpy(sp, von.sp, len);
		sp[len] = 0;
	}
}

ValueOrNull& ValueOrNull::operator=(ValueOrNull const& von) 
{
	if(&von != this) {
		ValueOrNull tmp(von);
		swap(tmp);
	}
	return (*this);
}

ValueOrNull::ValueOrNull(RCNum const& rcn)
	: x(rcn.GetValueInt64()), len(0), sp(0), string_owner(false), null(rcn.IsNull()) 
{
}

ValueOrNull::ValueOrNull(RCDateTime const& rcdt)
	: x(rcdt.GetInt64()), len(0), sp(0), string_owner(false), null(rcdt.IsNull()) 
{
}

ValueOrNull::ValueOrNull(RCBString const& rcs)
	: x(NULL_VALUE_64), len(rcs.len), sp(new char[rcs.len + 1]), string_owner(true), null(rcs.IsNull()) 
{
	memcpy(sp, rcs.val, len);
	sp[len] = 0;
}

void ValueOrNull::swap(ValueOrNull& von) 			
{
	if(&von != this) {
		using std::swap;
		swap(null, von.null); 
		swap(x, von.x); 
		swap(sp,von.sp);
		swap(len,von.len); 
		swap(string_owner,von.string_owner);
	}
}
