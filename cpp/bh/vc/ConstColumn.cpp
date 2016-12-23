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
#include "ConstColumn.h"
#include "core/MysqlExpression.h"
#include "core/RCAttr.h"

extern void GMTSec2GMTTime(MYSQL_TIME* tmp, my_time_t t);
extern bool IsTimeStampZero(MYSQL_TIME& t);

ConstColumn::ConstColumn(ValueOrNull const& val, ColumnType const& c, bool shift_to_UTC) : VirtualColumn(c, NULL), value(val)
{
	dim = -1;
	if(ct.IsString())				
		ct.SetPrecision(c.GetPrecision());
	RCDateTime();
	if(c.GetTypeName() == RC_TIMESTAMP && shift_to_UTC) {
		RCDateTime rcdt(val.x, RC_TIMESTAMP);
		// needs to convert value to UTC
		MYSQL_TIME myt;
		memset(&myt, 0, sizeof(MYSQL_TIME));
		myt.year = rcdt.Year();
		myt.month = rcdt.Month();
		myt.day = rcdt.Day();
		myt.hour = rcdt.Hour();
		myt.minute = rcdt.Minute();
		myt.second = rcdt.Second();
		myt.time_type = MYSQL_TIMESTAMP_DATETIME;
#ifndef PURE_LIBRARY
		if(!IsTimeStampZero(myt)) {
			my_bool myb;
			// convert local time to UTC seconds from beg. of EPOCHE
			my_time_t secs_utc = ConnectionInfoOnTLS.Get().Thd().variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
			// UTC seconds converted to UTC TIME
			GMTSec2GMTTime(&myt, secs_utc);
		}
#endif
		rcdt = RCDateTime((short)myt.year, (short)myt.month, (short)myt.day, (short)myt.hour, (short)myt.minute, (short)myt.second, RC_TIMESTAMP);
		value.x = rcdt.GetInt64();
	}
}

ConstColumn::ConstColumn(const RCValueObject& v, const ColumnType& c) : VirtualColumn(c, NULL), value()
{
	dim = -1;
	if(c.IsString()) {
		value = ValueOrNull(v.ToRCString());
		ct.SetPrecision(value.len);
	} else if(c.IsNumeric() && !c.IsDateTime()) {
		if(v.GetValueType() == NUMERIC_TYPE)
			value = ValueOrNull(static_cast<RCNum&>(v));
		else if(v.GetValueType() == STRING_TYPE) {
			RCNum rcn;
			if(c.IsFloat())
				RCNum::ParseReal(v.ToRCString(), rcn, c.GetTypeName());
			else
				RCNum::ParseNum(v.ToRCString(), rcn);
			value = rcn;
		} else if(v.GetValueType() == NULL_TYPE)
			value = ValueOrNull();
		else 
			throw DataTypeConversionRCException(BHERROR_DATACONVERSION);
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(v.GetValueType() == DATE_TIME_TYPE);
		// TODO: if it is non-date-time a proper conversion should be done here
		value = ValueOrNull(static_cast<RCDateTime&>(v));
	}
}

double ConstColumn::DoGetValueDouble(const MIIterator& mit) {
	assert(ATI::IsNumericType(TypeName()));
	double val = 0;
	if (value.IsNull())
		val = NULL_VALUE_D;
	else if (ATI::IsIntegerType(TypeName()))
		val = (double) value.Get64();
	else if(ATI::IsFixedNumericType(TypeName()))
		val = ((double) value.Get64()) / PowOfTen(ct.GetScale());
	else if(ATI::IsRealType(TypeName())) {
		union { double d; _int64 i;} u;
		u.i = value.Get64();
		val = u.d;
	} else if(ATI::IsDateTimeType(TypeName())) {
		RCDateTime vd(value.Get64(), TypeName());	// 274886765314048  ->  2000-01-01
		_int64 vd_conv = 0;
		vd.ToInt64(vd_conv);			// 2000-01-01  ->  20000101
		val = (double)vd_conv;
	} else if(ATI::IsStringType(TypeName())) {
		char *vs = value.GetStringCopy();
		if(vs)
			val = atof(vs);
		delete [] vs;
	} else
		assert(0 && "conversion to double not implemented");
	return val;
}

RCValueObject ConstColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num) {

	if (value.null)
		return RCValueObject();

	if(ATI::IsStringType((TypeName()))) {
		RCBString s;
		value.GetString(s);
		return s;
	}
	if(ATI::IsIntegerType(TypeName()))
		return RCNum(value.Get64(), -1, false, TypeName());
	if(ATI::IsDateTimeType(TypeName()))
		return RCDateTime(value.GetDateTime64() , TypeName());
	if(ATI::IsRealType(TypeName()))
		return RCNum(value.Get64(), 0, true, TypeName());
	if(lookup_to_num || TypeName() == RC_NUM)
		return RCNum((_int64)value.Get64(), Type().GetScale());
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Illegal execution path");
	return RCValueObject();
}

void ConstColumn::DoGetValueString(RCBString& s, const MIIterator &mit)
{
	s = GetValue(mit).ToRCString();
}

_int64 ConstColumn::DoGetSum(const MIIterator &mit, bool &nonnegative)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!ATI::IsStringType(TypeName()));
	nonnegative = true;
	if(value.IsNull())
		return NULL_VALUE_64;		// note that this is a bit ambiguous: the same is for sum of nulls and for "not implemented"
	if( ATI::IsRealType(TypeName()) ) {
		double res = value.GetDouble() * mit.GetPackSizeLeft();
		return *(_int64 *)&res;
	}
	return (value.Get64() * mit.GetPackSizeLeft());
}

RCBString ConstColumn::DoGetMinString(const MIIterator &mit) {
	RCBString s;
	value.GetString(s);
	return s;
}

RCBString ConstColumn::DoGetMaxString(const MIIterator &mit) {
	RCBString s;
	value.GetString(s);
	return s;
}

_int64 ConstColumn::DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind)
{
	return 1;
}

_int64 ConstColumn::GetExactDistVals()
{
	return (value.IsNull() ? 0 : 1);
}


ushort ConstColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return ct.GetDisplaySize();
}

PackOntologicalStatus ConstColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	if (value.IsNull())
		return NULLS_ONLY;
	return UNIFORM;
}

void ConstColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc)
{
	assert(0); 	// comparison of a const with a const should be simplified earlier
}

char *ConstColumn::ToString(char p_buf[], size_t buf_ct) const
{
	if(value.IsNull() || value.Get64() == NULL_VALUE_64)
		snprintf(p_buf, buf_ct, "<null>");
	else if(value.Get64() == PLUS_INF_64)
		snprintf(p_buf, buf_ct, "+inf");
	else if(value.Get64() == MINUS_INF_64)
		snprintf(p_buf, buf_ct, "-inf");
	else if(ct.IsInt())
		snprintf(p_buf, buf_ct, "%lld", value.Get64());
	else if(ct.IsFixed())
		snprintf(p_buf, buf_ct, "%g", value.Get64() / PowOfTen(ct.GetScale()));
	else if(ct.IsFloat())
		snprintf(p_buf, buf_ct, "%g", value.GetDouble());
	else if(ct.IsString()) {
		RCBString val;
		value.GetString(val);
		snprintf(p_buf, buf_ct - 2, "\"%.*s", (int)(val.len < buf_ct - 4 ? val.len : buf_ct - 4), val.GetDataBytesPointer());
		strcat(p_buf, "\"");
	}
	return p_buf;
}
