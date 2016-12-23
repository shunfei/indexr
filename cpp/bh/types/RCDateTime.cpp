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

#include "RCDataTypes.h"
#include "system/RCSystem.h"
#include "core/RCEngine.h"
#include "ValueParserForText.h"
#include "core/tools.h"

#define CUSTOM_WARNING(P_W) void

using namespace std;

RCDateTime::RCDateTime(_int64 v, AttributeType at)
	:	at(at)
{
	*(_int64*)&this->dt = 0;
	null = (v == NULL_VALUE_64);
	if(!null) {
		*(_int64*)&dt = v;
		if(at == RC_DATE) {
			dt.second = 0;
			dt.minute = 0;
			dt.hour = 	0;
		} else if(at == RC_TIME) {
			dt.day		= 0;
			dt.month	= 0;
			dt.year		= 0;
		}
	}
}

RCDateTime::RCDateTime(short year)
{
	*(_int64*)&dt = 0;
	at = RC_YEAR;
	null = false;
	if(year == NULL_VALUE_SH)
		null = true;
	else {
		dt.year = abs(year);
		if(year < 0)
			Negate();
	}
}

RCDateTime::RCDateTime(short yh, short mm, short ds, AttributeType at)
	: 	at(at)
{
	*(_uint64*)&dt = 0;
	null = false;
	if(at == RC_DATE) {
		dt.day		= abs(ds);
		dt.month	= abs(mm);
		dt.year		= abs(yh);

		if(yh < 0)
			Negate();
	} else if(at == RC_TIME) {
		dt.second = abs(ds);
		dt.minute = abs(mm);
		dt.hour = 	abs(yh);

		if(yh < 0 || mm < 0 || ds < 0)
			Negate();
	} else
		BHERROR("type not supported");
}

RCDateTime::RCDateTime(short year, short month, short day, short hour, short minute, short second, AttributeType at)
	:	at(at)
{
	BHASSERT(at == RC_DATETIME || at == RC_TIMESTAMP, "should be 'at == RC_DATETIME || at == RC_TIMESTAMP'");
	*(_int64*)&dt = 0;
	null = false;
	dt.year =	abs(year);
	dt.month =	abs(month);
	dt.day =	abs(day);
	dt.hour =	abs(hour);
	dt.minute = abs(minute);
	dt.second = abs(second);

	if(year < 0)
		Negate();
}

RCDateTime::RCDateTime(RCNum& rcn, AttributeType at)
	:	at(at)
{
	*(_uint64*)&dt = 0;
	null = rcn.null;
	this->at = at;
	if(!null) {
		if(ATI::IsRealType(rcn.Type()))
			throw  DataTypeConversionRCException(BHError(BHERROR_DATACONVERSION));
		if(rcn.Type() == RC_NUM && rcn.Scale() > 0)
			throw  DataTypeConversionRCException(BHError(BHERROR_DATACONVERSION));
		if(Parse((_int64)rcn, *this, at) != BHRC_SUCCESS)
			throw  DataTypeConversionRCException(BHError(BHERROR_DATACONVERSION));
	}
}


RCDateTime::RCDateTime(const RCDateTime& rcdt)
{
	*this = rcdt;
}

RCDateTime::~RCDateTime()
{
}

RCDateTime& RCDateTime::operator=(const RCDateTime& rcv)
{
	*(_int64*)&dt = *(_int64*)&rcv.dt;
	this->at = rcv.at;
	this->null = rcv.null;
	return *this;
}

RCDateTime& RCDateTime::operator=(const RCDataType& rcv)
{
	if(rcv.GetValueType() == DATE_TIME_TYPE)
		*this = (RCDateTime&)rcv;
	else {
		BHERROR("bad cast");
		null = true;
	}
	return *this;
}

RCDateTime& RCDateTime::Assign(_int64 v, AttributeType at)
{
	this->at = at;
	null = (v == NULL_VALUE_64);
	if(null)
		*(_int64*)&dt = 0;
	else
		*(_int64*)&dt = v;
	return *this;
}

_int64 RCDateTime::GetInt64() const
{
	if(null)
		return NULL_VALUE_64;
	return *(_int64*)&dt;
}

bool RCDateTime::ToInt64(_int64& value) const
{
	if(!IsNull()) {
		if(at == RC_YEAR) {
			value = (int)dt.year;
			return true;
		} else if(at == RC_DATE) {
			value = Year() * 10000 + Month() * 100 + Day();
			return true;
		} else if(at == RC_TIME) {
			value = Hour() * 10000 + Minute() * 100  + Second();
			if(IsNegative())
				value = -value;
			return true;
		} else {
			//BHERROR("type not supported");
			value = Year() * 10000 + Month() * 100 + Day();
			value *= 1000000;
			value += Hour() * 10000 + Minute() * 100  + Second();
			return true;
		}
	} else
		value = NULL_VALUE_64;
	return false;
}

bool RCDateTime::IsZero() const
{
	return *this == GetSpecialValue(Type());
}

RCBString RCDateTime::ToRCString() const
{
	if(!IsNull()) {
		RCBString rcs(0, 30, true);
		char* buf = rcs.val;
		if(IsNegative())
			*buf++ = '-';
		if(at == RC_YEAR) {
			sprintf(buf, "%04d", abs(Year()));
		} else if(at == RC_DATE) {
			sprintf(buf, "%04d-%02d-%02d", (int)abs(Year()), (int)abs(Month()) , (int)abs(Day()));
		} else if(at == RC_TIME) {
			sprintf(buf, "%02d:%02d:%02d", (int)Hour(), (int)Minute(), (int)Second());
		} else if(at == RC_DATETIME || at == RC_TIMESTAMP) {
			sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d",
					(int)abs(Year()), (int)abs(Month()), (int)abs(Day()),
					(int)Hour(), (int)Minute(), (int)Second());
		} else
			BHERROR("type not supported");
		rcs.len = (uint)strlen(rcs.val);
		return rcs;
	}
	return RCBString();
}

BHReturnCode RCDateTime::Parse(const RCBString& rcs, RCDateTime& rcv, AttributeType at)
{
	return ValueParserForText::ParseDateTime(rcs, rcv, at);
}

BHReturnCode RCDateTime::Parse(const _int64& v , RCDateTime& rcv, AttributeType at, int precision)
{
	_int64 tmp_v = v < 0 ? -v : v;
	int sign = 1;
	if(v < 0)
		sign = -1;

	rcv.at = at;
	if(v == NULL_VALUE_64) {
		rcv.null = true;
		return BHRC_SUCCESS;
	} else
		rcv.null = false;

	if(at == RC_YEAR) {
		uint vv = (uint)v;
		vv = ToCorrectYear(vv, at, (precision >= 0 && precision < 4));
		if(IsCorrectBHYear((short)vv))
		{
			rcv.dt.year = (short)vv;
			return BHRC_SUCCESS;
		}
	} else if(at == RC_DATE) {
		if(!CanBeDay(tmp_v % 100)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.day = tmp_v % 100;
		tmp_v /= 100;
		if(!CanBeMonth(tmp_v % 100)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.month = tmp_v % 100;
		tmp_v /= 100;
		uint vv = uint(tmp_v);
		vv = ToCorrectYear(vv, at);
		if(!CanBeYear(vv)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.year = vv;
		if(sign == 1 && IsCorrectBHDate(short(rcv.dt.year), short(rcv.dt.month), short(rcv.dt.day)))
			return BHRC_SUCCESS;
	}
	else if(at == RC_TIME)
	{
		if(!CanBeSecond(tmp_v % 100)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.second = tmp_v % 100;
		tmp_v /= 100;
		if(!CanBeMinute(tmp_v % 100)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.minute = tmp_v % 100;
		tmp_v /= 100;

		if((tmp_v * sign) > RC_TIME_MAX.Hour()) {
			rcv = RC_TIME_MAX;
			return BHRC_OUT_OF_RANGE_VALUE;
		} else if((tmp_v * sign) < -RC_TIME_MIN.Hour()) {
			rcv = RC_TIME_MIN;
			return BHRC_OUT_OF_RANGE_VALUE;
		}

		rcv.dt.hour = tmp_v;

		if(IsCorrectBHTime(short(rcv.dt.hour*sign), short(rcv.dt.minute*sign), short(rcv.dt.second*sign))) {
			if(sign == -1)
				rcv.Negate();
			return BHRC_SUCCESS;
		} else {
			rcv = RC_TIME_SPEC;
			return BHRC_VALUE_TRUNCATED;
		}
	} else if(at == RC_DATETIME || at == RC_TIMESTAMP) {
		if(v > 245959) {
			if(!CanBeSecond(tmp_v % 100)) {
				rcv = GetSpecialValue(at);
				return BHRC_OUT_OF_RANGE_VALUE;
			}
			rcv.dt.second = tmp_v % 100;
			tmp_v /= 100;
			if(!CanBeMinute(tmp_v % 100)) {
				rcv = GetSpecialValue(at);
				return BHRC_OUT_OF_RANGE_VALUE;
			}
			rcv.dt.minute = tmp_v % 100;
			tmp_v /= 100;
			if(!CanBeHour(tmp_v % 100))	{
				rcv = GetSpecialValue(at);
				return BHRC_OUT_OF_RANGE_VALUE;
			}
			rcv.dt.hour = tmp_v % 100;
			tmp_v /= 100;
		}
		if(!CanBeDay(tmp_v % 100)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.day = tmp_v % 100;
		tmp_v /= 100;
		if(!CanBeMonth(tmp_v % 100)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.month = tmp_v % 100;
		tmp_v /= 100;
		if(!CanBeYear(tmp_v)) {
			rcv = GetSpecialValue(at);
			return BHRC_OUT_OF_RANGE_VALUE;
		}
		rcv.dt.year = RCDateTime::ToCorrectYear((uint)tmp_v, at);
		if(sign == 1 && at == RC_DATETIME && IsCorrectBHDatetime(rcv.dt.year, rcv.dt.month, rcv.dt.day, rcv.dt.hour, rcv.dt.minute, rcv.dt.second))
				return BHRC_SUCCESS;
		if(sign == 1 && at == RC_TIMESTAMP && IsCorrectBHTimestamp(short(rcv.dt.year), short(rcv.dt.month), short(rcv.dt.day), short(rcv.dt.hour), short(rcv.dt.minute), short(rcv.dt.second)))
			return BHRC_SUCCESS;
	}
	else
		BHERROR("type not supported");

	rcv = GetSpecialValue(at);
	return BHRC_OUT_OF_RANGE_VALUE;
}

bool RCDateTime::CanBeYear(_int64 year)
{
	if(year >= 0 && year <= 9999)
		return true;
	return false;
}

bool RCDateTime::CanBeMonth(_int64 month)
{
	if(month >= 1 && month <= 12)
		return true;
	return false;
}

bool RCDateTime::CanBeDay(_int64 day)
{
	if(day >= 1 && day <= 31)
		return true;
	return false;
}

bool RCDateTime::CanBeHour(_int64 hour)
{
	if(hour >= 0 && hour <= 23)
		return true;
	return false;

}

bool RCDateTime::CanBeMinute(_int64 minute)
{
	if(minute >= 0 && minute <= 59)
		return true;
	return false;
}

bool RCDateTime::CanBeSecond(_int64 second)
{
	return RCDateTime::CanBeMinute(second);
}

bool RCDateTime::CanBeDate(_int64 year, _int64 month, _int64 day)
{
	if(year == RC_DATE_SPEC.Year() && month == RC_DATE_SPEC.Month() && day == RC_DATE_SPEC.Day())
		return true;
	if(CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))))
			return true;
	return false;
}

bool RCDateTime::CanBeTime(_int64 hour, _int64 minute, _int64 second)
{
	if(hour == RC_TIME_SPEC.Hour() && minute == RC_TIME_SPEC.Minute() && second == RC_TIME_SPEC.Second())
		return true;
	if(hour >= -838 && hour <= 838 && CanBeMinute(minute) && CanBeSecond(second))
		return true;
	return false;
}

bool RCDateTime::CanBeTimestamp(_int64 year, _int64 month, _int64 day, _int64 hour, _int64 minute, _int64 second)
{
	if(
		year == RC_TIMESTAMP_SPEC.Year() && month == RC_TIMESTAMP_SPEC.Month() && day == RC_TIMESTAMP_SPEC.Day() &&
		hour == RC_TIMESTAMP_SPEC.Hour() && minute == RC_TIMESTAMP_SPEC.Minute() && second == RC_TIMESTAMP_SPEC.Second()
		)
		return true;
	if(CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))) && CanBeHour(hour) && CanBeMinute(minute) && CanBeSecond(second))
		return true;
	return false;
}

bool RCDateTime::CanBeDatetime(_int64 year, _int64 month, _int64 day, _int64 hour, _int64 minute, _int64 second)
{
	if(
		year == RC_DATETIME_SPEC.Year() && month == RC_DATETIME_SPEC.Month() && day == RC_DATETIME_SPEC.Day() &&
		hour == RC_DATETIME_SPEC.Hour() && minute == RC_DATETIME_SPEC.Minute() && second == RC_DATETIME_SPEC.Second()
	)
		return true;
	if(CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth((ushort)year, (ushort)month))) && CanBeHour(hour) && CanBeMinute(minute) && CanBeSecond(second))
		return true;
	return false;
}

bool RCDateTime::IsCorrectBHYear(short year)
{
	return year == RC_YEAR_SPEC.Year() || (CanBeYear(year) && (year >= RC_YEAR_MIN.Year() && year <= RC_YEAR_MAX.Year()));
}

bool RCDateTime::IsCorrectBHDate(short year, short month, short day)
{
	if(year == RC_DATE_SPEC.Year() && month == RC_DATE_SPEC.Month() && day == RC_DATE_SPEC.Day())
		return true;
	if(CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month))))
	{
		if(
			((year >= RC_DATE_MIN.Year() && month >= RC_DATE_MIN.Month() && day >= RC_DATE_MIN.Day()) &&
			(year <= RC_DATE_MAX.Year() && month <= RC_DATE_MAX.Month() && day <= RC_DATE_MAX.Day()))
		)
		return true;
	}
	return false;
}

bool RCDateTime::IsCorrectBHTime(short hour, short minute, short second)
{
	if(hour == RC_TIME_SPEC.Hour() && minute == RC_TIME_SPEC.Minute() && second == RC_TIME_SPEC.Second())
		return true;
	bool haspositive = false;
	bool hasnegative = false;
	if(hour < 0 || minute < 0 || second < 0)
		hasnegative = true;
	if(hour > 0 || minute > 0 || second > 0)
		haspositive = true;

	if(hasnegative == haspositive && (hour != 0 || minute != 0 || second != 0))
		return false;

	if(hour >= -RC_TIME_MIN.Hour() && hour <= RC_TIME_MAX.Hour() && (CanBeMinute(minute) || CanBeMinute(-minute)) && (CanBeSecond(second) || CanBeSecond(-second)))
			return true;
	return false;
}

bool RCDateTime::IsCorrectBHTimestamp(short year, short month, short day, short hour, short minute, short second)
{
	if(
		year == RC_TIMESTAMP_SPEC.Year() && month == RC_TIMESTAMP_SPEC.Month() && day == RC_TIMESTAMP_SPEC.Day() &&
		hour == RC_TIMESTAMP_SPEC.Hour() && minute == RC_TIMESTAMP_SPEC.Minute() && second == RC_TIMESTAMP_SPEC.Second()
	)
		return true;
	if(CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month))) &&
			CanBeHour(hour) && CanBeMinute(minute) && CanBeSecond(second))
	{
		RCDateTime rcdt(year, month, day, hour, minute, second, RC_TIMESTAMP);
		if(rcdt >= RC_TIMESTAMP_MIN && rcdt <= RC_TIMESTAMP_MAX)
			return true;
	}
	return false;
}

bool RCDateTime::IsCorrectBHDatetime(short year, short month, short day, short hour, short minute, short second)
{
	if(
		year == RC_DATETIME_SPEC.Year() && month == RC_DATETIME_SPEC.Month() && day == RC_DATETIME_SPEC.Day() &&
		hour == RC_DATETIME_SPEC.Hour() && minute == RC_DATETIME_SPEC.Minute() && second == RC_DATETIME_SPEC.Second()
		)
		return true;
	if(CanBeYear(year) && CanBeMonth(month) && (day > 0 && (day <= NoDaysInMonth(year, month))) &&
			CanBeHour(hour) && CanBeMinute(minute) && CanBeSecond(second))
	{
		if(
			(
			year >= RC_DATETIME_MIN.Year() && month >= RC_DATETIME_MIN.Month() && day >= RC_DATETIME_MIN.Day() &&
			hour >= RC_DATETIME_MIN.Hour() && minute >= RC_DATETIME_MIN.Minute() && second >= RC_DATETIME_MIN.Second()
			) &&
			(
			year <= RC_DATETIME_MAX.Year() && month <= RC_DATETIME_MAX.Month() && day <= RC_DATETIME_MAX.Day() &&
			hour <= RC_DATETIME_MAX.Hour() && minute <= RC_DATETIME_MAX.Minute() && second <= RC_DATETIME_MAX.Second()
			)
		)
			return true;
	}
	return false;
}

bool RCDateTime::IsLeapYear(short year)
{
	if(year == 0)
		return false;
	if(!CanBeYear(year))
		throw DataTypeConversionRCException(BHERROR_DATACONVERSION);
	return ((year & 3) == 0 && year % 100) || ((year & 3) == 0 && year % 400 == 0);
}

ushort RCDateTime::NoDaysInMonth(short year, ushort month)
{
	static const ushort no_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	if(!CanBeYear(year) || !CanBeMonth(month))
		throw DataTypeConversionRCException(BHERROR_DATACONVERSION);
	if(month == 2 && IsLeapYear(year))
		return 29;
	return no_days[month - 1];
}

short RCDateTime::ToCorrectYear(uint v, AttributeType at, bool is_year_2 /*= false*/)
{
	switch(at)
	{
		case RC_YEAR :
			if(v == 0 && is_year_2) // 0 for year(2) corresponds to 2000
				return 2000;
			if(v <= 0 || v > 2155)
				return v;
			else if(v < 100)
			{
				if(v <= 69)
					return v + 2000;
				return v + 1900;
			}
			return (short)v;
		case RC_DATE :
		case RC_DATETIME :
		case RC_TIMESTAMP :
			if(v < 100)
			{
				if(v <= 69)
					return v + 2000;
				return v + 1900;
			}
			return (short)v;
		default :
			BHERROR("type not supported");
	}
	return 0;						// to avoid errors in release version
}

RCDateTime RCDateTime::GetSpecialValue(AttributeType at)
{
	switch(at) {
		case RC_YEAR :
			return RC_YEAR_SPEC;
		case RC_TIME :
			return RC_TIME_SPEC;
		case RC_DATE :
			return RC_DATE_SPEC;
		case RC_DATETIME :
			return RC_DATETIME_SPEC;
		case RC_TIMESTAMP :
			return RC_TIMESTAMP_SPEC;
		default :
			BHERROR("type not supported");
	}
	return RC_DATETIME_SPEC;		// to avoid errors in release version
}

RCDateTime RCDateTime::GetCurrent()
{
	time_t const curr = time(0);
	tm const tmt = *localtime (&curr);
	RCDateTime rcdt(tmt.tm_year + 1900, tmt.tm_mon + 1, tmt.tm_mday, tmt.tm_hour, tmt.tm_min, tmt.tm_sec, RC_DATETIME);
	return rcdt;
}

bool RCDateTime::operator==(const RCDataType& rcv)const
{
	if(!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull())
		return false;
	if(rcv.GetValueType() == DATE_TIME_TYPE)
		return compare((RCDateTime&)rcv) == 0;
	else if(rcv.GetValueType() == NUMERIC_TYPE) {
		return compare((RCNum&)rcv) == 0;
	}
	return false;
}

bool RCDateTime::operator<(const RCDataType& rcv) const
{
	if(!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull())
		return false;
	if(rcv.GetValueType() == DATE_TIME_TYPE)
		return compare((RCDateTime&)rcv) < 0;
	else if(rcv.GetValueType() == NUMERIC_TYPE)
		return compare((RCNum&)rcv) < 0;
	return false;
}

bool RCDateTime::operator>(const RCDataType& rcv) const
{
	if(!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull())
		return false;
	if(rcv.GetValueType() == DATE_TIME_TYPE)
		return compare((RCDateTime&)rcv) > 0;
	else if(rcv.GetValueType() == NUMERIC_TYPE)
			return compare((RCNum&)rcv) > 0;
	return false;
}

bool RCDateTime::operator>=(const RCDataType& rcv) const
{
	if(!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull())
		return false;
	if(rcv.GetValueType() == DATE_TIME_TYPE)
		return compare((RCDateTime&)rcv) >= 0;
	else if(rcv.GetValueType() == NUMERIC_TYPE)
			return compare((RCNum&)rcv) >= 0;
	return false;
}

bool RCDateTime::operator<=(const RCDataType& rcv) const
{
	if(!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull())
		return false;
	if(rcv.GetValueType() == DATE_TIME_TYPE)
		return compare((RCDateTime&)rcv) <= 0;
	else if(rcv.GetValueType() == NUMERIC_TYPE)
			return compare((RCNum&)rcv) <= 0;
	return false;
}

bool RCDateTime::operator!=(const RCDataType& rcv) const
{
	if(!AreComparable(at, rcv.Type()) || IsNull() || rcv.IsNull())
		return false;
	if(rcv.GetValueType() == DATE_TIME_TYPE)
			return compare((RCDateTime&)rcv) != 0;
	else if(rcv.GetValueType() == NUMERIC_TYPE)
			return compare((RCNum&)rcv) != 0;
	return false;
}

_int64 RCDateTime::operator-(const RCDateTime& sec) const
{
	if(at != RC_DATE || sec.at != RC_DATE || IsNull() || sec.IsNull())
		return NULL_VALUE_64;
	_int64 result = 0;			// span in days for [sec., ..., this]
	bool notless_than_sec = (this->operator>(sec));	
	if(notless_than_sec) {
		if(dt.year == sec.dt.year) {
			if(dt.month == sec.dt.month) {
				result = dt.day - sec.dt.day;
			} else {
				for(int i = int(sec.dt.month) + 1; i < dt.month; i++)
					result += NoDaysInMonth(dt.year, i);
				result += NoDaysInMonth(sec.dt.year, sec.dt.month) - sec.dt.day + 1;
				result += dt.day - 1;
			}
		} else {
			for(int i = int(sec.dt.year) + 1; i < dt.year; i++)
				result += (IsLeapYear(i) ? 366 : 365);
			for(int i = int(sec.dt.month) + 1; i <= 12; i++)
				result += NoDaysInMonth(sec.dt.year, i);
			for(int i = 1; i < dt.month; i++)
				result += NoDaysInMonth(dt.year, i);
			result += NoDaysInMonth(sec.dt.year, sec.dt.month) - sec.dt.day + 1;
			result += dt.day - 1;
		}
	} else {
		if(dt.year == sec.dt.year) {
			if(dt.month == sec.dt.month) {
				result =  sec.dt.day - dt.day;
			} else {
				for(int i = int(dt.month) + 1; i < sec.dt.month; i++)
					result += NoDaysInMonth(sec.dt.year, i);
				result += NoDaysInMonth(dt.year, dt.month) - dt.day + 1;
				result += sec.dt.day - 1;
			}
		} else {
			for(int i = int(dt.year) + 1; i < sec.dt.year; i++)
				result += (IsLeapYear(i) ? 366 : 365);
			for(int i = int(dt.month) + 1; i <= 12; i++)
				result += NoDaysInMonth(dt.year, i);
			for(int i = 1; i < sec.dt.month; i++)
				result += NoDaysInMonth(sec.dt.year, i);
			result += NoDaysInMonth(dt.year, dt.month) - dt.day + 1;
			result += sec.dt.day - 1;
		}
	}

	return notless_than_sec ? result : -result;
}

_int64 RCDateTime::ToDays()										// the number of days since year 0
{
	if((at != RC_DATE && at != RC_DATETIME && at != RC_TIMESTAMP) || IsNull())
		return NULL_VALUE_64;
	_int64 res = 0;
	int year = 0;
	if(dt.year >= 2000) {
		res = 730484;						// = to_days('2000-01-01') - 1
		year = 2000;
	} else {
		for(int i = 0; i < ((int) (dt.year) - 99); i += 100) {		// by centuries
			res += (IsLeapYear(i) ? 36525 : 36524);
			year += 100;
		}
	}
	for(int i = year; i < dt.year; i++)
		res += (IsLeapYear(i) ? 366 : 365);
	res += DayOfYear();
	return res;
}

_int64 RCDateTime::DayOfYear()									// Jan. 1st is day 1
{
	static const ushort no_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	if((at != RC_DATE && at != RC_DATETIME && at != RC_TIMESTAMP) || IsNull())
		return NULL_VALUE_64;
	_int64 res = 0;
	for(int i = 1; i < dt.month; i++) {
		if(i == 2 && IsLeapYear(dt.year))
			res += 29;
		else
			res += no_days[i - 1];
	}
	res += dt.day;
	return res;
}

AttributeType RCDateTime::Type() const
{
	return at;
}

uint RCDateTime::GetHashCode() const
{
	_uint64 v = *(_uint64*)&dt;
	return (uint)(v >> 32) + (uint)(v) /*+ *(short*)&tz*/;
}

int RCDateTime::compare(const RCDateTime& rcv) const
{
/*	if(!AreComparable(at, rcv.at)) {
		BHERROR("types not comparable");
	}
	if(IsNull() || rcv.IsNull())
		return false;
*/								// these cases are checked in operator implementations
	_int64 v1 = *(_int64*)&dt;
	_int64 v2 = *(_int64*)&rcv.dt;
	return (v1 < v2 ? -1 : (v1 > v2 ? 1 : 0));
}

int RCDateTime::compare(const RCNum& rcv) const
{
	if(IsNull() || rcv.IsNull())
		return false;
	_int64 tmp;
	ToInt64(tmp);
	return int(tmp - ((RCNum &)rcv).GetIntPart());
}

size_t RCDateTime::GetStorageByteSize() const
{
	return sizeof(null) + (null ? 0 : sizeof(at) + sizeof(DT));
}

void RCDateTime::ToByteStream(char*& buf) const
{
	store_bytes(buf, null);
	if(!null) {
		store_bytes(buf, at);
		store_bytes(buf, dt);
	}
}

void RCDateTime::AssignFromByteStream(char*& buf)
{
	unstore_bytes(null, buf);
	if(!null) {
		unstore_bytes(at, buf);
		unstore_bytes(dt, buf);
	}
}

void RCDateTime::ShiftOfPeriod(short sign_, short minutes_)
{
	//cerr << "To convert: " << (string)ToRCString() << endl;
#ifndef PURE_LIBRARY
	if(IsNull()) {
		return;
	}
	TIME t;
	t.year = Year();
	t.month = Month();
	t.day = Day();
	t.hour = Hour();
	t.minute = Minute();
	t.second = Second();
	t.second_part = 0;
	t.neg = 0;
	t.time_type = MYSQL_TIMESTAMP_DATETIME;
	if(t.year == 0 || t.month == 0 || t.day == 0)
		return;

	time_t secs = 0;
	if(minutes_ == NULL_VALUE_SH) {
		// System time zone
		my_bool myb;
		long tz;
		// time in system time zone is converted into UTC and expressed as seconds since EPOCHE
		secs = my_system_gmt_sec(&t, &tz, &myb);
	} else {
		// time in client time zone is converted into UTC and expressed as seconds since EPOCHE
		secs = sec_since_epoch_TIME(&t) + sign_ * minutes_ * 60;
	}
	// UTC seconds converted to UTC struct tm
	struct tm utc_t;
	gmtime_r(&secs, &utc_t);
	*this = RCDateTime((utc_t.tm_year + 1900) % 10000, utc_t.tm_mon + 1, utc_t.tm_mday, utc_t.tm_hour, utc_t.tm_min, utc_t.tm_sec, RC_TIMESTAMP);
	//cerr << "Converted: " << (string)ToRCString() << endl;
#else
	//cerr << "To convert: " << (string)ToRCString() << endl;
	if(IsNull() || IsZero())
		return;
	struct tm broken;
	memset(&broken, 0, sizeof(tm));
	broken.tm_isdst = -1;
	broken.tm_year = Year() - 1900;
	broken.tm_mon = Month() - 1;
	broken.tm_mday = Day();
	broken.tm_hour = Hour();
	broken.tm_min = Minute();
	broken.tm_sec = Second();
	time_t ts( mktime( &broken ) + ( sign_ * minutes_ * 60 ) );
	gmtime_r(&ts, &broken);
	operator = ( RCDateTime((broken.tm_year + 1900) % 10000, broken.tm_mon + 1, broken.tm_mday, broken.tm_hour, broken.tm_min, broken.tm_sec, RC_TIMESTAMP) );
	//cerr << "Converted: " << (string)ToRCString() << endl;
#endif
}

void RCDateTime::AdjustTimezone(RCDateTime& dt)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	// timezone conversion
	if(!dt.IsZero()) {
		THD& thd = ConnectionInfoOnTLS->Thd();
		TIME t;
		t.year = dt.Year();
		t.month = dt.Month();
		t.day = dt.Day();
		t.hour = dt.Hour();
		t.minute = dt.Minute();
		t.second = dt.Second();
		t.second_part = 0;
		t.time_type = MYSQL_TIMESTAMP_DATETIME;
		time_t secs = 0;
		short sign, minutes;
		RCEngine::ComputeTimeZoneDiffInMinutes(&thd, sign, minutes);
		if(minutes == NULL_VALUE_SH) {
			// System time zone
			// time in system time zone is converted into UTC and expressed as seconds since EPOCHE
			TIME time_tmp;
			thd.variables.time_zone->gmt_sec_to_TIME(&time_tmp, sec_since_epoch_TIME(&t));
			secs = sec_since_epoch_TIME(&time_tmp);

		} else {
			// time in client time zone is converted into UTC and expressed as seconds since EPOCHE
			secs = sec_since_epoch_TIME(&t) - sign * minutes * 60;
		}
		// UTC seconds converted to UTC struct tm
		struct tm utc_t;
		gmtime_r(&secs, &utc_t);
		// UTC time stored on server
		dt = RCDateTime((utc_t.tm_year + 1900) % 10000, utc_t.tm_mon + 1, utc_t.tm_mday, utc_t.tm_hour, utc_t.tm_min, utc_t.tm_sec, RC_TIMESTAMP);
	}
#endif
}

