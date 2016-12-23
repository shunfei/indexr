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

#include <boost/math/special_functions/round.hpp>

#include "common/bhassert.h"
#include "RCDataTypes.h"
#include "RCNum.h"
#include "ValueParserForText.h"
#include "system/TextUtils.h"
#include "core/tools.h"

using namespace std;

RCNum::RCNum(AttributeType attrt)
	:	value(0), m_scale(0), dbl(false), dot(false), attrt(attrt)
{
}

RCNum::RCNum(_int64 value, short scale, bool dbl, AttributeType attrt)
{
	Assign(value, scale, dbl, attrt);
}

RCNum::RCNum(double value)
	: value(*(_int64*)&value), m_scale(0), dbl(true), dot(false), attrt(RC_REAL)
{
	null = (value == NULL_VALUE_D ? true : false);
}

RCNum::RCNum(const RCNum& rcn)
	:	value(rcn.value), m_scale(rcn.m_scale), dbl(rcn.dbl), dot(rcn.dot), attrt(rcn.attrt)
{
	null = rcn.null;
}

RCNum::~RCNum()
{
}

RCNum& RCNum::Assign(_int64 value, short scale, bool dbl, AttributeType attrt)
{
	this->value = value;
	this->m_scale = scale;
	this->dbl = dbl;
	this->attrt = attrt;

	if(scale != -1 && !dbl) {
		if(scale != 0 || attrt == RC_UNKNOWN) {
			dot = true;
			this->attrt = RC_NUM;
		}
	}
	if(scale <= -1 && !dbl)
		m_scale = 0;
	if(dbl) {
		if(!(this->attrt == RC_REAL || this->attrt == RC_FLOAT))
			this->attrt = RC_REAL;
		this->dot = false;
		m_scale = 0;
		null = (value == *(_int64*) & NULL_VALUE_D ? true : false);
	}
	else
		null = (value == NULL_VALUE_64 ? true : false);
	return *this;
}

RCNum& RCNum::Assign(double value)
{
	this->value = *(_int64*)&value;
	this->m_scale = 0;
	this->dbl = true;
	this->dot = false;
	this->attrt = RC_REAL;
	double_int_t v(value);
	null = (v.i == NULL_VALUE_64 ? true : false);
	return *this;
}

BHReturnCode RCNum::Parse(const RCBString& rcs, RCNum& rcn, AttributeType at)
{
	return ValueParserForText::Parse(rcs, rcn, at);
}

BHReturnCode RCNum::ParseReal(const RCBString& rcbs, RCNum& rcn, AttributeType at)
{
	return ValueParserForText::ParseReal(rcbs, rcn, at);
}

BHReturnCode RCNum::ParseNum(const RCBString& rcs, RCNum& rcn, short scale)
{
	return ValueParserForText::ParseNum(rcs, rcn, scale);
}

RCNum& RCNum::operator=(const RCNum& rcn)
{
	value = rcn.value;
	dbl = rcn.dbl;
	m_scale = rcn.m_scale;
	null = rcn.null;
	attrt = rcn.attrt;
	return *this;
}

RCNum& RCNum::operator=(const RCDataType& rcdt)
{
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		*this = (RCNum&)rcdt;
	else {
		RCNum rcn1;
		if(BHReturn::IsError(RCNum::Parse(rcdt.ToRCString(), rcn1, this->attrt))) {
			*this = rcn1;
		} else {
			BHERROR("Unsupported assign operation!");
			null = true;
		}
	}
	return *this;
}

AttributeType RCNum::Type() const
{
	return attrt;
}

bool RCNum::IsDecimal(ushort scale) const
{
	if(ATI::IsIntegerType(this->attrt)) {
		return GetDecIntLen() <= (MAX_DEC_PRECISION - scale);
	} else if(attrt == RC_NUM) {
		if(this->GetDecFractLen() <= scale)
			return true;
		if(m_scale > scale)
			return value % (_int64)Uint64PowOfTen(m_scale - scale) == 0;
		return true;
	} else {
		double f = GetFractPart();
		return f == 0.0 || (fabs(f) >= (1.0 / (double)Uint64PowOfTen(scale)));
	}
	return false;
}

bool RCNum::IsInt() const
{
	if(!dbl) {
		if((value % (_int64)Uint64PowOfTen(m_scale)) != 0) {
			return false;
		}
		return true;
	}
	return false;
}

RCNum RCNum::ToDecimal(int scale) const
{
	_int64 tmpv = 0;
	short tmpp = 0;
	int sign = 1;
	if(dbl) {
		double intpart(0);
		double fracpart(modf(*(double*)&value, &intpart));

		if(intpart < 0 || fracpart < 0)	{
			sign = -1;
			intpart = fabs(intpart);
			fracpart = fabs(fracpart);
		}

		if(scale == -1)	{
			if(intpart >= Uint64PowOfTen(MAX_DEC_PRECISION)) {
				if(sign != 1)
					scale = 0;
			} else {
				int l = 0;
				if(intpart != 0)
					l = (int)floor(log10(intpart)) + 1;
				scale = MAX_DEC_PRECISION - l;
			}
		}

		if(intpart >= Uint64PowOfTen(MAX_DEC_PRECISION)) {
			tmpv = (Uint64PowOfTen(MAX_DEC_PRECISION) - 1);
		} else
			tmpv = (_int64)intpart * Uint64PowOfTen(scale) + boost::math::llround(fracpart * Uint64PowOfTen(scale));

		tmpp = scale;
	} else {
		tmpv = this->value;
		tmpp = this->m_scale;
		if(scale != -1)	{
			if(tmpp > scale)
				tmpv /= (_int64)Uint64PowOfTen(tmpp - scale);
			else
				tmpv *=  (_int64)Uint64PowOfTen(scale - tmpp);
			tmpp = scale;
		}
	}
	return RCNum(tmpv*sign, tmpp);
}

RCNum RCNum::ToReal() const
{
	if(ATI::IsRealType(attrt)) {
		return RCNum(*(double*)&value);
	}
	return RCNum((double)((double)(this->value / PowOfTen(m_scale))));
}

RCNum RCNum::ToInt() const
{
	return GetIntPart();
}

RCBString RCNum::ToRCString() const
{
	if(!IsNull()) {
		static int const SIZE(24);
		char buf[SIZE];
		if(ATI::IsRealType(attrt)) {
			gcvt(*(double*)&value, 15, buf);
			size_t s = strlen(buf);
			if(s && buf[s - 1] == '.')
				buf[s - 1] = 0;
		}
		else if(ATI::IsIntegerType(attrt))
			sprintf(buf, "%lld", value);
		else {
			return RCBString((const char*)Text(value, buf, m_scale), (int)0, true);
		}
		return RCBString(buf, (int)strlen(buf), true);
	}
	return RCBString();
}

RCNum::operator _int64() const
{
	return GetIntPart();
}

RCNum::operator double() const
{
	return (ATI::IsRealType(Type()) || Type() == RC_FLOAT) ? *(double*)&value : GetIntPart() + GetFractPart();
}

bool RCNum::operator==(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		return (compare((RCNum&)rcdt) == 0);
	if(rcdt.GetValueType() == DATE_TIME_TYPE)
		return (compare((RCDateTime&)rcdt) == 0);
	if(rcdt.GetValueType() == STRING_TYPE)
		return (rcdt == this->ToRCString());
	BHERROR("Bad cast inside RCNum");
	return false;
}

bool RCNum::operator!=(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		return (compare((RCNum&)rcdt) != 0);
	if(rcdt.GetValueType() == DATE_TIME_TYPE)
		return (compare((RCDateTime&)rcdt) != 0);
	if(rcdt.GetValueType() == STRING_TYPE)
		return (rcdt != this->ToRCString());
	BHERROR("Bad cast inside RCNum");
	return false;
}

bool RCNum::operator<(const RCDataType& rcdt) const
{
	if(IsNull() || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		return (compare((RCNum&)rcdt) < 0);
	if(rcdt.GetValueType() == DATE_TIME_TYPE)
		return (compare((RCDateTime&)rcdt) < 0);
	if(rcdt.GetValueType() == STRING_TYPE)
		return (this->ToRCString() < rcdt);
	BHERROR("Bad cast inside RCNum");
	return false;
}

bool RCNum::operator>(const RCDataType& rcdt) const
{
	if(IsNull() || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		return (compare((RCNum&)rcdt) > 0);
	if(rcdt.GetValueType() == DATE_TIME_TYPE)
		return (compare((RCDateTime&)rcdt) > 0);
	if(rcdt.GetValueType() == STRING_TYPE)
		return (this->ToRCString() > rcdt);
	BHERROR("Bad cast inside RCNum");
	return false;
}

bool RCNum::operator<=(const RCDataType& rcdt) const
{
	if(IsNull() || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		return (compare((RCNum&)rcdt) <= 0);
	if(rcdt.GetValueType() == DATE_TIME_TYPE)
		return (compare((RCDateTime&)rcdt) <= 0);
	if(rcdt.GetValueType() == STRING_TYPE)
		return (this->ToRCString() <= rcdt);
	BHERROR("Bad cast inside RCNum");
	return false;
}

bool RCNum::operator>=(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == NUMERIC_TYPE)
		return (compare((RCNum&)rcdt) >= 0);
	if(rcdt.GetValueType() == DATE_TIME_TYPE)
		return (compare((RCDateTime&)rcdt) >= 0);
	if(rcdt.GetValueType() == STRING_TYPE)
		return (this->ToRCString() >= rcdt);
	BHERROR("Bad cast inside RCNum");
	return false;
}

RCNum& RCNum::operator-=(const RCNum& rcn)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!null);
	if(rcn.IsNull() || rcn.IsNull())
		return *this;
	if(IsReal() || rcn.IsReal()) {
		if(IsReal() && rcn.IsReal())
			*(double*)&value -= *(double*)&rcn.value;
		else {
			if(IsReal())
				*this -= rcn.ToReal();
			else
				*this -= rcn.ToDecimal();
		}
	} else {
		if(m_scale < rcn.m_scale) {
			value = ((_int64)(value * PowOfTen(rcn.m_scale - m_scale)) - rcn.value);
			m_scale = rcn.m_scale;
		} else {
			value -= (_int64)(rcn.value * PowOfTen(m_scale - rcn.m_scale));
		}
	}
	return *this;
}

RCNum& RCNum::operator+=(const RCNum& rcn)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!null);
	if(rcn.IsNull() || rcn.IsNull())
		return *this;
	if(IsReal() || rcn.IsReal()) {
		if(IsReal() && rcn.IsReal())
			*(double*)&value -= *(double*)&rcn.value;
		else {
			if(IsReal())
				*this += rcn.ToReal();
			else
				*this += rcn.ToDecimal();
		}
	} else {
		if(m_scale < rcn.m_scale) {
			value = ((_int64)(value * PowOfTen(rcn.m_scale - m_scale)) + rcn.value);
			m_scale = rcn.m_scale;
		} else {
			value += (_int64)(rcn.value * PowOfTen(m_scale - rcn.m_scale));
		}
	}
	return *this;
}

RCNum& RCNum::operator*=(const RCNum& rcn)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!null);
	if(rcn.IsNull() || rcn.IsNull())
		return *this;
	if(IsReal() || rcn.IsReal()) {
		if(IsReal() && rcn.IsReal())
			*(double*)&value -= *(double*)&rcn.value;
		else {
			if(IsReal())
				*this /= rcn.ToReal();
			else
				*this /= rcn.ToDecimal();
		}
	} else {
		value *= rcn.value;
		m_scale += rcn.m_scale;
	}
	return *this;
}

#if ! (defined(__WIN__) || defined(__sun__))
void fcvt( char* buf_, double val_, int digits_, int* dec_, int* sign_ )
{
	static int const fmtlen = 10;
	char format[fmtlen + 1];
	::snprintf( format, fmtlen + 1, "%%.%df", digits_ );
	::snprintf( buf_, digits_, format, val_ );
	int len( ::std::strlen( buf_ ) );
	(*sign_) = ( buf_[0] == '-' ) ? 1 : 0;
	(*sign_) && ::memmove( buf_, buf_ + 1, len );
	char* pbuf( buf_ );
	::strsep( &pbuf, ".," );
	if ( pbuf ) {
		(*dec_) = pbuf - buf_ - 1;
		::memmove( pbuf - 1, pbuf, strlen( pbuf ) + 1 );
	}
	return;
}
#endif

RCNum& RCNum::operator/=(const RCNum& rcn)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!null);
	if(rcn.IsNull() || rcn.IsNull())
		return *this;
	if(IsReal() || rcn.IsReal()) {
		if(IsReal() && rcn.IsReal())
			*(double*)&value -= *(double*)&rcn.value;
		else {
			if(IsReal())
				*this /= rcn.ToReal();
			else
				*this /= rcn.ToDecimal();
		}
	} else {
		double tmv = ((double)(value / rcn.value) / PowOfTen(m_scale - rcn.m_scale));
		int decimal;
		int sign;
#if (defined(__WIN__) || defined(__sun__))
		char* buf(fcvt(tmv, 18, &decimal, &sign));
#else
		static int const MAX_NUM_DIGITS = 21 + 1;
		char buf[MAX_NUM_DIGITS - 1];
		fcvt(buf, tmv, 18, &decimal, &sign);
#endif
		buf[18] = 0;
		ptrdiff_t lz = strlen(buf) - 1;
		while(lz >= 0 && buf[lz--] == '0');
		buf[lz+2] = 0;
		value = strtoll(buf, NULL, 10) * (sign == 1 ? -1 : 1);
		m_scale = (short)((lz+2) - decimal);
	}
	return *this;
}

RCNum RCNum::operator-(const RCNum& rcn) const
{
	RCNum res(*this);
	return res -= rcn;
}

RCNum RCNum::operator+(const RCNum& rcn) const
{
	RCNum res(*this);
	return res += rcn;
}

RCNum RCNum::operator*(const RCNum& rcn) const
{
	RCNum res(*this);
	return res *= rcn;
}

RCNum RCNum::operator/(const RCNum& rcn) const
{
	RCNum res(*this);
	return res /= rcn;
}

uint RCNum::GetHashCode() const
{
	return uint(GetIntPart() * 1040021);
}

template<typename T>
int signum(T val)
{
	return val > 0 ? 1 : ( val < 0 ? -1 : 0 );
}

int RCNum::compare(const RCNum& rcn) const
{
	if(IsNull() || rcn.IsNull())
		return false;
	if(IsReal() || rcn.IsReal()) {
		if(IsReal() && rcn.IsReal())
			return (*(double*)&value > *(double*)&rcn.value ? 1 : (*(double*)&value == *(double*)&rcn.value ? 0 : -1));
		else {
			if(IsReal())
				return (*this > rcn.ToReal() ? 1 : (*this == rcn.ToReal() ? 0 : -1));
			else
				return (this->ToReal() > rcn ? 1 : (this->ToReal() == rcn ? 0 : -1));
		}
	} else {
		if(m_scale != rcn.m_scale) {
			if(value < 0 && rcn.value >= 0)
				return -1;
			if(value >= 0 && rcn.value < 0)
				return 1;
			if(m_scale < rcn.m_scale) {
				_int64 power_of_ten = (_int64)PowOfTen(rcn.m_scale - m_scale);
				_int64 tmpv = (_int64)(rcn.value / power_of_ten);
				if(value > tmpv)
					return 1;
				if(value < tmpv || rcn.value % power_of_ten > 0)
					return -1;
				if(rcn.value % power_of_ten < 0)
					return 1;					
				return 0;
			} else {
				_int64 power_of_ten = (_int64)PowOfTen(m_scale - rcn.m_scale);
				_int64 tmpv = (_int64)(value / power_of_ten);
				if(tmpv < rcn.value)
					return -1;				
				if(tmpv > rcn.value || value % power_of_ten > 0)
					return 1;				
				if(value % power_of_ten < 0)
					return -1;
				return 0;
			}
		} else
			return (value > rcn.value ? 1 : (value == rcn.value ? 0 : -1));
	}
}

int RCNum::compare(const RCDateTime& rcdt) const
{
	_int64 tmp;
	rcdt.ToInt64(tmp);
	return int(GetIntPart() - tmp);
}

_int64 RCNum::GetIntPart() const
{
	return dbl ? (_int64)GetIntPartAsDouble() :  value / (_int64)Uint64PowOfTen(m_scale);
}

double RCNum::GetIntPartAsDouble() const
{
	if(dbl) {
		double integer;
		modf(*(double*)&value, &integer);
		return integer;
	} else {
		return (double)(value / (_int64)Uint64PowOfTen(m_scale));
	}
}

double RCNum::GetFractPart() const
{
	if(dbl) {
		double fract, integer;
		fract = modf(*(double*)&value, &integer);
		return fract;
	} else {
		double tmpv = ((double)value / Uint64PowOfTen(m_scale));
		double fract, integer;
		fract = modf(tmpv, &integer);
		return fract;
	}
}

short RCNum::GetDecStrLen() const
{
	if(IsNull())
		return 0;
	if(dbl)
		return 18;
	short res = m_scale;
	_int64 tmpi = value / (_int64)PowOfTen(m_scale);
	while(tmpi != 0) {
		tmpi /= 10;
		res++;
	}
	return res;
}

short RCNum::GetDecIntLen() const
{
	if(IsNull())
		return 0;
	short res = 0;
	_int64 tmpi = 0;
	if(dbl)
		tmpi = GetIntPart();
	else {
		tmpi = value / (_int64)PowOfTen(m_scale);
	}
	while(tmpi != 0) {
		tmpi /= 10;
		res++;
	}
	return res;
}

short RCNum::GetDecFractLen() const
{
	if(IsNull())
		return 0;
	return m_scale;
}

void RCNum::Negate()
{
	if(IsNull())
		return;
	if(dbl)
		*(double*)&value = *(double*)&value * -1;
	else
		value *= -1;
}

size_t RCNum::GetStorageByteSize() const
{
	return sizeof(null) + (null ? 0 :  sizeof(attrt) + sizeof(m_scale) + sizeof(dbl) + sizeof(dot) + sizeof(value));
}

void RCNum::ToByteStream(char*& buf) const
{
	store_bytes(buf, null);
	if(!null) {
		store_bytes(buf, attrt);
		store_bytes(buf, m_scale);
		store_bytes(buf, dbl);
		store_bytes(buf, dot);
		store_bytes(buf, value);
	}
}

void RCNum::AssignFromByteStream(char*& buf)
{
	unstore_bytes(null, buf);
	if(!null) {
		unstore_bytes(attrt, buf);
		unstore_bytes(m_scale, buf);
		unstore_bytes(dbl, buf);
		unstore_bytes(dot, buf);
		unstore_bytes(value, buf);
	}
}

