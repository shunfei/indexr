/* Copyright (C)  2005-2011 Infobright Inc.

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

#include "LongDecimal.h"
#include "types/RCDataTypes.h"

#define E18  1000000000000000000ull
#define E6   1000000ull
#define E9   1000000000ull
#define E12  1000000000000ull
#define E5_17 500000000000000000ull

LongDecimal::LongDecimal()
{
	val_int = 0;
	val_frac = 0;
	sign = 1;
}

LongDecimal::LongDecimal(const LongDecimal& sec)
{
	val_int = sec.val_int;
	val_frac = sec.val_frac;
	sign = sec.sign;
}

LongDecimal::LongDecimal(_int64 _v, int _scale)
{
	Init(_v, _scale);
}

LongDecimal::LongDecimal(_int64 _v, const ColumnType &ct)
{
	if(ct.IsUnsigned() && _v < 0)
		OutOfRange();					// cannot support 20-digit unsigned
	Init(_v, ct.GetScale());
}

void LongDecimal::Init(_int64 _v, int _scale)
{
	assert(_scale >= 0 && _scale <= 18);
	sign = 1;
	if(_v < 0) {
		_v = -_v;
		sign = -1;
	}
	if(_scale == 0) {
		val_int = _v;
		val_frac = 0;
		if(val_int >= E18) 
			OutOfRange();
	} else {
		_uint64 s1 = Int64PowOfTen(_scale);
		_uint64 s2 = Int64PowOfTen(18 - _scale);
		val_int = _v / s1;
		val_frac = (_v % s1) * s2;
	}
}

LongDecimal& LongDecimal::operator+=(const LongDecimal &sec)
{
	if(sign == sec.sign) {
		val_frac += sec.val_frac;
		if(val_frac >= E18) {
			val_int += 1;
			val_frac -= E18;
		}
		val_int += sec.val_int;
		if(val_int >= E18) 
			OutOfRange();
	} else if(sign == 1 && sec.sign == -1) {
		LongDecimal v2(sec);
		v2.Negate();
		*this -= v2;
	} else {
		LongDecimal v2(sec);
		Negate();
		v2 -= *this;
		*this = v2;
	}
	return *this;
}

LongDecimal& LongDecimal::operator-=(const LongDecimal &sec)
{
	if(sign == sec.sign) {
		_uint64 bigger_int = val_int;
		_uint64 bigger_frac = val_frac;
		_uint64 smaller_int = sec.val_int;
		_uint64 smaller_frac = sec.val_frac;
		bool negative = false;
		if(val_int < sec.val_int || (val_int == sec.val_int && val_frac < sec.val_frac)) {
			bigger_int = sec.val_int;
			bigger_frac = sec.val_frac;
			smaller_int = val_int;
			smaller_frac = val_frac;
			negative = true;
		}
		if(bigger_frac < smaller_frac) {
			bigger_frac += E18;
			bigger_int--;
		}
		val_frac = bigger_frac - smaller_frac;
		val_int = bigger_int - smaller_int;
		if(negative)
			Negate();
	} else if(sign == 1 && sec.sign == -1) {
		LongDecimal v2(sec);
		v2.Negate();
		*this += v2;
	} else {
		Negate();
		*this += sec;
		Negate();
	}
	return *this;
}


void Mult(_uint64& v1, _uint64& v2, _uint64& res1, _uint64& res2)	
// multiplication of two decimals(18), result is a concatenation of res1 and res2
{
	// v1 * v2 = [res1][res2]
	res1 = 0;
	res2 =		(v1 % E9) * (v2 % E9);
	_uint64 p =	(v1 / E9) * (v2 % E9);
	res2 +=		(p % E9) * E9;				// no overflow: max. is 2e18
	if(res2 > E18) {
		res1 += res2 / E18;
		res2 = res2 % E18;
	}
	res1 +=		(p / E9);
	p =			(v1 % E9) * (v2 / E9);
	res2 +=		(p % E9) * E9;				// no overflow: max. is 2e18
	if(res2 > E18) {
		res1 += res2 / E18;
		res2 = res2 % E18;
	}
	res1 +=		(p / E9);
	res1 +=		(v1 / E9) * (v2 / E9);
}

LongDecimal& LongDecimal::operator*=(const LongDecimal &sec)
{
	// a.b * c.d = [r][ac + r].[ad + bc + r][bd],   r - residua from other operations
	// all variables are storing dec(18) numbers, so there is a safe margin for overflow (1.8e19 for _uint64)
	_uint64 a = val_int;
	_uint64 b = val_frac;
	_uint64 c = sec.val_int;
	_uint64 d = sec.val_frac;
	_uint64 res_int = 0, res_frac = 0;
	_uint64 r1, r2;
	Mult(b, d, r1, r2);
	res_frac = r1;
	if(r2 > E5_17)
		res_frac++;
	Mult(a, d, r1, r2);
	res_int = r1;
	res_frac += r2;				// no overflow: max. is 2e18
	res_int += res_frac / E18;
	res_frac = res_frac % E18;
	Mult(b, c, r1, r2);
	res_frac += r2;				// no overflow: max. is 2e18
	res_int += res_frac / E18;
	res_frac = res_frac % E18;
	res_int += r1;
	Mult(a, c, r1, r2);
	res_int += r2;				// no overflow: max. is 4e18
	if(res_int >= E18 || r1 > 0)
		OutOfRange();
	val_int = res_int;
	val_frac = res_frac;
	sign *= sec.sign;
	return *this;
}

LongDecimal& LongDecimal::operator/=(const LongDecimal &sec)
{
	if(sec.val_int == 0 && sec.val_frac == 0)
		OutOfRange();
	if(sec.val_int == 0) {		// less than 1
		// APPROXIMATE IMPLEMENTATION
		double d = (double(E18) / double(sec.val_frac));
		if(d < E6)
			*this *= LongDecimal(_int64(d * E12), 12);
		else if(d < E9)
			*this *= LongDecimal(_int64(d * E9), 9);
		else if(d < E12)
			*this *= LongDecimal(_int64(d * E6), 6);
		else
			*this *= LongDecimal(_int64(d), 0);
		if(sec.sign == -1)
			Negate();
	} else {
		double d = sec.GetDouble();
		if(d < 0)
			d = -d;
		LongDecimal res(0, 0);
		double d_frac, d_int;
		d_frac = modf((val_int / E9) / d, &d_int);
		res += LongDecimal(_int64(d_int) * E9, 0);
		res += LongDecimal(_int64(d_frac * E18), 9);
		d_frac = modf((val_int % E9) / d, &d_int);
		res += LongDecimal(_int64(d_int), 0);
		res += LongDecimal(_int64(d_frac * E18), 18);
		d_int = val_frac / d;
		res += LongDecimal(_int64(d_int), 18);
		int res_sign = sign * sec.sign;
		*this = res;
		sign = res_sign;
	}
	return *this;
}

LongDecimal& LongDecimal::operator%=(const LongDecimal &sec)
{
	// mod(x, y) = x - floor(x/y) * y
	bool neg = (sign == -1);
	sign = 1;
	LongDecimal a(*this);
	a /= sec;					// out of range for 0
	if(a.sign == -1)
		a.Negate();
	LongDecimal b(a.Floor(), 0);
	b *= sec;
	if(b.sign == -1)
		b.Negate();
	*this -= b;
	if(neg)
		sign = -1;
	return *this;
}

_int64 LongDecimal::GetDecimal(int output_scale)
{
	assert(output_scale >= 0 && output_scale <= 18);
	_int64 res;
	if(output_scale < 18) {
		res = val_int;
		_int64 s1 = Int64PowOfTen(output_scale);
		_int64 s2 = Int64PowOfTen(18 - output_scale - 1);
		_int64 res_frac = (val_frac / s2 + 5) / 10;
		if(res >= PLUS_INF_64 / s1)
			OutOfRange();
		res = res * s1 + res_frac;
	} else {
		if(val_frac == 0) {
			res = val_int;
			_int64 s1 = Int64PowOfTen(output_scale);
			if(res >= PLUS_INF_64 / s1)
				OutOfRange();
			res = res * s1;
		} else {
			if(val_int > 0)
				OutOfRange();
			res = val_frac;
		}
	}
	if(sign == -1)
		res = -res;
	return res;
}

double LongDecimal::GetDouble() const
{
	double res = double(val_int) + double(val_frac) / E18;
	if(sign == -1)
		res = -res;
	return res;
}
