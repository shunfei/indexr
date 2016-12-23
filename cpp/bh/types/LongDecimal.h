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

#ifndef _LONGDECIMAL_H_
#define _LONGDECIMAL_H_

#include "common/CommonDefinitions.h"
#include "system/RCException.h"
#include "core/ColumnType.h"

// This class stores a decimal number as an integer with a given scale.
// Conversions between scales may be performed on initialization/getting value (results are rounded),
// arithmetical operations may change scale of a LongDecimal.
class LongDecimal
{
public:
	LongDecimal();
	LongDecimal(const LongDecimal& sec);
	LongDecimal(_int64 _v, int _scale);
	LongDecimal(_int64 _v, const ColumnType &ct);

	void Init(_int64 _v, int _scale);				// a common part of constructors

	LongDecimal &operator+=(const LongDecimal &sec);
	LongDecimal &operator-=(const LongDecimal &sec);
	LongDecimal &operator*=(const LongDecimal &sec);
	LongDecimal &operator/=(const LongDecimal &sec);
	LongDecimal &operator%=(const LongDecimal &sec);
	void Negate()											{ sign = -sign; }
	_int64 Floor()											{ return sign == 1 ? val_int : (val_frac == 0 ? -val_int : -(val_int + 1)); }
	
	_int64 GetDecimal(int output_scale);					// performs scale conversion (if needed), rounded
	double GetDouble() const;

	static void OutOfRange()
	{	
		throw RCException(
			"Numeric value in an expression is larger than DECIMAL(18) and cannot be handled by Infobright."
			);
	}

private:
	_int64 val_int;
	_int64 val_frac;
	int sign;
};



#endif
