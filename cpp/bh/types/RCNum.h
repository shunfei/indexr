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

#ifndef RCNUM_H_
#define RCNUM_H_

#include "RCDataTypes.h"

class RCBString;
class RCBStringW;

class RCNum : public ValueBasic<RCNum>
{
	friend class ValueParserForText;
	friend class RCEngine;
public:
	RCNum(AttributeType attrt = RC_NUM);
	RCNum(_int64 value, short scale = -1, bool dbl = false, AttributeType attrt = RC_UNKNOWN);
	RCNum(double value);
	RCNum(const RCNum&);
	~RCNum(void);

	RCNum& Assign(_int64 value, short scale = -1, bool dbl = false, AttributeType attrt = RC_UNKNOWN);
	RCNum& Assign(double value);

	static BHReturnCode Parse(const RCBString& rcs, RCNum& rcn, AttributeType at = RC_UNKNOWN);
	static BHReturnCode ParseReal(const RCBString&, RCNum&, AttributeType at);
	static BHReturnCode ParseNum(const RCBString&, RCNum&, short scale = -1);

	RCNum& operator=(const RCNum& rcn);
	RCNum& operator=(const RCDataType& rcdt);

	AttributeType Type() const;

	bool operator==(const RCDataType& rcdt)const;
	bool operator<(const RCDataType& rcdt) const;
	bool operator>(const RCDataType& rcdt) const;
	bool operator>=(const RCDataType& rcdt) const;
	bool operator<=(const RCDataType& rcdt) const;
	bool operator!=(const RCDataType& rcdt) const;

	RCNum& operator-=(const RCNum& rcn);
	RCNum& operator+=(const RCNum& rcn);
	RCNum& operator*=(const RCNum& rcn);
	RCNum& operator/=(const RCNum& rcn);

	RCNum operator-(const RCNum& rcn) const;
	RCNum operator+(const RCNum& rcn) const;
	RCNum operator*(const RCNum& rcn) const;
	RCNum operator/(const RCNum& rcn) const;

	bool IsDecimal(ushort scale) const;
	bool IsReal() const {return dbl;}
	bool IsInt() const;

	RCBString ToRCString() const;
	RCNum ToDecimal(int scale = -1) const;
	RCNum ToReal() const;
	RCNum ToInt() const;
	operator _int64() const;
	operator double() const;
	operator float() const { return (float)(double)*this; }

	short Scale() const {return m_scale;}
	_int64 Value() const {return value;}
	char* GetDataBytesPointer() const {return (char*)&value;}

	_int64 GetIntPart() const;
	_int64 GetValueInt64() const {return value;}
	double GetIntPartAsDouble() const;
	double GetFractPart() const;


	short GetDecStrLen() const;
	short GetDecIntLen() const;
	short GetDecFractLen() const;

	uint GetHashCode() const;
	void Negate();

	virtual size_t GetStorageByteSize() const;
	virtual void ToByteStream(char* &buf) const;
	virtual void AssignFromByteStream(char* &buf);

private:
	int compare(const RCNum& rcn) const;
	int compare(const RCDateTime& rcn) const;

private:
	_int64 value;
	ushort m_scale;		//means 'scale' actually
	bool dbl;
	bool dot;
	AttributeType attrt;


public:
	const static ValueTypeEnum value_type = NUMERIC_TYPE;
};

#endif /*RCNUM_H_*/
