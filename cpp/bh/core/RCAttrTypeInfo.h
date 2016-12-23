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

#ifndef RCATTRTYPEINFO_H_INCLUDED
#define RCATTRTYPEINFO_H_INCLUDED

#include <vector>
#include <string>

#include "common/CommonDefinitions.h"

class RCAttr;
class RCDataType;

class AttributeTypeInfo
{
public:
	AttributeTypeInfo(AttributeType attrt, bool notnull, ushort precision = 0, ushort scale = 0, bool lookup = false, 
		bool for_insert_ = false, DTCollation collation = DTCollation())
		: attrt(attrt), precision(precision), scale(scale), notnull(notnull), lookup(lookup), for_insert(for_insert_), 
		parameter(0), collation(collation), decomposition()
	{
	}

	uint	ExtrnalSize(EDF edf) const;
	int		TextSize();

	static bool IsFixedSize(AttributeType attr_type, EDF edf);
	static uint ExternalSize(RCAttr* attr, EDF edf);
	static uint ExternalSize(AttributeType attrt, int precision, int scale, EDF edf);
	static ushort ObjPrefixSizeByteSize(AttributeType attr_type, EDF edf);
	static int	TextSize(AttributeType attrt, ushort precision, ushort scale, DTCollation col = DTCollation());

	static bool IsIntervalType(AttributeType attr_type);

	inline static bool IsInteger32Type(AttributeType attr_type)
	{
		return attr_type == RC_INT 	|| attr_type == RC_BYTEINT || attr_type == RC_SMALLINT || attr_type == RC_MEDIUMINT;
	}

	inline static bool IsIntegerType(AttributeType attr_type)
	{
		return IsInteger32Type(attr_type) || attr_type == RC_BIGINT;
	}

	inline static bool IsFixedNumericType(AttributeType attr_type)
	{
		return IsInteger32Type(attr_type) || attr_type == RC_BIGINT || attr_type == RC_NUM;
	}

	inline static bool IsRealType(AttributeType attr_type)
	{
		return attr_type == RC_FLOAT || attr_type == RC_REAL;
	}

	inline static bool IsNumericType(AttributeType attr_type)
	{
		return IsInteger32Type(attr_type) || attr_type == RC_BIGINT || attr_type == RC_NUM || attr_type == RC_FLOAT || attr_type == RC_REAL;
	}

	inline static bool IsBinType(AttributeType attr_type)
	{
		return attr_type == RC_BYTE || attr_type == RC_VARBYTE || attr_type == RC_BIN;
	}

	inline static bool IsTxtType(AttributeType attr_type)
	{
		return attr_type == RC_STRING || attr_type == RC_VARCHAR;
	}

	inline static bool IsCharType(AttributeType attr_type)
	{
		return attr_type == RC_STRING;
	}

	inline static bool IsStringType(AttributeType attr_type)
	{
		return attr_type == RC_STRING || attr_type == RC_VARCHAR || IsBinType(attr_type);
	}

	inline static bool IsDateTimeType(AttributeType attr_type)
	{
		return attr_type == RC_DATE || attr_type == RC_TIME || attr_type == RC_YEAR || attr_type == RC_DATETIME
				|| attr_type == RC_TIMESTAMP;
	}

	inline static bool IsDateTimeNType(AttributeType attr_type)
	{
		return attr_type == RC_TIME_N || attr_type == RC_DATETIME_N || attr_type == RC_TIMESTAMP_N;
	}

	/*static _int64 MaxValue(AttributeType attrt);
	static _int64 MinValue(AttributeType attrt);
	static _int64 ErrorValue(AttributeType attrt);*/

	AttributeType	Type() const	{ return attrt; }
	AttrPackType	PackType() const	{ return IsDateTimeType(attrt) || IsNumericType(attrt) || Lookup() ? PackN : PackS; }
	ushort Precision() const 	{ return precision; }
	ushort Scale() const		{ return scale; }
	ushort CharLen() const		{ return precision / collation.collation->mbmaxlen; }
	bool NotNull() const		{ return notnull; }
	bool Lookup() const			{ return lookup; }
	uint Params() const
	{
		uint parameter = 0;
		if(lookup)
			parameter |= 0x00000002;
		if(notnull)
			parameter |= 0x00000001;
		if(for_insert)
			parameter |= 0x00000004;
		return parameter;
	}

	operator AttributeType() const	{ return Type(); }


	void			SetCollation(const DTCollation& collation) 		{ this->collation = collation; }
	void			SetCollation(CHARSET_INFO* charset_info)		{ this->collation.set(charset_info); }

	DTCollation 	GetCollation() const 							{ return collation; }
	CHARSET_INFO*	CharsetInfo() const 							{ return this->collation.collation; }

	void			SetDecomposition(const std::string& decomposition) { this->decomposition = decomposition; }
	std::string		GetDecomposition() const { return decomposition; }

	const RCDataType& ValuePrototype() const;

private:
	AttributeType attrt;
	int precision;
	int scale;
	bool notnull;
	bool lookup;
	bool for_insert;
	uint parameter;
	DTCollation collation;
	std::string decomposition;
};
typedef AttributeTypeInfo ATI;

#endif /* not RCATTRTYPEINFO_H_INCLUDED */

