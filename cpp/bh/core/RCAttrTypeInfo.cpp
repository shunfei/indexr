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

#include "RCAttrTypeInfo.h"
#include "RCAttr.h"
#include "common/DataFormat.h"
#include "common/TxtDataFormat.h"
#include "types/RCDataTypes.h"
#include "types/RCNum.h"

ushort AttributeTypeInfo::ObjPrefixSizeByteSize(AttributeType attr_type, EDF edf)
{
	return DataFormat::GetDataFormat(edf)->ObjPrefixSizeByteSize(attr_type);
}

int AttributeTypeInfo::TextSize(AttributeType attrt, ushort precision, ushort scale, DTCollation collation)
{
	return TxtDataFormat::StaticExtrnalSize(attrt, precision, scale, &collation);
}

const RCDataType& AttributeTypeInfo::ValuePrototype() const
{
	if(Lookup() || IsNumericType(attrt))
		return RCNum::NullValue();
	if(ATI::IsStringType(attrt))
		return RCBString::NullValue();
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ATI::IsDateTimeType(attrt));
	return RCDateTime::NullValue();
}
