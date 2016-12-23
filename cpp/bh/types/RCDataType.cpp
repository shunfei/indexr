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

#include "core/RCAttr.h"
#include "core/RCAttrTypeInfo.h"
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace boost;

RCDataType::~RCDataType()
{
}

bool RCDataType::AreComperable(const RCDataType& rcdt) const
{
	return RCDataType::AreComperable(*this, rcdt);
}

bool RCDataType::AreComperable(const RCDataType& rcdt1, const RCDataType& rcdt2)
{
	AttributeType att1 = rcdt1.Type();
	AttributeType att2 = rcdt2.Type();
	return AreComparable(att1, att2);
}


bool RCDataType::compare(const RCDataType& rcdt1, const RCDataType& rcdt2, Operator op, char like_esc)
{
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(RCDataType::AreComperable(rcdt1, rcdt2));
	if(op == O_LIKE || op == O_NOT_LIKE) {
		if(rcdt1.IsNull() || rcdt2.IsNull())
			return false; 
		RCBString x, y;
		RCBString* rcbs1 = dynamic_cast<RCBString*>(const_cast<RCDataType*>(&rcdt1));
		if(!rcbs1) {
			x = rcdt1.ToRCString();
			rcbs1 = &x;
		}
		RCBString* rcbs2 = dynamic_cast<RCBString*>(const_cast<RCDataType*>(&rcdt2));
		if(!rcbs2) {
			y = rcdt2.ToRCString();
			rcbs2 = &y;
		}
		bool res = rcbs1->Like(*rcbs2, like_esc);
		if(op == O_LIKE)	
			return res;
		else
			return !res;
	} else if(!rcdt1.IsNull() && !rcdt2.IsNull() && (
		((op == O_EQ) && rcdt1 == rcdt2) ||
		(op == O_NOT_EQ && rcdt1 != rcdt2) ||
		(op == O_LESS && rcdt1 < rcdt2) ||
		(op == O_LESS_EQ && (rcdt1 < rcdt2 || rcdt1 == rcdt2)) ||
		(op == O_MORE && (!(rcdt1 < rcdt2) && rcdt1 != rcdt2)) ||
		(op == O_MORE_EQ && (!(rcdt1 < rcdt2) || rcdt1 == rcdt2)))
	)
		return true;
	return false;
}

bool RCDataType::compare(const RCDataType& rcdt, Operator op, char like_esc) const
{
	return RCDataType::compare(*this, rcdt, op, like_esc);
}

bool AreComparable(AttributeType attr1_t, AttributeType attr2_t)
{
	if(attr1_t == attr2_t) return true;
	if((ATI::IsDateTimeType(attr1_t)) && (ATI::IsDateTimeType(attr2_t)))
		return true;
	if ((ATI::IsTxtType(attr2_t) && attr1_t == RC_VARBYTE) || (ATI::IsTxtType(attr1_t) && attr2_t == RC_VARBYTE))
		return true;
	if(
		( ((attr1_t == RC_TIME) || (attr1_t == RC_DATE)) && attr2_t != RC_DATETIME ) ||
		( ((attr2_t == RC_TIME) || (attr2_t == RC_DATE)) && attr1_t != RC_DATETIME ) ||
		(ATI::IsBinType(attr1_t) && !ATI::IsBinType(attr2_t)) ||
		(ATI::IsBinType(attr2_t) && !ATI::IsBinType(attr1_t)) ||
		(ATI::IsTxtType(attr1_t) && !ATI::IsTxtType(attr2_t)) ||
		(ATI::IsTxtType(attr2_t) && !ATI::IsTxtType(attr1_t))
	)
		return false;
	return true;
}

bool RCDataType::ToDecimal(const RCDataType& in, int scale, RCNum& out)
{
	if(RCNum* rcn = dynamic_cast<RCNum*>(const_cast<RCDataType*>(&in))) {
		if(rcn->IsDecimal(scale)) {
			out = rcn->ToDecimal(scale);
			return true;
		}
	} else if(RCBString* rcs = dynamic_cast<RCBString*>(const_cast<RCDataType*>(&in))) {
		if(RCNum::Parse(*rcs, out))
			return true;
	}
	return false;
}

bool RCDataType::ToInt(const RCDataType& in, RCNum& out)
{
	if(RCNum* rcn = dynamic_cast<RCNum*>(const_cast<RCDataType*>(&in))) {
		if(rcn->IsInt()) {
			out = rcn->ToInt();
			return true;
		}
	} else if(RCBString* rcs = dynamic_cast<RCBString*>(const_cast<RCDataType*>(&in))) {
		if(RCNum::Parse(*rcs, out))
			return true;
	}
	return false;
}

bool RCDataType::ToReal(const RCDataType& in, RCNum& out)
{
	if(RCNum* rcn = dynamic_cast<RCNum*>(const_cast<RCDataType*>(&in))) {
		if(rcn->IsReal()) {
			out = rcn->ToReal();
			return true;
		}
	} else if(RCBString* rcs = dynamic_cast<RCBString*>(const_cast<RCDataType*>(&in))) {
		if(RCNum::ParseReal(*rcs, out, RC_UNKNOWN))
			return true;
	}
	return false;
}


ValueTypeEnum RCDataType::GetValueType(AttributeType attr_type)
{
	if(ATI::IsNumericType(attr_type))
		return NUMERIC_TYPE;
	else if(ATI::IsDateTimeType(attr_type))
		return DATE_TIME_TYPE;
	else if(ATI::IsStringType(attr_type))
		return STRING_TYPE;
	return NULL_TYPE;
}

char* Text(_int64 value, char buf[], int scale)
{
	bool sign = true;
	if(value < 0) {
		sign = false;
		value *= -1;
	}
#ifndef PURE_LIBRARY
	longlong2str(value, buf, 10);
#else
	strcpy(buf, boost::lexical_cast<string>(value).c_str());
#endif
	int l = (int)strlen(buf);
	memset(buf + l + 1, ' ', 21 - l);
	int pos = 21;
	int i = 0;
	for(i = l; i >= 0; i--) {
		if(scale != 0 && pos + scale == 20) {
			buf[pos--] = '.';
			i++;
		} else {
			buf[pos--] = buf[i];
			buf[i] = ' ';
		}
	}

	if(scale >= l) {
		buf[20-scale] = '.';
		buf[20-scale-1] = '0';
		i = 20-scale+1;
		while(buf[i] == ' ')
			buf[i++] = '0';
	}
	pos = 0;
	while(buf[pos] == ' ')
		pos++;
	if(!sign)
		buf[--pos] = '-';
	return buf + pos;
}


