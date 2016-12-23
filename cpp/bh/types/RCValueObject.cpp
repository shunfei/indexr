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
#include "core/ValueOrNull.h"

using namespace std;
RCValueObject::RCValueObject()
{
}

RCValueObject::RCValueObject(const RCValueObject& rcvo)
{
	if(rcvo.value.get())
		construct(*rcvo.value);
}

RCValueObject::RCValueObject(const RCDataType& rcdt)
{
	construct(rcdt);
}

RCValueObject::~RCValueObject()
{
}

//std::auto_ptr<ValueOrNull> RCValueObject::ToValueOrNullAPtr() const
//{
//	return new ValueOrNull(this->ToValueOrNullAPtr());
//}

//std::auto_ptr<ValueOrNull> ValueBasic<typename T>::ToValueOrNullAPtr() const
//{
//	return new ValueOrNull((T&)*this);
//}

RCValueObject& RCValueObject::operator=(const RCValueObject& rcvo)
{
	if(rcvo.value.get())
		construct(*rcvo.value);
	else
		value.reset();
	return *this;
}

//RCDataType& RCValueObject::operator=(const RCDataType& rcdt)
//{
//	construct(rcdt);
//	return *this;
//}

inline void RCValueObject::construct(const RCDataType& rcdt)
{
	value = rcdt.Clone();
}

bool RCValueObject::compare(const RCValueObject& rcvo1, const RCValueObject& rcvo2, Operator op, char like_esc)
{
	if (rcvo1.IsNull() || rcvo2.IsNull())
		return false;
	else
		return RCDataType::compare(*rcvo1.value, *rcvo2.value, op, like_esc);
}

bool RCValueObject::compare(const RCValueObject& rcvo, Operator op, char like_esc) const
{
	return compare(*this, rcvo, op, like_esc);
}

bool RCValueObject::operator==(const RCValueObject& rcvo) const
{
	if(IsNull() || rcvo.IsNull())
		return false;
	return *value == *rcvo.value;
}

bool RCValueObject::operator<(const RCValueObject& rcvo) const
{
	if(IsNull() || rcvo.IsNull())
		return false;
	return *value < *rcvo.value;
}

bool RCValueObject::operator>(const RCValueObject& rcvo) const
{
	if(IsNull() || rcvo.IsNull())
		return false;
	return *value > *rcvo.value;
}

bool RCValueObject::operator>=(const RCValueObject& rcvo) const
{
	if(IsNull() || rcvo.IsNull())
		return false;
	return *value >= *rcvo.value;
}

bool RCValueObject::operator<=(const RCValueObject& rcvo) const
{
	if(IsNull() || rcvo.IsNull())
		return false;
	return *value <= *rcvo.value;
}

bool RCValueObject::operator!=(const RCValueObject& rcvo) const
{
	if(IsNull() || rcvo.IsNull())
		return false;
	return *value != *rcvo.value;
}

bool RCValueObject::operator==(const RCDataType& rcn) const
{
	if(IsNull() || rcn.IsNull())
		return false;
	return *value == rcn;
}

bool RCValueObject::operator<(const RCDataType& rcn) const
{
	if(IsNull() || rcn.IsNull())
		return false;
	return *value < rcn;
}

bool RCValueObject::operator>(const RCDataType& rcn) const
{
	if(IsNull() || rcn.IsNull())
		return false;
	return *value > rcn;
}

bool RCValueObject::operator>=(const RCDataType& rcn) const
{
	if(IsNull() || rcn.IsNull())
		return false;
	return *value >= rcn;
}

bool RCValueObject::operator<=(const RCDataType& rcdt) const
{
	if(IsNull() || rcdt.IsNull())
		return false;
	return *value <= rcdt;
}

bool RCValueObject::operator!=(const RCDataType& rcn) const
{
	if(IsNull() || rcn.IsNull())
		return false;
	return *value != rcn;
}

bool RCValueObject::IsNull() const
{
	return value.get() ? value->IsNull() : true;
}

RCDataType&	RCValueObject::operator*() const
{
	return value.get() ? *value.get() : RCNum::NullValue();
}

RCValueObject::operator RCNum&() const
{
	if(IsNull())
		return RCNum::NullValue();
	if(GetValueType() == NUMERIC_TYPE || GetValueType() == DATE_TIME_TYPE)
		return static_cast<RCNum&>(*value);
	BHERROR("Bad cast in RCValueObject::RCNum&()");
	return static_cast<RCNum&>(*value);
}

RCValueObject::operator RCDateTime&() const
{
	if(IsNull())
		return RCDateTime::NullValue();
	if(GetValueType() == DATE_TIME_TYPE)
		return static_cast<RCDateTime&>(*value);
	BHERROR("Bad cast in RCValueObject::RCDateTime&()");
	return static_cast<RCDateTime&>(*value);
}

RCBString RCValueObject::ToRCString() const
{
	if(IsNull())
		return RCBString();
	return value->ToRCString();
}

uint RCValueObject::GetHashCode() const
{
	if(IsNull())
		return 0;
	return value->GetHashCode();
}

void RCValueObject::MakePersistent()
{
	if(GetValueType() == STRING_TYPE)
		static_cast<RCBString*>(Get())->MakePersistent() ;
}

