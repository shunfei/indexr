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

#include "ColumnType.h"
#include "ValueOrNull.h"
#include "QuickMath.h"

ColumnType::ColumnType(const DataType& dt)
{
	if(dt.IsFixed()) {
		ColumnType ct(dt.attrtype, AS_MISSED, false, QuickMath::precision10(dt.fixmax), dt.fixscale);
		std::swap(ct, *this);
	} else if(dt.IsFloat()) {
		ColumnType ct(RC_REAL, AS_MISSED, false, QuickMath::precision10(dt.fixmax), -1);
		std::swap(ct, *this);
	} else if(dt.IsString()) {
		ColumnType ct(dt.attrtype, AS_MISSED, false, dt.precision , 0, dt.collation);
		std::swap(ct, *this);
	} else if(dt.IsDateTime()) {
		ColumnType ct(dt.attrtype, AS_MISSED, false, dt.precision , 0, dt.collation);
		std::swap(ct, *this);
	} else 
		BHERROR("invalid conversion DataType -> ColumnType");
}

bool ColumnType::operator==(const ColumnType& ct2) const
{
	if( type == ct2.type && is_lookup == ct2.is_lookup && 
		strcmp(collation.collation->csname, ct2.collation.collation->csname) == 0 &&
		(type != RC_NUM ||
		(type == RC_NUM && precision == ct2.precision && scale == ct2.scale)) )
		return true;
	else
		return false;
}

int ColumnType::InternalSize() 	{
	if(is_lookup)
		return 4;
	else if(ATI::IsStringType(type))
		return precision;
	else if(type == RC_INT || type == RC_MEDIUMINT)
		return 4;
	else if(type == RC_SMALLINT)
		return 2;
	else if(type == RC_BYTEINT || type == RC_YEAR)
		return 1;
	return 8;
}

ColumnType ColumnType::RemovedLookup() const
{
	ColumnType noLookup(*this);
	noLookup.is_lookup = false;
	return noLookup;
}
