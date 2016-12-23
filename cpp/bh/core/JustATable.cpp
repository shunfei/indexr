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

#include "JustATable.h"
#include "edition/local.h"
#include "TempTable.h"
#include "Filter.h"
#include "CQTerm.h"
#include "common/bhassert.h"

using namespace std;

bool IsTimeStampZero(MYSQL_TIME& t);

ValueOrNull JustATable::GetComplexValue(const _int64 obj, const int attr)
{
#ifndef PURE_LIBRARY
	if(obj == NULL_VALUE_64 || IsNull(obj, attr))
		return ValueOrNull();

	ColumnType ct = GetColumnType(attr);
	if(ct.GetTypeName() == RC_TIMESTAMP) {
		// needs to convert UTC/GMT time stored on server to time zone of client
		RCBString s;
		GetTable_S(s, obj, attr);
		MYSQL_TIME myt;
		int not_used;
		// convert UTC timestamp given in string into TIME structure
		str_to_datetime(s.Value(), s.len, &myt, TIME_DATETIME_ONLY, &not_used);
		return ValueOrNull(RCDateTime(myt.year, myt.month, myt.day, myt.hour, myt.minute, myt.second, RC_TIMESTAMP).GetInt64());
	}
	if(ct.IsFixed() || ct.IsFloat() || ct.IsDateTime())
		return ValueOrNull(GetTable64(obj, attr));
	if(ct.IsString()) {
		ValueOrNull val;
		RCBString s;
		GetTable_S(s, obj, attr);
		val.SetString(s);
		return val;
	}
	throw RCException("Unrecognized data type in JustATable::GetComplexValue");
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	ValueOrNull val;
	return val;
#endif
}
