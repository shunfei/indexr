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

#include <cstdio>
#include "DataConverter.h"
#include "common/bhassert.h"

using namespace boost;

RCBString& DC_TxtOracle::Convert(const RCDateTime& dt, RCBString& rcs)
{
	if(dt.IsNull())
	{
		rcs = ZERO_LENGTH_STRING;
	}
	rcs = RCBString(0, 30, true);
	char* buf = rcs.val;
	if(dt.IsNegative())
		*buf++ = '-';
	switch(dt.Type())
	{
		case RC_YEAR :
		{
			sprintf(buf, "%04d", abs((int)dt.Year()));
			break;
		}
		case RC_DATE :
		{
			sprintf(buf, "%02d/%02d/%04d", abs((int)dt.Month()),  abs((int)dt.Day()), abs((int)dt.Year()));
			break;
		}
		case RC_TIME :
		{
			sprintf(buf, "%02d:%02d:%02d", (int)dt.Hour(), (int)dt.Minute(), (int)dt.Second());
			break;
		}
		case RC_DATETIME  :
		case RC_TIMESTAMP :
		{
			sprintf(buf, "%02d/%02d/%04d %02d:%02d:%02d",abs((int)dt.Month()), abs((int)dt.Day()), abs((int)dt.Year()), (int)dt.Hour(), (int)dt.Minute(), (int)dt.Second());
			break;
		}
		default :
			BHERROR("not implemented");
	}

	rcs.len = (uint)strlen(rcs.val);
	return rcs;
}
