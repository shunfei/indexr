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
#ifndef DATACONVERTER_H
#define DATACONVERTER_H

#include "RCDataTypes.h"
#include "system/RCException.h"
#include "core/RCAttrTypeInfo.h"

class DC_TxtOracle /*: public DataConverter*/
{
public:
	//DC_TxtOracle(void);
	static RCBString& Convert(const RCDateTime& rcdt, RCBString& rcs);
	//static RCBString& Convert(const RCNum& rcn, RCBString& rcs);
	//static BHReturnCode Parse(const RCBString& rcs, const bhdt::DataType& dt, RCDateTime& rcdt);
};


#endif
