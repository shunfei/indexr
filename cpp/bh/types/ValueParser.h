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

#ifndef VALUEPARSER_H_
#define VALUEPARSER_H_

#include <boost/function.hpp>
#include <boost/bind.hpp>

#include "core/RCAttrTypeInfo.h"
#include "system/RCException.h"
#include "RCDataTypes.h"

class RCBString;
typedef boost::function2<BHReturnCode, RCBString const&, _int64&> ParsingFunction;

class ValueParser
{
public:	
	virtual ~ValueParser() {}
	virtual ParsingFunction GetParsingFuntion(const AttributeTypeInfo& at) const = 0;
};

#endif /*VALUEPARSER_H_*/
