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

#ifndef VALUEPARSERFORTEXT_H_
#define VALUEPARSERFORTEXT_H_

#include "ValueParser.h"
#include "common/bhassert.h"

class ValueParserForText : public ValueParser
{
public:
	ValueParserForText(){};
public:
	static ParsingFunction ParsingFuntion(const AttributeTypeInfo& at);

	virtual ParsingFunction GetParsingFuntion(const AttributeTypeInfo& at) const { return ValueParserForText::ParsingFuntion(at); };

	static BHReturnCode ParseNumeric(RCBString const& rcs, _int64& out, AttributeType at);
	static BHReturnCode ParseBigIntAdapter(const RCBString& rcs, _int64& out);
	static BHReturnCode ParseDecimal(RCBString const& rcs, _int64& out, short precision, short scale);
	static BHReturnCode ParseDateTimeAdapter(RCBString const& rcs, _int64& out, AttributeType at);

	static BHReturnCode Parse(const RCBString& rcs, RCNum& rcn, AttributeType at);
	static BHReturnCode ParseNum(const RCBString& rcs, RCNum& rcn, short scale);
	static BHReturnCode ParseBigInt(const RCBString& rcs, RCNum& out);
	static BHReturnCode ParseReal(const RCBString& rcbs, RCNum& rcn, AttributeType at);
	static BHReturnCode ParseDateTime(const RCBString& rcs, RCDateTime& rcv, AttributeType at);
private:
	static BHReturnCode ParseDateTimeOrTimestamp(const RCBString& rcs, RCDateTime& rcv, AttributeType at);
	static BHReturnCode ParseTime(const RCBString& rcs, RCDateTime& rcv);
	static BHReturnCode ParseDate(const RCBString& rcs, RCDateTime& rcv);
	static BHReturnCode ParseYear(const RCBString& rcs, RCDateTime& rcv);
};


#endif /*VALUEPARSERFORTEXT_H_*/
