/* Copyright (C)  2012 Infobright Inc.

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

#ifndef PARSINGSTRATEGY_H_
#define PARSINGSTRATEGY_H_

#include <boost/shared_ptr.hpp>

#include "NewValuesSetBase.h"
#include "core/RCAttrTypeInfo.h"
#include "common/DataFormat.h"

class NewValuesSetBase;
class ValueCache;

class ParseError
{
public:
	int value;
};

class ParsingStrategy
{
public:
	enum ParseResult { OK, END_OF_BUFFER, ERROR };

public:
	ParsingStrategy(const IOParameters& iop/*, int64 loader_start_time*/);
	virtual ~ParsingStrategy() {}

public:

	virtual int ParseHeader(const char* const buf, size_t size) const = 0;
	virtual ParseResult GetRow(const char* const buf, size_t size, std::vector<boost::shared_ptr<ValueCache> >&, uint& rowsize, ParseError& errorinfo) = 0;

	//short GetTzSign() const				{ return tz_sign; }
	//short GetTzMinutes() const			{ return tz_minute; }
	//int64 GetLoaderStartTime() const	{ return loader_start_time; }

protected:
	ATI&			GetATI(ushort col) { return atis[col]; }
	const ushort	NoColumns() const { return (ushort)atis.size(); }

private:
	std::vector<ATI> atis;

	//short tz_sign;
	//short tz_minute;
	//int64 loader_start_time;
};

#endif /* PARSINGSTRATEGY_H_ */
