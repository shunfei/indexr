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

#ifndef DATAPARSER2_H_
#define DATAPARSER2_H_

#include <vector>
#include <boost/shared_array.hpp>

#include "edition/loader/RCAttr_load.h"
#include "ParsingStrategy.h"
#include "Rejecter.h"

class NewValuesSetBase;
class ValueParser;
class ValueCache;

class DataParser2 : public boost::noncopyable
{
	friend class NewValuesSet;
public:
	DataParser2(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop);
	virtual ~DataParser2();

	typedef boost::shared_ptr<ValueCache> ValueCachePtr;
	typedef std::vector<ValueCachePtr> value_buffers_t;
	value_buffers_t GetPackrow(uint no_of_rows, uint& no_of_rows_returned);

	int64	GetNoRejectedRows() const { return rejecter.GetNoRejectedRows(); }
	bool	ThresholdExceeded(int64 no_rows) const { return rejecter.ThresholdExceeded(no_rows); }
private:
	uint no_attrs;
	Buffer& buffer;
	std::vector<RCAttrLoad*> attrs; //TODO: Does it have to be a vector of naked pointers?
	std::vector<bool> string_type_attrs;
	typedef std::vector<_int64> last_pack_size_t;
	last_pack_size_t last_pack_size;
	int64 start_time;
	short tz_sign;
	short tz_minute;

	ParsingStrategyAutoPtr strategy;

	const char* cur_ptr;
	const char* buf_end;

	uint cur_row;
	Rejecter rejecter;

	short	GetTzSign() const		{ return tz_sign; }
	short	GetTzMinutes() const	{ return tz_minute; }
	bool	MakeRow(value_buffers_t& value_buffers);
	bool	MakeValue(uint col, ValueCache& buffer);
};

#endif /*DATAPARSER2_H_*/
