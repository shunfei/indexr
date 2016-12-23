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

#include "DataParser2.h"
#include "NewValueSet.h"
#include "ValueCache.h"

using namespace std;

DataParser2::DataParser2(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop)
	:	no_attrs((uint)attrs.size()), buffer(buffer), attrs(attrs),
	last_pack_size(),
	start_time(RCDateTime::GetCurrent().GetInt64()),
	strategy(DataFormat::GetDataFormat(iop.GetEDF())->CreateParsingStrategy(iop)),
	cur_row(0),
	rejecter(iop.GetPackrowSize(), iop.GetRejectFile(), iop.GetAbortOnCount(), iop.GetAbortOnThreshold())
{
	for(uint att = 0; att < no_attrs; ++att)
		string_type_attrs.push_back(ATI::IsStringType(attrs[att]->TypeName()));

	if((buffer.BufStatus() != 1 && buffer.BufStatus() != 4 && buffer.BufStatus() != 5))
		throw FileRCException("Unable to read data. Wrong file or pipe name.");
	cur_ptr = buffer.Buf(0);
	buf_end = cur_ptr + buffer.BufSize();
	cur_ptr += strategy->ParseHeader(cur_ptr, buf_end - cur_ptr);

	iop.GetTimeZone(tz_sign, tz_minute);

}

DataParser2::~DataParser2()
{
}

std::vector<boost::shared_ptr<ValueCache> > DataParser2::GetPackrow(uint no_of_rows, uint& no_of_rows_returned)
{
	std::vector<boost::shared_ptr<ValueCache> > value_buffers;
	for(uint att = 0; att < no_attrs; att++) {
		_int64 init_capacity;
		if(last_pack_size.size() > att)
			init_capacity = static_cast<_int64>(last_pack_size[att] * 1.1) + 128;
		else {
			int max_value_size = (int)sizeof(_int64);
			if (string_type_attrs[att] && attrs[att]->Type().GetPrecision() < max_value_size)
				max_value_size = attrs[att]->Type().GetPrecision();
			init_capacity = MAX_NO_OBJ * max_value_size + 512;
		}
		value_buffers.push_back(boost::shared_ptr<ValueCache>(new ValueCache(MAX_NO_OBJ, init_capacity, attrs[att]->PackType(), attrs[att]->GetCollation())));
	}
	for (no_of_rows_returned = 0; no_of_rows_returned < no_of_rows; no_of_rows_returned++)
		if(!MakeRow(value_buffers))
			break;
	last_pack_size.clear();
	for (value_buffers_t::const_iterator it(value_buffers.begin()), end(value_buffers.end()); it != end; ++ it) {
		last_pack_size.push_back((*it)->SumarizedSize());
	}
	return value_buffers;
}

bool DataParser2::MakeRow(std::vector<boost::shared_ptr<ValueCache> >& value_buffers)
{
	uint rowsize = 0;
	ParseError errorinfo;
	bool cont = true;
	while(cont) {
		bool make_value_ok;
		switch(strategy->GetRow(cur_ptr, buf_end - cur_ptr, value_buffers, rowsize, errorinfo)) {
			case ParsingStrategy::END_OF_BUFFER :
				if(buffer.BufFetch(int(buf_end - cur_ptr))) {
					cur_ptr = buffer.Buf(0);
					buf_end = cur_ptr + buffer.BufSize();
				} else {
					if(cur_ptr != buf_end)
						rejecter.ConsumeBadRow(cur_ptr, buf_end - cur_ptr, cur_row + 1, errorinfo.value == -1 ? -1 : errorinfo.value + 1);
					cur_row++;
					cont = false;
				}
				break;
			case ParsingStrategy::ERROR:
				rejecter.ConsumeBadRow(cur_ptr, rowsize, cur_row + 1, errorinfo.value + 1);
				cur_ptr += rowsize;
				cur_row++;
				break;
			case ParsingStrategy::OK :
				make_value_ok = true;
				for(uint att = 0; make_value_ok && att < no_attrs; ++att)
					if (!MakeValue(att, *value_buffers[att])) {
						rejecter.ConsumeBadRow(cur_ptr, rowsize, cur_row + 1, att + 1);
						make_value_ok = false;
					}
				cur_ptr += rowsize;
				cur_row++;
				if (make_value_ok) {
					for(uint att = 0; att < no_attrs; ++att)
						value_buffers[att]->Commit();
					return true;
				}
				break;
		}
	}
	return false;
}

bool DataParser2::MakeValue(uint att, ValueCache& buffer)
{
	if(attrs[att]->TypeName() == RC_TIMESTAMP) {
		if(buffer.ExpectedNull() && attrs[att]->Type().GetNullsMode() == NO_NULLS) {
			*reinterpret_cast<int64*>(buffer.Prepare(sizeof(int64))) = start_time;
			buffer.ExpectedSize(sizeof(int64));
			buffer.ExpectedNull(false);
		}
		if(!buffer.ExpectedNull()) {
			RCDateTime dt(*reinterpret_cast<int64*>(buffer.PreparedBuffer()), RC_TIMESTAMP);
			dt.ShiftOfPeriod(tz_sign, tz_minute);
			*reinterpret_cast<int64*>(buffer.PreparedBuffer()) = dt.GetInt64();
		}
	}

	// validate the value length
	if (string_type_attrs[att] && !buffer.ExpectedNull() && buffer.ExpectedSize() > attrs[att]->Type().GetPrecision())
		return false;

	// TODO: EncodeValue_T adds new value to dictionary, is that ok?
	if(attrs[att]->Type().IsLookup() && !buffer.ExpectedNull()) {
		RCBString s(ZERO_LENGTH_STRING, 0);
		buffer.Prepare(sizeof(int64));
		s.val = (char*)buffer.PreparedBuffer();
		s.len = buffer.ExpectedSize();
		*reinterpret_cast<int64*>(buffer.PreparedBuffer()) = attrs[att]->EncodeValue_T(s, 1);
		buffer.ExpectedSize(sizeof(int64));
	}

	return true;
}
