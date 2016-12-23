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

#include <boost/shared_ptr.hpp>

#include "DataParser.h"
#include "types/ValueParser.h"
#include "core/tools.h"


#ifdef __WIN__
#define gmtime_r(timearg, tmarg) gmtime_s((tmarg),(timearg))
#endif /* __WIN__ */

using namespace std;
using namespace boost;

DataParser::DataParser()
	:	max_parse_rows(0)
{
	no_attr = 0;
	sign = 0;
	minute = 0;
	no_prepared = 0;
	cur_attr = 0;
	row_header_byte_len = 0;
	row_incomplete = true;
	buffer = NULL;
	buf_start = NULL;
	buf_ptr = NULL;
	buf_end = NULL;
}

DataParser::DataParser(vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr)
	:	attrs(attrs), buffer(&buffer), values_ptr(mpr), rows_ptr(mpr), nulls(mpr), sizes(mpr), row_sizes(mpr), error_value(-1, -1), max_parse_rows(mpr)
{
	if((buffer.BufStatus() != 1 && buffer.BufStatus() != 4 && buffer.BufStatus() != 5))
		throw FileRCException("Unable to read data. Wrong file or pipe name.");
	edf = iop.GetEDF();
	iop.GetTimeZone(sign, minute);

	no_attr = int(attrs.size());
	no_prepared = 0;
	buf_start = buf_end = buf_ptr = 0;
	cur_attr = -1;
	row_header_byte_len = 0;
	for(int i = 0; i < no_attr; i++)
		objs_sizes.push_back(shared_ptr<vector<uint> >(new vector<uint>(max_parse_rows)));

	buf_ptr = buf_start = this->buffer->Buf(0);
	buf_end = buf_start + this->buffer->BufSize();

	loaderStartTime = RCDateTime::GetCurrent().GetInt64();

	InitAttributesTypeInfo();

	for(int i = 0; i < no_attr; i++) {
		if (atis[i].Type() == RC_STRING) {
			value_sizes.push_back(shared_ptr<vector<uint> >(new vector<uint>(max_parse_rows)));
		} else
			value_sizes.push_back(objs_sizes[i]);
	}

	value_parser = DataFormat::GetDataFormat(iop.GetEDF())->CreateValueParser();
}

DataParser::~DataParser()
{
}

void DataParser::InitAttributesTypeInfo()
{
	for(int i = 0; i < no_attr; i++) {
		atis.push_back(AttributeTypeInfo(attrs[i]->TypeName(), attrs[i]->Type().GetNullsMode() == NO_NULLS,
			attrs[i]->Type().GetPrecision(), attrs[i]->Type().GetScale()));
	}
}

int DataParser::Prepare(int no_rows)
{
	MEASURE_FET("DataParser::Prepare()");
	ReleaseCopies();
	cur_attr = - 1;

	int row_size = GetRowSize(buf_ptr, no_prepared);
	if (row_size == -1)
		return 0;
	while((row_size >= 0) && (no_prepared < no_rows)) {
		rows_ptr[no_prepared] = buf_ptr;
		values_ptr[no_prepared] = buf_ptr + row_header_byte_len;
		row_sizes[no_prepared] = row_size;
		no_prepared++;
		buf_ptr += row_size;
		if(no_prepared < no_rows)
			row_size = GetRowSize(buf_ptr, no_prepared);
	};
	return no_prepared;
}

void DataParser::PrepareNextCol()
{
	MEASURE_FET("DataParser::PrepareNextCol()");
	cur_attr++;
	cur_attr_type = atis[cur_attr].Type();

	PrepareValuesPointers();
	PrepareObjsSizes();
	PrepareNulls();
	FormatSpecificProcessing();

	if(!CheckData())
		throw FormatRCException(BHERROR_DATA_ERROR, error_value.first, error_value.second);

	if(attrs[cur_attr]->PackType() == PackN)
		PrepareNumValues();
}

uint DataParser::GetObjSize(int ono)
{
	return (*objs_sizes_ptr)[ono];
}

uint DataParser::GetValueSize(int ono)
{
	return (*value_sizes[cur_attr])[ono];
}

void DataParser::PrepareNumValuesForLookup()
{
	int size = 0;
	for(int i = 0; i < no_prepared; i++) {
		if(!IsNull(i)) {
			size = GetValueSize(i);
			num_values64[i] = attrs[cur_attr]->EncodeValue_T(RCBString(size ? values_ptr[i] : 0, size), 1);
		}
	}
}

void DataParser::PrepareNumValues()
{
	MEASURE_FET("DataParser::PrepareNumValues()");

	BHASSERT(attrs[cur_attr]->PackType() == PackN, "should be 'attrs[cur_attr]->PackType() == PackN'");
	if(num_values64.empty())
		num_values64.resize(max_parse_rows);

	if(attrs[cur_attr]->Type().IsLookup()) {
		PrepareNumValuesForLookup();
	} else {
		int os = 0;
		RCBString rcs(ZERO_LENGTH_STRING, 0);
		ParsingFunction parsing_function = value_parser->GetParsingFuntion(atis[cur_attr]);

		for(int i = 0; i < no_prepared; i++) {
			if(!IsNull(i)) {
				os = GetObjSize(i);
				TemporalValueReplacement<char> tmpvr(values_ptr[i][os], 0);
				rcs.val = values_ptr[i];
				rcs.len = os;

				if(parsing_function(rcs, num_values64[i]) == BHRC_FAILD)
					throw FormatRCException(BHERROR_DATA_ERROR, i + 1, cur_attr + 1);

			} else if(cur_attr_type == RC_TIMESTAMP && attrs[cur_attr]->Type().GetNullsMode() == NO_NULLS) {
				num_values64[i] = loaderStartTime;
				nulls[i] = 0;
			}
			if(cur_attr_type == RC_TIMESTAMP) {
				RCDateTime dt(num_values64[i], RC_TIMESTAMP);				
				dt.ShiftOfPeriod(sign, minute);
				num_values64[i] = dt.GetInt64();
			}
		}
	}

}

bool DataParser::IsLastColumn()
{
	return (cur_attr == (no_attr - 1));
}

bool DataParser::IsNull(int ono)
{
	return nulls[ono] == 1;
}

bool DataParser::CheckPreparedValuesSizes()
{
	if(ATI::IsStringType(cur_attr_type)) {
		for(int i = 0; i < no_prepared; i++) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(GetObjPtr(i) >= rows_ptr[i]);
			if(!IsNull(i) && (GetValueSize(i) > (uint)attrs[cur_attr]->Type().GetPrecision() || (GetObjPtr(i) + GetObjSize(i) > rows_ptr[i] + row_sizes[i]))) {
				error_value.first = i + 1;
				error_value.second = cur_attr + 1;
				return false;
			}
		}
	} else {
		for(int i = 0; i < no_prepared; i++) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(GetObjPtr(i) >= rows_ptr[i]);
			if(!IsNull(i) && GetObjPtr(i) + GetObjSize(i) > rows_ptr[i] + row_sizes[i]) {
				error_value.first = i + 1;
				error_value.second = cur_attr + 1;
				return false;
			}
		}
	}
	return true;
}

bool DataParser::CheckData()
{
	return CheckPreparedValuesSizes() && FormatSpecificDataCheck();
}

char* DataParser::GetValue(int ono)
{
	if(attrs[cur_attr]->PackType() == PackN)
		return (char*)&num_values64[ono];
	return values_ptr[ono];
}

char ** DataParser::copy_values_ptr(int start_ono, int end_ono) {
	char **result = new char*[end_ono-start_ono];
	for( int i=0, j=start_ono; j<end_ono; i++,j++ )
		result[i] = values_ptr[j];
		
	return result;		
}
	
char * DataParser::copy_nulls(int start_ono, int end_ono) {
	char *result = new char[end_ono-start_ono];
	memcpy( result, &nulls[start_ono], end_ono - start_ono);
	return result;
}
	
_int64* DataParser::copy_num_values(int start_ono, int end_ono) {
	_int64 *result = new _int64[end_ono-start_ono];
	//memcpy( result, &num_values64[start_ono], (end_ono-start_ono)*sizeof(_int64));
	for( int i=0, j=start_ono; j<end_ono; i++,j++ )
		result[i] = num_values64[j];

	return result;
}

int* DataParser::copy_sizes(int start_ono, int end_ono) {
	int *result = new int[end_ono-start_ono];
	memcpy( result, &sizes[start_ono], (end_ono-start_ono)*sizeof(int));
	return result;
}

int* DataParser::copy_obj_sizes(int start_ono, int end_ono) {
	int *result = new int[end_ono-start_ono];
	//memcpy( result, &(objs_sizes[attr][start_ono]), (end_ono-start_ono)*sizeof(int));
	for( int i=0, j=start_ono; j<end_ono; i++,j++ )
		result[i] = (*objs_sizes_ptr)[j];
	return result;
}

uint* DataParser::copy_value_sizes(int attr, int start_ono, int end_ono) {
	uint *result = new uint[end_ono-start_ono];
	//memcpy( result, &value_sizes[attr][start_ono], (end_ono-start_ono)*sizeof(uint));
	for( int i=0, j=start_ono; j<end_ono; i++,j++ )
		//result[i] = value_sizes[cur_attr][j];
		result[i] = GetValueSize(j);

	return result;
}


