/* Copyright (C)  2005-2012 Infobright Inc.

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

#include "ValueMatchingTable.h"
#include "ValueMatchingHashTable.h"

using namespace std;

ValueMatchingTable::ValueMatchingTable()
{
	total_width = 0;
	matching_width = 0;
	input_buffer_width = 0;
	no_rows = 0;
}

ValueMatchingTable::~ValueMatchingTable()
{
}

ValueMatchingTable::ValueMatchingTable(ValueMatchingTable &sec)
{
	total_width = sec.total_width;
	matching_width = sec.matching_width;
	input_buffer_width = sec.input_buffer_width;
	no_rows = sec.no_rows;
}

void ValueMatchingTable::Clear()
{
	no_rows = 0;
}
////////////////////////////   Selection algorithm   ////////////////////////

ValueMatchingTable* ValueMatchingTable::CreateNew_ValueMatchingTable(_int64 mem_available, _int64 max_no_groups,   _int64 max_group_code,
														int    _total_width,   int    _input_buf_width, int   _match_width)
{
	// trivial case: one group only
	if(_input_buf_width == 0) {
		ValueMatching_OnePosition* new_object = new ValueMatching_OnePosition();
		new_object->Init(_total_width);
		return new_object;
	}

	// easy case: narrow scope of group codes, which may be used
	if(max_group_code < PLUS_INF_64 && max_group_code < max_no_groups * 1.5) {
		ValueMatching_LookupTable* new_object = new ValueMatching_LookupTable();
		new_object->Init(max_group_code, _total_width, _input_buf_width, _match_width);
		return new_object;
	}

	// default
	ValueMatching_HashTable* new_object = new ValueMatching_HashTable();
	new_object->Init(mem_available, max_no_groups, _total_width, _input_buf_width, _match_width);
	return new_object;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

ValueMatching_OnePosition::ValueMatching_OnePosition()
{
	t_aggr = NULL;
	iterator_valid = false;
}

ValueMatching_OnePosition::ValueMatching_OnePosition(ValueMatching_OnePosition &sec) : ValueMatchingTable(sec)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(total_width > 0);
	iterator_valid = sec.iterator_valid;

	t_aggr = NULL;
	t_aggr = new unsigned char [total_width];
	memcpy(t_aggr, sec.t_aggr, total_width);
}

ValueMatching_OnePosition::~ValueMatching_OnePosition()
{
	delete [] t_aggr;
}

void ValueMatching_OnePosition::Init(int  _total_width)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(_total_width > 0);
	total_width = _total_width;
	matching_width = input_buffer_width = 0;
	t_aggr = new unsigned char [total_width];
}


void ValueMatching_OnePosition::Clear()
{
	ValueMatchingTable::Clear();
	memset(t_aggr, 0, total_width);
}

bool ValueMatching_OnePosition::FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new)
{
	row = 0;
	if(no_rows == 1)
		return true;
	if(!add_if_new) {
		row = NULL_VALUE_64;
		return false;
	}
	no_rows = 1;
	return false;
}

////////////////////////////////////////////////////////////////////////////////////////////////

ValueMatching_LookupTable::ValueMatching_LookupTable()
{
	t = NULL;
	t_aggr = NULL;
	occupied = NULL;
	occupied_table = NULL;
	max_no_rows = 0;
	occupied_iterator = 0;

}

ValueMatching_LookupTable::ValueMatching_LookupTable(ValueMatching_LookupTable &sec) : ValueMatchingTable(sec)
{
	max_no_rows = sec.max_no_rows;
	occupied_iterator = sec.occupied_iterator;

	if(sec.occupied)
		occupied = new Filter(*sec.occupied);
	else
		occupied = NULL;

	if(sec.occupied_table) {
		occupied_table = new int [max_no_rows];
		memcpy(occupied_table, sec.occupied_table, max_no_rows * sizeof(int));
	} else 
		occupied_table = NULL;

	t = (unsigned char*)alloc(total_width * max_no_rows, BLOCK_TEMPORARY);
	t_aggr = t + input_buffer_width;
	memcpy(t, sec.t, total_width * max_no_rows);
}

ValueMatching_LookupTable::~ValueMatching_LookupTable()
{
	delete occupied;
	delete [] occupied_table;
	dealloc(t);
}

void ValueMatching_LookupTable::Clear()
{
	ValueMatchingTable::Clear();
	memset(t, 0, total_width * max_no_rows);
	occupied->Reset();
}

void ValueMatching_LookupTable::Init(_int64 max_group_code, int _total_width, int _input_buf_width, int _match_width)
{
	total_width = _total_width;
	matching_width = _match_width;
	input_buffer_width = _input_buf_width;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(matching_width > 0);			// else another class should be used

	max_no_rows = max_group_code + 1;			// easy case: the row number is just the coded value

	t = (unsigned char*)alloc(total_width * max_no_rows, BLOCK_TEMPORARY);
	t_aggr = t + input_buffer_width;

	// initialize structures
	occupied = new Filter(max_no_rows);
	occupied_table = new int [max_no_rows];
	Clear();
}

bool ValueMatching_LookupTable::FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new)
{
	row = 0;
	memcpy(&row, input_buffer, matching_width);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(row < max_no_rows);
	if(occupied->Get(row))
		return true;
	if(!add_if_new) {
		row = NULL_VALUE_64;
		return false;
	}
	memcpy(t + row * total_width, input_buffer, input_buffer_width);
	occupied->Set(row);
	occupied_table[no_rows] = (int)row;
	no_rows++;
	return false;
}

