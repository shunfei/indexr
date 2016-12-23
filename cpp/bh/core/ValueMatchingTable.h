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

#ifndef VALUEMATCHINGTABLE_H_INCLUDED
#define VALUEMATCHINGTABLE_H_INCLUDED

#include "bintools.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "Filter.h"

// A structure to store large data rows consisting of <matching_data> and <store_data>, divided into "columns" defined as byte offsets.
// Functionality:
// - checking if an input row containing <matching_data> match any of existing rows (return row number),
// - direct access to a row identified by a row number,
// - direct access to a row element identified by a row and column number,
// - adding new <matching_data> element if not found (<store_data> is default in this case),
// - iterative access to all filled positions of table.

class ValueMatchingTable				// abstract class: interface for value matching (Group By etc.)
{
public:
	ValueMatchingTable();
	ValueMatchingTable(ValueMatchingTable &sec);
	virtual ~ValueMatchingTable();
	virtual ValueMatchingTable* Clone() = 0;

	virtual void Clear();

	virtual unsigned char* GetGroupingRow(_int64 row) = 0;
	virtual unsigned char* GetAggregationRow(_int64 row) = 0;

	// Set a position (row number) in the current table, row==NULL_VALUE_64 if not found and not added
	// return value: true if already exists, false if put as a new row
	virtual bool FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new = true) = 0;

	_int64 NoRows()							{ return _int64(no_rows); }		// rows stored so far
	virtual bool IsOnePass() = 0;			// true if the aggregator is capable of storing all groups up to max_no_groups declared in Init()
	virtual bool NoMoreSpace() = 0;			// hard stop: any new value will be rejected / we already found all groups
	virtual int MemoryBlocksLeft() = 0;		// soft stop: if 0, finish the current pack and start rejecting new values, default: 999
	virtual _int64 ByteSize() = 0;			// total size of the structure (for reporting / parallel threads number evaluation)
	virtual _int64 RowNumberScope() = 0;	// max_nr + 1, where max_nr is the largest row number returned by FindCurrentRow, GetCurrentRow etc.

	// Iterator
	virtual void Rewind(bool release = false) = 0;				// rewind a row iterator, release=true - free memory occupied by traversed rows
	virtual _int64 GetCurrentRow() = 0;		// return a number of current row, or NULL_VALUE_64 if there is no more
	virtual void NextRow() = 0;				// iterate to a next row
	virtual bool RowValid() = 0;			// false if there is no more rows to iterate

	////////////////////////////////////////////////////////////
	// Selection algorithm and initialization of the created object. Input values:
	// - max. memory to take,
	// - upper approx. of a number of groups, maximal group number (code),
	// - buffer sizes: total, input (grouping attrs plus grouping UTF), matching (grouping attrs only)

	static ValueMatchingTable* CreateNew_ValueMatchingTable(_int64 mem_available, _int64 max_no_groups,   _int64 max_group_code,
											int    _total_width,   int    _input_buf_width, int   _match_width);

	////////////////////////////////////////////////////////////
protected:
	int total_width;						// whole row
	int matching_width;						// a part of one row used to compare matching values
	int input_buffer_width;					// a buffer for matching values plus additional info (e.g. UTF buffer)

	unsigned int no_rows;							// rows stored so far
};

//////////////////////////////////////////////////////////////////////////////////////
// Trivial version: just one position

class ValueMatching_OnePosition :  public ValueMatchingTable
{
public:
	ValueMatching_OnePosition();
	ValueMatching_OnePosition(ValueMatching_OnePosition &sec);
	virtual ~ValueMatching_OnePosition();

	virtual void Init(int _total_width);		// assumption: match_width = input_buf_width = 0, max_group = 1

	virtual ValueMatchingTable* Clone()						{ return new ValueMatching_OnePosition(*this); }

	virtual void Clear();

	virtual unsigned char* GetGroupingRow(_int64 row)		{ return NULL; }
	virtual unsigned char* GetAggregationRow(_int64 row)	{ return t_aggr; }

	virtual bool FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new = true);

	virtual void Rewind(bool release = false)									{ iterator_valid = (no_rows > 0); }
	virtual _int64 GetCurrentRow()							{ return 0; }
	virtual void NextRow()									{ iterator_valid = false; }
	virtual bool RowValid()									{ return iterator_valid; }

	virtual bool NoMoreSpace()								{ return no_rows > 0; }
	virtual int MemoryBlocksLeft()							{ return 999; }			// one-pass only
	virtual bool IsOnePass()								{ return true; }
	virtual _int64 ByteSize()								{ return total_width; }
	virtual _int64 RowNumberScope()							{ return 1; }

protected:
	unsigned char*		t_aggr;
	bool				iterator_valid;
};

//////////////////////////////////////////////////////////////////////////////////////
// Easy case: a group number indicates its position in lookup table

class ValueMatching_LookupTable :  public TrackableObject, public ValueMatchingTable
{
public:
	ValueMatching_LookupTable();
	ValueMatching_LookupTable(ValueMatching_LookupTable &sec);
	virtual ~ValueMatching_LookupTable();

	virtual void Init(_int64 max_group_code, int _total_width, int _input_buf_width, int _match_width);

	virtual ValueMatchingTable* Clone()						{ return new ValueMatching_LookupTable(*this); }

	virtual void Clear();

	// TO BE OPTIMIZED: actually we don't need any grouping buffer if there's no UTF part
	virtual unsigned char* GetGroupingRow(_int64 row)		{ 	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(row < max_no_rows); 
																return t + row * total_width; }
	virtual unsigned char* GetAggregationRow(_int64 row)	{ 	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(row < max_no_rows);
																return t_aggr + row * total_width; }

	virtual bool FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new = true);

	virtual void Rewind(bool release = false)				{ occupied_iterator = 0; }
	virtual _int64 GetCurrentRow()							{ return occupied_table[occupied_iterator]; }
	virtual void NextRow()									{ occupied_iterator++; }
	virtual bool RowValid()									{ return (occupied_iterator < no_rows); }

	virtual bool NoMoreSpace()								{ return no_rows >= max_no_rows; }
	virtual int MemoryBlocksLeft()							{ return 999; }			// one-pass only
	virtual bool IsOnePass()								{ return true; }
	virtual _int64 ByteSize()								{ return (total_width + sizeof(int)) * max_no_rows; }
	virtual _int64 RowNumberScope()							{ return max_no_rows; }

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

protected:
	unsigned char*		t;					// common buffer for grouping values and results of aggregations
	unsigned char*		t_aggr;				// = t + input_buffer_width, just for speed

	Filter*				occupied;
	int*				occupied_table;
	_int64				max_no_rows;
	_int64				occupied_iterator;
};

#endif
