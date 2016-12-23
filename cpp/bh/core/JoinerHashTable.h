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

#ifndef JOINERHASHTABLE_H_INCLUDED
#define JOINERHASHTABLE_H_INCLUDED

#include "bintools.h"
#include "RCAttrTypeInfo.h"
#include "Filter.h"
#include "edition/vc/VirtualColumn.h"
#include "ColumnBinEncoder.h"
#include "system/MemoryManagement/TrackableObject.h"


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// JoinerHashTable - a tool for storing values and counters, implementing hash table mechanism
//
//	Usage:
//
//
//
////////////////////////////////////////////////////////////////////////////////////////////////////////
class HashJoinNotImplementedHere : public RCException
{
public:
	HashJoinNotImplementedHere() throw() : RCException(std::string()) {}
};

class JoinerHashTable : public TrackableObject
{
public:
	JoinerHashTable();
	~JoinerHashTable();

	////////// Hash table construction ///////////

	bool AddKeyColumn(VirtualColumn *vc, VirtualColumn *vc_matching);	// Lookups should not be numerical in case of join, as the code values may be incompatible.
											// Return false if not compatible.
	void AddTupleColumn(int b_size = 8);			// just a tuple number for some dimension (interpreted externally)
	// in future: AddOutputColumn(information for materialized data)?

	void Initialize(_int64 max_table_size, bool easy_roughable);
												// initialize buffers basing on the previously defined columns
												// and the best possible upper approximation of number of tuples

	///////// Hash table usage //////////////////

	// put values to a temporary buffer (note that it will contain the previous values, which may be reused
	void PutKeyValue(int col, MIIterator &mit)			// for all values EXCEPT NULLS
	{
		encoder[col].Encode(input_buffer, mit, NULL, true);		// true: update statistics
	}
	void PutMatchedValue(int col, VirtualColumn *vc, MIIterator &mit)			// for all values EXCEPT NULLS
	{
		encoder[col].Encode(input_buffer, mit, vc);
	}
	void PutMatchedValue(int col, _int64 v)			// for constants
	{
		bool encoded = encoder[col].PutValue64(input_buffer, v, true, false);		// true: the second encoded column false: don't update stats
		assert(encoded);
	}
	_int64 FindAndAddCurrentRow();	// a position in the current JoinerHashTable, NULL_VALUE_64 if no place left
	bool ImpossibleValues(int col, _int64 local_min, _int64 local_max);
	bool ImpossibleValues(int col, RCBString& local_min, RCBString& local_max);
									// return true only if for sure no value between local_min and local_max was put

	bool StringEncoder(int col);	// return true if the values are encoded as strings (e.g. lookup is not a string)
	bool IsColumnSizeValid(int col, int size);
	bool TooManyConflicts()		{ return too_many_conflicts; }

	_int64 InitCurrentRowToGet( );	// find the current key buffer in the JoinerHashTable, initialize internal iterators
	// return value: a number of rows found (a multiplier)
	_int64 GetNextRow();		// the next row, with the value equal to the buffer,
	// return value == NULL_VALUE_64 when no more rows found
	//
	// Usage, to retrieve matching tuples:
	// PutKeyValue(.....)  <- all keys we are searching for
	// InitCurrentRowToGet();
	// do... r = GetNextRow(); ... while( r != NULL_VALUE_64 );

	// columns have common numbering, both key and tuple ones
	void PutTupleValue(int col, _int64 row, _int64 v)		// NULL_VALUE_64 stored as 0
	{
		assert( col>=no_key_attr );
		if( size[col]==4 ) {
			if( v == NULL_VALUE_64 )
				*((int*)   (t + row * total_width + column_offset[col]))	= 0;
			else
				*((int*)   (t + row * total_width + column_offset[col]))	= int(v + 1);
		} else	{
			if( v == NULL_VALUE_64 )
				*((_int64*)(t + row * total_width + column_offset[col]))	= 0;
			else
		 		*((_int64*)(t + row * total_width + column_offset[col]))	= v + 1;
		}
	}

	///////// Hash table output and info //////////////////

	int NoAttr()				{ return no_attr; }		// total number of columns
	int NoKeyAttr()				{ return no_key_attr; }	// number of grouping columns
	_int64 NoRows()				{ return no_rows; }		// buffer size (max. number of rows)

	_int64 GetTupleValue(int col, _int64 row)	// columns have common numbering
	{
		assert( col>=no_key_attr );
		if( size[col] == 4 ) {
			int v = *((int*)(t + row * total_width + column_offset[col]));
			if( v == 0 )
				return NULL_VALUE_64;
			return v - 1;
		} else {
			_int64 v = *((_int64*)(t + row * total_width + column_offset[col]));
			if( v == 0)
				return NULL_VALUE_64;
			return v - 1;
		}
	}


	void ClearAll();								// initialize all

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

private:

	int 				no_of_occupied;

	// content and additional buffers

	// Input buffer format:		<key_1><key_2>...<key_n>
	// Table format:			<key_1><key_2>...<key_n><tuple_1>...<tuple_m><multiplier>
	unsigned char*		input_buffer;
	unsigned char*		t;					// common table for key values and all other
	int*				column_offset;		// a table of offsets (bytes) of column beginnings wrt. the beginning of a row
	int					mult_offset;		// multiplier offset (one of the above)
	int 				key_buf_width;		// in bytes
	int 				total_width;		// in bytes
	bool				too_many_conflicts;	// true if there is more conflicts than a threshold

	_int64 				no_rows;			// actual buffer size (vertical)
	_int64 				rows_limit;			// a capacity of one memory buffer

	std::vector<int>			size;				// byte size of the column (used for tuple columns)
	std::vector<ColumnBinEncoder> encoder;

	// column/operation descriptions

	bool 				initialized;

	int					no_attr;
	int					no_key_attr;

	bool				for_count_only;	// true if there is no other columns than key and multiplier

	_int64				current_row;	// row value to be returned by the next GetNextRow() function
	_int64				to_be_returned;	// the number of rows left for GetNextRow()
	_int64				current_iterate_step;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////


#endif /*JOINERHASHTABLE_H_INCLUDED*/
