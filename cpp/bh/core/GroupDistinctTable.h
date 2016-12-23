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

#ifndef GROUPDISTINCTTABLE_H_
#define GROUPDISTINCTTABLE_H_

#include "bintools.h"
#include "Filter.h"
#include "ColumnBinEncoder.h"
#include "BlockedMemTable.h"
#include "edition/vc/VirtualColumn.h"

enum GDTResult { 	GDT_ADDED,			// value successfully added to a table as a new one
					GDT_EXISTS, 		// value already in table
					GDT_FULL			// value not found, but cannot add (table full)
};

static const _int64 zero_const = 0;

// Note: this class is less flexible but more packed comparing with GroupTable.
//      Memory usage efficiency and searching speed are key factors.
//
// Functionality: to allow fast adding value vectors (number,number) or (number,RCBString)
//		with checking repetitions. To be used in DISTINCT inside GROUP BY.
//		Limited memory is assumed (specified in the constructor).
//
class GroupDistinctTable : public TrackableObject
{
public:
	GroupDistinctTable();					// maximal size of structures; 0 - default
	~GroupDistinctTable();
	_int64 BytesTaken();					// actual size of structures
	void InitializeB(int max_len, _int64 max_no_rows);		// char* initialization (for arbitrary vector of bytes)
	void InitializeVC(_int64 max_no_groups,
						VirtualColumn *vc,
						_int64 max_no_rows,
						_int64 max_bytes,
						bool decodable);

	// Assumption: group >= 0
	GDTResult	Add(_int64 group, MIIterator &mit);		// a value from the virtual column used to initialize
	GDTResult	Add(_int64 group, _int64 val);			// numeric values
	GDTResult	Add(unsigned char *v);					// arbitrary vector of bytes
	GDTResult	AddFromCache(unsigned char *input_vector);	// input vector from cache
	GDTResult	Find(_int64 group, _int64 val);			// check whether the value is already found
	bool 		AlreadyFull()				{ return no_of_occupied >= rows_limit; }
	void 		Clear();					// clear the table

	int			InputBufferSize()		{ return total_width; }
	unsigned char*	InputBuffer()		{ return input_buffer; }	// for caching purposes

	_int64		GroupNoFromInput();		// decode group number from the current input vector
	_int64		ValueFromInput();		// decode original value from the current input vector
	void		ValueFromInput(RCBString &); // decode original value from the current input vector

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

private:
	GDTResult	FindCurrentRow(bool find_only = false);			// find / insert the current buffer
	bool		RowEmpty(unsigned char* p)	// is this position empty? only if it starts with zeros
	{ return (memcmp(&zero_const, p, group_bytes) == 0); }
	void 		InitializeBuffers(_int64 max_no_rows);
											// create buffers for sizes determined and stored in class fields
											// max_no_rows = 0   =>  limit not known

	// Internal representation: <group><value> (no. of bytes given by variables below)
	// "group" is stored with offset 1, because value 0 means "position empty"
	unsigned char*		input_buffer;		// buffer for (encoded) input values
	unsigned char*		t;					// buffer for all values

//	MemBlockManager		mem_mngr;
//	BlockedRowMemStorage*	t;

	int group_bytes;						// byte width for groups, up to 8
	int value_bytes;						// byte width for values ?? JB: size of elements in hash table
	int input_length;						// byte width for input values;
	int total_width;						// byte width for all the vector
	_int64 max_total_size;					// max. size of hash table in memory
	_int64 no_rows;							// number of rows in table

	_int64 rows_limit;						// a number of rows which may be safely occupied
	_int64 no_of_occupied;					// a number of rows already occupied in buffer

	bool initialized;
	bool use_CRC;

	///////////// Encoder
	ColumnBinEncoder *encoder;

	///////////// Filter implementation
	bool filter_implementation;
	Filter *f;
	_int64 group_factor;					// (g, v)  ->  f( g + group_factor * v )
};

#endif /*GROUPDISTINCTTABLE_H_*/
