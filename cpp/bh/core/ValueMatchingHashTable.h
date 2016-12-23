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

#ifndef VALUEMATCHINGHASHTABLE_H_INCLUDED
#define VALUEMATCHINGHASHTABLE_H_INCLUDED

#include "ValueMatchingTable.h"
#include "BlockedMemTable.h"

//////////////////////////////////////////////////////////////////////////////////////
// Expandable hash table

class ValueMatching_HashTable :  public TrackableObject, public ValueMatchingTable
{
public:
	ValueMatching_HashTable();
	ValueMatching_HashTable(ValueMatching_HashTable &sec);
	virtual ~ValueMatching_HashTable();

	virtual void Init(_int64 mem_available, _int64 max_no_groups, int _total_width, int _input_buf_width, int _match_width);

	virtual ValueMatchingTable* Clone()						{ return new ValueMatching_HashTable(*this); }

	virtual void Clear();

	virtual unsigned char* GetGroupingRow(_int64 row)		{ return (unsigned char*)t.GetRow(row); }
	virtual unsigned char* GetAggregationRow(_int64 row)	{ return (unsigned char*)t.GetRow(row) + input_buffer_width; }

	virtual bool FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new = true);

	virtual void Rewind(bool release = false)				{ t.Rewind(release); }
	virtual _int64 GetCurrentRow()							{ return t.GetCurrent(); }
	virtual void NextRow()									{ t.NextRow(); }
	virtual bool RowValid()									{ return (t.GetCurrent() != -1); }

	virtual bool NoMoreSpace()								{ return _int64(no_rows) >= max_no_rows; }
	virtual int MemoryBlocksLeft()							{ return (one_pass ? 999 : bmanager->MemoryBlocksLeft()); }
	virtual bool IsOnePass()								{ return one_pass; }
	virtual _int64 ByteSize();
	virtual _int64 RowNumberScope()							{ return max_no_rows; }

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

protected:
	// Definition of the internal structures:
	// ht - constant size hash table of integers: 0xFFFFFFFF is empty position, other - an index of value stored in t
	// t - a container (expandable storage) of numbered rows:
	//     row = <group_val><group_UTF><aggr_buffer><offset_integer>
	//     where <group_val><group_UTF><aggr_buffer> is a buffer to be used by external classes,
	//           <offset_integer> is a position of the next row with the same hash value, or 0 if there is no such row

	unsigned int*		ht;					// table of offsets of hashed values, or 0xFFFFFFFF if position empty
	unsigned int		ht_mask;			// (crc_code & ht_mask) is a position of code in ht table; ht_mask + 1 is the total size (in integers) of ht

	_int64				max_no_rows;
	bool				one_pass;			// true if we guarantee one-pass processing
	int					next_pos_offset;	// position of the "next position" indicator in stored row

	BlockedRowMemStorage t;					// row storage itself
	MemBlockManager*	bmanager;			// either external link, or created internally
	bool				bmanager_owner;		// if true then this is an owner of bmanager (should be deleted in destructor)
};

#endif
