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

#ifndef INDEXTABLE_H_INCLUDED
#define INDEXTABLE_H_INCLUDED

#include "system/CacheableItem.h"
#include "system/MemoryManagement/TrackableObject.h"
#include <vector>

class Filter;

class IndexTable : private CacheableItem, public TrackableObject
{
public:
	IndexTable(_int64 _size, _int64 _orig_size, int mem_modifier);
	// size - number of objects, bytes_... = 2, 4 or 8
	// mem_modifier - default 0, may also be +1,2... or -1,2..., when positive => IndexTable will take more memory for a buffer
	IndexTable(IndexTable &sec);
	~IndexTable();

	inline void    Set64(_uint64 n,_uint64 val)
	{
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsLocked());
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n < size);
		int b = int(n >> block_shift);
		if(b != cur_block)
			LoadBlock(b);
		block_changed = true;
		if(bytes_per_value == 4)		
			((unsigned int*)buf)[n & block_mask]   = (unsigned int)val;
		else if(bytes_per_value == 8)		
			((_uint64*)buf)[n & block_mask]		   = val;
		else // bytes_per_value == 2
			((unsigned short*)buf)[n & block_mask] = (unsigned short)val;
	}
	void    SetByFilter(Filter *f);						// add all positions where filter is one

	inline _uint64 Get64(_uint64 n)
	{
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsLocked());
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n < size);
		int b = int(n >> block_shift);
		if(b != cur_block) 
			LoadBlock(b);
		_uint64 ndx = n & block_mask;
		_uint64 res;
		if(bytes_per_value == 4)		
			res = ((unsigned int*)buf)[ndx];
		else if(bytes_per_value == 8)	
			res = ((_uint64*)buf)[ndx];
		else 						
			res = ((unsigned short*)buf)[ndx];
		return res;
	}

	inline _uint64 Get64InsideBlock(_uint64 n)		// faster version (no block checking)
	{
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(int(n >> block_shift) == cur_block);
		_uint64 ndx = n & block_mask;
		_uint64 res;
		if(bytes_per_value == 4)		
			res = ((unsigned int*)buf)[ndx];
		else if(bytes_per_value == 8)	
			res = ((_uint64*)buf)[ndx];
		else 						
			res = ((unsigned short*)buf)[ndx];
		return res;
	}

	inline _uint64 EndOfCurrentBlock(_uint64 n)				// return the upper bound of this large block (n which will cause reload)
	{
		return ((n >> block_shift) + 1) << block_shift;
	}

	// use only on newly added data, assumption: block_changed == true
	inline void Swap(_uint64 n1, _uint64 n2)
	{
		int b1 = int(n1 >> block_shift);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsLocked());
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b1 == int(n2 >> block_shift)); // Warning: otherwise serious performance problem
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b1 == cur_block); // Warning: otherwise serious performance problem
		assert(block_changed); // use only on newly added data
		_uint64 ndx1 = n1 & block_mask;
		_uint64 ndx2 = n2 & block_mask;
		if(bytes_per_value == 4)		
			std::swap(((unsigned int*)buf)[ndx1], ((unsigned int*)buf)[ndx2]);
		else if(bytes_per_value == 8)	
			std::swap(((_uint64*)buf)[ndx1], ((_uint64*)buf)[ndx2]);
		else 						
			std::swap(((unsigned short*)buf)[ndx1], ((unsigned short*)buf)[ndx2]);
	}

	_int64 OrigSize()			{ return orig_size; }		// the size of the original table, or the largest (incl. 0 = NULL) value which may occur in table
	_uint64 N()					{ return size; }			// note: this is the upper size, the table can be used partially!
	int BlockShift()			{ return block_shift; } 	// block = int( tuple >> block_shift )

	void ExpandTo(_int64 new_size);

	//TrackableObject functionality
	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_INDEXTABLE;}

private:
	void LoadBlock(int b);

	unsigned char *buf;				// polymorphic: unsigned short, unsigned int or _int64

	int max_buffer_size_in_bytes;
	int bytes_per_value;
	int max_block_used;
	size_t buffer_size_in_bytes;
	int block_shift;
	_uint64 block_mask;
	_uint64 size;
	_int64 orig_size;			// the size of the original table, or the largest value (incl. 0 = NULL) which may occur in the table

	int  cur_block;
	bool block_changed;
	ConnectionInfo& m_conn;		// external pointer
};

///////////////////////////////////////////////////////////////////////////////////////////////////


#endif
