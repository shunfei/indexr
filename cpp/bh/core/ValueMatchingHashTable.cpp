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

#include "ValueMatchingHashTable.h"
#include "bintools.h"

using namespace std;



ValueMatching_HashTable::ValueMatching_HashTable()
{
	max_no_rows = 0;
	one_pass = false;
	ht = NULL;
	ht_mask = 0;
	next_pos_offset = 0;
	bmanager = NULL;
	bmanager_owner = false;
}

ValueMatching_HashTable::ValueMatching_HashTable(ValueMatching_HashTable &sec) : ValueMatchingTable(sec), t(sec.t)
{
	max_no_rows = sec.max_no_rows;
	one_pass = false;
	if(sec.ht) {
		ht = new unsigned int [sec.ht_mask + 1];
		memcpy(ht, sec.ht, (sec.ht_mask + 1)* sizeof(int));
	} else
		ht = NULL;
	ht_mask = sec.ht_mask;
	next_pos_offset = sec.next_pos_offset;
	bmanager = sec.bmanager;	// use external bmanager
	bmanager_owner = false;		
}

ValueMatching_HashTable::~ValueMatching_HashTable()
{
	delete [] ht;
	if(bmanager_owner)
		delete bmanager;			// it's safe to destroy bmanager before value tables
}

void ValueMatching_HashTable::Clear()
{
	ValueMatchingTable::Clear();
	t.Clear();
	memset(ht, 0xFF, (ht_mask + 1) * sizeof(int));
}

void ValueMatching_HashTable::Init(_int64 mem_available, _int64 max_no_groups,
										 int _total_width, int _input_buf_width, int _match_width)
{
	total_width = _total_width;
	matching_width = _match_width;
	input_buffer_width = _input_buf_width;
	assert(input_buffer_width > 0);					// otherwise another class should be used
	one_pass = false;

	// add 4 bytes for offset
	total_width = 4 * ((total_width + 3) / 4);		// round up to 4-byte offset
	next_pos_offset = total_width;
	total_width += 4;

	// grouping table sizes
	_int64 desired_mem = max_no_groups * sizeof(int) * 2 + max_no_groups * total_width;		// oversized ht + normal sized t
	if(desired_mem < mem_available) {
		if(max_no_groups > 100000000) {
			// 100 mln is a size which will create too large ht (>1 GB), so it is limited:
			ht_mask = 0x07FFFFFF;	// 134 mln
		} else {
			ht_mask = (unsigned int)(max_no_groups * 1.2);
			ht_mask = (1 << CalculateBinSize(ht_mask)) - 1;		// 001001010  ->  001111111
		}
		one_pass = true;
	} else {		// not enough memory - split between ht and t
		_int64 hts = _int64(mem_available * double(sizeof(int)) / double(total_width + sizeof(int)));	// proportional split
		hts /= sizeof(int);		// number of positions in ht
		if(hts > 100000000) {
			// 100 mln is a size which will create too large ht (>1 GB), so it is limited:
			ht_mask = 0x07FFFFFF;	// 134 mln
		} else {
			ht_mask = (unsigned int)(hts * 1.2);
			ht_mask = (1 << CalculateBinSize(ht_mask)) - 1;		// 001001010  ->  001111111
		}
	}
	if(ht_mask < 63)
		ht_mask = 63;

	max_no_rows = max_no_groups;
	if(max_no_rows > 2000000000) {
		max_no_rows = 2000000000;		// row number should be 32-bit
		one_pass = false;
	}

	// initialize structures
	mem_available -= (ht_mask + 1) * sizeof(int);
	assert(mem_available > 0);
	ht = new unsigned int [ht_mask + 1];
	_int64 min_block_len = max_no_rows * total_width;
	if(min_block_len > GBYTE && mem_available > 256 * MBYTE)	// very large space needed (>1 GB) and substantial memory available
		min_block_len = 16 * MBYTE;
	else if(min_block_len > 4 * MBYTE)
		min_block_len = 4 * MBYTE;

	bmanager = new MemBlockManager(mem_available, 1);
	bmanager_owner = true;
	t.Init(total_width, bmanager, 0, (int)min_block_len);
	Clear();
}

_int64 ValueMatching_HashTable::ByteSize()								
{ 
	_int64 res = (ht_mask + 1) * sizeof(int);
	_int64 max_t_size = bmanager->MaxSize();
	if(max_t_size < max_no_rows * total_width)
		return res + max_t_size;
	if(max_no_rows * total_width < bmanager->BlockSize())		// just one block
		return res + bmanager->BlockSize();
	return res + max_no_rows * total_width; 
}


bool ValueMatching_HashTable::FindCurrentRow(unsigned char *input_buffer, _int64 &row, bool add_if_new)
{
	unsigned int crc_code = HashValue(input_buffer, matching_width);
	unsigned int ht_pos = (crc_code & ht_mask);
	unsigned int row_no = ht[ht_pos];
	if(row_no == 0xFFFFFFFF) {			// empty hash position
		if(!add_if_new) {
			row = NULL_VALUE_64;
			return false;
		}
		row_no = no_rows;
		ht[ht_pos] = row_no;
	}
	while(row_no < no_rows) {
		unsigned char *cur_row = (unsigned char *)t.GetRow(row_no);
		if(memcmp(cur_row, input_buffer, matching_width) == 0) {
			row = row_no;
			return true;			// position found
		}

		unsigned int *next_pos = (unsigned int*)(cur_row + next_pos_offset);
		if(*next_pos == 0) {		// not found and no more conflicted values
			if(add_if_new)
				*next_pos = no_rows;
			row_no = no_rows;
		} else {
			assert(row_no < *next_pos);
			row_no = *next_pos;
		}
	}
	// row_no == no_rows in this place
	if(!add_if_new) {
		row = NULL_VALUE_64;
		return false;
	}
	row = row_no;
	_int64 new_row = t.AddEmptyRow();		// 0 is set as a "NextPos"
	assert(new_row == row);
	memcpy(t.GetRow(row), input_buffer, input_buffer_width);
	no_rows++;
	return false;
}

