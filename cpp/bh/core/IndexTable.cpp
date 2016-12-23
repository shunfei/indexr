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

#include "IndexTable.h"
#include "system/ConnectionInfo.h"
#include "system/RCSystem.h"
#include "Filter.h"

using namespace std;

//////////////////////////////////////////////////////////////////////////////////////

IndexTable::IndexTable(_int64 _size, _int64 _orig_size, int mem_modifier)
:	CacheableItem("JW", "INT"), m_conn(ConnectionInfoOnTLS.Get())
{
	// Note: buffer size should be 2^n
	assert(_orig_size >= 0);
	orig_size = _orig_size;
	bytes_per_value = ((orig_size + 1) > 0xFFFF ? ((orig_size + 1) > 0xFFFFFFFF ? 8 : 4 ) : 2);

	max_buffer_size_in_bytes = 32 * MBYTE;				// 32 MB = 2^25
	if(_size * bytes_per_value > 32 * MBYTE)
		max_buffer_size_in_bytes = int(TrackableObject::MaxBufferSize(-1));

	buffer_size_in_bytes = max_buffer_size_in_bytes;
	CI_SetDefaultSize((int)max_buffer_size_in_bytes);
	size = _size;
	if(size == 0)
		size = 1;
	uint values_per_block = uint(max_buffer_size_in_bytes / bytes_per_value);
	block_shift = CalculateBinSize(values_per_block) - 1;	// e.g. BinSize(16)==5, but shift by 4. WARNING: should it be (val...-1)? Now it works, because v... is only 2^25, 2^24, 2^23
	block_mask = (_uint64(1)<<block_shift) - 1;

	if(size * bytes_per_value < buffer_size_in_bytes)
		buffer_size_in_bytes = int(size * bytes_per_value);		// the whole table in one buffer
	buf = (unsigned char*)alloc(buffer_size_in_bytes, BLOCK_TEMPORARY, true);
	if (!buf) {
		Unlock();
		rclog << lock << "Could not allocate memory for IndexTable!" << unlock;
		throw OutOfMemoryRCException();
	}
	memset(buf, 0, buffer_size_in_bytes);
	cur_block = 0;
	max_block_used = 0;
	block_changed = false;
	Unlock();
}

IndexTable::IndexTable(IndexTable &sec)
:	CacheableItem("JW", "INT"), max_buffer_size_in_bytes(sec.max_buffer_size_in_bytes),
bytes_per_value(sec.bytes_per_value), max_block_used(sec.max_block_used),
buffer_size_in_bytes(sec.buffer_size_in_bytes),	block_shift(sec.block_shift),
block_mask(sec.block_mask), size(sec.size), orig_size(sec.orig_size), cur_block(0),
block_changed(false), m_conn(sec.m_conn)
{
	sec.Lock();
	CI_SetDefaultSize((int)max_buffer_size_in_bytes);
	buf = (unsigned char*)alloc(buffer_size_in_bytes, BLOCK_TEMPORARY, true);
	if (!buf) {
		sec.Unlock();
		Unlock();
		rclog << lock << "Could not allocate memory for IndexTable(sec)!" << unlock;
		throw OutOfMemoryRCException();
	}
	_int64 used_size = size;
	if(used_size > ((_int64(max_block_used) + 1) << block_shift))
		used_size = ((_int64(max_block_used) + 1) << block_shift);
	if (max_block_used == 0 && size > 0) {
		memcpy(buf,sec.buf, min(_uint64(buffer_size_in_bytes), size * bytes_per_value));
		block_changed = true;
	} else
		for(uint k = 0; k < used_size; k++)
			Set64(k, sec.Get64(k));
	sec.Unlock();
	Unlock();
}

IndexTable::~IndexTable()
{
	DestructionLock();
	if(buf) dealloc(buf);
}

void IndexTable::LoadBlock(int b)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( IsLocked() );
	if(buf == NULL) 									// possible after block caching on disk
	{
		buf = (unsigned char*)alloc(buffer_size_in_bytes, BLOCK_TEMPORARY, true);
		if (!buf) {
			rclog << lock << "Could not allocate memory for IndexTable(LoadBlock)." << unlock;
			throw OutOfMemoryRCException();
		}
	}
	else if(block_changed)
		CI_Put(cur_block,buf);
	assert(buf != NULL);
	CI_Get(b,buf);
	if(m_conn.killed())			// from time to time...
		throw KilledRCException();
	max_block_used = max(max_block_used, b);
	cur_block = b;
	block_changed = false;
}

void IndexTable::ExpandTo(_int64 new_size)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( IsLocked() );
	if(new_size <= (_int64)size) 
		return;
	if(size * bytes_per_value == _int64(buffer_size_in_bytes)) {			// the whole table was in one buffer
		if(buffer_size_in_bytes < 32 * MBYTE && new_size * bytes_per_value > 32 * MBYTE) {
			max_buffer_size_in_bytes = int(TrackableObject::MaxBufferSize(-1));	// recalculate, as it might not be done earlier
			CI_SetDefaultSize((int)max_buffer_size_in_bytes);		// redefine disk block sized
			uint values_per_block = uint(max_buffer_size_in_bytes / bytes_per_value);
			block_shift = CalculateBinSize(values_per_block) - 1;	// e.g. BinSize(16)==5, but shift by 4. WARNING: should it be (val...-1)? Now it works, because v... is only 2^25, 2^24, 2^23
			block_mask = (_uint64(1)<<block_shift) - 1;
		}

		int new_buffer_size_in_bytes;
		if(new_size * bytes_per_value < (_int64)max_buffer_size_in_bytes)
			new_buffer_size_in_bytes = int(new_size * bytes_per_value);
		else
			new_buffer_size_in_bytes = max_buffer_size_in_bytes;
		//TODO: check the rc_alloc status
		buf = (unsigned char*)rc_realloc( buf, new_buffer_size_in_bytes, BLOCK_TEMPORARY );
		buffer_size_in_bytes = new_buffer_size_in_bytes;
	}
	// else: the table is buffered anyway, so we don't need to do anything
	size = new_size;
}

void IndexTable::SetByFilter(Filter *f)						// add all positions where filter is one
{
	FilterOnesIterator it(f);
	ExpandTo(f->NoOnes());
	_int64 loc_obj = 0;
	while(it.IsValid()) {
		Set64(loc_obj++, *it + 1);
		++it;
	}
}