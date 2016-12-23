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

#include "BufferBlock.h"
#include <string.h>

BufferBlockBase::BufferBlockBase(TRACKABLEOBJECT_TYPE t, CacheableItem *p, unsigned int _block, 
						unsigned int _size, unsigned int _esize)
{
	parent=p;
	block=_block;
	modified=false;
	stored=false;
	block_size=_size;
	loaded=true;
	otype=t;
	elem_size=_esize;
	buf = alloc((size_t)block_size, BLOCK_TEMPORARY);
	_logical_coord.ID=bh::COORD_TYPE::BUFFER_BLOCK;
	//_logical_coord.co.block[0] = p->GetUID();
	_logical_coord.co.block[1] = _block;
}


BufferBlockBase::BufferBlockBase(BufferBlockBase& org, CacheableItem *p)
{
	parent=p;
	block=org.block;
	block_size=org.block_size;
	loaded=org.loaded;
	modified=org.modified;
	stored=org.stored;
	otype=org.otype;
	elem_size=org.elem_size;
	if( org.buf != NULL) {
		buf = alloc((size_t)block_size, BLOCK_TEMPORARY);
		memcpy(buf,org.buf,block_size);	
	} else
		buf = NULL;
	_logical_coord.ID=bh::COORD_TYPE::BUFFER_BLOCK;
	//_logical_coord.co.block[0] = p->GetUID();
	_logical_coord.co.block[1] = org._logical_coord.co.block[1];
}

BufferBlockBase::~BufferBlockBase() 
{ 
	StopAccessTracking(); 
	if(buf) dealloc(buf); 
}

void BufferBlockBase::Initialize(int value)
{
	memset(buf,value,block_size);
}

void BufferBlockBase::Release() 
{
#if 0
	BHASSERT(parent != NULL,"Buffer not releasable");
	if(modified && buf != NULL) {
		parent->CI_Put(block,(unsigned char *)buf,block_size);
		stored=true;
	}
	if(buf) dealloc(buf);
		
	buf = NULL;
	loaded=false;
	modified=false;
#endif
}

void BufferBlockBase::Load()
{
#if 0
	BHASSERT(parent != NULL,"Buffer not loadable");

	if( !stored  || loaded ) {
		loaded=true;
		return;
	}
	
	if(buf!=NULL) dealloc(buf);
	buf = alloc((size_t)block_size, BLOCK_TEMPORARY);
	parent->CI_Get(block,(unsigned char *)buf,block_size);
	loaded=true;
#endif
}


template class BufferBlock<short>;
template class BufferBlock<int>;
template class BufferBlock<_int64>;
template class BufferBlock<_uint64>;
template class BufferBlock<char>;
template class BufferBlock<double>;
template class BufferBlock<RCBString>;
template class BufferBlock<char*>;
