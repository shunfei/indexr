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

#ifndef HEAPPOLICY_H
#define HEAPPOLICY_H

#include "MemoryBlock.h"
#include "common.h"

class HeapPolicy
{
public:
	HeapPolicy(size_t s) : m_size(s), m_hs(HEAP_ERROR) {}
	virtual ~HeapPolicy() {}
	virtual MEM_HANDLE_MP	alloc(	size_t size ) = 0;
	virtual void		dealloc(MEM_HANDLE_MP mh) = 0;
	virtual MEM_HANDLE_MP	rc_realloc(MEM_HANDLE_MP mh, size_t size) = 0;
	virtual size_t		getBlockSize(MEM_HANDLE_MP mh) = 0;

	HEAP_STATUS getHeapStatus(){return m_hs;}

protected:
	size_t m_size;
	HEAP_STATUS m_hs;
};



#endif

