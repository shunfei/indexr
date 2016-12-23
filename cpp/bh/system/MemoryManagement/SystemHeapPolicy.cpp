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

#include "SystemHeapPolicy.h"
#include "common/bhassert.h"
#include "system/RCSystem.h"
#include <stdlib.h>

using namespace std;


SystemHeap::~SystemHeap()
{
}

MEM_HANDLE_BH SystemHeap::alloc(size_t size)
{
	if(m_size > 0 && (m_allocsize + size > m_size))
		return NULL;
		
	MEM_HANDLE_BH res = malloc(size);
	m_blockSizes.insert( std::make_pair(res,size) );
	m_allocsize += size;
	return res;
}

void SystemHeap::dealloc(MEM_HANDLE_BH mh)
{
	m_allocsize -= getBlockSize(mh);
	m_blockSizes.erase(mh);
	free(mh);
}

MEM_HANDLE_BH SystemHeap::rc_realloc(MEM_HANDLE_BH mh, size_t size)
{
	m_allocsize -= getBlockSize(mh);
	m_blockSizes.erase(mh);
	MEM_HANDLE_BH res = realloc(mh,size);
	m_blockSizes.insert( std::make_pair(res,size) );
	m_allocsize += size;
	return res;
}

size_t SystemHeap::getBlockSize(MEM_HANDLE_BH mh)
{
	SizeMap::iterator it = m_blockSizes.find(mh);
	
	BHASSERT(it != m_blockSizes.end(), "Invalid block address");
	return it->second;
}



