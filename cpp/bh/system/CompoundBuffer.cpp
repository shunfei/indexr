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

#include "CompoundBuffer.h"

template <class T>
CompoundBuffer<T>::CompoundBuffer(TRACKABLEOBJECT_TYPE t, uint ne, uint _elem_size)
: num_elements(ne), elem_size(_elem_size), otype(t)
{
	page_shift = 0;
	if( elem_size == 0 )
		elem_size = sizeof(T);

	elem_size_stored = elem_size + 3 & (~3);
	_uint32 max_elements = (4*MBYTE)/elem_size;
	_uint32 block_elements = 1;
	// round max_elements down to nearest power of two into block_elements
	while(max_elements >= (block_elements<<1)) {
		page_shift++;
		block_elements <<= 1;
	}
	page_size = elem_size * block_elements;
	inblock_mask = (1<<page_shift)-1;
	block.resize(num_elements/block_elements + 1);
	for( int i=0; i<block.size(); i++ ) {
		block[i] = new BufferBlock<T>(otype, NULL, i, page_size,elem_size);
		block[i]->Initialize(0);
	}
}


template<class T>
CompoundBuffer<T>::CompoundBuffer(CompoundBuffer& org) 
{
	BufferBlock<T> *buf;
	page_shift = org.page_shift;
	inblock_mask = org.inblock_mask;
	elem_size = org.elem_size;
	page_size = org.page_size;
	num_elements = org.num_elements;
	elem_size_stored = org.elem_size_stored;
	block.resize(org.block.size(), NULL);

	for(int i = 0; i < org.block.size(); i++) {
		buf = org.block[i];
		if( buf != NULL ) {
			block[i] = new BufferBlock<T>(*buf,NULL);
		}
	}	
}


template <class T>
CompoundBuffer<T>::~CompoundBuffer(void)
{
	for(typename std::vector<BufferBlock<T>*>::iterator elem = block.begin();
		elem != block.end();
		elem++)
		if( *elem != NULL )
			delete *elem;
}

template <class T> void 
CompoundBuffer<T>::Initialize(int v)
{
	for(typename std::vector<BufferBlock<T>*>::iterator elem = block.begin();
		elem != block.end();
		elem++)
		if( *elem != NULL )
			(*elem)->Initialize(v);	
}

template class CompoundBuffer<char *>;




