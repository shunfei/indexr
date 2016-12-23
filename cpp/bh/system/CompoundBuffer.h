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

#ifndef COMPOUNDBUFFER_H
#define COMPOUNDBUFFER_H

#include <vector>
#include "../system/MemoryManagement/TrackableObject.h"
#include "system/BufferBlock.h"

// Fixed size buffer composed of smaller blocks, sizes in number of elements
// Build for low level efficiency, do safety checking at a higher level
template<class T>
class CompoundBuffer 
{
public:
	CompoundBuffer(TRACKABLEOBJECT_TYPE t, uint size, uint elem_size=sizeof(T));	
	CompoundBuffer(CompoundBuffer<T>&);
	virtual ~CompoundBuffer(void);

	inline T& operator[](_uint64 idx) { return Get(idx); }
	inline T& Get(_uint64 idx)
	{
		return block[idx>>page_shift]->Get(idx & (inblock_mask));
	}

	inline void	Set(_uint64 idx, T& value)
	{
		block[idx>>page_shift]->Set(idx & (inblock_mask), value);
	}

	inline void * GetPtr(_uint64 idx)
	{
		return block[idx>>page_shift]->GetPtr(idx & (inblock_mask));
	}

	void Initialize(int);
	
protected:
	uint			num_elements;
	uint			page_size;		    // size of each buffer stored in memory
	uint			page_shift;
	uint			inblock_mask;
	uint			elem_size;		    // size of one element in BYTES; for most types computed by sizeof()
	uint			elem_size_stored;   // internally elements are stored on 32-bit aligned boundaries if elem_size > 4
	std::vector<BufferBlock<T>*>	 block;
	TRACKABLEOBJECT_TYPE otype;
};

class FixedWidthCompoundBuffer: public CompoundBuffer<char *>
{
public:
	FixedWidthCompoundBuffer(TRACKABLEOBJECT_TYPE t, uint size, uint _elem_size)	
	: CompoundBuffer<char *>(t, size, _elem_size)
	{}
	~FixedWidthCompoundBuffer() {}
	
	inline char* operator[](_uint64 idx) { return Get(idx); }
	inline char* Get(_uint64 idx)
	{
		return (char *)GetPtr(idx);
	}

	inline void	Set(_uint64 idx, char *value)
	{
		memcpy(GetPtr(idx), value, elem_size);
	}
};

#endif  // COMPOUNDBUFFER_H
