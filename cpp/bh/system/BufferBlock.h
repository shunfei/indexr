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

#ifndef BUFFERBLOCK_H_
#define BUFFERBLOCK_H_

#include "common/bhassert.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "system/CacheableItem.h"
#include "types/RCDataTypes.h"

class BufferBlockBase : public TrackableObject
{
protected:
	CacheableItem *parent;
	TRACKABLEOBJECT_TYPE otype;
	unsigned int block,block_size,elem_size;
	void* buf;
	bool loaded,modified,stored;

public:
	BufferBlockBase(TRACKABLEOBJECT_TYPE t, CacheableItem *p, unsigned int block, 
				unsigned int size=0, unsigned int elem_size=0);
	BufferBlockBase(BufferBlockBase&, CacheableItem *);
	virtual ~BufferBlockBase(); 

	void Load();
	void Initialize(int value);
	inline void SetModified() { modified = true; }
	
	inline void * GetPtr(unsigned int i) {
		return (void *)((char*)buf + i*elem_size);
	}
	
	void Release();
	TRACKABLEOBJECT_TYPE TrackableType() const { return otype; }

};

template <class T>
class BufferBlock : public BufferBlockBase
{
	// need to return a reference to non temp object sometimes
	// need to revisit this
	T tmp;
public:
	BufferBlock(TRACKABLEOBJECT_TYPE t, CacheableItem *p, unsigned int block, 
				unsigned int size=0, unsigned int es=sizeof(T))
		: BufferBlockBase(t,p,block,size,es) {}
	BufferBlock(BufferBlock<T>& o, CacheableItem *p)
		: BufferBlockBase(o,p) { tmp=o.tmp; }
	virtual ~BufferBlock() {}

	// Single threaded query assumption: if locked then current thread is only thing that can unlock/load
	inline T& Get (unsigned int i) { 
		return ((T*)buf)[i];
	}
	
	inline void Set(unsigned int id, T& val) { 
		((T*)buf)[id] = val; 
		modified = true;
	}
};

// fixed width (but unspecified at compile time) elements
class FixedWidthBufferBlock : public BufferBlock<char *>
{
public:
	FixedWidthBufferBlock(TRACKABLEOBJECT_TYPE t, CacheableItem *p, unsigned int block, 
				unsigned int size=0, unsigned int es=0)
		: BufferBlock<char*>(t,p,block,size,es) 
		{}
		
	FixedWidthBufferBlock(FixedWidthBufferBlock &o, CacheableItem *p)
		: BufferBlock<char*>(o,p) {}

	inline char* Get(unsigned int i) 
	{ 
		return ((char*)buf) + (i*elem_size); 
	}

	inline void Set(unsigned int id, char* val)
	{
		memcpy(((char*)buf) + (id*elem_size), val, elem_size);
		modified=true;
	}

};

template<>
inline RCBString& BufferBlock<RCBString>::Get (unsigned int i) { 		
	unsigned int* size = (unsigned int *)((char*)buf + (i*elem_size));
	if(*size == NULL_VALUE_U) // 0 -NULL, 1 -NOT NULL
		tmp = RCBString();
	else {
		// Creates unnecessary temporaries???
		tmp = RCBString( (char *)buf + (i*elem_size), -2);
	}
	return tmp;
}
	
template<>
inline void BufferBlock<RCBString>::Set (unsigned int id, RCBString& value) { 		
	char *elem = (char *)buf + (id*elem_size);
	unsigned int *size = (unsigned int*)elem;
	elem += 4;
	if(value.IsNull())
		*size = NULL_VALUE_U;
	else {
		*size = value.len;
		memcpy(elem, value.val, value.len);
	}
	modified=true;
}


#endif /*BUFFERBLOCK_H_*/
