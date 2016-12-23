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

#include "TCMHeapPolicy.h"

//#include "system/RCSystem.h"
#include "core/tools.h"
#include "common/bhassert.h"
#include "tcm/span.h"
#include <algorithm>

using namespace std;
using namespace ib_tcmalloc;

IBMutex TCMHeap::_mutex;

TCMHeap::TCMHeap(size_t heap_size) : HeapPolicy(heap_size)
{
	if(heap_size > 0 ) 
		m_heap.GrowHeap((heap_size)>>kPageShift);		
	m_sizemap.Init();
	for(int i=0; i<kNumClasses; i++)
		m_freelist[i].Init();
}

TCMHeap::~TCMHeap(void)
{
}

MEM_HANDLE_MP TCMHeap::alloc(size_t size)
{
    //IBGuard guard(_mutex);
	void *res;

	// make size a multiple of 16
	//size = size + 0xf & (~0xf);
	
	if( size > kMaxSize ) {
		int pages = int(size >> kPageShift);
		Span *s = m_heap.New(pages+1);
		if( s == NULL ) return NULL;
		s->objects = NULL;
		s->next = NULL;
		s->prev = NULL;
		s->refcount = 1;
		s->size = uint(size);
		s->sizeclass = 0;
		m_heap.RegisterSizeClass(s,0);
		return reinterpret_cast<void*>(s->start << kPageShift);
	}
	const size_t cl = size_t(m_sizemap.SizeClass((int)size));
	const size_t alloc_size = m_sizemap.ByteSizeForClass(cl);
	FreeList* list = &m_freelist[cl];
	if (list->empty()) {
		// Get a new span of pages (potentially large enough for multiple 
		// allocations of this size)
		int pages = int(m_sizemap.class_to_pages(cl));
		Span *res = m_heap.New(pages);
		if( res == NULL ) return NULL;

		m_heap.RegisterSizeClass(res,cl);
		for(int i=0;i<pages;i++)
			m_heap.CacheSizeClass(res->start + i, cl);

		// from CentralFreeList::Populate
		// initialize the span of pages into a "list" of smaller objects
		void** tail = &res->objects;
		char* ptr = reinterpret_cast<char*>(res->start << kPageShift);
		char* limit = ptr + (pages << kPageShift);
		int num = 0;
		while (ptr + alloc_size <= limit) {
			*tail = ptr;
			tail = reinterpret_cast<void**>(ptr);
			ptr += alloc_size;
			num++;
		}
		ASSERT(ptr <= limit);
		*tail = NULL;
		res->refcount = 0;
		list->PushRange(num,(void*)(res->start << kPageShift),tail);
	}
	if( list->empty() ) return NULL;

	res = list->Pop();
	Span *s = m_heap.GetDescriptor(ADDR_TO_PAGEID(res));
	s->refcount++;
	return res;
}

size_t		
TCMHeap::getBlockSize(MEM_HANDLE_MP mh)
{
	size_t result;
    //IBGuard guard(_mutex);
	Span *span = m_heap.GetDescriptor(ADDR_TO_PAGEID(mh));
	ASSERT(span != NULL);
	if(span->sizeclass == 0)
		result = span->size;	
	else
		result = m_sizemap.ByteSizeForClass(span->sizeclass);
		
	BHASSERT(result != 0, "Block size error");
	return result;
}

void TCMHeap::dealloc(MEM_HANDLE_MP mh)
{
    //IBGuard guard(_mutex);
	
	// be an enabler for broken code
	if(mh == NULL) return;
	
	Span *span = m_heap.GetDescriptor(ADDR_TO_PAGEID(mh));
	ASSERT( span != NULL );
	span->refcount--;
	if(span->sizeclass == 0) {
		m_heap.Delete(span);		
	} else {
		int sclass = span->sizeclass;
		if(span->refcount == 0) {
			m_freelist[sclass].RemoveRange(reinterpret_cast<void*>(span->start << kPageShift),
				reinterpret_cast<void*>((span->start << kPageShift) + (span->length*kPageSize) - 1));
			m_heap.Delete(span);
		} else {
			m_freelist[sclass].Push(mh);			
		}
	}
}

MEM_HANDLE_MP TCMHeap::rc_realloc(MEM_HANDLE_MP mh, size_t size)
{
    //IBGuard guard(_mutex);
	MEM_HANDLE_MP res = alloc(size);

    if( mh == NULL ) return res;
	Span *span = m_heap.GetDescriptor(ADDR_TO_PAGEID(mh));
	if( span->sizeclass == 0) {
		memcpy(res,mh,std::min(span->length * kPageSize,size));			
		dealloc(mh);
	} else {
		memcpy(res,mh,std::min(m_sizemap.ByteSizeForClass(span->sizeclass),size));		
		dealloc(mh);
	}

	return res;
}

