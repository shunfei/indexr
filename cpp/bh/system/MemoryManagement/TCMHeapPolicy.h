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

#ifndef TCMHEAPPOLICY_H
#define TCMHEAPPOLICY_H

#include <map>
#include <string>

#include "MemoryBlock.h"
#include "common.h"
#include "tcm/page_heap.h"
#include "tcm/page_heap_allocator.h"
#include "HeapPolicy.h"
#include "system/ib_system.h"
#include "tcm/tccommon.h"
#include "tcm/linked_list.h"

class TCMHeap : public HeapPolicy
{
protected:	
	// All TCMHeaps share access to a single page allocator
	static IBMutex _mutex;
	
	ib_tcmalloc::PageHeap m_heap;
	ib_tcmalloc::PageHeapAllocator<ib_tcmalloc::Span> m_span_allocator;
	ib_tcmalloc::SizeMap m_sizemap;

	// from tcmalloc::ThreadCache
  class FreeList {
   private:
    void*    list_;       // Linked list of nodes
    uint32_t length_;      // Current length.
    uint32_t lowater_;     // Low water mark for list length.
    uint32_t max_length_;  // Dynamic max list length based on usage.
    // Tracks the number of times a deallocation has caused
    // length_ > max_length_.  After the kMaxOverages'th time, max_length_
    // shrinks and length_overages_ is reset to zero.
    uint32_t length_overages_;

   public:
    void Init() {
      list_ = NULL;
      length_ = 0;
      lowater_ = 0;
      max_length_ = 1;
      length_overages_ = 0;
    }

    // Return current length of list
    size_t length() const {
      return length_;
    }

    // Return the maximum length of the list.
    size_t max_length() const {
      return max_length_;
    }

    // Set the maximum length of the list.  If 'new_max' > length(), the
    // client is responsible for removing objects from the list.
    void set_max_length(size_t new_max) {
      max_length_ = (uint32_t)new_max;
    }

    // Return the number of times that length() has gone over max_length().
    size_t length_overages() const {
      return length_overages_;
    }

    void set_length_overages(size_t new_count) {
      length_overages_ = (uint32_t)new_count;
    }

    // Is list empty?
    bool empty() const {
      return list_ == NULL;
    }

    // Low-water mark management
    int lowwatermark() const { return lowater_; }
    void clear_lowwatermark() { lowater_ = length_; }

    void Push(void* ptr) {
      ib_tcmalloc::SLL_Push(&list_, ptr);
      length_++;
    }

    void* Pop() {
      ASSERT(list_ != NULL);
      length_--;
      if (length_ < lowater_) lowater_ = length_;
      return ib_tcmalloc::SLL_Pop(&list_);
    }

    void PushRange(int N, void *start, void *end) {
      ib_tcmalloc::SLL_PushRange(&list_, start, end);
      length_ += N;
    }

    void PopRange(int N, void **start, void **end) {
      ib_tcmalloc::SLL_PopRange(&list_, N, start, end);
      ASSERT(length_ >= (uint32_t)N);
      length_ -= N;
      if (length_ < lowater_) lowater_ = length_;
    }
	
	void RemoveRange(void *low, void *hi) {
		void *prev,*current;
		while( list_ <= hi && list_ >= low ) {
			list_ = ib_tcmalloc::SLL_Next(list_);
		}
		current = list_;
		while( current != NULL) {
			prev = current;
			current = ib_tcmalloc::SLL_Next(current);
			while( current <= hi && current >= low ) {
				ib_tcmalloc::SLL_SetNext(prev,ib_tcmalloc::SLL_Next(current));
				current = ib_tcmalloc::SLL_Next(current);
			}
		}
	}
  };

  FreeList      m_freelist[kNumClasses];     // Array indexed by size-class


public:
	TCMHeap(size_t hsize);
	virtual ~TCMHeap();

	/*
		allocate memory block of size [size] and for data of type [type]
		type != BLOCK_FREE
	*/
	MEM_HANDLE_MP	alloc(	size_t size );
	void		dealloc(MEM_HANDLE_MP mh);
	MEM_HANDLE_MP	rc_realloc(MEM_HANDLE_MP mh, size_t size);
	size_t		getBlockSize(MEM_HANDLE_MP mh);

};



#endif

