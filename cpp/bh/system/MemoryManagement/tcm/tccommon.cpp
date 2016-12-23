// Copyright (c) 2008, Google Inc.
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---
// Author: Sanjay Ghemawat <opensource@google.com>

//#include "config.h"
#include "tccommon.h"

#define CRASH
#define CHECK_CONDITION
#define PRIuS

namespace ib_tcmalloc {

// Note: the following only works for "n"s that fit in 32-bits, but
// that is fine since we only use it for small sizes.
static inline int LgFloor(size_t n) {
  int log = 0;
  for (int i = 4; i >= 0; --i) {
    int shift = (1 << i);
    size_t x = n >> shift;
    if (x != 0) {
      n = x;
      log += shift;
    }
  }
  ASSERT(n == 1);
  return log;
}

int SizeMap::NumMoveSize(size_t size) {
  if (size == 0) return 0;
  // Use approx 64k transfers between thread and central caches.
  int num = static_cast<int>(64.0 * 1024.0 / size);
  if (num < 2) num = 2;

  // Avoid bringing too many objects into small object free lists.
  // If this value is too large:
  // - We waste memory with extra objects sitting in the thread caches.
  // - The central freelist holds its lock for too long while
  //   building a linked list of objects, slowing down the allocations
  //   of other threads.
  // If this value is too small:
  // - We go to the central freelist too often and we have to acquire
  //   its lock each time.
  // This value strikes a balance between the constraints above.
  if (num > 32) num = 32;

  return num;
}

// Initialize the mapping arrays
void SizeMap::Init() {
  // Do some sanity checking on add_amount[]/shift_amount[]/class_array[]
  if (ClassIndex(0) < 0) {
    CRASH("Invalid class index %d for size 0\n", ClassIndex(0));
  }
  if (ClassIndex(kMaxSize) >= sizeof(class_array_)) {
    CRASH("Invalid class index %d for kMaxSize\n", ClassIndex(kMaxSize));
  }

  // Compute the size classes we want to use
  int sc = 1;   // Next size class to assign
  size_t alignment = kAlignment;
  CHECK_CONDITION(kAlignment <= 16);
  int last_lg = -1;
  for (size_t size = kAlignment; size <= kMaxSize; size += alignment) {
    int lg = LgFloor(size);
    if (lg > last_lg) {
      // Increase alignment every so often to reduce number of size classes.
      if (size >= 2048) {
        // Cap alignment at 256 for large sizes
        alignment = 256;
      } else if (size >= 128) {
        // Space wasted due to alignment is at most 1/8, i.e., 12.5%.
        alignment = size / 8;
      } else if (size >= 16) {
        // We need an alignment of at least 16 bytes to satisfy
        // requirements for some SSE types.
        alignment = 16;
      }
      CHECK_CONDITION(size < 16 || alignment >= 16);
      CHECK_CONDITION((alignment & (alignment - 1)) == 0);
      last_lg = lg;
    }
    CHECK_CONDITION((size % alignment) == 0);

    // Allocate enough pages so leftover is less than 1/8 of total.
    // This bounds wasted space to at most 12.5%.
    size_t psize = kPageSize;
    while ((psize % size) > (psize >> 3)) {
      psize += kPageSize;
    }
    const size_t my_pages = psize >> kPageShift;

    if (sc > 1 && my_pages == class_to_pages_[sc-1]) {
      // See if we can merge this into the previous class without
      // increasing the fragmentation of the previous class.
      const size_t my_objects = (my_pages << kPageShift) / size;
      const size_t prev_objects = (class_to_pages_[sc-1] << kPageShift)
                                  / class_to_size_[sc-1];
      if (my_objects == prev_objects) {
        // Adjust last class to include this size
        class_to_size_[sc-1] = size;
        continue;
      }
    }

    // Add new class
    class_to_pages_[sc] = my_pages;
    class_to_size_[sc] = size;
    sc++;
  }
  if (sc != kNumClasses) {
    CRASH("wrong number of size classes: found %d instead of %d\n",
          sc, int(kNumClasses));
  }

  // Initialize the mapping arrays
  int next_size = 0;
  for (int c = 1; c < kNumClasses; c++) {
    const int max_size_in_class = (const int)class_to_size_[c];
    for (int s = next_size; s <= max_size_in_class; s += kAlignment) {
      class_array_[ClassIndex(s)] = c;
    }
    next_size = max_size_in_class + kAlignment;
  }

  // Double-check sizes just to be safe
  for (size_t size = 0; size <= kMaxSize; size++) {
    const int sc = SizeClass((int)size);
    if (sc <= 0 || sc >= kNumClasses) {
      CRASH("Bad size class %d for %" PRIuS "\n", sc, size);
    }
    if (sc > 1 && size <= class_to_size_[sc-1]) {
      CRASH("Allocating unnecessarily large class %d for %" PRIuS
            "\n", sc, size);
    }
    const size_t s = class_to_size_[sc];
    if (size > s) {
      CRASH("Bad size %" PRIuS " for %" PRIuS " (sc = %d)\n", s, size, sc);
    }
    if (s == 0) {
      CRASH("Bad size %" PRIuS " for %" PRIuS " (sc = %d)\n", s, size, sc);
    }
  }

  // Initialize the num_objects_to_move array.
  for (size_t cl = 1; cl  < kNumClasses; ++cl) {
    num_objects_to_move_[cl] = NumMoveSize(ByteSizeForClass(cl));
  }
}

// Metadata allocator -- keeps stats about how many bytes allocated.
static uint64_t metadata_system_bytes_ = 0;

void* MetaDataAlloc(size_t bytes) {
  void* result = TCMalloc_SystemAlloc(bytes, NULL,kPageSize);
  if (result != NULL) {
    metadata_system_bytes_ += bytes;
  }
  return result;
}

#if !defined(__GNUC__)
// Windows only
int getpagesize() {
    SYSTEM_INFO system_info;
    GetSystemInfo(&system_info);
    return system_info.dwPageSize;
}

#endif

// -----------------------------------------------------------------------
// These functions replace system-alloc.cc

// This is mostly like MmapSysAllocator::Alloc, except it does these weird
// munmap's in the middle of the page, which is forbidden in windows.
void* TCMalloc_SystemAlloc(size_t size, size_t *actual_size,
                                  size_t alignment) {
#if defined(__GNUC__) && !defined(__sun__)
	void* ptr( NULL );
    if (actual_size) {
      *actual_size = size;
    }
	
	int err( posix_memalign(&ptr,alignment,size) );
	assert( err == 0 );
	return ptr;
#elif defined(__sun__)
    if (actual_size) {
      *actual_size = size;
    }

	return memalign(alignment,size);
#else	// Windows						  
  // Safest is to make actual_size same as input-size.
  if (actual_size) {
    *actual_size = size;
  }

  // Align on the pagesize boundary
  const int pagesize = getpagesize();
  if (alignment < pagesize) alignment = pagesize;
  size = ((size + alignment - 1) / alignment) * alignment;
  // Ask for extra memory if alignment > pagesize
  size_t extra = 0;
  if (alignment > pagesize) {
    extra = alignment - pagesize;
  }

  void* result = VirtualAlloc(0, size + extra,
                              MEM_COMMIT|MEM_RESERVE, PAGE_READWRITE);
  if (result == NULL)
    return NULL;

  // Adjust the return memory so it is aligned
  uintptr_t ptr = reinterpret_cast<uintptr_t>(result);
  size_t adjust = 0;
  if ((ptr & (alignment - 1)) != 0) {
    adjust = alignment - (ptr & (alignment - 1));
  }

  ptr += adjust;

  return reinterpret_cast<void*>(ptr);
#endif
}

}  // namespace tcmalloc
