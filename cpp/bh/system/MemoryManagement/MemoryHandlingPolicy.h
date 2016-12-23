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

#ifndef _MEMORY_HANDLING_POLICY
#define _MEMORY_HANDLING_POLICY

#include <map>
#include <iostream>

#ifdef __GNUC__
#include <ext/hash_map>
#include <ext/hash_set>
#include "system/linux/hash_set_ext.h"
#ifndef stdext
#define stdext __gnu_cxx
#endif
#else
#include <hash_map>
#include <hash_set>
#endif

#include "MemoryBlock.h"
#include "common/bhassert.h"
#include "system/ib_system.h"
#include "HeapPolicy.h"
#include "common.h"

class TrackableObject;


#ifdef __GNUC__
namespace __gnu_cxx
{

template<>
  struct hash<TrackableObject*>
  {
    size_t
    operator()(const TrackableObject* __x) const
    { return hash<size_t>()( (size_t) __x); }
  };

}
#endif

class DataCache;
class ReleaseStrategy;
// Systemwide memory responsibilities
//  -- alloc/dealloc on any heap by delegating
//  -- track objects
class MemoryHandling
{
	friend class TrackableObject;

	typedef stdext::hash_map<MEM_HANDLE_MP,HeapPolicy*> PtrHeapMap;
	typedef stdext::hash_map<TrackableObject*, PtrHeapMap* > OwnerMemoryMap;
	OwnerMemoryMap m_objs;


	HeapPolicy *m_main_heap,
		*m_huge_heap,
		*m_system,
		*m_large_temp;
	int main_heap_MB,comp_heap_MB;
	bool m_hard_limit;
	IBMutex m_mutex;
	IBMutex m_release_mutex;
	
	// status counters
	unsigned long m_alloc_blocks,m_alloc_objs,m_alloc_size,m_alloc_pack,
		m_alloc_temp,m_free_blocks,m_alloc_temp_size,m_alloc_pack_size,
		m_free_pack,m_free_temp, m_free_pack_size,m_free_temp_size,m_free_size;

	ReleaseStrategy *_releasePolicy;
	int m_release_count;
	
	IBMutex& GetMemoryReleaseMutex() { return m_release_mutex; }

public:

	MemoryHandling(size_t comp_heap_size, size_t uncomp_heap_size, size_t cmp_rls, size_t uncomp_rls, std::string hugedir = "", DataCache *d = NULL, size_t hugesize = 0);
	virtual ~MemoryHandling(void);

	MEM_HANDLE_MP	alloc(	size_t size, BLOCK_TYPE type, TrackableObject* owner,bool nothrow=false);
	void		dealloc(MEM_HANDLE_MP mh, TrackableObject* owner);
	MEM_HANDLE_MP	rc_realloc(MEM_HANDLE_MP mh, size_t size, TrackableObject* owner, BLOCK_TYPE type);
	size_t		rc_msize(MEM_HANDLE_MP mh, TrackableObject* owner); //returns size of a memory block represented by [mh]

	void TrackAccess(TrackableObject *);
	void StopAccessTracking(TrackableObject *);

	void AssertNoLeak(TrackableObject*);
#ifdef __GNUC__
	enum RELEASE_HEAP
	{
		RH_UNCOMPRESSED,
		RH_COMPRESSED
	};
#else
	enum RELEASE_HEAP:char
	{
		RH_UNCOMPRESSED,
		RH_COMPRESSED
	};
#endif
	bool ReleaseMemory(size_t, RELEASE_HEAP, TrackableObject* untouchable); //Release given amount of memory

	void ReportLeaks();
	void EnsureNoLeakedTrackableObject();
	void CleanPacks();
	void CompressPacks();


	unsigned long getAllocBlocks() { return m_alloc_blocks; }
	unsigned long getAllocObjs() { return m_alloc_objs; }
	unsigned long getAllocSize() { return m_alloc_size; }
	unsigned long getAllocPack() { return m_alloc_pack; }
	unsigned long getAllocTemp() { return m_alloc_temp; }
	unsigned long getFreeBlocks() { return m_free_blocks; }
	unsigned long getAllocTempSize() { return m_alloc_temp_size; }
	unsigned long getAllocPackSize() { return m_alloc_pack_size; }
	unsigned long getFreePacks() { return m_free_pack; }
	unsigned long getFreeTemp() { return m_free_temp; }
	unsigned long getFreePackSize() { return m_free_pack_size; }
	unsigned long getFreeTempSize() { return m_free_temp_size; }
	unsigned long getFreeSize() { return m_free_size; }
	unsigned long long getReleaseCount1(); 
	unsigned long long getReleaseCount2();
	unsigned long long getReleaseCount3();
	unsigned long long getReleaseCount4();
	unsigned long long getReloaded();

	void HeapHistogram(std::ostream &);

};

#endif
