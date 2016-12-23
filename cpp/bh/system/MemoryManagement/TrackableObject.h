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

#ifndef _TRACKABLE_OBJECT_DD9E054B_5B06_4cc8_B404_D3C1C1AC934B
#define _TRACKABLE_OBJECT_DD9E054B_5B06_4cc8_B404_D3C1C1AC934B

#include <map>
#include "common/bhassert.h"
#include "core/tools.h"
#include "MemoryHandlingPolicy.h"
#include "fwd.h"

/*
	Classes that will use memory manager should inherit from this class
*/

#ifdef __GNUC__
enum TRACKABLEOBJECT_TYPE
{
	TO_PACK = 0,
	TO_SORTER,
	TO_CACHEDBUFFER,
	TO_FILTER,
	TO_MULTIFILTER2,
	TO_INDEXTABLE,
	TO_RSINDEX,
	TO_TEMPORARY,
	TO_FTREE,
	TO_SPLICE,
	TO_INITIALIZER,
	TO_DEPENDENT, // object is dependent from other one
	TO_REFERENCE, // Logical reference to an object on disk
	TO_DATABLOCK
};
#else
enum TRACKABLEOBJECT_TYPE:char
{
	TO_PACK,
	TO_SORTER,
	TO_CACHEDBUFFER,
	TO_FILTER,
	TO_MULTIFILTER2,
	TO_INDEXTABLE,
	TO_RSINDEX,
	TO_TEMPORARY,
	TO_FTREE,
	TO_SPLICE,
	TO_INITIALIZER,
	TO_DEPENDENT, // object is dependent from other one
	TO_REFERENCE, // Logical reference to an object on disk
	TO_DATABLOCK
};
#endif


//ooh!, class should be named Traceable not Trackable! To be changed later
class DataCache;
class TrackableAccounting;
class MemoryHandling;
class TOCoordinate;

struct DPN;
class ReleaseTracker;

class TrackableObject
{
    friend class DataCache;
    friend class GlobalDataCache;
	friend class TrackableAccounting;
	friend class ReleaseTracker;
	friend class ReleaseStrategy;
	
public:

	TrackableObject();
	TrackableObject(size_t, size_t, size_t, size_t, std::string = "", DataCache* = NULL, size_t = 0);
	TrackableObject(const TrackableObject& to);
	virtual ~TrackableObject();

	virtual void StayCompressed()				{BHERROR("method should not be invoked");}
	virtual TRACKABLEOBJECT_TYPE TrackableType() const = 0;
	virtual bool UpToDate()						{return false;}
	virtual int Collapse()						{BHERROR("method should not be invoked"); return 0;}

	virtual void Uncompress()					{BHERROR("method should not be invoked");} //Used in Locking mechanism
	virtual bool IsCompressed()				{BHERROR("method should not be invoked"); return 0;} //Used in Locking mechanism
	
	void Lock();
	void Unlock();
	static void UnlockAndResetOrDeletePack(AttrPackPtr& pack);
	static void UnlockAndResetPack(AttrPackPtr& pack);

	void CopyLockStatus(TrackableObject &sec)
	{
		m_Locked = sec.m_Locked;
		m_Blocked = sec.m_Blocked;
		m_lock_count = sec.m_lock_count;
	}

	static int MemorySettingsScale();					// Memory scale factors: 0 - <0.5GB, 1 - <1.2GB, 2 - <2.5GB, ..., 99 - higher than all these
	static _int64 MaxBufferSize(int coeff = 0);			// how much bytes may be used for typical large buffers
														// WARNING: potentially slow function (e.g. on Linux)
	static _int64 MaxBufferSizeForAggr(int coeff = 0);		// as above but can recommend large buffers for large memory machines

	//Lock the object during destruction to prevent garbage collection
	void DestructionLock();

	bool IsLocked() const	{ return m_Locked; }
	bool LastLock() const	{ return (m_lock_count==1); }

	/*
		Blocked is not equal to Locked!
		Blocking is used on a memory management level (its more internal locking)
		Locking is used by RCAttr etc. for packs manipulation purposes
	*/
	bool IsBlocked() const	{ return m_Blocked; }
	short NoLocks() const	{ return m_lock_count; }
	void SetNoLocks(int n);

	void SetBlocked(bool blocked)
	{
		m_Blocked = blocked;
	}

	static void CleanPacks()
	{
		//Instance()->GetCriticalSection()->EnterCountingCriticalSection();
		//Instance()->ReleaseUnlockedDataPacks();
		//Instance()->GetCriticalSection()->LeaveCountingCriticalSection();
	}

	void	SetOwner(DataCache* new_owner) { owner = new_owner; }
	DataCache* GetOwner() { return owner; }

	static size_t GetFreeableSize() { return globalFreeable; }
	static size_t GetUnFreeableSize() { return globalUnFreeable; }
	// DataPacks can be prefetched but not used yet
	// this is a hint to memory release algorithm
	bool IsPrefetchUnused() { return m_preUnused; }
	void setPrefetchUnused() { m_preUnused = true; }
	void clearPrefetchUnused() { m_preUnused = false; }

	static MemoryHandling* Instance()
	{
		if(!m_MemHandling)
		{
			//m_MemHandling = new MemoryHandling<void*, BasicHeap<void*> >(COMP_SIZE, UNCOMP_SIZE, COMPRESSED_HEAP_RELEASE, UNCOMPRESSED_HEAP_RELEASE);
			BHERROR("Memory manager is not instantiated");
			return NULL;
		}
		else
			return m_MemHandling;
	}

	void TrackAccess() { Instance()->TrackAccess(this); }
	void StopAccessTracking() { Instance()->StopAccessTracking(this); }
	bool IsTracked() { return tracker!=NULL; }
	virtual void Release() { BHERROR("Release functionality not implemented for this object"); }
	TOCoordinate& GetCoordinate();
	
protected:

	// For release tracking purposes, used by ReleaseTracker and ReleaseStrategy
	TrackableObject *next,*prev;
	ReleaseTracker *tracker;
	// -- ReleaseTracker
	
	void* alloc(size_t size, BLOCK_TYPE type, bool nothrow=false);
	void dealloc(void* ptr);
	void* rc_realloc(void* ptr, size_t size, BLOCK_TYPE type = BLOCK_FIXED);
	size_t rc_msize(void* ptr);

	void deinitialize(bool detect_leaks);

	static IBMutex& GetLockingMutex() { return Instance()->GetMemoryReleaseMutex(); }

	
	//static MemoryHandling<void*, BasicHeap<void*> >* m_MemHandling;
	static MemoryHandling* m_MemHandling;

	static MemoryHandling* Instance(size_t comp_size, size_t uncomp_size, size_t cmp_rls, size_t uncmp_rls, std::string hugedir = "", DataCache *d = NULL, size_t hugesize = 0)
	{
		if(!m_MemHandling)
			m_MemHandling = new MemoryHandling(comp_size, uncomp_size, cmp_rls, uncmp_rls, hugedir, d, hugesize);
		return m_MemHandling;
	}

	bool m_Locked;
	bool m_Blocked;	//Used to block memory blocks when due to compression process compressed_heap must be released
	short m_lock_count;
	bool m_preUnused;
	
	static size_t globalFreeable, globalUnFreeable;
	static IBMutex globalMutex;
	
	size_t m_sizeAllocated;
	
	DataCache* owner;

    IBMutex& m_locking_mutex;
	TOCoordinate _logical_coord;

private:
	static _int64 MemScale2BufSizeLarge(int ms);
	static _int64 MemScale2BufSizeSmall(int ms);
};

#endif

