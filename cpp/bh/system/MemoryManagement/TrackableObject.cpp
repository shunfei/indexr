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

#include <boost/lexical_cast.hpp>
#include "TrackableObject.h"
#include "system/RCSystem.h"
#include "core/RCAttrPack.h"
#include "common/bhassert.h"
#include "core/RCEngine.h"
#include "core/tools.h"
#include "edition/system/ResourceManager.h"
#include "system/MemoryManagement/ReleaseTracker.h"
#include "system/fet.h"

using namespace std;

//MemoryHandling<void*, BasicHeap<void*> >* TrackableObject::m_MemHandling = NULL;
MemoryHandling* TrackableObject::m_MemHandling = NULL;
size_t TrackableObject::globalFreeable = 0;
size_t TrackableObject::globalUnFreeable = 0;
IBMutex TrackableObject::globalMutex;


void* TrackableObject::alloc(size_t size, BLOCK_TYPE type, bool nothrow)
{
	void* addr = Instance()->alloc(size, type, this, nothrow);
	if( addr != NULL ) {
		size_t s = Instance()->rc_msize(addr, this);
		m_sizeAllocated += s;
		IBGuard global_guard(globalMutex);
		if(!m_Locked && TrackableType() == TO_PACK)
			globalFreeable += s;
		else
			globalUnFreeable += s;
	}
	return addr;
}

void TrackableObject::dealloc(void* ptr)
{
	size_t s;
	if( ptr == NULL ) return;
	s = Instance()->rc_msize(ptr, this);
	Instance()->dealloc(ptr, this);
	m_sizeAllocated -= s;
	IBGuard global_guard(globalMutex);
	if(!m_Locked && TrackableType() == TO_PACK)
		globalFreeable -= s;
	else
		globalUnFreeable -= s;
}

void* TrackableObject::rc_realloc(void* ptr, size_t size, BLOCK_TYPE type)
{
	if( ptr == NULL ) return alloc(size,type);

	size_t s1 = Instance()->rc_msize(ptr, this);
	void* addr = Instance()->rc_realloc(ptr, size, this, type);
	if( addr != NULL ) {
		size_t s = Instance()->rc_msize(addr, this);
		m_sizeAllocated += s;
		m_sizeAllocated -= s1;
		IBGuard global_guard(globalMutex);
		if(!m_Locked && TrackableType() == TO_PACK) {
			globalFreeable += s;
			globalFreeable -= s1;
		} else {
			globalUnFreeable += s;
			globalUnFreeable -= s1;
		}
	}
	return addr;
}

size_t TrackableObject::rc_msize(void *ptr)
{
	return Instance()->rc_msize(ptr, this);
}

TrackableObject::TrackableObject()
	: next(NULL), prev(NULL), tracker(NULL),
		m_Locked(true), m_Blocked(false), m_lock_count(1), m_preUnused(false), m_sizeAllocated(0), owner(NULL),
		m_locking_mutex(Instance()->GetMemoryReleaseMutex())
{

}
TrackableObject::TrackableObject(size_t comp_size, size_t uncomp_size, size_t cmp_rls, size_t uncmp_rls, string hugedir, DataCache* owner_,size_t hugesize)
	: next(NULL), prev(NULL), tracker(NULL),
	  m_Locked(true), m_Blocked(false), m_lock_count(1), m_preUnused(false), m_sizeAllocated(0), owner(owner_),
	  m_locking_mutex( Instance(comp_size, uncomp_size, cmp_rls, uncmp_rls, hugedir,owner_,hugesize)->GetMemoryReleaseMutex() )	   
{
}

TrackableObject::TrackableObject(const TrackableObject& to)
: next(NULL), prev(NULL), tracker(NULL), 
  m_Locked(true), m_Blocked(false), m_lock_count(1), m_preUnused(false), m_sizeAllocated(0), owner(NULL),
  m_locking_mutex( Instance()->GetMemoryReleaseMutex() ), _logical_coord(to._logical_coord)
{
}

TrackableObject::~TrackableObject()
{
	//Instance()->AssertNoLeak(this);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(m_sizeAllocated == 0/*, "TrackableObject size accounting"*/);
	if( IsTracked() ) StopAccessTracking();
}

void TrackableObject::Lock()
{
	MEASURE_FET("TrackableObject::Lock");
	if(TrackableType() == TO_PACK) {
		IBGuard locking_guard(m_locking_mutex);
		clearPrefetchUnused();
		if(!m_Locked) {
			IBGuard global_guard(globalMutex);
			globalFreeable -= m_sizeAllocated;
			globalUnFreeable += m_sizeAllocated;
		}
		m_lock_count++;
		m_Locked = true;

		//Locking is done externally
		if(!((AttrPack*) this)->IsEmpty() && IsCompressed())
			Uncompress();
	} else {
		IBGuard locking_guard(m_locking_mutex);
		m_lock_count++;

		m_Locked = true;
	}
	if(m_lock_count == 32766) {
		string message = (string("TrackableObject locked to many times. Object type: ") + boost::lexical_cast<string>((int)this->TrackableType()));
		rccontrol << lock << message << unlock;
		BHERROR(message.c_str());
	}
}

void TrackableObject::SetNoLocks(int n)
{
	if(TrackableType() == TO_PACK) {
		IBGuard locking_guard(m_locking_mutex);
		clearPrefetchUnused();
		if(!m_Locked) {
			IBGuard global_guard(globalMutex);
			globalFreeable -= m_sizeAllocated;
			globalUnFreeable += m_sizeAllocated;
		}
		m_lock_count = n;
		m_Locked = (n > 0);

		//Locking is done externally
		if(n > 0 && !((AttrPack*) this)->IsEmpty() && IsCompressed())
			Uncompress();
	} else {
		IBGuard locking_guard(m_locking_mutex);
		m_lock_count = n;
		m_Locked = (n > 0);
	}
	if(m_lock_count == 32766) {
		string message = (string("TrackableObject locked to many times. Object type: ") + boost::lexical_cast<string>((int)this->TrackableType()));
		rccontrol << lock << message << unlock;
		BHERROR(message.c_str());
	}
}

void TrackableObject::Unlock()
{
	MEASURE_FET("TrackableObject::UnLock");
	IBGuard locking_guard(m_locking_mutex);
	m_lock_count--;
	if(m_lock_count < 0)
	{
//		rclog << lock << "Internal error: An object unlocked too many times in memory manager." << unlock;
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Internal error: An object unlocked too many times in memory manager.");
		m_lock_count = 0;
	}
	m_lock_count ? m_Locked = true : m_Locked = false;
	if( !m_Locked && TrackableType() == TO_PACK ) {
		IBGuard global_guard(globalMutex);
		globalUnFreeable -= m_sizeAllocated;
		globalFreeable += m_sizeAllocated;
	} 
}

void TrackableObject::UnlockAndResetOrDeletePack(AttrPackPtr& pack)
{
	bool reset_outside = false;
	{
		IBGuard locking_guard(GetLockingMutex());
		if (pack.use_count()>1) {
			pack->Unlock();
			pack.reset();
		}
		else reset_outside = true;
	}
	if (reset_outside)
		pack.reset();
}

void TrackableObject::UnlockAndResetPack(AttrPackPtr& pack)
{
	IBGuard locking_guard(GetLockingMutex());
	pack->Unlock();
	pack.reset();
}

void TrackableObject::DestructionLock()
{
	IBGuard locking_guard(m_locking_mutex);
	if(!m_Locked && TrackableType() == TO_PACK) {
		IBGuard global_guard(globalMutex);
		globalUnFreeable += m_sizeAllocated;
		globalFreeable -= m_sizeAllocated;
	}
	m_lock_count++;
	m_Locked = true;
}

int TrackableObject::MemorySettingsScale()
{
	return rceng->getResourceManager()->GetMemoryScale();
}

void TrackableObject::deinitialize(bool detect_leaks)
{
	if(TrackableType() != TO_INITIALIZER)
	{
		BHERROR("TrackableType() not equals 'TO_INITIALIZER'");
		return;
	}

	if (m_MemHandling) //Used only in MemoryManagerInitializer!
	{
		// uncomment to print total mutex wait time on shutdown
		//IBMutex::MutexTime mt;
		//m_MemHandling->GetCriticalSection()->counting_mutex.GetTime(mt);
		//cout << "Time in MM mutex: " << mt.seconds << "s " << mt.miliseconds << "ms" << endl;
		if (detect_leaks) m_MemHandling->ReportLeaks();
		delete m_MemHandling;
	}
}
_int64 TrackableObject::MemScale2BufSizeSmall(int mem_scale) {
	_int64 max_total_size;
	if(mem_scale < 0)		// low settings with negative coefficient
		max_total_size =   32 * MBYTE;
	else if(mem_scale == 0)		// low settings (not more than 0.5 GB)
		max_total_size =   64 * MBYTE;
	else if(mem_scale <= 2)		// normal settings (up to 1.2 GB is reserved)
		max_total_size =  128 * MBYTE;
	else if(mem_scale == 3)		// high settings (when 1.2 - 2.5 GB is reserved in 64-bit version)
		max_total_size =  256 * MBYTE;
	else if(mem_scale == 4)		// high settings (when 2.5 - 5 GB is reserved in 64-bit version)
		max_total_size =  512 * MBYTE;
	else					// high settings >5 GB
		max_total_size = 1024 * MBYTE;
	return max_total_size;
}

_int64 TrackableObject::MemScale2BufSizeLarge(int ms) {
	assert(ms > 2);
	_int64 max_total_size = (ms - 2) * 512 * MBYTE;
	int sc16 = ms - 5;

	// additionally add progressively more mem for each additional 16GB of free mem :
	// +2GB for 16 Gb free, +3GB for 32Gb free, +6GB if 48GB free
	_int64 to_add = 2 * GBYTE;
	//single user:
	//	_int64 to_add = 8*1024*MBYTE;

	for(; sc16 > 0; sc16--) {
		max_total_size += to_add;
		if(to_add < 12 * GBYTE)
			to_add += GBYTE;
	}
	return max_total_size;
}

_int64 TrackableObject::MaxBufferSize(int coeff)		// how much bytes may be used for typical large buffers
{
	int mem_scale = MemorySettingsScale() + coeff;
	return MemScale2BufSizeSmall(mem_scale);
}

_int64 TrackableObject::MaxBufferSizeForAggr(int coeff)		// how much bytes may be used for buffers for aggregation
{
	int mem_scale = MemorySettingsScale() + coeff;
	if(mem_scale <= 2)
		return MemScale2BufSizeSmall(mem_scale);
	else
		return MemScale2BufSizeLarge(mem_scale);
}


TOCoordinate& TrackableObject::GetCoordinate() { return _logical_coord; }

