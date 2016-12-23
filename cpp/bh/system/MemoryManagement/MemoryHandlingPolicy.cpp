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

#include <map>
#include <algorithm>
//#include "system/linux/hash_set_ext.h"

#include "MemoryHandlingPolicy.h"
#include "TrackableObject.h"
#include "core/RCAttrPack.h"
#include "core/tools.h"
#include "system/RCSystem.h"
#include "system/RCException.h"
#include "common/bhassert.h"
#include "edition/core/GlobalDataCache.h"
#include "tcm/page_heap.h"
#include "MySQLHeapPolicy.h"
#include "TCMHeapPolicy.h"
#include "NUMAHeapPolicy.h"
#include "HugeHeapPolicy.h"
#include "SystemHeapPolicy.h"
#include "system/fet.h"
#include "system/MemoryManagement/ReleaseAll.h"
#include "system/MemoryManagement/Release2Q.h"
#include "system/MemoryManagement/ReleaseFIFO.h"
#include "system/MemoryManagement/ReleaseLRU.h"
#include "system/MemoryManagement/ReleaseNULL.h"


//#define MEM_INIT 			0xfe
//#define MEM_CLEAR 			0xef

using namespace std;
using namespace ib_tcmalloc;

unsigned long long MemoryHandling::getReleaseCount1() { return  _releasePolicy->getCount1(); }
unsigned long long MemoryHandling::getReleaseCount2() { return  _releasePolicy->getCount2(); }
unsigned long long MemoryHandling::getReleaseCount3() { return  _releasePolicy->getCount3(); }
unsigned long long MemoryHandling::getReleaseCount4() { return  _releasePolicy->getCount4(); }
unsigned long long MemoryHandling::getReloaded() { return  _releasePolicy->getReloaded(); }

MemoryHandling::MemoryHandling(size_t comp_heap_size, size_t uncomp_heap_size, size_t cmp_rls, size_t uncomp_rls, string hugedir, DataCache *d, size_t hugesize)
	: 	m_alloc_blocks(0),m_alloc_objs(0),
		m_alloc_size(0),m_alloc_pack(0),m_alloc_temp(0),m_free_blocks(0),m_alloc_temp_size(0),
		m_alloc_pack_size(0),m_free_pack(0),m_free_temp(0), m_free_pack_size(0),m_free_temp_size(0),
		m_free_size(0),_releasePolicy(NULL), m_release_count(0)

{
	_int64 adj_mh_size, lt_size; 
	string conf_error;
	string rpolicy = ConfMan.GetValueString("brighthouse/memorymanagement/releasepolicy",conf_error);
	string hpolicy = ConfMan.GetValueString("brighthouse/memorymanagement/policy",conf_error);
	int hlimit = ConfMan.GetValueInt("brighthouse/memorymanagement/hardlimit",conf_error);
	float ltemp = ConfMan.GetValueFloat("brighthouse/memorymanagement/largetempratio",conf_error);

	//rccontrol << lock << "Large Temp Ratio: " << ltemp << unlock;

	// Do we want to enforce a hard limit
	m_hard_limit = (hlimit > 0);

	// calculate adjusted heap sizes if appropriate
	if (ltemp > 0.0) {
		adj_mh_size = (_int64)(uncomp_heap_size * (1.0 - ltemp));
		lt_size = uncomp_heap_size - adj_mh_size;
	} else {
		adj_mh_size = uncomp_heap_size;
		lt_size = 0;
	}

	//rccontrol << lock << "Adjusted Main Heap: " << adj_mh_size << unlock;
	//rccontrol << lock << "Adjusted LT Heap: " << lt_size << unlock;

	// Which heaps to use 
	if( hpolicy == "system" ) {
		m_main_heap= new SystemHeap(uncomp_heap_size);
		m_huge_heap= new HugeHeap(hugedir, hugesize);
		m_system = new SystemHeap(0);
		m_large_temp = NULL;
	} else if( hpolicy == "mysql" ) {
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
		m_main_heap= new MySQLHeap(uncomp_heap_size);
		m_huge_heap= new HugeHeap(hugedir, hugesize);
		m_system = new SystemHeap(0);
		m_large_temp = NULL;
#endif
	} else { // default or ""
#ifdef USE_NUMA
		m_main_heap= new NUMAHeap(adj_mh_size);
		if( lt_size != 0 )
			m_large_temp = new NUMAHeap(lt_size);
		else
			m_large_temp = NULL;
#else
		m_main_heap= new TCMHeap(adj_mh_size);
		if( lt_size != 0 )
			m_large_temp = new TCMHeap(lt_size);
		else
			m_large_temp = NULL;
#endif
		m_huge_heap= new HugeHeap(hugedir, hugesize);
		m_system = new SystemHeap(0);
	}

	ib_tcmalloc::_span_allocator.Init();
	main_heap_MB = int(adj_mh_size >> 20);
	// set this large enough to get a low number of collisions
	//m_objs.resize(main_heap_MB);
	m_objs.clear();

	//rccontrol << lock << "Release Policy: " << rpolicy << unlock;
	
	if( rpolicy == "fifo" )
		_releasePolicy = new ReleaseFIFO( );
	else if( rpolicy == "all" )
		_releasePolicy = new ReleaseALL( );
	else if( rpolicy == "lru" )
		_releasePolicy = new ReleaseLRU( );
	else if( rpolicy == "2q" )
		_releasePolicy = new Release2Q( 1024, main_heap_MB*4, 128); 
	else //default
		_releasePolicy = new Release2Q( 1024, main_heap_MB*4, 128); 		
		//_releaseStrat = new ReleaseALL( this );
		//_releaseStrat = new ReleaseNULL( this );

}

MemoryHandling::~MemoryHandling(void)
{
	delete m_main_heap;
	delete m_huge_heap;
	delete m_system;
	delete m_large_temp;
	delete _releasePolicy;
}

MEM_HANDLE_MP MemoryHandling::alloc(size_t size, BLOCK_TYPE type, TrackableObject* owner, bool nothrow)
{
	MEASURE_FET("MemoryHandling::alloc");
    IBGuard guard(m_mutex);
	HeapPolicy *heap;
	
	BHASSERT(size < (1<<31), "Oversized alloc request!");
	// choose appropriate heap for this request
	switch(type) {
		case BLOCK_TEMPORARY:
			if(m_huge_heap->getHeapStatus() == HEAP_SUCCESS)
				heap = m_huge_heap;
			else if((m_large_temp != NULL) && (size >= 16*MBYTE))
				switch(owner->TrackableType()) {
					case TO_SORTER:
					case TO_CACHEDBUFFER:
					case TO_INDEXTABLE:
					case TO_TEMPORARY:
						heap = m_large_temp;
						break;
					default:
						heap = m_main_heap;
						break;
				}
			else
				heap = m_main_heap;
			break;
		default:
			heap = m_main_heap;
	};
	
	MEM_HANDLE_MP res = heap->alloc(size);
	if( res == NULL ) {
		heap = m_main_heap;
		ReleaseMemory(size,RH_UNCOMPRESSED,NULL);
		res = heap->alloc(size);

		if( res == NULL )  {
			if( m_hard_limit ) {
				if (nothrow) return res;
				throw OutOfMemoryRCException();
			}
			res = m_system->alloc(size);
			if( res == NULL ) {
				if (nothrow) return res;
				rccontrol.lock(ConnectionInfoOnTLS.Get().GetThreadID()) << "Failed to alloc block of size " << static_cast<int>(size) << unlock;
				throw OutOfMemoryRCException();
			} else {
				heap = m_system;
			}
		}
	}	
	OwnerMemoryMap::iterator it = m_objs.find(owner);

	if( it != m_objs.end() ) {
		it->second->insert(std::make_pair((void *)res,heap));
	} else {
		m_alloc_objs++;
		// TBD: pool these objects
		PtrHeapMap *hs = new PtrHeapMap();
		hs->insert(std::make_pair((void *)res,heap));
		m_objs.insert( std::make_pair( owner, hs ) );
	}
	
	int bsize = int(heap->getBlockSize(res));
	m_alloc_blocks++;
	m_alloc_size += bsize;
	if (owner != NULL)
		if( owner->TrackableType() == TO_PACK ) {
			m_alloc_pack_size += bsize ;
			m_alloc_pack++;
		} else {
			// I want this for type = BLOCK_TEMPORARY but will take this for now
			m_alloc_temp_size+= bsize;
			m_alloc_temp++;			
		}
	else {
			m_alloc_temp_size+= bsize;
			m_alloc_temp++;			
	}
#ifdef MEM_INIT
	memset(res,MEM_INIT,bsize);
#endif
	return res;
}

size_t MemoryHandling::rc_msize(MEM_HANDLE_MP mh, TrackableObject* owner)
{
	MEASURE_FET("MemoryHandling::rc_msize");

    IBGuard guard(m_mutex);
	//if( owner == NULL || mh == 0 )
	if( mh == 0 )
				return 0;
	OwnerMemoryMap::iterator it = m_objs.find(owner);
	BHASSERT( it != m_objs.end(), "MSize Owner not found");

	PtrHeapMap::iterator h = (it->second)->find(mh);
	if( h != it->second->end() )
		return h->second->getBlockSize(mh);
	BHASSERT("Did not find msize block in map", false);
	return 0;
}

void MemoryHandling::dealloc(MEM_HANDLE_MP mh, TrackableObject* owner)
{
	MEASURE_FET("MemoryHandling::dealloc");

    IBGuard guard(m_mutex);
	
	if(mh == NULL) return;
	
	OwnerMemoryMap::iterator it = m_objs.find(owner);
	BHASSERT( it != m_objs.end(), "DeAlloc owner not found" );

	PtrHeapMap::iterator h = it->second->find(mh);
	BHASSERT( h != it->second->end(), "Dealloc heap not found." );
	int bsize = int(h->second->getBlockSize(mh));
#ifdef MEM_CLEAR
	memset(mh,MEM_CLEAR,bsize);
#endif
	m_free_blocks++;
	m_free_size+=bsize;
	if( owner != NULL )
		if( owner->TrackableType() == TO_PACK ) {
			m_free_pack_size += bsize;
			m_free_pack++;
		} else {
			m_free_temp_size += bsize;
			m_free_temp++;		
		}
	else {
			m_free_temp_size += bsize;
			m_free_temp++;				
	}

	h->second->dealloc(mh);
	it->second->erase(h);
	if( it->second->empty() ) {
		delete it->second;
		m_objs.erase(it);
		m_alloc_objs--;
	}
}

bool MemoryHandling::ReleaseMemory(size_t size, RELEASE_HEAP rh, TrackableObject* untouchable)
{
	MEASURE_FET("MemoryHandling::ReleaseMemory");

	bool result = false;
	if(process_type == ProcessType::BHLOADER || process_type == ProcessType::DATAPROCESSOR) return false;
	
	IBGuard guard(m_release_mutex);

	// release 10 packs for every MB requested + 20
	unsigned objs = uint(10 * (size >> 20) + 20);
	
	_releasePolicy->Release(objs);
	m_release_count++;
	// some kind of experiment to reduce the cumulative effects of fragmentation
	// - release all packs that have an allocation outside of main heap on non-small size alloc
	// - dont do this everytime because walking through the entire heap is slow
	if( size > 4*MBYTE || m_release_count > 100 ) {
		m_release_count = 0;
		vector<TrackableObject*> dps;

		for(OwnerMemoryMap::iterator it = m_objs.begin(); it != m_objs.end(); ++it) {
			PtrHeapMap *map = it->second;			

			if( (*it).first == NULL ) 
				continue;
			if( (*it).first->IsLocked() || it->first->TrackableType() != TO_PACK ) 
				continue;

			for(PtrHeapMap::iterator mit = map->begin(); mit != map->end(); ++mit) {
				HeapPolicy *hp = mit->second;			
				if( hp == m_system ) {
					dps.push_back((*it).first);
					break;
				}
			}
		}
		
		vector<TrackableObject*>::iterator iter = dps.begin();
		for(; iter != dps.end(); ++iter) {
			//((AttrPack*)*iter)->GetOwner()->DropObjectByMM(((AttrPack*)*iter)->GetPackCoordinate());
			(*iter)->Release();
		}

	}
	
#if 0
// follow the current tradition of throwing away everything that isn't locked
	vector<TrackableObject*> dps;
	OwnerMemoryMap::iterator it = m_objs.begin();
	while(it != m_objs.end())
	{
		if( (*it).first != NULL )
			if((*it).first->TrackableType() == TO_PACK && (*it).first->IsLocked() == false && (*it).first->IsBlocked() == false )
			{
				if( (*it).first->IsPrefetchUnused() )
					(*it).first->clearPrefetchUnused();
				else
					dps.push_back((*it).first);
				result = true;
			}
		it++;
	}

	vector<TrackableObject*>::iterator iter = dps.begin();
	for(iter; iter != dps.end(); iter++)
		((AttrPack*)*iter)->GetOwner()->DropObjectByMM(((AttrPack*)*iter)->GetPackCoordinate());
#endif
	return result;
}

MEM_HANDLE_MP MemoryHandling::rc_realloc(MEM_HANDLE_MP mh, size_t size, TrackableObject* owner, BLOCK_TYPE type)
{
	MEASURE_FET("MemoryHandling::rc_realloc");

    IBGuard guard(m_mutex);
	MEM_HANDLE_MP res = alloc(size,type,owner);

    if( mh == NULL ) return res;

	size_t oldsize = rc_msize(mh,owner);
	memcpy(res,mh,std::min(oldsize,size));
	dealloc(mh,owner);
	return res;
}

void MemoryHandling::ReportLeaks()
{
	int blocks = 0;
	size_t size = 0;
	for(OwnerMemoryMap::iterator it = m_objs.begin(); it != m_objs.end(); ++it) {
		blocks++;
		for(PtrHeapMap::iterator it2 = it->second->begin(); it2 != it->second->end(); ++it2) {
			size += it2->second->getBlockSize(it2->first);
		}
	}
	if (blocks > 0)
		rclog << lock << "Warning: " << blocks << " memory block(s) leaked total size =" << size << unlock;

}

void MemoryHandling::EnsureNoLeakedTrackableObject()
{
	bool error_found = false;
	for(OwnerMemoryMap::iterator it = m_objs.begin(); it != m_objs.end(); it++) {
		if(it->first->IsLocked() && (it->first->NoLocks() > 1 || it->first->TrackableType() == TO_PACK)) {
			error_found = true;
			rcdev << lock << "Object locked too many times found. Object type: " << (int)it->first->TrackableType() << ", no. locks: " << it->first->NoLocks() << unlock;
		}
	}
	BHASSERT_EXTRA_GUARD(!error_found, "Objects locked too many times found.");
}

#ifndef _MSC_VER

namespace stdext {
	template<> struct hash<HeapPolicy *> {
		size_t operator()(HeapPolicy* const & x) const { return (size_t)x; }
	};
};

#endif

class SimpleHist {
	static const int bins = 10;
	_uint64 total_count,total_sum,bin_count[bins],bin_total[bins],bin_max[bins];
	const char *label;
	
	inline void update_bin( int bin, _uint64 value ) {
		assert(bin<bins);
		bin_count[bin]++;
		bin_total[bin]+=value;
	}
public:
	SimpleHist(const char *l) : total_count(0), total_sum(0), label(l) {
		for(int i=0; i<bins; i++) 
			bin_count[i] = bin_total[i] = 0;

		bin_max[0] = 32;
		bin_max[1] = 1024;
		bin_max[2] = 4*1024;
		bin_max[3] = 16*1024;
		bin_max[4] = 64*1024;
		bin_max[5] = 512*1024;
		bin_max[6] = 4*1024*1024;
		bin_max[7] = 64*1024*1024;
		bin_max[8] = 256*1024*1024;
		bin_max[9] = 0xffffffffffffffffULL;
	}

	void accumulate( _uint64 value ) {
		for(int i=0; i<10; i++ )
			if(value < bin_max[i]) {
				update_bin(i,value);
				break;
			}
		total_count++;
		total_sum+=value;
	}

	void print( std::ostream &out ) {
		out << "Histogram for " << label << " " << total_sum << " bytes in " << total_count << " blocks " << endl;
		out << "Block size [max] = \t\t(block count,sum(block size) over all blocks in bin)" << endl;
		for(int i=0;i<bins;i++) {
			out << "Block size bin [" << bin_max[i] << "] = \t\t(" << bin_count[i] << "," << bin_total[i] << ")" << endl;
		}		
		out << endl;
	}
};

void MemoryHandling::HeapHistogram( std::ostream &out )
{
	typedef stdext::hash_map<HeapPolicy *, SimpleHist *> HeapMapT;
	unsigned packs_locked = 0;
	
	SimpleHist main_heap("Main Heap Total"),
				huge_heap("Huge Heap Total"),
				system_heap("System Heap Total"),
				large_temp("Large Temporary Heap"),
				packs("TO_PACK objects"),
				sorter("TO_SORTER objects"),
				cbit("TO_CACHEDBUFFER+TO_INDEXTABLE objects"),
				filter("TO_FILTER+TO_MULTIFILTER2 objects"),
				rsisplice("TO_RSINDEX+TO_SPLICE objects"),
				temp("TO_TEMPORARY objects"),
				ftree("TO_FTREE objects"),
				other("other objects");

	HeapMapT used_blocks;
	
	used_blocks.insert( std::make_pair(m_main_heap,&main_heap) );
	//used_blocks.insert( std::make_pair(m_huge_heap,&huge_heap) );
	used_blocks.insert( std::make_pair(m_system,&system_heap) );
	used_blocks.insert( std::make_pair(m_large_temp,&large_temp) );

	{ // scope for guard
    IBGuard guard(m_mutex);

	for(OwnerMemoryMap::iterator it = m_objs.begin(); it != m_objs.end(); ++it) {
		PtrHeapMap *map = it->second;
			SimpleHist *block_type;

		if( it->first->TrackableType() == TO_PACK && it->first->IsLocked() )
			packs_locked++;

			switch( it->first->TrackableType() ) {
				case TO_PACK:
					block_type = &packs;
					break;
				case TO_SORTER:
					block_type = &sorter;
					break;
				case TO_CACHEDBUFFER:
				case TO_INDEXTABLE:
					block_type = &cbit;
					break;
				case TO_FILTER:
				case TO_MULTIFILTER2:
					block_type = &filter;
					break;
				case TO_RSINDEX:
				case TO_SPLICE:
					block_type = &rsisplice;
					break;
				case TO_TEMPORARY:
					block_type = &temp;
					break;
				case TO_FTREE:
					block_type = &ftree;
					break;
				default:
					block_type = &other;
			}
		for(PtrHeapMap::iterator mit = map->begin(); mit != map->end(); ++mit) {
			void *ptr = mit->first;
			HeapPolicy *hp = mit->second;
			HeapMapT::iterator hist = used_blocks.find(hp);
			
			if(hist!=used_blocks.end()) {
				hist->second->accumulate(hp->getBlockSize(ptr));
				if(block_type != NULL)
					block_type->accumulate(hp->getBlockSize(ptr));
			}
		}
	}
	} // close scope for m_mutex guard

	//rccontrol << lock;
	for(HeapMapT::iterator h = used_blocks.begin(); h != used_blocks.end(); ++h) {
		h->second->print(out);
	}
	packs.print(out);
	sorter.print(out);
	cbit.print(out);
	filter.print(out);
	rsisplice.print(out);
	temp.print(out);
	ftree.print(out);
	other.print(out);

	out << "Total number of locked packs: " << packs_locked << endl;
	//rccontrol << unlock;
		
}


void MemoryHandling::AssertNoLeak(TrackableObject* o)
{
	BHASSERT(m_objs.find(o) == m_objs.end(), "MemoryLeakAssertion");
}

void 
MemoryHandling::TrackAccess(TrackableObject *o)
{
	MEASURE_FET("MemoryHandling::TrackAccess");
	IBGuard guard(m_release_mutex);

	//if( o->TrackableType() == TO_PACK );
		_releasePolicy->Access(o);
}

void 
MemoryHandling::StopAccessTracking(TrackableObject *o)
{
	MEASURE_FET("MemoryHandling::TrackAccess");
	IBGuard guard(m_release_mutex);

	if( o->IsTracked() )
		_releasePolicy->Remove(o);
}


