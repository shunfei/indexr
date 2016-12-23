/* Copyright (C)  2005-2008 Infobright Inc.

 */


#include "system/MemoryManagement/ReleaseStrategy.h"
#include "system/MemoryManagement/ReleaseTracker.h"
#include "system/MemoryManagement/ReferenceObject.h"
#include "core/RCAttrPack.h"
#include "core/DataCache.h"
#include "Release2Q.h"
#include <algorithm>

#ifdef __GNUC__
	#include <ext/hash_map>
	#ifndef stdext
	#define stdext __gnu_cxx
	#endif
#else
	#include <hash_map>
#endif


void 
Release2Q::Access( TrackableObject *o )
{
	//BHASSERT(o->TrackableType() == TO_PACK, "Access wrong type");
	if(!o->IsTracked()) {
		A1outLookupT::iterator it = A1outLookup.find(o->GetCoordinate());
		if( it != A1outLookup.end() ) {
			m_reloaded++;
			TrackableObject *d = it->second;
			Am.insert(o);
			A1out.remove(d);
			A1outLookup.erase(it);
			delete d;
		} else {
			A1in.insert(o);
		} 
	} else {
		Touch(o);
	}	
}

void 
Release2Q::Remove( TrackableObject *o )
{
	UnTrack(o);
}
	
/*
 * Try to drop packs by keeping A1in and Am equal in size
 */
void Release2Q::Release( unsigned num_objs )
{
	uint count = 0;
	for (int max_loop = std::max(A1in.size(), Am.size()); (max_loop > 0) && (count < num_objs); max_loop--) {
		if( A1in.size() >= Am.size() ) {
			if( A1in.size() == 0 ) 
				break;

			TrackableObject *o = A1in.removeTail();
			if( o->IsLocked() ) {
				Am.insert(o);
			} else {
				ReferenceObject *to = new ReferenceObject(o->GetCoordinate());
				BHASSERT(!to->IsTracked(),"Tracking error");
				//BHASSERT(to->GetCoordinate().ID == bh::COORD_TYPE::PACK,"ReferenceObject improperly constructed");
				A1out.insert(to);
				A1outLookup.insert( std::make_pair(to->GetCoordinate(), to) );
				o->Release();
				count++;
				if( A1out.size() > Kout ) {
					TrackableObject *ao = A1out.removeTail();
					A1outLookup.erase(ao->GetCoordinate());
					delete ao;
				}
			}
		} else {
			TrackableObject *o = Am.removeMax();
			if( o->IsLocked() ) {
				Am.insert(o);
				continue;
			}
			o->Release();
			count++;
		}
	}
}
	
	
void 
Release2Q::ReleaseFull( )
{
	while( A1in.size() > 0 ) {
		TrackableObject *o = A1in.removeTail();
		if( o->IsLocked() ) {
			Am.insert(o);
		} else {
			A1out.insert(new ReferenceObject(o->GetCoordinate()));
			o->Release();
			if( A1out.size() > Kout ) {
				TrackableObject *ao = A1out.removeTail();
				A1outLookup.erase(ao->GetCoordinate());
				delete ao;
			}
		}
	}
	
	while( Am.size() > 0 ) {
		TrackableObject *o = Am.removeMax();
		if( o->IsLocked() ) {
			Am.insert(o);
			// not guaranteed that the rest of Am is locked, but close enough?
			break;
		} else {
			o->Release();
		}
	}
}






