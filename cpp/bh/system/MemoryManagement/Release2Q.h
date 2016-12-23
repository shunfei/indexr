/* Copyright (C)  2005-2008 Infobright Inc.

 */

#ifndef RELEASE2Q_H
#define RELEASE2Q_H

#include "system/MemoryManagement/ReleaseStrategy.h"
#include "system/MemoryManagement/ReleaseTracker.h"
#include "system/MemoryManagement/ReferenceObject.h"

#ifdef __GNUC__
	#include <ext/hash_map>
	#ifndef stdext
	#define stdext __gnu_cxx
	#endif
#else
	#include <hash_map>
#endif


class TrackableObject;
/*
 * Classic 2Q algorithm from Johnson and Shasha
 * 
 * Modifications: objects are released on request only and in such a way that 
 *                |A1in| and |Am| become as close to equal as possible.
 * 
 */
class Release2Q : public ReleaseStrategy {
	// Using names from the original paper, maybe we shouldn't?
	FIFOTracker A1in,A1out;
	LRUTracker Am;
	unsigned Kin, Kout, KminAm;
	// needs to be a hash_map pointing to an element on A1out
	typedef stdext::hash_map<TOCoordinate, ReferenceObject *, TOCoordinate> A1outLookupT;
	A1outLookupT A1outLookup;
public:
	Release2Q( unsigned kin, unsigned kout, unsigned kminam ) : 
		Kin(kin), Kout(kout), KminAm(kminam) 
	{}
	
	~Release2Q()
	{
		for( A1outLookupT::iterator it=A1outLookup.begin();
			it!=A1outLookup.end();
			it++ )
		{
			delete it->second;
		}
		A1outLookup.clear();
	}

	unsigned getA1insize() { return A1in.size(); }
	unsigned getA1outsize() { return A1out.size(); }
	unsigned getAmsize() { return Am.size(); }
	
	void Access( TrackableObject *o );
	void Remove( TrackableObject *o );
	void Release( unsigned );
	void ReleaseFull( );

	unsigned long long getCount1() { return getAmsize(); }
	unsigned long long getCount2() { return getA1insize(); }
	unsigned long long getCount3() { return getA1outsize(); }
	unsigned long long getCount4() { return A1outLookup.size(); }

protected :

};


#endif

