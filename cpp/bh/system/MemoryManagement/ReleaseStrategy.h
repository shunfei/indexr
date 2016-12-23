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

#ifndef RELEASESTRATEGY_H
#define RELEASESTRATEGY_H

#include "system/MemoryManagement/TrackableObject.h"
#include "system/MemoryManagement/ReleaseTracker.h"

class ReleaseStrategy {
protected:
	_uint64 m_reloaded;
	
	void SetTracker(TrackableObject *o, ReleaseTracker *t) { o->tracker = t; }
	ReleaseTracker *GetTracker(TrackableObject *o) { return o->tracker; }
	void Touch(TrackableObject *o) { o->tracker->touch(o); }
	void UnTrack(TrackableObject *o) { o->tracker->remove(o); }

public:
	ReleaseStrategy( ) : m_reloaded(0) {}
	virtual ~ReleaseStrategy() {}
	virtual void Access( TrackableObject * ) = 0;
	virtual void Remove( TrackableObject * ) = 0;
	
	virtual void Release(unsigned) = 0;
	virtual void ReleaseFull( ) = 0;
	
	virtual unsigned long long getCount1() = 0;
	virtual unsigned long long getCount2() = 0;
	virtual unsigned long long getCount3() = 0;
	virtual unsigned long long getCount4() = 0;
	virtual unsigned long long getReloaded() { return m_reloaded; }

};


#endif

