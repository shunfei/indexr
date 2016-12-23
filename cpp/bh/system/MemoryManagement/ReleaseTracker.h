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

#ifndef RELEASETRACKER_H
#define RELEASETRACKER_H

#include "system/MemoryManagement/TrackableObject.h"

class ReleaseTracker
{
protected:
	unsigned _size;

	// Access the tracking structures inside Releasable objects from friend relationship
	inline TrackableObject *GetRelNext(TrackableObject *o) { return o->next; }
	inline TrackableObject *GetRelPrev(TrackableObject *o) { return o->prev; }
	inline ReleaseTracker *GetRelTracker(TrackableObject *o) { return o->tracker; }
	inline void SetRelNext(TrackableObject *o, TrackableObject *v) { o->next = v; }
	inline void SetRelPrev(TrackableObject *o, TrackableObject *v) { o->prev = v; }
	inline void SetRelTracker(TrackableObject *o, ReleaseTracker *v) { o->tracker = v; }

public:
	ReleaseTracker() : _size(0) {}
	virtual void insert(TrackableObject *) = 0;
	virtual void remove(TrackableObject *) = 0;
	virtual void touch(TrackableObject *) = 0;
	unsigned size() { return _size; }

};

class FIFOTracker : public ReleaseTracker
{
	TrackableObject *head,*tail;
public:
	FIFOTracker() : ReleaseTracker(), head(0), tail(0) {}
	void insert(TrackableObject *);
	void remove(TrackableObject *);
	void touch(TrackableObject *);

	TrackableObject *removeHead();
	TrackableObject *removeTail();
};

class LRUTracker : public FIFOTracker
{
public:
	TrackableObject *removeMin() { return removeHead(); }
	TrackableObject *removeMax() { return removeTail(); }

	void touch(TrackableObject *);
};



#endif

