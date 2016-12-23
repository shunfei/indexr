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

#include "system/MemoryManagement/ReleaseTracker.h"
#include "common/bhassert.h"
#include "TrackableObject.h"

void FIFOTracker::insert(TrackableObject *o)
{
	BHASSERT(GetRelTracker(o) == NULL, "Object has multiple trackers");
	BHASSERT(GetRelPrev(o) == NULL, "Object was not removed or initialized properly");
	BHASSERT(GetRelNext(o) == NULL, "Object was not removed or initialized properly");
	SetRelTracker(o,this);
	SetRelPrev(o,NULL);
	SetRelNext(o,head);
	if( head != NULL)
		SetRelPrev(head,o);
	head = o;
	if( tail == NULL )
		tail = head;
	_size++;
}

TrackableObject *FIFOTracker::removeTail()
{
	TrackableObject *res = tail;
	if( res == NULL ) return NULL;
	tail = GetRelPrev(tail);
	if( tail != NULL )
		SetRelNext(tail,NULL);
	else {
		BHASSERT(_size==1,"FIFOTracker size error");
		head = NULL;		
	}
	SetRelTracker(res,NULL);
	SetRelPrev(res,NULL);
	SetRelNext(res,NULL);
	_size--;
	return res;
}

TrackableObject *FIFOTracker::removeHead()
{
	TrackableObject *res = head;
	if( res == NULL ) return NULL;
	head = GetRelNext(head);
	if( head != NULL )
		SetRelPrev(head,NULL);
	else {
		BHASSERT(_size==1,"FIFOTracker size error");
		tail = NULL;		
	}
	SetRelTracker(res,NULL);
	SetRelPrev(res,NULL);
	SetRelNext(res,NULL);
	_size--;
	return res;
}

void FIFOTracker::remove(TrackableObject *o)
{
	BHASSERT(GetRelTracker(o) == this, "Removing object from wrong tracker");
	SetRelTracker(o,NULL);
	if( (o == head) && (o == tail) ) {
		head = tail = NULL;
	} else if( o == head ) {
		head = GetRelNext(o);
		SetRelPrev(head,NULL);
	} else if( o == tail ) {
		tail = GetRelPrev(o);
		SetRelNext(tail,NULL);
	} else {
		TrackableObject *p = GetRelPrev(o),
			*n = GetRelNext(o);
		SetRelNext(p,n);
		SetRelPrev(n,p);
	}
	SetRelNext(o,NULL);
	SetRelPrev(o,NULL);
	_size--;
}

void FIFOTracker::touch(TrackableObject *)
{
		// do nothing
}


void LRUTracker::touch(TrackableObject *o)
{
	remove(o);
	insert(o);
}




