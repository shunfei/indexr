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

#include "ReleaseFIFO.h"
#include "TrackableObject.h"
#include "core/DataCache.h"

void ReleaseFIFO::Access( TrackableObject *o )
{
	tracker.touch(o);
}

void ReleaseFIFO::Remove( TrackableObject *o )
{
	tracker.remove(o);
}
	
void ReleaseFIFO::Release(unsigned num)
{
	TrackableObject *o = NULL;
	for(uint i = 0; i < num; i++) {
		o = tracker.removeTail();
		if( o->IsLocked() ) {
			tracker.touch(o);
		} else {
			o->Release();
		}
	}
}

void ReleaseFIFO::ReleaseFull( )
{
	TrackableObject *o = NULL;
	int num=tracker.size();
	for( int i=0; i<num; i++) {
		o = tracker.removeTail();
		if( o->IsLocked() ) {
			tracker.touch(o);
		} else {
			o->Release();
		}
	}

}
	




