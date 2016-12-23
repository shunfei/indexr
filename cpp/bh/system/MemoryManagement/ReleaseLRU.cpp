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

#include "ReleaseLRU.h"
#include "TrackableObject.h"
#include "core/DataCache.h"

void ReleaseLRU::Access( TrackableObject *o )
{
	tracker.touch(o);
}

void ReleaseLRU::Remove( TrackableObject *o )
{
	tracker.remove(o);
}
	
void ReleaseLRU::Release(unsigned num)
{
	TrackableObject *o = NULL;
	for(uint i = 0; i < num; i++) {
		o = tracker.removeMax();
		if( o->IsLocked() ) {
			tracker.touch(o);
		} else {
			o->Release();
		}
	}
}

void ReleaseLRU::ReleaseFull( )
{
	TrackableObject *o = NULL;
	int num=tracker.size();
	for( int i=0; i<num; i++) {
		o = tracker.removeMax();
		if( o->IsLocked() ) {
			tracker.touch(o);
		} else {
			o->Release();
		}
	}

}
	




