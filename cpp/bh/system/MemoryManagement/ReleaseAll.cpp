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

#include "ReleaseAll.h"
#include "TrackableObject.h"
#include "core/DataCache.h"

void ReleaseALL::Access( TrackableObject *o )
{
	trackable.touch(o);
}

void ReleaseALL::Remove( TrackableObject *o )
{
	trackable.remove(o);
}
	
void ReleaseALL::Release(unsigned sz)
{
	uint size = trackable.size();
	TrackableObject *o;
	for(uint i = 0; i < size; i++) {
		o = trackable.removeTail();
		if( o->IsLocked() )
			trackable.touch(o);
		else
			o->Release();
	}
}

void ReleaseALL::ReleaseFull( )
{
	Release(0);
}
	




