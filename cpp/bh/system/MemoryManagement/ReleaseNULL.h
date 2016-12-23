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

#ifndef RELEASENULL_H
#define RELEASENULL_H

#include "ReleaseStrategy.h"
#include "ReleaseTracker.h"

class TrackableObject;

// do not perform any releasing or tracking
// for testing and debugging purposes only
class ReleaseNULL : public ReleaseStrategy {
public:
	ReleaseNULL() : ReleaseStrategy( ) {}
	
	void Access( TrackableObject * ) {}
	void Remove( TrackableObject * ) {}
	
	void Release(unsigned) {}
	void ReleaseFull( ) {}
	
	unsigned long long getCount1() { return 0; }
	unsigned long long getCount2() { return 0; }
	unsigned long long getCount3() { return 0; }
	unsigned long long getCount4() { return 0; }

};


#endif

