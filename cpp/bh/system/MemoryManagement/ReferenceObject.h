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

#ifndef REFERENCE_OBJECT_H
#define REFERENCE_OBJECT_H

#include "system/MemoryManagement/TrackableObject.h"
#include "core/tools.h"


class ReferenceObject : public TrackableObject {
public:
	ReferenceObject(PackCoordinate &r) : TrackableObject() 
	{ 
		_logical_coord.ID = bh::COORD_TYPE::PACK;
		_logical_coord.co.pack = r;
	}
	ReferenceObject(RCAttrCoordinate &r) : TrackableObject() 
	{ 
		_logical_coord.ID = bh::COORD_TYPE::RCATTR;
		_logical_coord.co.rcattr = r;
	}
	ReferenceObject(TOCoordinate &r) : TrackableObject() { _logical_coord = r; }
	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_REFERENCE; }
};



#endif


