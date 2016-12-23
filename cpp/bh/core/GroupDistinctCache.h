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

#ifndef GROUPDISTINCTCACHE_H_
#define GROUPDISTINCTCACHE_H_

#include "system/CacheableItem.h"
#include "system/MemoryManagement/TrackableObject.h"

/*
 * Functionality:
 *   - remember byte vectors (of defined width) in a table, cached on disk,
 *   - provide one-by-one access for put/get value,
 *   - allow filtering values by marking some of them as "preserved",
 *   - allow switching to "preserved" values.
 * Assumption: values will be written once and read once (unless preserved).
 * */

class GroupDistinctCache : private CacheableItem, public TrackableObject
{
public:
	GroupDistinctCache();
	~GroupDistinctCache();

	void SetNoObj(_int64 max_no_obj)		{ no_obj = max_no_obj; orig_no_obj = max_no_obj; }
	void SetWidth(int w)					{ width = w; }		// w - byte size of a single object

	void Reset();							// reset the number of objects, then rewind (used for reusing the object)
	void Rewind();							// rewind iterator, start from object 0
	bool NextRead();						// go to the next position, return false if out of scope
	bool NextWrite();						// go to the next position, return false if out of scope

	unsigned char *GetCurrentValue()					{ return cur_pos; }
	void SetCurrentValue(unsigned char *val);

	void MarkCurrentAsPreserved();			// move current value to beginning, or just after previously preserved (reuse buffer)
	void SwitchToPreserved();				// switch to values marked as preserved
	void Omit(_int64 obj_to_omit);			// move a reading position forward by obj_to_omit objects

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

private:
	void Initialize();	

	unsigned char*	t;						// value buffer
	unsigned char*	t_write;				// value buffer for preserved objects, if different than t
	int				upper_byte_limit;		// upper byte size of t, t_write, it is also the actual size for multi-block case
	unsigned char*	cur_pos;				// current location in buffer; NULL - out of scope
	_int64			cur_obj;				// current (virtual) object number
	_int64			no_obj;					// a number of all (virtual) objects; current state (may be lowered by switching to preserved)
	_int64			orig_no_obj;			// a number of all objects (original value, no_obj will be switched to it on Reset())
	int				width;					// number of bytes for one value

	unsigned char*	cur_write_pos;			// current location for writing while reusage
	_int64			cur_write_obj;			// current (virtual) object number, as above

	_int64			buf_size;				// buffer size in objects
};

#endif /*GROUPDISTINCTCACHE_H_*/
