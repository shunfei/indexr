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

#ifndef __COMMMON_CACHEDBUFFER_H
#define __COMMMON_CACHEDBUFFER_H

#include "../common/CommonDefinitions.h"
#include "../types/RCDataTypes.h"
#include "../system/CacheableItem.h"

#include "../system/MemoryManagement/TrackableObject.h"

/*
This is a general purpose class to store 64bit number of elements of any built-in type in an array.
One page of elements is kept in memory (2^25 = 33 554 432). If there are more
elements they are cached on a disk (class CacheableItem). Methods Get and Set
are to read/write a value.
NOTE: it is not suitable to store structures containing pointers.
*/

#include "../system/ConnectionInfo.h"
//class ConnectionInfo;

template<class T>
class CachedBuffer : public CacheableItem, public TrackableObject
{
public:
	explicit CachedBuffer(uint page_size = 33554432, uint elem_size = 0, ConnectionInfo* conn = NULL);
	virtual ~CachedBuffer(void);

	_uint64			PageSize() { return page_size; }
	void			SetNewPageSize(uint new_page_size);
	T&				Get(_uint64 idx);
	void 			Get(RCBString& s, _uint64 idx) {assert("use only for RCBString" && 0);}
	void			Set(_uint64 idx, T& value);

	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_CACHEDBUFFER;}

protected:
	void			LoadPage(uint n);// load page n
	int				loaded_page;	// number of page loaded to memory?
	bool			page_changed;	// is current page changed and has to be saved?
	uint			page_size;		// size of one page stored in memory (number of elements)
	uint			elem_size;		// size of one element in BYTES; for most types computed by sizeof()
	T*				buf;
	ConnectionInfo*	m_conn;
};


/* buffers, in the case of RCBString, keep the char * only. first 4 bytes define the length of entry
or if it is null (NULL_VALUE_U) or not.
must be of equal size, thus elem_size must be set */

template<>
class CachedBuffer<RCBString> : public CacheableItem, public TrackableObject
{
public:
	CachedBuffer(uint page_size = 33554432, uint elem_size = 0, ConnectionInfo* conn = NULL);
	CachedBuffer(_uint64 size, RCBString& value, uint page_size = 33554432, uint elem_size = 0, ConnectionInfo* conn = NULL);
	virtual ~CachedBuffer(void);

	_uint64			PageSize() { return page_size; }
	void			SetNewPageSize(uint new_page_size);
	RCBString&		Get(_uint64 idx);
	void 			Get(RCBString& s, _uint64 idx);
	void			Set(_uint64 idx, RCBString& value);

	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_CACHEDBUFFER;}

protected:
	void			LoadPage(uint n);// load page n
	int				loaded_page;	// number of page loaded to memory?
	bool			page_changed;	// is current page changed and has to be saved?
	uint			page_size;		// size of one page stored in memory (number of elements)
	uint			elem_size;		// size of one element in BYTES; for most types computed by sizeof()
	char*			buf;
	RCBString		bufS;
	ConnectionInfo*	m_conn;
};

#endif  //__COMMMON_CACHEDBUFFER_H
