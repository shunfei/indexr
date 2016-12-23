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

#include "CachedBuffer.h"

template <class T>
CachedBuffer<T>::CachedBuffer(uint page_size, uint _elem_size, ConnectionInfo* conn)
: CacheableItem("PS", "CB"), page_size(page_size), elem_size(_elem_size), m_conn(conn)
{
	if(!elem_size)
		elem_size = sizeof(T);
	CI_SetDefaultSize(page_size * elem_size);

	buf = (T*)alloc(sizeof(T) * (size_t)page_size, BLOCK_TEMPORARY);
	memset(buf, 0, sizeof(T) * (size_t)page_size);
	loaded_page = 0;
	page_changed = false;
}

CachedBuffer<RCBString>::CachedBuffer(uint page_size, uint elem_size, ConnectionInfo* conn)
: CacheableItem("PS", "CB"), page_size(page_size), elem_size(elem_size), m_conn(conn)
{
	//assert(elem_size);
	CI_SetDefaultSize(page_size * (elem_size + 4));

	size_t buf_size = sizeof(char) * (size_t)page_size * (elem_size + 4);
	if(buf_size) {
		buf = (char*)alloc(buf_size, BLOCK_TEMPORARY);
		memset(buf, 0, buf_size);
	} else
		buf = 0;
	loaded_page = 0;
	page_changed = false;
}

template <class T>
CachedBuffer<T>::~CachedBuffer(void)
{
	dealloc(buf);
}

CachedBuffer<RCBString>::~CachedBuffer(void)
{
	dealloc(buf);
}

template <class T>
T& CachedBuffer<T>::Get(_uint64 idx)
{
	assert(page_size>0);
	if(idx / page_size  != loaded_page)
		LoadPage((uint)(idx / page_size));
	return buf[idx % page_size];
}

void CachedBuffer<RCBString>::Get(RCBString& s, _uint64 idx)
{
	if(idx / page_size  != loaded_page)
		LoadPage((uint)(idx / page_size));
	_uint64 pos = (idx % page_size) * (elem_size + 4);
	uint * size = (uint *)&buf[pos];
	if(*size == NULL_VALUE_U) // 0 -NULL, 1 -NOT NULL
		s = RCBString();
	else
		s = RCBString(&buf[pos], -2);
}

RCBString& CachedBuffer<RCBString>::Get(_uint64 idx)
{
	Get(bufS, idx);
	return bufS;
}

template <class T>
void CachedBuffer<T>::Set(_uint64 idx, T& value)
{
	if(idx / page_size != loaded_page)
		LoadPage((uint)(idx / page_size));
	buf[idx % page_size] = value;
	page_changed = true;
}

void CachedBuffer<RCBString>::Set(_uint64 idx, RCBString& value)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(value.len <= elem_size);
	if(idx / page_size != loaded_page)
		LoadPage((uint)(idx / page_size));
	uint pos = (uint)(idx % page_size) * (elem_size + 4);
	uint * size = (uint *)&buf[pos];
	if(value.IsNull())
		*size = NULL_VALUE_U;
	else {
		*size = value.len;
		memcpy(&buf[pos + 4], value.val, value.len);
	}
	page_changed = true;
}

template <class T>
void CachedBuffer<T>::LoadPage(uint page)
{
	if(m_conn && m_conn->killed())
		throw KilledRCException();			// cleaning is implemented below (we are inside try{})
	if(page_changed) { // Save current page
		CI_Put(loaded_page, (unsigned char *)buf);
		page_changed = false;
	}
	// load new page to memory
	CI_Get(page, (unsigned char *)buf);
	loaded_page = page;
}

void CachedBuffer<RCBString>::LoadPage(uint page)
{
	if(m_conn && m_conn->killed())
		throw KilledRCException();			// cleaning is implemented below (we are inside try{})
	if(page_changed) { // Save current page
		CI_Put(loaded_page, (unsigned char *)buf);
		page_changed = false;
	}
	// load new page to memory
	CI_Get(page, (unsigned char *)buf);
	loaded_page = page;
}

template<class T>
void CachedBuffer<T>::SetNewPageSize(uint new_page_size)
{
	if(page_size == new_page_size)
		return;
	if(m_conn && m_conn->killed())
		throw KilledRCException();			// cleaning is implemented below (we are inside try{})
	page_size = new_page_size;
	CI_SetDefaultSize(page_size * elem_size);
	buf = (T*)rc_realloc(buf, sizeof(T) * (size_t)page_size, BLOCK_TEMPORARY);
}

void CachedBuffer<RCBString>::SetNewPageSize(uint new_page_size)
{
	if(page_size == new_page_size)
		return;
	if(m_conn && m_conn->killed())
		throw KilledRCException();			// cleaning is implemented below (we are inside try{})
	page_size = new_page_size;
	CI_SetDefaultSize(page_size * (elem_size + 4));
	size_t buf_size = (size_t)page_size * (elem_size + 4) * sizeof(char);
	buf = (char*)rc_realloc(buf, buf_size, BLOCK_TEMPORARY);
}

template class CachedBuffer<short>;
template class CachedBuffer<int>;
template class CachedBuffer<_int64>;
template class CachedBuffer<char>;
template class CachedBuffer<double>;

