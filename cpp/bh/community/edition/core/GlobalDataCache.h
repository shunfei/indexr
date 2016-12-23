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

#ifndef GLOBALDATACACHE_H
#define GLOBALDATACACHE_H

#include "core/DataCache.h"

class GlobalDataCache : public DataCache
{
public:
	GlobalDataCache() : DataCache(false) {	}
	static GlobalDataCache& GetGlobalDataCache();
	_int64 getPacksPrefetched() { return 0;}	

	void Init();
	void Release(size_t size);

};

#endif
