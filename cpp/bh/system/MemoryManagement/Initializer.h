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

#ifndef INITIALIZER_H_5F601A83_AA51_4a3a_9B23_395F3522DF77
#define INITIALIZER_H_5F601A83_AA51_4a3a_9B23_395F3522DF77

#include "TrackableObject.h"
//#include "system/RCSystem.h"
#include "system/RCException.h"
#include "common/bhassert.h"

class MemoryManagerInitializer: public TrackableObject
{
public:
	static MemoryManagerInitializer* Instance(size_t comp_size, size_t uncomp_size, size_t cmp_rls, size_t uncmp_rls, std::string hugedir="", int hugesize = 0)
	{
		if (instance == NULL) {
			try {
				instance = new MemoryManagerInitializer(comp_size, uncomp_size, cmp_rls, uncmp_rls, hugedir, hugesize);
			} catch (OutOfMemoryRCException&) {
				throw;
			}
		}
		return instance;
	}

	static void deinit(bool report_leaks = false)
	{
		m_report_leaks = report_leaks;
		delete instance;
	}

	static void EnsureNoLeakedTrackableObject()
	{
		m_MemHandling->EnsureNoLeakedTrackableObject();
	}
	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_INITIALIZER;}

private:

	MemoryManagerInitializer(size_t comp_size, size_t uncomp_size, size_t cmp_rls, size_t uncmp_rls, std::string hugedir = "", int hugesize = 0):TrackableObject(comp_size, uncomp_size, cmp_rls, uncmp_rls, hugedir, NULL, hugesize){}
	virtual ~MemoryManagerInitializer(void)
	{
		deinitialize(m_report_leaks);
	}

	static MemoryManagerInitializer* instance;
	static bool m_report_leaks;
};

#endif

