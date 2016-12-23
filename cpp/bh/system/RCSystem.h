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

#ifndef __RS_SYSTEM_H
#define __RS_SYSTEM_H

#include <boost/scoped_ptr.hpp>
#include "Channel.h"
#include "system/Configuration.h"
#include "core/RSI_Framework.h"
#include "system/ConnectionInfo.h"
#include "system/ib_system.h"
#ifndef __BH_COMMUNITY__
#include "enterprise/edition/system/IBResourceMon.h"
#include "enterprise/edition/system/IBThrottler.h"
#endif
#include "system/IBConfigurationMan.h"

extern Channel rccontrol;                       // Channel for debugging information,
                                                // not displayed in the standard running mode.

extern Channel rclog;							// Channel for control and warning messages,
                                                // saved to the control log file when running as server
												// and displayed to a user when running from the console.

extern Channel rcdev;							// Channel for internal messages, saved to the file
												// For internal usage only

extern TableLockManager table_lock_manager;

extern boost::scoped_ptr<RSI_Manager> rsi_manager;				// RS Index manager, constructed in RCBase constructors

class RCEngine;
extern RCEngine* rceng;

extern IBMutex global_mutex;

extern IBMutex drop_rename_mutex;

extern int BufferingLevel;

extern bool DefaultPushDown;

//extern bool Caching;
extern int CachingLevel;

extern uint sum_of_page_faults;

extern bool sync_buffers;

extern IBThreadStorage<ConnectionInfo> ConnectionInfoOnTLS;

int		MakeDirs(char const* aPath);

#ifndef __BH_COMMUNITY__
extern IBResourceMon    RMon;
extern IBQueryThrottler QThrottler;
#endif

extern IBConfigurationManager ConfMan;

extern const char *ha_rcbase_exts[];

#endif

