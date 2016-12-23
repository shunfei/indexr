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

#include <sys/stat.h>
#include <boost/scoped_array.hpp>

#include "RCSystem.h"
#include "core/tools.h"
#include "system/fet.h"

Channel rccontrol;
Channel rclog(true);
Channel rcdev(true);

boost::scoped_ptr<RSI_Manager>	rsi_manager;
TableLockManager	table_lock_manager;

RCEngine* rceng = 0;

IBMutex global_mutex;

IBMutex drop_rename_mutex;

using namespace std;

int		BufferingLevel = 2;
bool	DefaultPushDown = true;
int		CachingLevel = 1;

bool 	loader_process = false;
bool	data_integrity_manager = false;

bool    sync_buffers = true;

IBConfigurationManager ConfMan;

#ifdef FUNCTIONS_EXECUTION_TIMES
LoadedDataPackCounter count_distinct_dp_loads;
LoadedDataPackCounter count_distinct_dp_decompressions;
FunctionsExecutionTimes* fet = NULL;
uint64 NoBytesReadByDPs = 0;
uint64 SizeOfUncompressedDP = 0;
#endif

// WARNING: not thread safe implementation
int MakeDirs(char const* aPath)
{
	size_t aPathLen = strlen(aPath);
	BHASSERT( aPathLen > 0, "request to create a directory with empty path" );
	boost::scoped_array<char> pathBufHolder(new char[aPathLen + 1]); /* + 1 for EOS */
	char* path = pathBufHolder.get();
	strncpy(path, aPath, aPathLen + 1); /* + 1 for EOS */
	char* segment = strtok(path, DIR_SEPARATOR_STRING); /* *CAUTION* strtok usage of strtok should be avoided  */

	while(true) {
		if(!DoesFileExist(path)) {
			try {
				CreateDir(infobright_data_dir + path);
			} catch (DatabaseRCException& e) {
				rclog << lock << e.what() << " Configuration::Exception " << __FILE__ << __LINE__ << unlock;
				return -1;
			}
		}
		segment = strtok(NULL, DIR_SEPARATOR_STRING);
		if(segment != NULL)
			*(segment - 1) = DIR_SEPARATOR_CHAR;
		else
			return 0;
	}
	return 0;
}

const char *ha_rcbase_exts[] = { ".bht", 0 };


