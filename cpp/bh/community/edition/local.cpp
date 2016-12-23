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

#include "local.h"

#include "common/DataFormat.h"
#include "common/TxtDataFormat.h"

#include "system/ib_system.h"

using namespace std;

//#include <boost/assign/list_of.hpp>

// SOLARIS -- changed by Michael

static IBMutex df_map_mutex;
static DataFormat::map_type* df_map = NULL;

DataFormat::map_type* DataFormat::GetExternalDataFormats() // SOLARIS
{
    if (df_map != NULL) return df_map;

    { // guarded section
        IBGuard g(df_map_mutex);

        // Check the pointer again. It could be changed on another thread
        // while this was blocked on df_map_mutex
        if (df_map != NULL) return df_map;

        DataFormat::map_type* temp = new DataFormat::map_type;
        temp->insert(make_pair("TXT_VARIABLE", DataFormatPtr(new TxtDataFormat())));

        df_map = temp;
    }

    return df_map;
}

