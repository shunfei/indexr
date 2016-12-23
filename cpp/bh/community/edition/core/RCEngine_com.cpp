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

#include "core/RCEngine.h"
#include "core/RCTableImpl.h"
#include "edition/core/Transaction.h"

#include <string>

#include <boost/shared_ptr.hpp>

using namespace std;

RCTablePtr RCEngine::GetTableForWrite(const char* table_path, struct st_table_share* table_share)
{
	char db_name[256] = { 0 };
	char tab_name[256] = { 0 };
	GetNames(table_path, db_name, tab_name, 256);
	string tab_path = table_path;
	tab_path += m_tab_ext;
	vector<DTCollation> charsets;
	if(table_share) {
		for(int i = 0; i < table_share->fields; i++) {
			const Field_str* fstr = dynamic_cast<const Field_str*>(table_share->field[i]);
			if(fstr)
				charsets.push_back(DTCollation(fstr->charset(), fstr->derivation()));
			else
				charsets.push_back(DTCollation());
		}
	}
	return RCTablePtr(new RCTable(tab_path.c_str(), charsets, 0, RCTableImpl::OpenMode::FORCE));
}

void RCEngine::RenameTable(const char* from, const char* to)
{
	BHERROR("Not supported");
}

void RCEngine::DatadirMigrator::Upgrade()
{
	BHASSERT(dv.GetDataVersion() < CURRENT_DATA_VERSION, "An attempt to upgrade to previous version!");
	dv.UpdateTo(CURRENT_DATA_VERSION, InfobrightServerVersion());
}
