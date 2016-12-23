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

#ifndef EXPORT_2_FILE_9404C094_D6C8_4898_ACED_F16BEC876411
#define EXPORT_2_FILE_9404C094_D6C8_4898_ACED_F16BEC876411

#include "core/TempTable.h"
#include "system/IOParameters.h"

class select_bh_export : public select_export
{
public:
	//select_bh_export(sql_exchange *ex);
	select_bh_export(select_export* se);
	~select_bh_export() { };
	int prepare(List<Item> &list, SELECT_LEX_UNIT *u);
	void SetRowCount(ha_rows x);
	void SendOk(THD* thd);
	sql_exchange* SqlExchange();
	bool IsPrepared() const { return prepared; };
	mysql_bool send_data(List<Item> &items);
private:
	select_export* se;
	bool prepared;
};

#endif

