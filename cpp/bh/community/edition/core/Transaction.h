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

#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "core/TransactionBase.h"

class Transaction : public TransactionBase {
		// This is the parent in a nested transaction
	Transaction *m_parent;
public:
	Transaction( Transaction *p, RCEngine &r, GlobalDataCache *d ):
		TransactionBase(r,d), m_parent(p)
	{}

	Transaction *getParent() { return m_parent; }

	RCTablePtr GetTableShared(const std::string& table_path, struct st_table_share* table_share);

	void AddTableWR(const std::string& table_path, THD* thd, st_table* table);
	RCTablePtr RefreshTable(std::string const& table_path, uint packrow_size = MAX_PACK_ROW_SIZE, bool fill_up_last_packrow = false);
	void ApplyPendingChanges(THD* thd, const std::string& table_path, struct st_table_share* table_share);
	void ApplyPendingChanges(THD* thd, const std::string& table_path, struct st_table_share* table_share, RCTable* tab);
	void Commit(THD* thd);
	void Rollback(THD* thd, bool force_error_message);

	bool ReadyForEnd() { return true; }
};

#endif
