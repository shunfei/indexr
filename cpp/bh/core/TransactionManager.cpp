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

#include "TransactionManager.h"

TransactionManager trs_mngr;

void TransactionManager::AddTransaction(Transaction* trs)
{
	IBGuard mutex_guard(mutex);
	m_transactions.push_back(trs);
}

void TransactionManager::RemoveTransaction(Transaction* trs)
{
	IBGuard mutex_guard(mutex);
	m_transactions.remove(trs);
}

#ifndef __BH_COMMUNITY__
void TransactionManager::LockLastPacks(const std::string& table_path)
{
	Transaction* thisTrs = ConnectionInfoOnTLS.Get().GetTransaction();
	IBGuard mutex_guard(mutex);
	std::list<Transaction*>::iterator iter;
	for(iter = m_transactions.begin(); iter != m_transactions.end(); ++iter) {
		if (*iter!=thisTrs)
			(*iter)->LockLastPacks(table_path);
	}
}
#endif

void TransactionManager::DropTable(const std::string& table_path)
{
	IBGuard mutex_guard(mutex);
	std::list<Transaction*>::iterator iter;
	for(iter = m_transactions.begin(); iter != m_transactions.end(); ++iter) {
		(*iter)->DropTable(table_path);
	}
}
