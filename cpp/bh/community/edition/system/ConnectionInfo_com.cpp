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

#include <boost/lambda/lambda.hpp>

#include "system/ConnectionInfo.h"
#include "edition/core/Transaction.h"

using namespace std;
namespace bl = boost::lambda;

TableLockWeakPtr IBTableShared::AcquireWriteLock(const TransactionBase& trans, volatile THD::killed_state& killed, bool& some_tables_deleted)
{
	while(!killed && !some_tables_deleted) {
		{
			IBGuard guard(lock_request_sync);
			if(HasWriteLockForThread(trans))
				return write_lock;
			else if(!HasWriteLock() && !HasReadLockForOtherThread(trans))
				return CreateWriteLockForThread(trans);
		}
		SleepNow(100);
	}
	return TableLockPtr();
}


TableLockWeakPtr IBTableShared::AcquireReadLock(const TransactionBase& trans, volatile THD::killed_state& killed, bool& some_tables_deleted)
{
	while(!killed && !some_tables_deleted) {
		{
			IBGuard guard(lock_request_sync);
			if(HasWriteLockForThread(trans) || !HasWriteLock())
				return CreateReadLockForThread(trans);
		}
		SleepNow(100);
	}
	return TableLockPtr();
}

void IBTableShared::ReleaseLock(TableLockWeakPtr lock)
{
	IBGuard guard(lock_request_sync);
	TableLockPtr tlock = lock.lock();
	if(tlock) {
		if(tlock->IsWriteLock()) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(write_lock.get() == tlock.get());
			write_lock.reset();
		} else {
			TablesLocksList::iterator to_del = find_if(read_lock_list.begin(), read_lock_list.end(), bl::_1 == tlock);
			if(to_del != read_lock_list.end())
				read_lock_list.erase(to_del);
		}
	}
}

void IBTableShared::ReleaseWriteLocks(const TransactionBase& trans)
{
	IBGuard guard(lock_request_sync);
	if(HasWriteLockForThread(trans)) {
		write_lock.reset();
	}
}

void IBTableShared::ReleaseLocks(const TransactionBase& trans)
{
	IBGuard guard(lock_request_sync);
	if(HasWriteLockForThread(trans)) {
		write_lock.reset();
	}

	read_lock_list.erase(
		remove_if(read_lock_list.begin(), read_lock_list.end(),	*bl::_1 == TableLock(table_path, boost::ref(trans), F_RDLCK)),
		read_lock_list.end()
		);
}

