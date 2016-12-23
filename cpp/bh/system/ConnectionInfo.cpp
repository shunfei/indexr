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

#include <vector>
#include <set>
#include <algorithm>
#include <boost/bind.hpp>
#include <boost/lambda/lambda.hpp>

#include "BHToolkit.h"
#include "ConnectionInfo.h"
#include "Channel.h"
#include "RCSystem.h"
#include "system/ib_system.h"
#include "edition/core/Transaction.h"
#include "core/TransactionManager.h"
#include "core/RCEngine.h"


using namespace std;
using namespace boost;
namespace bl = boost::lambda;

IBThreadStorage<ConnectionInfo> ConnectionInfoOnTLS;

Transaction* GetCurrentTransaction()
{
#ifdef PURE_LIBRARY
	return 0; //Dataprocessor should not need Transaction
#else
	if ( (( process_type == ProcessType::MYSQLD ) || ( process_type == ProcessType::EMBEDDED )) && ConnectionInfoOnTLS.IsValid() )
		return ConnectionInfoOnTLS.Get().GetTransaction();
	else // BHLoader
		return 0;
#endif
}

bool TableLock::operator==(const TableLock& table_lock) const
{
	return	table_path == table_lock.table_path &&
			lock_type == table_lock.lock_type &&
			trans.GetID() == table_lock.trans.GetID();
}

ConnectionInfo::ConnectionInfo(THD* thd)
	:	current_transaction(NULL),thd(thd)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( getenv("BHUT") || thd!=NULL );
	memset(&pi, 0, sizeof(pi));
	display_attr_stats = false;	
	display_lock = 0;
	srand((uint)time(0) + (uint)(uint64)&thd);
	GetNewTransaction(*rceng, false);
	//control_messages = Configuration::GetProperty(Configuration::ControlMessages);
}

ConnectionInfo::~ConnectionInfo()
{
	Transaction *t;
	while(current_transaction) {
		t = current_transaction;
		current_transaction = t->getParent();
		trs_mngr.RemoveTransaction(t);
		delete t;
		
	}
}

ulong ConnectionInfo::GetThreadID() const
{
#ifndef PURE_LIBRARY
	return thd ? thd->thread_id : 0;
#else
	return get_raw_thread_id( this_thread::get_id() );
#endif
}

THD& ConnectionInfo::Thd() const
{
	return *thd;
}

bool ConnectionInfo::killed() const
{
	return (thd ? thd->killed != 0 : false);
}

void ConnectionInfo::SuspendDisplay()
{			
	dlmx.Lock();
	display_lock++;
	dlmx.Unlock();
}
void ConnectionInfo::ResumeDisplay()
{	
	dlmx.Lock();
	display_lock--;
	dlmx.Unlock();
}

void ConnectionInfo::ResetDisplay()
{	
	dlmx.Lock();
	display_lock = 0;
	dlmx.Unlock();
	ConfigureRCControl();
}

void ConnectionInfo::EndTransaction()
{
	BHASSERT(current_transaction != NULL, "Transaction is NULL");
	bool explicit_lock_tables = current_transaction->m_explicit_lock_tables;
	Transaction *prev = current_transaction;
	current_transaction = current_transaction->getParent();
	trs_mngr.RemoveTransaction(prev);
	delete prev;
	if(!current_transaction)
		GetNewTransaction(*rceng, explicit_lock_tables);
}

bool IBTableShared::HasWriteLockForThread(const TransactionBase& trans) const
{
	return !!write_lock && write_lock->Trans().GetID() == trans.GetID();
}

bool IBTableShared::HasWriteLock() const
{
	return !!write_lock;
}

bool IBTableShared::HasReadLock() const
{
	return read_lock_list.size() != 0;
}

bool IBTableShared::HasAnyLockForThread(const TransactionBase& trans)
{
	IBGuard guard(lock_request_sync);
	return HasWriteLockForThread(trans) || HasReadLockForThread(trans);
}

bool IBTableShared::HasReadLockForThread(const TransactionBase& trans)
{
	IBGuard guard(lock_request_sync);
	return find_if(read_lock_list.begin(), read_lock_list.end(),
			*bl::_1 == TableLock(table_path, trans, F_RDLCK)) !=  read_lock_list.end();
}

bool IBTableShared::HasReadLockForOtherThread(const TransactionBase& trans)
{
	IBGuard guard(lock_request_sync);
	TablesLocksList::const_iterator iter = read_lock_list.begin();
	TablesLocksList::const_iterator end  = read_lock_list.end();
	for(; iter != end; ++iter)
		if((*iter)->IsReadLock() && (*iter)->Trans().GetID() != trans.GetID())
			return true;
	return false;
}

TableLockWeakPtr IBTableShared::CreateReadLockForThread(const TransactionBase& trans)
{
	TableLockPtr table_lock(new TableLock(table_path, trans, F_RDLCK));
	read_lock_list.push_back(table_lock);
	return TableLockWeakPtr(table_lock);
}

TableLockWeakPtr IBTableShared::CreateWriteLockForThread(const TransactionBase& trans)
{
	if(!HasWriteLockForThread(trans))
		write_lock = TableLockPtr(new TableLock(table_path, trans, F_WRLCK));
	return TableLockWeakPtr(write_lock);
}

void IBTableShared::ReleaseIfReadLock(TableLockWeakPtr lock)
{
	IBGuard guard(lock_request_sync);
	TableLockPtr tlock = lock.lock();
	if(tlock && tlock->IsReadLock()) {
		TablesLocksList::iterator to_del = find_if(read_lock_list.begin(), read_lock_list.end(), bl::_1 == tlock);
		if(to_del != read_lock_list.end())
			read_lock_list.erase(to_del);
	}
}

Transaction * 
ConnectionInfo::GetNewTransaction( const RCEngine &engine, bool explict_lock_tables) {
	current_transaction = new Transaction(current_transaction, const_cast<RCEngine&>(engine), engine.getGlobalDataCache());
	current_transaction->m_explicit_lock_tables = explict_lock_tables;
	trs_mngr.AddTransaction(current_transaction);
	return current_transaction;
}

Transaction * 
ConnectionInfo::GetTransaction( ) { return current_transaction; }

void TableLockManager::RegisterTable(const std::string& table_path)
{
	IBGuard guard(mutex);
	registered_tables.insert(make_pair(table_path, IBTableSharedPtr(new IBTableShared(table_path))));
}

void TableLockManager::UnRegisterTable(const std::string& table_path)
{
	IBGuard guard(mutex);
	std::map<std::string, IBTableSharedPtr>::iterator it = registered_tables.find(table_path);
	registered_tables.erase(table_path);
}

TableLockWeakPtr TableLockManager::AcquireWriteLock(const TransactionBase& trans, const std::string& table_path, ThreadKilledState_T& killed, bool& some_tables_deleted )
{
	TableLockWeakPtr lock = GetTableShare(table_path)->AcquireWriteLock(trans, killed, some_tables_deleted);
	return lock;
}

TableLockWeakPtr TableLockManager::AcquireReadLock(const TransactionBase& trans, const std::string& table_path, ThreadKilledState_T& killed, bool& some_tables_deleted)
{
	return GetTableShare(table_path)->AcquireReadLock(trans, killed, some_tables_deleted);
}

void TableLockManager::ReleaseLocks(const TransactionBase& trans, const std::string& table_path)
{
	GetTableShare(table_path)->ReleaseLocks(trans);
}

void TableLockManager::ReleaseLocks(const TransactionBase& trans)
{
	IBGuard guard(mutex);
	std::map<std::string, IBTableSharedPtr>::iterator iter = registered_tables.begin();
	std::map<std::string, IBTableSharedPtr>::iterator end =	registered_tables.end();
	for(; iter != end; ++iter)
		iter->second->ReleaseLocks(trans);
}

void TableLockManager::ReleaseWriteLocks(const TransactionBase& trans)
{
	IBGuard guard(mutex);
	std::map<std::string, IBTableSharedPtr>::iterator iter = registered_tables.begin();
	std::map<std::string, IBTableSharedPtr>::iterator end =	registered_tables.end();
	for(; iter != end; ++iter)
		iter->second->ReleaseWriteLocks(trans);
}

void TableLockManager::ReleaseLockIfNeeded(TableLockWeakPtr table_lock, const string& table_path, bool is_autocommit_on, bool in_alter_table)
{
	if(is_autocommit_on || in_alter_table)
		GetTableShare(table_path)->ReleaseLock(table_lock);
	else
		GetTableShare(table_path)->ReleaseIfReadLock(table_lock);
}

bool TableLockManager::HasAnyLockForTable(const TransactionBase& trans, const std::string& table_path)
{
	return GetTableShare(table_path)->HasAnyLockForThread(trans);
}

IBTableSharedPtr TableLockManager::GetTableShare(const std::string& table_path)
{
	IBGuard guard(mutex);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(registered_tables.find(table_path) != registered_tables.end());
	return registered_tables[table_path];
}
