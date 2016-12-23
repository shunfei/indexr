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

#ifndef _SYSTEM_CONNECTIONINFO_H_
#define _SYSTEM_CONNECTIONINFO_H_

#include "common/CommonDefinitions.h"
#include "system/ib_system.h"

#include <set>
#include <map>
#include <vector>
#include <list>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>

class THD;
class Channel;
class Transaction;
class TransactionBase;
class RCEngine;

#ifndef __WIN__
typedef pid_t process_info;
#else
typedef PROCESS_INFORMATION process_info;
#endif


//TODO: <Michal> move TableLock to separated file

class TableLock
{
public:
	TableLock(const std::string& table_path, const TransactionBase& trans, int lock_type)
		:	table_path(table_path), lock_type(lock_type), trans(trans)
	{}

	bool operator==(const TableLock& table_lock) const;

	bool IsWriteLock() const	{ return lock_type == F_WRLCK; }
	bool IsReadLock() const		{ return lock_type == F_RDLCK; }

	//const THD& Thd() const		{ return *thd; }
	const TransactionBase& Trans() const { return trans; }

	const std::string& TablePath() const { return table_path; }

private:
	std::string table_path;
	int lock_type;
	const TransactionBase& trans;
};

typedef boost::shared_ptr<TableLock> TableLockPtr;
typedef std::vector<TableLockPtr> TablesLocksList;
typedef std::set<std::string> TablesList;
typedef std::auto_ptr<TablesList > TablesListAutoPtr;


Transaction* GetCurrentTransaction();

class ConnectionInfo : public boost::noncopyable
{
public:
	ConnectionInfo(THD* thd);
	~ConnectionInfo();
	
	ulong	GetThreadID() const;
	THD&	Thd() const;
	bool 	killed() const;

	/** Notify the end of transaction - switch transaction number
	 */
	void				EndTransaction();
	
	Transaction * GetNewTransaction( const RCEngine &engine, bool explicit_lock_tables); 
	Transaction * GetTransaction( );


	// Statistical stuff
	void	ResetStatistics()						{ plmx.Lock(); packs_loaded=0; plmx.Unlock();}
	void	NotifyPackLoad()						{ plmx.Lock(); packs_loaded++; plmx.Unlock();}
	_int64	GetPackCount()							{ return packs_loaded; }

	void	SetDisplayAttrStats(bool v = true)		{ display_attr_stats = v; }
	bool	DisplayAttrStats()						{ return display_attr_stats; }
	void SuspendDisplay();
	void ResumeDisplay() ;
	void ResetDisplay() ;	
	void	SetDisplayLock(int v)				{ dlmx.Lock(); display_lock = v; dlmx.Unlock();}
	int		GetDisplayLock()					{ return display_lock; }

private:
	IBMutex dlmx;
	IBMutex plmx;
	int		display_lock;			// if >0 disable messages e.g. in subqueries	
	//ulong m_thd_id;
	Transaction *current_transaction;
	
	//const int volatile& m_killed;

	_int64		packs_loaded;			// for query statistics
	bool		display_attr_stats;		// if set, then statistics on attributes should be displayed at the end of query
	THD* 		thd;
	//int			control_messages;
public:
	process_info pi;
};


class IBTableShared
{
	friend class TableLockManager;
public:
	IBTableShared(const std::string& table_path)
		:	table_path(table_path)
	{
	}

	TableLockWeakPtr 	AcquireWriteLock(const TransactionBase& trans, ThreadKilledState_T& killed, bool& some_tables_deleted);
	TableLockWeakPtr	AcquireReadLock(const TransactionBase& trans, ThreadKilledState_T& killed, bool& some_tables_deleted);
	void				ReleaseLock(TableLockWeakPtr lock);
	void				ReleaseIfReadLock(TableLockWeakPtr lock);
	void 				ReleaseWriteLocks(const TransactionBase& trans);
	void 				ReleaseLocks(const TransactionBase& trans);
	bool 				HasAnyLockForThread(const TransactionBase& trans);
	bool 				HasReadLockForOtherThread(const TransactionBase& trans);

	bool 				HasWriteLock() const;
	//const THD&			WriteLockOwner() const { return write_lock->Thd(); }

private:
	bool 				HasWriteLockForThread(const TransactionBase& trans) const;

	bool 				HasReadLock() const;
	bool 				HasReadLockForThread(const TransactionBase& trans);

	/** Create Table READ LOCK.
	 * 	\param thd Thread which ask for a LOCK
	 *  \return weak pointer to new TableLock
	 */
	TableLockWeakPtr	CreateReadLockForThread(const TransactionBase& trans);

	/** Create Table READ LOCK.
	 *  \param thd Thread which ask for a LOCK
	 *  \return weak pointer to new TableLock
	 */
	TableLockWeakPtr	CreateWriteLockForThread(const TransactionBase& trans);

private:
	TablesLocksList read_lock_list;
	TableLockPtr 	write_lock;
	std::string 	table_path;
	IBMutex 		lock_request_sync;		 	
#ifndef __BH_COMMUNITY__
	std::list<TableLockPtr> write_locks_queue;  // Queue of waiting write locks threads
#endif
};


typedef boost::shared_ptr<IBTableShared> IBTableSharedPtr;

class TableLockManager
{
public:

	void RegisterTable(const std::string& table_path);
	void UnRegisterTable(const std::string& table_path);

	TableLockWeakPtr AcquireWriteLock(const TransactionBase& trans, const std::string& table_path, ThreadKilledState_T& killed, bool& some_tables_deleted);
	TableLockWeakPtr AcquireReadLock(const TransactionBase& trans, const std::string& table_path, ThreadKilledState_T& killed, bool& some_tables_deleted);


	/** Release all if
	 * 	it is READ LOCK
	 *	it is WRITE LOCK and AUTOCOMMIT is turned off
	 *  \param table_lock table lock
	 */
	void ReleaseLockIfNeeded(TableLockWeakPtr table_lock, const std::string&, bool is_autocommit_on, bool in_alter_table);

	/** Release all LOCKS owned by a given thread on a given table
	 *  \param thd thread object
	 * 	\param table_path table identifier
	 */
	void ReleaseLocks(const TransactionBase& trans, const std::string& table_path);
	void ReleaseLocks(const TransactionBase& trans);
	void ReleaseWriteLocks(const TransactionBase& trans);

	bool HasAnyLockForTable(const TransactionBase& trans, const std::string& table_path);

private:
	IBTableSharedPtr GetTableShare(const std::string& table_path);

private:

	std::map<std::string, IBTableSharedPtr> registered_tables;
	IBMutex mutex;
};


#endif //_SYSTEM_CONNECTIONINFO_H_

