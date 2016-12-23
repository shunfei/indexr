/* Copyright (C) 2000 MySQL AB, portions Copyright (C) 2005-2008 Infobright Inc.

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

#ifndef PURE_LIBRARY


#include <iostream>

#include <boost/function.hpp>
#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>

#define MYSQL_SERVER  1

#include "edition/local.h"
#include "BrighthouseHandler.h"
#include "mysql/plugin.h"

#include "system/RCException.h"
#include "core/compilation_tools.h"
#include "common/bhassert.h"
#include "core/tools.h"
#include "core/TempTable.h"
#include "edition/vc/VirtualColumn.h"
#include "edition/core/Transaction.h"
#include "domaininject/DomainInjectionsDictionary.h"
#include "system/MemoryManagement/Initializer.h"

using namespace std;

handler *rcbase_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);
int rcbase_panic_func(handlerton *hton, enum ha_panic_function flag);
int rcbase_close_connection(handlerton *hton, THD* thd);
int rcbase_commit(handlerton *hton, THD *thd, mysql_bool all);
int rcbase_rollback(handlerton *hton, THD *thd, mysql_bool all);
int rcbase_init_func(void *p);
bool rcbase_show_status(handlerton *hton, THD *thd, stat_print_fn *print, enum ha_stat_type stat);

handlerton *rcbase_hton;

/* Variables for rcbase share methods */
HASH rcbase_open_tables; // Hash used to track open tables
//pthread_mutex_t rcbase_mutex; // This is the mutex we use to init the hash
IBMutex rcbase_mutex;

/*
 Function we use in the creation of our hash to get key.
 */
uchar* rcbase_get_key(RCBASE_SHARE *share, size_t *length, my_bool not_used __attribute__((unused)))
{
	*length=share->table_name_length;
	return (uchar*) share->table_name;
}

/*
 If frm_error() is called then we will use this to to find out what file extentions
 exist for the storage engine. This is also used by the default rename_table and
 delete_table method in handler.cc.
 */
my_bool infobright_bootstrap = 0;

static int rcbase_done_func(void *p)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
//	int error= 0;

//	if(rcbase_open_tables.records)
//		error= 1;   //anyway unused
	if (rceng) {
		MemoryManagerInitializer::EnsureNoLeakedTrackableObject();
		delete rceng;
		rceng = NULL;
	}
		
	hash_free(&rcbase_open_tables);
	//pthread_mutex_destroy(&rcbase_mutex);

	DBUG_RETURN(0);
}

int rcbase_panic_func(handlerton *hton, enum ha_panic_function flag)
{
	if(infobright_bootstrap)
		return 0;

	if(flag == HA_PANIC_CLOSE) {
		MemoryManagerInitializer::EnsureNoLeakedTrackableObject();
		delete rceng;
		rceng = NULL;
	}
	return 0;
}

int rcbase_close_connection(handlerton *hton, THD* thd)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = 1;
	try {
		rcbase_rollback(hton, thd, true);
		table_lock_manager.ReleaseLocks(*rceng->GetThreadParams(*thd)->GetTransaction());
		rceng->ClearThreadParams(*thd);
		ret = 0;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}

int rcbase_commit(handlerton *hton, THD *thd, mysql_bool all)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = 1;
	string error_message;

	// GA: double check that thd->no_errors is the same as query_error
	if(!(thd->no_errors != 0 || thd->killed || thd->transaction_rollback_request)) {
		try {
			rceng->Commit(thd, all);
			ret = 0;
		} catch (std::exception& e) {
			error_message = string("Error: ") + e.what();
		} catch (...) {
			error_message = "An unknown system exception error caught.";
		}
	}

	if(ret) {
		try {
			rceng->Rollback(thd, all, true);
			if(!error_message.empty()) {
				rclog << lock << error_message << unlock;
				my_message(BHERROR_UNKNOWN, error_message.c_str(), MYF(0));
			}
		} catch(std::exception& e) {
			if(!error_message.empty()) {
				rclog << lock << error_message << unlock;
				my_message(BHERROR_UNKNOWN, error_message.c_str(), MYF(0));
			}
			my_message(BHERROR_UNKNOWN, (string("Failed to rollback transaction. Error: ") + e.what()).c_str(), MYF(0));
			rclog << lock << string("Failed to rollback transaction. Error: ") + e.what() << unlock;
		} catch(...) {
			if(!error_message.empty()) {
				rclog << lock << error_message << unlock;
				my_message(BHERROR_UNKNOWN, error_message.c_str(), MYF(0));
			}
			my_message(BHERROR_UNKNOWN, string("Failed to rollback transaction. Unknown error.").c_str(), MYF(0));
			rclog << lock << string("Failed to rollback transaction. Unknown error.").c_str() << unlock;
		}
	}

	DBUG_RETURN(ret);
}

int rcbase_rollback(handlerton *hton, THD *thd, mysql_bool all)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = 1;
	try {
		rceng->Rollback(thd, all);
		ret = 0;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}

/*
 Example of simple lock controls. The "share" it creates is structure we will
 pass to each rcbase handler. Do you have to have one of these? Well, you have
 pieces that are used for locking, and they are needed to function.
 */
static RCBASE_SHARE *get_share(const char *table_name, TABLE *table)
{
	RCBASE_SHARE *share;
	uint length;
	char *tmp_name;

    rcbase_mutex.Lock();
	length=(uint) strlen(table_name);

	if(!(share=(RCBASE_SHARE*) hash_search(&rcbase_open_tables, (uchar*) table_name, length))) {
		if(!(share=(RCBASE_SHARE *) my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(*share), &tmp_name,
				length+1, NullS))) {
            rcbase_mutex.Unlock();
			return NULL;
		}

		share->use_count=0;
		share->table_name_length=length;
		share->table_name=tmp_name;
		strmov(share->table_name, table_name);
		if(my_hash_insert(&rcbase_open_tables, (uchar*) share))
			goto error;
		thr_lock_init(&share->lock);
	}
	share->use_count++;
    rcbase_mutex.Unlock();

	return share;

error:
	my_free((uchar*) share, MYF(0));
	return NULL;
}

/*
 Free lock controls. We call this whenever we close a table. If the table had
 the last reference to the share then we free memory associated with it.
 */
static int free_share(RCBASE_SHARE *share)
{
	//pthread_mutex_lock(&rcbase_mutex);
    rcbase_mutex.Lock();
	if(!--share->use_count) {
		hash_delete(&rcbase_open_tables, (uchar*) share);
		thr_lock_delete(&share->lock);
		//pthread_mutex_destroy(&share->mutex);
		my_free((uchar*) share, MYF(0));
	}
    rcbase_mutex.Unlock();
	//pthread_mutex_unlock(&rcbase_mutex);

	return 0;
}

handler* rcbase_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
	handler* ret = 0;
	try	{
		ret = new (mem_root) BrighthouseHandler(hton, table);
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
	return ret;
}

my_bool rcbase_query_caching_of_table_permitted(THD* thd, char* full_name, uint full_name_len, ulonglong *unused)
{
	if (!(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
		return ((my_bool)TRUE);
	return ((my_bool)FALSE);
}


const static char *hton_name = "BRIGHTHOUSE";

bool rcbase_show_status(handlerton *hton, THD *thd, stat_print_fn *pprint, enum ha_stat_type stat)
{
	std::ostringstream buf(std::ostringstream::out);

	buf << endl;

	if( stat == HA_ENGINE_STATUS) {
		TrackableObject::Instance()->HeapHistogram(buf);
		return pprint( thd, hton_name, uint(strlen(hton_name)),
				"Heap Histograms", uint(strlen("Heap Histograms")),
				buf.str().c_str(), uint(buf.str().length()) );
	}
	return false;
}

BrighthouseHandler::BrighthouseHandler(handlerton *hton, TABLE_SHARE *table_arg)
	: handler(hton, table_arg), share(NULL), m_conn(NULL), m_table_path(NULL), release_read_table(false), release_insert_from_select(false),
		table_ptr( NULL ), m_result(false), copy_data_in_alter_table(false), m_rctable(NULL)
{
	err_msg[0] = '\0';
	ref_length = sizeof(uint64);
}

BrighthouseHandler::~BrighthouseHandler()
{
	delete [] m_table_path;
}

const char **BrighthouseHandler::bas_ext() const
{
	return ha_rcbase_exts;
}

namespace {

//JustATable::RcTableLockPolicy::attr_uses_t get_attr_uses(st_table* table) {
//	int col_id = 0;
//	JustATable::RcTableLockPolicy::attr_uses_t attr_uses;
//	for(Field** field = table->field; *field; ++field, ++col_id) {
//		if(bitmap_is_set(table->read_set, col_id)) { //this time no '+1' is needed
//			attr_uses.push_back(col_id);
//		}
//	}
//	return attr_uses;
//}

std::vector<bool> GetAttrsUseIndicator(st_table* table)
{
	int col_id = 0;
	std::vector<bool> attr_uses;
	for(Field** field = table->field; *field; ++field, ++col_id) {
		attr_uses.push_back(bitmap_is_set(table->read_set, col_id));
	}
	return attr_uses;
}

}

/*
 ::info() is used to return information to the optimizer.
 see my_base.h for the complete description

 Currently this table handler doesn't implement most of the fields
 really needed. SHOW also makes use of this data
 Another note, you will probably want to have the following in your
 code:
 if (records < 2)
 records = 2;
 The reason is that the server will optimize for cases of only a single
 record. If in a table scan you don't know the number of records
 it will probably be better to set records to two so you can return
 as many records as you need.
 Along with records a few more variables you may wish to set are:
 records
 deleted
 data_file_length
 index_file_length
 delete_length
 check_time
 Take a look at the public variables in handler.h for more information.

 Called in:
 filesort.cc
 ha_heap.cc
 item_sum.cc
 opt_sum.cc
 sql_delete.cc
 sql_delete.cc
 sql_derived.cc
 sql_select.cc
 sql_select.cc
 sql_select.cc
 sql_select.cc
 sql_select.cc
 sql_show.cc
 sql_show.cc
 sql_show.cc
 sql_show.cc
 sql_table.cc
 sql_union.cc
 sql_update.cc

 */
int BrighthouseHandler::info(uint flag)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	int ret = 1;
	try {
		if(m_table_path) {
			IBGuard global_mutex_guard(global_mutex);
			RCTablePtr tab;
			if(ConnectionInfoOnTLS.IsValid()) {
				#if defined(__WIN__) && defined(EMBEDDED_LIBRARY)
				THD& thd = ConnectionInfoOnTLS.Get().Thd();
				ConnectionInfoOnTLS.Get().GetTransaction()->ApplyPendingChanges(&thd, m_table_path, table_share);
				#else
				ConnectionInfoOnTLS.Get().GetTransaction()->ApplyPendingChanges(current_thd, m_table_path, table_share);
				#endif
				tab = ConnectionInfoOnTLS.Get().GetTransaction()->GetTableShared(m_table_path, table_share);
			}
			else tab = rceng->GetTableShared(m_table_path, table_share);
			if(flag & HA_STATUS_VARIABLE) {
				stats.records = (ha_rows)tab->NoValues();
				stats.data_file_length = 0;
				stats.mean_rec_length = 0;
				if(stats.records > 0) {
					std::vector<AttrInfo> attr_info(tab->GetAttributesInfo());
					uint no_attrs = tab->NoAttrs();
					for(uint j=0; j < no_attrs; j++)
						stats.data_file_length += attr_info[j].comp_size; // compressed size
					stats.mean_rec_length = ulong(stats.data_file_length / stats.records);
				}
			}
			if(flag & HA_STATUS_CONST)
				stats.create_time = ulong(tab->GetCreateTime());
			if(flag & HA_STATUS_TIME)
				stats.update_time = ulong(tab->GetUpdateTime());
		}
		ret = 0;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}

/*
 Used for opening tables. The name will be the name of the file.
 A table is opened when it needs to be opened. For instance
 when a request comes in for a select on the table (tables are not
 open and closed for each request, they are cached).

 Called from handler.cc by handler::ha_open(). The server opens all tables by
 calling ha_open() which then calls the handler specific open().
 */
int BrighthouseHandler::open(const char *name, int mode, uint test_if_locked)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = 1;
	m_conn = rceng->GetThreadParams(*table->in_use);
	table_lock_manager.RegisterTable(name);

	if(!(share = get_share(name, table)))
		DBUG_RETURN(ret);
	thr_lock_data_init(&share->lock, &m_lock, NULL);

	m_table_path = new char[strlen(name) + 1];
	strcpy(m_table_path, name);
	try {
		m_conn->GetTransaction()->GetTable(name, table_share);
		ret = 0;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	info(HA_STATUS_CONST);

	DBUG_RETURN(ret);
}

/*
 Closes a table. We call the free_share() function to free any resources
 that we have allocated in the "shared" structure.

 Called from sql_base.cc, sql_select.cc, and table.cc.
 In sql_select.cc it is only used to close up temporary tables or during
 the process where a temporary table is converted over to being a
 myisam table.
 For sql_base.cc look at close_data_tables().
 */
int BrighthouseHandler::close(void)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(free_share(share));
}

/*
 Positions an index cursor to the index specified in the handle. Fetches the
 row if available. If the key value is null, begin at the first key of the
 index.
 */
int BrighthouseHandler::index_read(uchar * buf, const uchar * key, uint key_len __attribute__((unused)), enum ha_rkey_function find_flag
                           __attribute__((unused)))
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 Used to read forward through the index.
 */
int BrighthouseHandler::index_next(uchar * buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 Used to read backwards through the index.
 */
int BrighthouseHandler::index_prev(uchar * buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 index_first() asks for the first key in the index.

 Called from opt_range.cc, opt_sum.cc, sql_handler.cc,
 and sql_select.cc.
 */
int BrighthouseHandler::index_first(uchar * buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 index_last() asks for the last key in the index.

 Called from opt_range.cc, opt_sum.cc, sql_handler.cc,
 and sql_select.cc.
 */
int BrighthouseHandler::index_last(uchar * buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/*
 rnd_init() is called when the system wants the storage engine to do a table
 scan.
 See the example in the introduction at the top of this file to see when
 rnd_init() is called.

 Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
 and sql_update.cc.
 */
int BrighthouseHandler::rnd_init(mysql_bool scan)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	//FIXME there is no exception safety here - try-catch was added here to block exceptions
	//			and not letting them crash the server
	int ret = 1;
	try {
		if(m_query && !m_result && table_ptr->NoObj() != 0) {
			m_cq->Result(m_tmp_table); //it is ALWAYS -2 though....
			m_result = true;

			try {
				FunctionExecutor fe(
					boost::bind(&Query::LockPackInfoForUse, boost::ref(m_query)),
					boost::bind(&Query::UnlockPackInfoFromUse, boost::ref(m_query))
				);

				TempTable* push_down_result = m_query->Preexecute(*m_cq, NULL, false);
				if(!push_down_result || push_down_result->NoTables() != 1)
					throw InternalRCException("Query execution returned no result object");

				Filter* filter(push_down_result->GetMultiIndexP()->GetFilter(0));
				if(!filter)
					filter_ptr = FilterAutoPtr(new Filter(push_down_result->GetMultiIndexP()->OrigSize(0)));
				else
					filter_ptr = FilterAutoPtr(new Filter(*filter));

				table_ptr = push_down_result->GetTableP(0);
				//table_iter = table_ptr->begin(m_conn, *filter, get_attr_uses(table));
				//table_iter_end = table_ptr->end(m_conn);
				table_new_iter = 		((RCTable*)table_ptr)->Begin(GetAttrsUseIndicator(table), *filter);
				table_new_iter_end =	((RCTable*)table_ptr)->End();
				SimpleDelete();
			} catch(RCException const& e) {
				rccontrol << lock << "Error in push-down execution, push-down execution aborted: " << e.what() << unlock;
				rclog << lock << "Error in push-down execution, push-down execution aborted: " << e.what() << unlock;
			}
			m_query.reset();
			m_cq.reset();
		} else {
			if(scan && filter_ptr.get()) {
				//table_iter = table_ptr->begin(m_conn, *filter_ptr, get_attr_uses(table));
				//table_iter_end = table_ptr->end(m_conn);
				table_new_iter = 		((RCTable*)table_ptr)->Begin(GetAttrsUseIndicator(table), *filter_ptr);
				table_new_iter_end =	((RCTable*)table_ptr)->End();
			} else {
				//rceng->GetTableIterator(m_table_path, table_iter, table_iter_end, table_ptr, get_attr_uses(table), table->in_use, m_conn);
				rceng->GetTableIterator(m_table_path, table_new_iter, table_new_iter_end, table_ptr, GetAttrsUseIndicator(table), table->in_use, m_conn);
				filter_ptr.reset();
			}
		}
		ret = 0;
		blob_buffers.resize(0);
		if (table_ptr!=NULL)
			blob_buffers.resize(table_ptr->NoDisplaybleAttrs());
	} catch(std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch(...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}

int BrighthouseHandler::rnd_end()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	reset();
	table->sort.found_records = 0;

	DBUG_RETURN(0);
}

/*
 This is called for each row of the table scan. When you run out of records
 you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
 The Field structure for the table is the key to getting data into buf
 in a manner that will allow the server to understand it.

 Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
 and sql_update.cc.
 */
int BrighthouseHandler::rnd_next(uchar* buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = HA_ERR_END_OF_FILE;
	try {
		table->status = 0;
		if(fill_row(buf) == HA_ERR_END_OF_FILE) {
			table->status = STATUS_NOT_FOUND;
			DBUG_RETURN(ret);
		}
		ret = 0;
	} catch(std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch(...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}

/*
 position() is called after each call to rnd_next() if the data needs
 to be ordered. You can do something like the following to store
 the position:
 my_store_ptr(ref, ref_length, current_position);

 The server uses ref to store data. ref_length in the above case is
 the size needed to store current_position. ref is just a byte array
 that the server will maintain. If you are using offsets to mark rows, then
 current_position should be the offset. If it is a primary key like in
 BDB, then it needs to be a primary key.

 Called from filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc.
 */
void BrighthouseHandler::position(const uchar *record)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	my_store_ptr(ref, ref_length, current_position);

	DBUG_VOID_RETURN;
}

/*
 This is like rnd_next, but you are given a position to use
 to determine the row. The position will be of the type that you stored in
 ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
 or position you saved when position() was called.
 Called from filesort.cc records.cc sql_insert.cc sql_select.cc sql_update.cc.
 */
int BrighthouseHandler::rnd_pos(uchar* buf, uchar* pos)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = HA_ERR_END_OF_FILE;
	try {
		uint64 position = my_get_ptr(pos, ref_length);
		//table_iter = table_ptr->find(m_conn, position, filter_ptr.get(), get_attr_uses(table));
		table_new_iter.MoveToRow(position);
		table->status = 0;
		if(fill_row(buf) == HA_ERR_END_OF_FILE) {
			table->status = STATUS_NOT_FOUND;
			DBUG_RETURN(ret);
		}
		ret = 0;
	} catch(std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch(...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}


/*
 extra() is called whenever the server wishes to send a hint to
 the storage engine. The myisam engine implements the most hints.
 ha_innodb.cc has the most exhaustive list of these hints.
 */
int BrighthouseHandler::extra(enum ha_extra_function operation)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	/* This preemptive delete might cause problems here.
	 * Other place where it can be put is BrighthouseHandler::external_lock().
	 */
	if ( operation == HA_EXTRA_NO_CACHE ) {
		m_cq.reset();
		m_query.reset();

	}
	DBUG_RETURN(0);
}

int BrighthouseHandler::start_stmt(THD *thd, thr_lock_type lock_type)
{
	try {
		m_conn = rceng->GetThreadParams(*thd);
		if(lock_type == TL_WRITE_CONCURRENT_INSERT || lock_type==TL_WRITE_DEFAULT || lock_type==TL_WRITE) { 
			if((table_lock = table_lock_manager.AcquireWriteLock(*m_conn->GetTransaction(), string(m_table_path), thd->killed, thd->some_tables_deleted)).expired())
				return 1;
			trans_register_ha(thd, true, rcbase_hton);
			trans_register_ha(thd, false, rcbase_hton);
			m_conn->GetTransaction()->AddTableWR(m_table_path, thd, table);
		}

	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
	return 0;
}

/*
 Ask rcbase handler about permission to cache table during query registration.
 If current thread is in non-autocommit, we don't permit any mysql query caching.
 */
my_bool BrighthouseHandler::register_query_cache_table(THD *thd, char *table_key,
   uint key_length,
   qc_engine_callback *call_back,
   ulonglong *engine_data )
{
	*call_back = rcbase_query_caching_of_table_permitted;
	return rcbase_query_caching_of_table_permitted(thd, table_key,	key_length,	0);
}

/*
 Used to delete a table. By the time delete_table() has been called all
 opened references to this table will have been closed (and your globally
 shared references released. The variable name will just be the name of
 the table. You will need to remove any files you have created at this point.

 If you do not implement this, the default delete_table() is called from
 handler.cc and it will delete all files with the file extentions returned
 by bas_ext().

 Called from handler.cc by delete_table and  ha_create_table(). Only used
 during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
 the storage engine.
 */
int BrighthouseHandler::delete_table(const char *name)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int ret = 1;
	try {
		rceng->DropTable(name);
		ret = 0;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(ret);
}


/*
 Given a starting key, and an ending key estimate the number of rows that
 will exist between the two. end_key may be empty which in case determine
 if start_key matches any rows.

 Called from opt_range.cc by check_quick_keys().
 */
ha_rows BrighthouseHandler::records_in_range(uint inx, key_range *min_key, key_range *max_key)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(10); // low number to force index usage
}

/*
 create() is called to create a database. The variable name will have the name
 of the table. When create() is called you do not need to worry about opening
 the table. Also, the FRM file will have already been created so adjusting
 create_info will not do you any good. You can overwrite the frm file at this
 point if you wish to change the table definition, but there are no methods
 currently provided for doing that.

 Called from handle.cc by ha_create_table().
 */
int BrighthouseHandler::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	BHErrorCode bhec = BHERROR_UNKNOWN;
	try {
		bhec = rceng->CreateTable(name, table_arg);
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
	if(bhec != BHERROR_SUCCESS) {
		BHError bhe(bhec);
		my_message(bhe.ErrorCode(), bhe.Message().c_str(), MYF(0));
		DBUG_RETURN(TRUE);
	}
	m_table_path = new char[strlen(name)+1];
	strcpy(m_table_path, name);
	info(HA_STATUS_CONST);

	DBUG_RETURN(0);
}

int BrighthouseHandler::fill_row(uchar* buf)
{
	if (table_new_iter == table_new_iter_end)
		return HA_ERR_END_OF_FILE;

//	if(!table_new_iter.UsedAttrsUpdated())
//		table_new_iter.UpdateUsedAttrs(GetAttrsUseIndicator(table));
	my_bitmap_map * org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

	int is_null = 0;
	int col_id = 0;

	for(Field **field = table->field; *field; field++, col_id++) {
		if(bitmap_is_set(table->read_set, col_id)) {
			RCEngine::Convert(is_null, *field, (*field)->ptr, *(*table_new_iter)[col_id], &blob_buffers[col_id]);
			if(is_null == 0)
				(*field)->set_notnull();
			else {
				::memset( (*field)->ptr, 0, 2 );
				(*field)->set_null();
			}
		} else {
			::memset( (*field)->ptr, 0, 2 );
			(*field)->set_null();
		}
	}
	current_position = table_new_iter.GetCurrentRowId();
	table_new_iter++;

	dbug_tmp_restore_column_map(table->write_set, org_bitmap);

	return 0;
}

char* BrighthouseHandler::update_table_comment(const char * comment)
{
	//FIXME there is no exception safety here - try-catch was added here to block exceptions
	//			and not letting them crash the server
	char* ret = const_cast<char*>(comment);
	try {
		uint length = (uint) strlen(comment);
		char* str= NULL;
		uint extra_len = 0;

		if(length > 64000 - 3) {
			return ((char*)comment); // string too long
		}

		//  get size & ratio
		int64 sum_c=0, sum_u=0, rsize=0;
		std::vector<AttrInfo> attr_info = rceng->GetTableAttributesInfo(m_table_path, table_share);
		for(uint j=0; j < attr_info.size(); j++) {
			sum_c += attr_info[j].comp_size;
			sum_u += attr_info[j].uncomp_size;
		}
		char buf[256];
#if ! defined (PURE_LIBRARY) && ! defined (__BH_COMMUNITY__)
		RCTablePtr table = rceng->GetTableShared(m_table_path, table_share);
		if (table->GetDeleteMask()->NoObj() == 0)
			rsize = 0;
		else
			rsize = (_int64)((double)(sum_u) * table->GetDeleteMask()->NoOnes() / table->GetDeleteMask()->NoObj());
		//int count = _snprintf(buf, 256, "Overall compression ratio: %.3f", ratio);
		double ratio = (sum_c > 0 ? (rsize) / double(sum_c) : 0);
		int count = sprintf(buf, "Overall compression ratio: %.3f, Raw size=%lld MB", ratio, (rsize >> 20));
#else
		double ratio = (sum_c > 0 ? double(sum_u) / double(sum_c) : 0);
		int count = sprintf(buf, "Overall compression ratio: %.3f, Raw size=%ld MB", ratio, sum_u >> 20);
#endif
		extra_len += count;

		str = (char*)my_malloc(length + extra_len + 3, MYF(0));
		if(str) {
			char* pos = str + length;
			if(length) {
				memcpy(str, comment, length);
				*pos++ = ';';
				*pos++ = ' ';
			}

			//memcpy_s(pos, extra_len, buf, extra_len);
			memcpy(pos, buf, extra_len);
			pos[extra_len] = 0;
		}
		ret = str;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	return ret;
	//	return (char*) comment;
}

mysql_bool BrighthouseHandler::get_error_message(int error, String* buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	DBUG_PRINT("enter", ("error: %d", error));
	if(error == HA_RCBASE_ERROR_TABLE_NOT_OPEN && err_msg) {
		buf->append(err_msg, (uint32)strlen(err_msg));
		err_msg[0] = '\0';
	}
	DBUG_PRINT("exit", ("message: %s", buf->ptr()));

	DBUG_RETURN(FALSE);
}

const COND* BrighthouseHandler::cond_push(const COND* a_cond)
{
	//FIXME there is no exception safety here - try-catch was added here to block exceptions
	//			and not letting them crash the server
	COND const* ret = a_cond;
	try {
		if(!m_query)
			rceng->GetTableIterator(m_table_path, table_new_iter, table_new_iter_end, table_ptr, GetAttrsUseIndicator(table), table->in_use, m_conn);

		COND* cond = const_cast<COND*>(a_cond);

		if(!m_query) {
			BHASSERT(m_conn != NULL, "should be 'm_conn!=NULL'");
			m_query.reset(new Query(m_conn));
			m_cq.reset(new CompiledQuery);
			m_result = false;

			m_query->Add((RCTable*)table_ptr);
			TabID t_out;
			m_cq->TableAlias(t_out, TabID(0)); //we apply it to the only table in this query
			m_cq->TmpTable(m_tmp_table, t_out);

			string ext_alias;
			if(table->pos_in_table_list->referencing_view)
				ext_alias = string(table->pos_in_table_list->referencing_view->table_name);
			else
				ext_alias = string(table->s->table_name.str);
			ext_alias += string(":") + string(table->alias);
			m_query->table_alias2index_ptr.insert(make_pair(ext_alias, make_pair(t_out.n, table)));

			int col_no = 0;
			AttrID col, at, vc;
			TabID tab(m_tmp_table);
			for(Field** field = table->field; *field; field++) {
				AttrID at;
				if(bitmap_is_set(table->read_set, col_no) ) {
					col.n = col_no++;
					m_cq->CreateVirtualColumn(vc.n, m_tmp_table, t_out, col);
					m_cq->AddColumn(at, m_tmp_table, CQTerm(vc.n), LISTING, (*field)->field_name, false);
				}
			}
		}

		if(m_result)
			return a_cond; // if m_result there is already a result command in compilation

		std::auto_ptr<CompiledQuery> tmp_cq(new CompiledQuery(*m_cq));
		CondID cond_id;
		if(!m_query->BuildConditions(cond, cond_id, tmp_cq.get(), m_tmp_table, WHERE_COND, false)) {
			m_query.reset();
			return a_cond;
		} 
		tmp_cq->AddConds(m_tmp_table, cond_id, WHERE_COND);
		tmp_cq->ApplyConds(m_tmp_table);
		m_cq.reset(tmp_cq.release());
		
		ret = 0;
	} catch(std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch(...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
	return ret;
}

void BrighthouseHandler::cond_pop()
{
	return;
}

int BrighthouseHandler::reset()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	//FIXME there is no exception safety here - try-catch was added here to block exceptions
	//			and not letting them crash the server
	int ret = 1;
	try {
		//table_iter = JustATable::RecordIterator();
		//table_iter_end = JustATable::RecordIterator();
		table_new_iter = 		RCTable::Iterator();
		table_new_iter_end =	RCTable::Iterator();
		table_ptr = NULL;
		m_rctable = NULL;
		filter_ptr.reset();
		m_query.reset();
		m_cq.reset();
		m_result = false;
		blob_buffers.resize(0);
		ret = 0;
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(0);
}

//int BrighthouseHandler::repair(THD* thd, HA_CHECK_OPT* check_opt)
//{
//	cerr << "BrighthouseHandler::ha_repair(THD* thd, HA_CHECK_OPT* check_opt)" << endl;
//	return 0;
//}

struct st_mysql_storage_engine brighthouse_storage_engine= { MYSQL_HANDLERTON_INTERFACE_VERSION };

int get_Freeable_StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) 
{ 
	*((_int64 *)tmp) = TrackableObject::GetFreeableSize(); 
	outvar->value = tmp; 
	outvar->type = SHOW_LONGLONG; 
	return 0; 
}

int get_UnFreeable_StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) 
{ 
	*((_int64 *)tmp) = TrackableObject::GetUnFreeableSize(); 
	outvar->value = tmp; 
	outvar->type = SHOW_LONGLONG; 
	return 0; 
}

int get_MemoryScale_StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) \
{ 
	*((_int64 *)tmp) = TrackableObject::MemorySettingsScale(); 
	outvar->value = tmp; 
	outvar->type = SHOW_LONGLONG; 
	return 0; 
}

/*
int get_packsLocked_StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) 
{ 
	*((_int64 *)tmp) = TrackableObject::GetPacksLocked(); 
	outvar->value = tmp; 
	outvar->type = SHOW_LONGLONG; 
	return 0; 
}
*/
//  showtype
//  =====================
//  SHOW_UNDEF, SHOW_BOOL, SHOW_INT, SHOW_LONG,
//  SHOW_LONGLONG, SHOW_CHAR, SHOW_CHAR_PTR,
//  SHOW_ARRAY, SHOW_FUNC, SHOW_DOUBLE


struct st_mysql_sys_var
{
  MYSQL_PLUGIN_VAR_HEADER;
};


int brighthouse_throw_error_func(MYSQL_THD thd, struct st_mysql_sys_var* var, void* save, struct st_mysql_value* value)
{
	int buffer_length = 512;
	char buff[512] = {0};

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(value->value_type(value) == MYSQL_VALUE_TYPE_STRING);

	my_message(BHERROR_UNKNOWN, value->val_str(value, buff, &buffer_length), MYF(0));
	return -1;
}

static void ib_update_func_str(THD *thd, struct st_mysql_sys_var *var, void *tgt, const void *save)
{
	char *old = *(char **)tgt;
	*(char **) tgt = *(char **)save;
	if(var->flags & PLUGIN_VAR_MEMALLOC) {
		*(char **) tgt = my_strdup(*(char**) save, MYF(0));
		my_free(old, MYF(0));
	}
}

void brighthouse_refresh_sys_infobright_func(MYSQL_THD thd, struct st_mysql_sys_var *var, void *tgt, const void *save)
{
	*(my_bool *) tgt= *(my_bool *) save ? TRUE : FALSE;
	DomainInjectionsDictionary::InvalidateTables(thd);
}


#define BH_STATUS_FUNCTION(name,showtype,member) \
int get_ ##name## _StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) \
{ \
	*((_int64 *)tmp) = GlobalDataCache::GetGlobalDataCache().member(); \
	outvar->value = tmp; \
	outvar->type = showtype; \
	return 0; \
}

#define BH_MM_STATUS_FUNCTION(name,showtype,member) \
int get_ ##name## _StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) \
{ \
	*((_int64 *)tmp) = TrackableObject::Instance()->member(); \
	outvar->value = tmp; \
	outvar->type = showtype; \
	return 0; \
}

#if !defined(__BH_COMMUNITY__)
#define BH_RMON_STATUS_FUNCTION(name,showtype, member) \
int get_ ##name## _StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) \
{ \
	*((_int64 *)tmp) = IBResourceMon::member; \
	outvar->value = tmp; \
	outvar->type = showtype; \
	return 0; \
}
#else
#define BH_RMON_STATUS_FUNCTION(name,showtype, member) \
int get_ ##name## _StatusVar(MYSQL_THD thd, struct st_mysql_show_var* outvar, char *tmp) \
{ \
	*((_int64 *)tmp) = 0; \
	outvar->value = tmp; \
	outvar->type = showtype; \
	return 0; \
}
#endif

#define BH_STATUS_MEMBER(name,label) { label, (char*)get_ ##name## _StatusVar, SHOW_FUNC }

BH_STATUS_FUNCTION(bh_gdchits,SHOW_LONGLONG,getCacheHits)
BH_STATUS_FUNCTION(bh_gdcmisses,SHOW_LONGLONG,getCacheMisses)
BH_STATUS_FUNCTION(bh_gdcreleased,SHOW_LONGLONG,getReleased)
BH_STATUS_FUNCTION(bh_gdcreadwait,SHOW_LONGLONG,getReadWait)
BH_STATUS_FUNCTION(bh_gdcfalsewakeup,SHOW_LONGLONG,getFalseWakeup)
BH_STATUS_FUNCTION(bh_gdcreadwaitinprogress,SHOW_LONGLONG,getReadWaitInProgress)
BH_STATUS_FUNCTION(bh_gdcpackloads,SHOW_LONGLONG,getPackLoads)
BH_STATUS_FUNCTION(bh_gdcloaderrors,SHOW_LONGLONG,getLoadErrors)
BH_STATUS_FUNCTION(bh_gdcredecompress,SHOW_LONGLONG,getReDecompress)
BH_STATUS_FUNCTION(bh_gdcprefetched,SHOW_LONGLONG,getPacksPrefetched)

BH_MM_STATUS_FUNCTION(bh_mmallocblocks,SHOW_LONGLONG,getAllocBlocks)
BH_MM_STATUS_FUNCTION(bh_mmallocobjs,SHOW_LONGLONG,getAllocObjs)
BH_MM_STATUS_FUNCTION(bh_mmallocsize,SHOW_LONGLONG,getAllocSize)
BH_MM_STATUS_FUNCTION(bh_mmallocpack,SHOW_LONGLONG,getAllocPack)
BH_MM_STATUS_FUNCTION(bh_mmalloctemp,SHOW_LONGLONG,getAllocTemp)
BH_MM_STATUS_FUNCTION(bh_mmalloctempsize,SHOW_LONGLONG,getAllocTempSize)
BH_MM_STATUS_FUNCTION(bh_mmallocpacksize,SHOW_LONGLONG,getAllocPackSize)
BH_MM_STATUS_FUNCTION(bh_mmfreeblocks,SHOW_LONGLONG,getFreeBlocks)
BH_MM_STATUS_FUNCTION(bh_mmfreepacks,SHOW_LONGLONG,getFreePacks)
BH_MM_STATUS_FUNCTION(bh_mmfreetemp,SHOW_LONGLONG,getFreeTemp)
BH_MM_STATUS_FUNCTION(bh_mmfreepacksize,SHOW_LONGLONG,getFreePackSize)
BH_MM_STATUS_FUNCTION(bh_mmfreetempsize,SHOW_LONGLONG,getFreeTempSize)
BH_MM_STATUS_FUNCTION(bh_mmfreesize,SHOW_LONGLONG,getFreeSize)
BH_MM_STATUS_FUNCTION(bh_mmrelease1,SHOW_LONGLONG,getReleaseCount1)
BH_MM_STATUS_FUNCTION(bh_mmrelease2,SHOW_LONGLONG,getReleaseCount2)
BH_MM_STATUS_FUNCTION(bh_mmrelease3,SHOW_LONGLONG,getReleaseCount3)
BH_MM_STATUS_FUNCTION(bh_mmrelease4,SHOW_LONGLONG,getReleaseCount4)
BH_MM_STATUS_FUNCTION(bh_mmreloaded,SHOW_LONGLONG,getReloaded)

BH_RMON_STATUS_FUNCTION(bh_rmon_readbytes,SHOW_LONGLONG, readbytes)
BH_RMON_STATUS_FUNCTION(bh_rmon_writebytes,SHOW_LONGLONG, writebytes)

BH_RMON_STATUS_FUNCTION(bh_rmon_readcount,SHOW_LONGLONG, readcount)
BH_RMON_STATUS_FUNCTION(bh_rmon_writecount,SHOW_LONGLONG, writecount)

struct st_mysql_show_var bh_statusvars[] = {
	BH_STATUS_MEMBER(bh_gdchits, "BH_gdc_hits"),
	BH_STATUS_MEMBER(bh_gdcmisses, "BH_gdc_misses"),
	BH_STATUS_MEMBER(bh_gdcreleased, "BH_gdc_released"),
	BH_STATUS_MEMBER(bh_gdcreadwait, "BH_gdc_readwait"),
	BH_STATUS_MEMBER(bh_gdcfalsewakeup, "BH_gdc_false_wakeup"),
	BH_STATUS_MEMBER(bh_gdcreadwaitinprogress, "BH_gdc_read_wait_in_progress"),
	BH_STATUS_MEMBER(bh_gdcpackloads, "BH_gdc_pack_loads"),
	BH_STATUS_MEMBER(bh_gdcloaderrors, "BH_gdc_load_errors"),
	BH_STATUS_MEMBER(bh_gdcredecompress, "BH_gdc_redecompress"),
	BH_STATUS_MEMBER(bh_gdcprefetched, "BH_gdc_prefetched"),
	BH_STATUS_MEMBER(bh_mmrelease1, "BH_mm_release1"),
	BH_STATUS_MEMBER(bh_mmrelease2, "BH_mm_release2"),
	BH_STATUS_MEMBER(bh_mmrelease3, "BH_mm_release3"),
	BH_STATUS_MEMBER(bh_mmrelease4, "BH_mm_release4"),
	BH_STATUS_MEMBER(bh_mmallocblocks, "BH_mm_alloc_blocs"),
	BH_STATUS_MEMBER(bh_mmallocobjs, "BH_mm_alloc_objs"),
	BH_STATUS_MEMBER(bh_mmallocsize, "BH_mm_alloc_size"),
	BH_STATUS_MEMBER(bh_mmallocpack, "BH_mm_alloc_packs"),
	BH_STATUS_MEMBER(bh_mmalloctemp, "BH_mm_alloc_temp"),
	BH_STATUS_MEMBER(bh_mmalloctempsize, "BH_mm_alloc_temp_size"),
	BH_STATUS_MEMBER(bh_mmallocpacksize, "BH_mm_alloc_pack_size"),
	BH_STATUS_MEMBER(bh_mmfreeblocks, "BH_mm_free_blocks"),
	BH_STATUS_MEMBER(bh_mmfreepacks, "BH_mm_free_packs"),
	BH_STATUS_MEMBER(bh_mmfreetemp, "BH_mm_free_temp"),
	BH_STATUS_MEMBER(bh_mmfreepacksize, "BH_mm_free_pack_size"),
	BH_STATUS_MEMBER(bh_mmfreetempsize, "BH_mm_free_temp_size"),
	BH_STATUS_MEMBER(bh_mmfreesize, "BH_mm_free_size"),
	BH_STATUS_MEMBER(bh_mmreloaded, "BH_mm_reloaded"),
	{ "BH_mm_freeable", (char*)get_Freeable_StatusVar, SHOW_FUNC },
	{ "BH_mm_unfreeable", (char*)get_UnFreeable_StatusVar, SHOW_FUNC },
	{ "BH_mm_scale", (char*)get_MemoryScale_StatusVar, SHOW_FUNC },
	//{ "BH_mm_packs_locked", (char*)get_packsLocked_StatusVar, SHOW_FUNC },
	BH_STATUS_MEMBER(bh_rmon_readbytes, "BH_readbytes"),
	BH_STATUS_MEMBER(bh_rmon_writebytes, "BH_writebytes"),
	BH_STATUS_MEMBER(bh_rmon_readcount, "BH_readcount"),
	BH_STATUS_MEMBER(bh_rmon_writecount, "BH_writecount"),
	{ 0, 0, SHOW_UNDEF }
};

extern char bh_sysvar_refresh_sys_infobright;
static MYSQL_SYSVAR_BOOL(refresh_sys_infobright, bh_sysvar_refresh_sys_infobright, PLUGIN_VAR_BOOL, "-", NULL, brighthouse_refresh_sys_infobright_func, TRUE);

static MYSQL_THDVAR_STR(trigger_error, PLUGIN_VAR_STR | PLUGIN_VAR_THDLOCAL, "-", brighthouse_throw_error_func, ib_update_func_str, NULL);

extern int bh_sysvar_allowmysqlquerypath;
extern char* bh_sysvar_cachefolder;
extern char* bh_sysvar_licensefile;
extern int bh_sysvar_controlmessages;
extern char bh_sysvar_internalmessages;
extern char* bh_sysvar_knfolder;
extern int bh_sysvar_knlevel;
extern int bh_sysvar_loadermainheapsize;
extern char bh_sysvar_pushdown;
extern int bh_sysvar_servermainheapsize;
extern char bh_sysvar_usemysqlimportexportdefaults;
extern char bh_sysvar_autoconfigure;

// Parameters from brighthouse.ini file
static MYSQL_SYSVAR_INT(ini_allowmysqlquerypath, bh_sysvar_allowmysqlquerypath, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 1, 0);
static MYSQL_SYSVAR_STR(ini_cachefolder, bh_sysvar_cachefolder, PLUGIN_VAR_READONLY, "-", NULL, NULL, NULL);
static MYSQL_SYSVAR_STR(ini_licensefile, bh_sysvar_licensefile, PLUGIN_VAR_READONLY, "-", NULL, NULL, NULL);
static MYSQL_SYSVAR_INT(ini_controlmessages, bh_sysvar_controlmessages, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 100, 0);
static MYSQL_SYSVAR_BOOL(ini_internalmessages, bh_sysvar_internalmessages, PLUGIN_VAR_READONLY, "-", NULL, NULL, FALSE);
static MYSQL_SYSVAR_STR(ini_knfolder, bh_sysvar_knfolder, PLUGIN_VAR_READONLY, "-", NULL, NULL, NULL);
static MYSQL_SYSVAR_INT(ini_knlevel, bh_sysvar_knlevel, PLUGIN_VAR_READONLY, "-", NULL, NULL, 99, 0, 99, 0);
static MYSQL_SYSVAR_INT(ini_loadermainheapsize, bh_sysvar_loadermainheapsize, PLUGIN_VAR_READONLY, "-", NULL, NULL, 320, 0, 2000, 0);
static MYSQL_SYSVAR_BOOL(ini_pushdown, bh_sysvar_pushdown, PLUGIN_VAR_READONLY, "-", NULL, NULL, TRUE);
static MYSQL_SYSVAR_INT(ini_servermainheapsize, bh_sysvar_servermainheapsize, PLUGIN_VAR_READONLY, "-", NULL, NULL, 600, 0, 1000000, 0);
static MYSQL_SYSVAR_BOOL(ini_usemysqlimportexportdefaults, bh_sysvar_usemysqlimportexportdefaults, PLUGIN_VAR_READONLY, "-", NULL, NULL, FALSE);
static MYSQL_SYSVAR_BOOL(ini_autoconfigure, bh_sysvar_autoconfigure, PLUGIN_VAR_READONLY, "-", NULL, NULL, FALSE);

#ifndef __BH_COMMUNITY__
extern int bh_sysvar_prefetch_threads;
extern int bh_sysvar_prefetch_queueLength;
extern int bh_sysvar_prefetch_depth;
extern int bh_sysvar_parscan_maxthreads;
extern int bh_sysvar_parscan_nodps_atonce;
extern int bh_sysvar_parscan_mindps_per_thread;
#endif
extern char* bh_sysvar_hugefiledir;
extern int bh_sysvar_clustersize;
extern int bh_sysvar_loadersavethreadnumber;
extern int bh_sysvar_cachinglevel;
extern int bh_sysvar_bufferinglevel;
extern char* bh_sysvar_mm_policy;
extern int bh_sysvar_mm_hardlimit;
extern char* bh_sysvar_mm_releasepolicy;
extern int bh_sysvar_mm_largetempratio;
#ifndef __BH_COMMUNITY__
extern int bh_sysvar_throttlelimit;
#endif
extern int bh_sysvar_sync_buffers;

// Parameters from .infobright file
#ifndef __BH_COMMUNITY__
static MYSQL_SYSVAR_INT(ib_prefetch_threads, bh_sysvar_prefetch_threads, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 128, 0);
static MYSQL_SYSVAR_INT(ib_prefetch_queueLength, bh_sysvar_prefetch_queueLength, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_INT(ib_prefetch_depth, bh_sysvar_prefetch_depth, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_INT(ib_parscan_maxthreads, bh_sysvar_parscan_maxthreads, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_INT(ib_parscan_nodps_atonce, bh_sysvar_parscan_nodps_atonce, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_INT(ib_parscan_mindps_per_thread, bh_sysvar_parscan_mindps_per_thread, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 512, 0);
#endif
static MYSQL_SYSVAR_STR(ib_hugefiledir, bh_sysvar_hugefiledir, PLUGIN_VAR_READONLY, "-", NULL, NULL, NULL);
static MYSQL_SYSVAR_INT(ib_clustersize, bh_sysvar_clustersize, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 20000, 0);
static MYSQL_SYSVAR_INT(ib_loadersavethreadnumber, bh_sysvar_loadersavethreadnumber, PLUGIN_VAR_READONLY, "-",  NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_INT(ib_cachinglevel, bh_sysvar_cachinglevel, PLUGIN_VAR_READONLY, "-", NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_INT(ib_bufferinglevel, bh_sysvar_bufferinglevel, PLUGIN_VAR_READONLY, "-",  NULL, NULL, 0, 0, 512, 0);
static MYSQL_SYSVAR_STR(ib_memorymanagement_policy, bh_sysvar_mm_policy, PLUGIN_VAR_READONLY, "-", NULL, NULL, NULL);
static MYSQL_SYSVAR_INT(ib_memorymanagement_hardlimit, bh_sysvar_mm_hardlimit, PLUGIN_VAR_READONLY, "-", NULL, NULL, 1, 0, 1, 0);
static MYSQL_SYSVAR_STR(ib_memorymanagement_releasepolicy, bh_sysvar_mm_releasepolicy, PLUGIN_VAR_READONLY, "-", NULL, NULL, NULL);
static MYSQL_SYSVAR_INT(ib_memorymanagement_largetempratio, bh_sysvar_mm_largetempratio, PLUGIN_VAR_READONLY, "-", NULL, NULL, 1, 0, 1, 0);
#ifndef __BH_COMMUNITY__
static MYSQL_SYSVAR_INT(ib_throttle_limit, bh_sysvar_throttlelimit, PLUGIN_VAR_READONLY, "-", NULL, NULL, 1, 0, 1, 0);
#endif
static MYSQL_SYSVAR_INT(ib_sync_buffers, bh_sysvar_sync_buffers, PLUGIN_VAR_READONLY, "-", NULL, NULL, 1, 0, 1, 0);

static struct st_mysql_sys_var* bh_showvars[]= {
  MYSQL_SYSVAR(refresh_sys_infobright),
  MYSQL_SYSVAR(trigger_error),
  MYSQL_SYSVAR(ini_allowmysqlquerypath),
  MYSQL_SYSVAR(ini_cachefolder),
  MYSQL_SYSVAR(ini_licensefile),
  MYSQL_SYSVAR(ini_controlmessages),
  MYSQL_SYSVAR(ini_internalmessages),
  MYSQL_SYSVAR(ini_knfolder),
  MYSQL_SYSVAR(ini_knlevel),
  MYSQL_SYSVAR(ini_loadermainheapsize),
  MYSQL_SYSVAR(ini_pushdown),
  MYSQL_SYSVAR(ini_servermainheapsize),
  MYSQL_SYSVAR(ini_usemysqlimportexportdefaults),
  MYSQL_SYSVAR(ini_autoconfigure),
#ifndef __BH_COMMUNITY__
  MYSQL_SYSVAR(ib_prefetch_threads),
  MYSQL_SYSVAR(ib_prefetch_queueLength),
  MYSQL_SYSVAR(ib_prefetch_depth),
  MYSQL_SYSVAR(ib_parscan_maxthreads),
  MYSQL_SYSVAR(ib_parscan_nodps_atonce),
  MYSQL_SYSVAR(ib_parscan_mindps_per_thread),
#endif
  MYSQL_SYSVAR(ib_hugefiledir),
  MYSQL_SYSVAR(ib_clustersize),
  MYSQL_SYSVAR(ib_loadersavethreadnumber),
  MYSQL_SYSVAR(ib_cachinglevel),
  MYSQL_SYSVAR(ib_bufferinglevel),
  MYSQL_SYSVAR(ib_memorymanagement_policy),
  MYSQL_SYSVAR(ib_memorymanagement_hardlimit),
  MYSQL_SYSVAR(ib_memorymanagement_releasepolicy),
  MYSQL_SYSVAR(ib_memorymanagement_largetempratio),
#ifndef __BH_COMMUNITY__
  MYSQL_SYSVAR(ib_throttle_limit),
#endif
  MYSQL_SYSVAR(ib_sync_buffers),
  NULL
};

mysql_declare_plugin(brighthouse)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&brighthouse_storage_engine,
	"BRIGHTHOUSE",
	"InfoBright",
	"Brighthouse storage engine",
#ifndef __BH_COMMUNITY__
	PLUGIN_LICENSE_PROPRIETARY,
#else
	PLUGIN_LICENSE_PROPRIETARY,
#endif
	rcbase_init_func, /* Plugin Init */
	rcbase_done_func, /* Plugin Deinit */
	0x0001 /* 0.1 */,
	bh_statusvars, /* status variables                */
	bh_showvars, /* system variables        */
	NULL /* config options                  */
}
mysql_declare_plugin_end;

#endif

