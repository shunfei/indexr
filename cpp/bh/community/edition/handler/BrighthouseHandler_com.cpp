/* Copyright (C) 2000 MySQL AB, portions Copyright (C) 2005-2008 Infobright Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2.0 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
General Public License version 2.0 for more details.

You should have received a copy of the GNU General Public License
version 2.0 along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307 USA */

#include "handler/BrighthouseHandler.h"
#include "core/RCTableImpl.h"

using namespace std;

char infobright_version[] = "Infobright Community Edition";

handler *rcbase_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);
int rcbase_panic_func(handlerton *hton, enum ha_panic_function flag);
int rcbase_close_connection(handlerton *hton, THD* thd);
int rcbase_commit(handlerton *hton, THD *thd, mysql_bool all);
int rcbase_rollback(handlerton *hton, THD *thd, mysql_bool all);
uchar* rcbase_get_key(RCBASE_SHARE *share, size_t *length, my_bool not_used __attribute__((unused)));
bool rcbase_show_status(handlerton *hton, THD *thd, stat_print_fn *print, enum ha_stat_type stat);

extern my_bool infobright_bootstrap;
extern HASH rcbase_open_tables; // Hash used to track open tables

ulonglong
BrighthouseHandler::table_flags() const
{
        return HA_REC_NOT_IN_SEQ | HA_PARTIAL_COLUMN_READ; 
}

int rcbase_init_func(void *p)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	rcbase_hton= (handlerton *)p;
	//VOID(pthread_mutex_init(&rcbase_mutex, MY_MUTEX_INIT_FAST));
	(void) hash_init(&rcbase_open_tables, system_charset_info, 32, 0, 0, (hash_get_key) rcbase_get_key, 0, 0);

	rcbase_hton->state = SHOW_OPTION_YES;
	rcbase_hton->db_type = DB_TYPE_BRIGHTHOUSE_DB;
	rcbase_hton->create = rcbase_create_handler;
	rcbase_hton->flags = HTON_NO_FLAGS;
	rcbase_hton->flags |= HTON_ALTER_NOT_SUPPORTED;
	rcbase_hton->panic = rcbase_panic_func;
	rcbase_hton->close_connection = rcbase_close_connection;
	rcbase_hton->commit = rcbase_commit;
	rcbase_hton->rollback = rcbase_rollback;
	rcbase_hton->show_status = rcbase_show_status;
	
	// When mysqld runs as bootstrap mode, we do not need to initialize memmanager.
	if (infobright_bootstrap)
		DBUG_RETURN(0);

	int ret = 1;
	rceng = NULL;

	try {
		rceng = new RCEngine();
		ret = rceng->Init(ha_rcbase_exts[0], total_ha);
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	//FIX: Can not be deleted right now. Some internal variables need to
	// deleted properly before deleting rceng.
	//if (ret ==1 && rceng != NULL)
	//	delete rceng;

	DBUG_RETURN(ret);
}

/*
 The idea with handler::store_lock() is the following:

 The statement decided which locks we should need for the table
 for updates/deletes/inserts we get WRITE locks, for SELECT... we get
 read locks.

 Before adding the lock into the table lock handler (see thr_lock.c)
 mysqld calls store lock with the requested locks.  Store lock can now
 modify a write lock to a read lock (or some other lock), ignore the
 lock (if we don't want to use MySQL table locks at all) or add locks
 for many tables (like we do when we are using a MERGE handler).

 Berkeley DB for example  changes all WRITE locks to TL_WRITE_ALLOW_WRITE
 (which signals that we are doing WRITES, but we are still allowing other
 reader's and writer's.

 When releasing locks, store_lock() are also called. In this case one
 usually doesn't have to do anything.

 In some exceptional cases MySQL may send a request for a TL_IGNORE;
 This means that we are requesting the same lock as last time and this
 should also be ignored. (This may happen when someone does a flush
 table when we have opened a part of the tables, in which case mysqld
 closes and reopens the tables and tries to get the same locks at last
 time).  In the future we will probably try to remove this.

 Called from lock.cc by get_lock_data().
 */
THR_LOCK_DATA **BrighthouseHandler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
	if(lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
		m_lock.type = lock_type;

	*to++= &m_lock;
	return to;
}

/*
 First you should go read the section "locking functions for mysql" in
 lock.cc to understand this.
 This create a lock on the table. If you are implementing a storage engine
 that can handle transacations look at ha_berkely.cc to see how you will
 want to goo about doing this. Otherwise you should consider calling flock()
 here.

 Called from lock.cc by lock_external() and unlock_external(). Also called
 from sql_table.cc by copy_data_between_tables().
 */
int BrighthouseHandler::external_lock(THD* thd, int lock_type)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	if (thd->lex->sql_command == SQLCOM_LOCK_TABLES) {
		DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}
	try {
		m_conn = rceng->GetThreadParams(*thd);
		if(lock_type == F_UNLCK) { // RELEASE LOCK
			m_rctable = NULL;
			if (thd->lex->sql_command == SQLCOM_UNLOCK_TABLES)
				m_conn->GetTransaction()->ExplicitUnlockTables();

			// Rollback the transaction if operation finished with error
			if(thd->killed)
				rceng->Rollback(thd, true);
			table_lock_manager.ReleaseLockIfNeeded(table_lock, string(m_table_path), !(thd->options & OPTION_NOT_AUTOCOMMIT), thd->lex->sql_command == SQLCOM_ALTER_TABLE);

		} else {
			if (thd->lex->sql_command == SQLCOM_LOCK_TABLES)
				m_conn->GetTransaction()->ExplicitLockTables();
			if(lock_type == F_RDLCK) { // READ LOCK
				if((table_lock = table_lock_manager.AcquireReadLock(*m_conn->GetTransaction(), m_table_path, thd->killed, thd->some_tables_deleted)).expired())
					DBUG_RETURN(1);
			} else {
				if((table_lock = table_lock_manager.AcquireWriteLock( *m_conn->GetTransaction(), string(m_table_path), thd->killed, thd->some_tables_deleted)).expired())
					DBUG_RETURN(1);
				trans_register_ha(thd, true, rcbase_hton);
				trans_register_ha(thd, false, rcbase_hton);
				if (thd->lex->sql_command != SQLCOM_ALTER_TABLE) {
					m_conn->GetTransaction()->AddTableWR(m_table_path, thd, table);
				}
			}
		}

	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}

	DBUG_RETURN(0);
}

/*
 write_row() inserts a row. No extra() hint is given currently if a bulk load
 is happeneding. buf() is a byte array of data. You can use the field
 information to extract the data from the native byte array type.
 Example of this would be:
 for (Field **field=table->field ; *field ; field++)
 {
 ...
 }

 See ha_tina.cc for an example of extracting all of the data as strings.
 ha_berekly.cc has an example of how to store it intact by "packing" it
 for ha_berkeley's own native storage type.

 See the note for update_row() on auto_increments and timestamps. This
 case also applied to write_row().

 Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
 sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.
 */
int BrighthouseHandler::write_row(uchar* buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 Yes, update_row() does what you expect, it updates a row. old_data will have
 the previous row record in it, while new_data will have the newest data in
 it.
 Keep in mind that the server can do updates based on ordering if an ORDER BY
 clause was used. Consecutive ordering is not guarenteed.
 Currently new_data will not have an updated auto_increament record, or
 and updated timestamp field. You can do these for example by doing these:
 if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
 table->timestamp_field->set_time();
 if (table->next_number_field && record == table->record[0])
 update_auto_increment();

 Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.
 */
int BrighthouseHandler::update_row(const uchar* old_data, uchar* new_data)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
 This will delete a row. buf will contain a copy of the row to be deleted.
 The server will call this right after the current row has been called (from
 either a previous rnd_nexT() or index call).
 If you keep a pointer to the last row or can access a primary key it will
 make doing the deletion quite a bit easier.
 Keep in mind that the server does no guarentee consecutive deletions. ORDER BY
 clauses can be used.

 Called in sql_acl.cc and sql_udf.cc to manage internal table information.
 Called in sql_delete.cc, sql_insert.cc, and sql_select.cc. In sql_select it is
 used for removing duplicates while in insert it is used for REPLACE calls.
 */
int BrighthouseHandler::delete_row(const uchar * buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

void BrighthouseHandler::SimpleDelete()
{

}

/*
 Used to delete all rows in a table. Both for cases of truncate and
 for cases where the optimizer realizes that all rows will be
 removed as a result of a SQL statement.

 Called from item_sum.cc by Item_func_group_concat::clear(),
 Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
 Called from sql_delete.cc by mysql_delete().
 Called from sql_select.cc by JOIN::rein*it.
 Called from sql_union.cc by st_select_lex_unit::exec().
 */
int BrighthouseHandler::delete_all_rows()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int BrighthouseHandler::rename_table(const char * from, const char * to)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

/*
  Sets brighthouse alter table flag.

  Called from mysql_alter_table (sql_table.cc)
*/
void BrighthouseHandler::set_brighthouse_alter_table(int& brighthouse_alter_table)
{
}

/*
  Alters table except for rename table.

  Called from mysql_alter_table (sql_table.cc)
*/
int BrighthouseHandler::brighthouse_alter_table(int drop_last_column)
{
	return FALSE;
}
