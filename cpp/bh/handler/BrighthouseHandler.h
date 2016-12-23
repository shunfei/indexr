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

#ifndef _HA_BHHANDLER_H_
#define _HA_BHHANDLER_H_

#ifndef PURE_LIBRARY
/*
 Please read ha_rcbase.cpp before reading this file.
 Please keep in mind that the rcbase storage engine implements all methods
 that are required to be implemented. handler.h has a full list of methods
 that you can implement.
 */

#include "common/CommonDefinitions.h"
#include "core/RCEngine.h"
#include "edition/core/Transaction.h"


#define HA_RCBASE_ERROR_TABLE_NOT_OPEN 10000

/*
 RCBASE_SHARE is a structure that will be shared amoung all open handlers
 The rcbase implements the minimum of what you will probably need.
 */
typedef struct st_rcbase_share
{
	char *table_name;
	uint table_name_length, use_count;
	THR_LOCK lock;
} RCBASE_SHARE;

#ifdef __WIN__
#undef open
#undef close
#endif

class RCDataType;

/*
 Class definition for the storage engine
 */
class BrighthouseHandler : public handler
{
public:
	BrighthouseHandler(handlerton *hton, TABLE_SHARE *table_arg);
	virtual ~BrighthouseHandler();
	/* The name that will be used for display purposes */
	const char *table_type() const { return "BRIGHTHOUSE"; }
	/*
	 Get the row type from the storage engine.  If this method returns
	 ROW_TYPE_NOT_USED, the information in HA_CREATE_INFO should be used.
	 */
	enum row_type get_row_type() const { return ROW_TYPE_COMPRESSED; }
	/*
	 The name of the index type that will be used for display
	 don't implement this method unless you really have indexes
	 */
	const char *index_type(uint inx) const { return "ROUGH SET"; }
	const char **bas_ext() const;
	/*
	 This is a list of flags that says what the storage engine
	 implements. The current table flags are documented in
	 handler.h
	 */
	ulonglong table_flags() const;
	/*
	 This is a bitmap of flags that says how the storage engine
	 implements indexes. The current index flags are documented in
	 handler.h. If you do not implement indexes, just return zero
	 here.

	 part is the key part to check. First key part is 0
	 If all_parts it's set, MySQL want to know the flags for the combined
	 index up to and including 'part'.
	 */
	ulong index_flags(uint inx, uint part, mysql_bool all_parts) const { return 0; }
	/*
	 unireg.cc will call the following to make sure that the storage engine can
	 handle the data it is about to send.

	 Return *real* limits of your storage engine here. MySQL will do
	 min(your_limits, MySQL_limits) automatically

	 There is no need to implement ..._key_... methods if you don't suport
	 indexes.
	 */
	uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }
	uint max_supported_keys() const { return 0; }
	uint max_supported_key_parts() const { return 0; }
	uint max_supported_key_length() const {	return 0; }
	/*
	 Called in test_quick_select to determine if indexes should be used.
	 */
	virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }
	/*
	 The next method will never be called if you do not implement indexes.
	 */
	virtual double read_time(ha_rows rows) { return (double) rows / 20.0+1; }

	/*
	 Everything below are methods that we implement in ha_rcbase.cpp.

	 Most of these methods are not obligatory, skip them and
	 MySQL will treat them as not implemented
	 */
	int open(const char *name, int mode, uint test_if_locked); // required
	int close(void); // required

	int write_row(uchar *buf __attribute__((unused)));
	int update_row(const uchar* old_data, uchar* new_data);
	int delete_row(const uchar * buf);
	int index_read(uchar * buf, const uchar * key, uint key_len, enum ha_rkey_function find_flag);
	int index_next(uchar * buf);
	int index_prev(uchar * buf);
	int index_first(uchar * buf);
	int index_last(uchar * buf);
	/*
	 unlike index_init(), rnd_init() can be called two times
	 without rnd_end() in between (it only makes sense if scan=1).
	 then the second call should prepare for the new table scan
	 (e.g if rnd_init allocates the cursor, second call should
	 position it to the start of the table, no need to deallocate
	 and allocate it again
	 */
	int rnd_init(mysql_bool scan); //required

	void SimpleDelete();
	int rnd_end();
	int rnd_next(uchar *buf); //required
	int rnd_pos(uchar * buf, uchar *pos); //required
	void position(const uchar *record); //required
	int info(uint); //required

	int extra(enum ha_extra_function operation);
	int external_lock(THD *thd, int lock_type); //required
	int start_stmt(THD *thd, thr_lock_type lock_type);
	int delete_all_rows(void);
	ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
	int delete_table(const char *from);
	int rename_table(const char * from, const char * to);
	int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info); //required
	
	void set_brighthouse_alter_table(int& brighthouse_alter_table);
	int brighthouse_alter_table(int drop_last_column);
	void before_copy_data_in_alter_table() { copy_data_in_alter_table = true; };
	void after_copy_data_in_alter_table() { copy_data_in_alter_table = false; };

	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type); //required

	char* update_table_comment(const char * comment);
	mysql_bool get_error_message(int error, String *buf);

	/* Condition push-down operation */
	const COND *cond_push(const COND *cond);
	void cond_pop();
	int reset();

	my_bool register_query_cache_table(THD *thd, char *table_key, uint key_length, qc_engine_callback *call_back,
	   		ulonglong *engine_data);
	char* GetTablePath() { return m_table_path; }
protected:
	int fill_row(uchar* buf);

	THR_LOCK_DATA m_lock; /* MySQL lock */
	RCBASE_SHARE *share; /* Shared lock info */

	ConnectionInfo* m_conn;
	char* m_table_path;

	TableLockWeakPtr table_lock;
	bool release_read_table;
	bool release_insert_from_select;
	JustATable* table_ptr;
	FilterAutoPtr filter_ptr;
	_uint64 current_position;

	RCTableImpl::Iterator table_new_iter;
	RCTableImpl::Iterator table_new_iter_end;

	char err_msg[256];

	boost::scoped_ptr<Query> m_query;
	TabID m_tmp_table;
	boost::scoped_ptr<CompiledQuery> m_cq;
	bool m_result;
	
	bool copy_data_in_alter_table;

	// this should be null if there is a possibility that the RCTable object could be modified
	// (ie if the table is not locked)
	RCTable *m_rctable;
	void RetrieveRCTable() {
		std::string table_path = rceng->GetTablePath(table->in_use, table);
		m_rctable = m_conn->GetTransaction()->GetTable(table_path, table_share);
	}

	std::vector<std::vector<uchar> > blob_buffers;
};

#endif //_HA_BHHANDLER_H_

#endif
