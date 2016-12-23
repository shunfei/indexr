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

#ifndef _RCENGINE_H_
#define _RCENGINE_H_

#ifndef RCENGINE_H
#define RCENGINE_H

#include <boost/function.hpp>
#include <boost/shared_array.hpp>
#include <boost/shared_ptr.hpp>

#include "fwd.h"
#include "edition/loader/LoaderFactory.h"
#include "system/RCSystem.h"
#include "core/TempTable.h"
#include "core/RCTableImpl.h"
#include "system/RCException.h"
#include "system/IOParameters.h"
#include "system/ConnectionInfo.h"
#include "system/DatadirVersion.h"
#include "exporter/DataExporter.h"
#include "core/Query.h"
#include "loader/Loader.h"
#include "edition/core/GlobalDataCache.h"
#include "common/bhassert.h"
#include "exporter/export2file.h"

#ifdef __GNUC__
#include <ext/hash_map>
#ifndef stdext
#define stdext __gnu_cxx
#endif
#else
#include <hash_map>
#endif

#define BHLoaderAppName "bhloader"

enum BHEngineReturnValues {LD_Successed = 100, LD_Failed = 101, LD_Continue = 102};

#ifdef __WIN__
void unlock_locked_tables(THD *thd);
#endif

extern handlerton* 			rcbase_hton;
extern const int 			CURRENT_DATA_VERSION;
extern const int 			DATADIR_VERSION_IN_4_0_7;
extern const std::string	CURRENT_SERVER_VERSION;

//////////////////////////////////////////////////////////////////////////////////////////
///////////////////  RCEngine:   ///////////////////////////////////////////////////////////
//
// A class representing RCBase engine
//
// Assumption:	only one RCEngine per MySQL server will be created,
//
class select_bh_export;
class RCDataType;
class Field;
struct AttrInfo;
class ResultSender;
class ResourceManager;

enum TABLE_LOCK_ENUM {READ_LOCK = 0, WRITE_LOCK};

class RCEngine
{
	friend class BrighthouseHandler;
	friend class TableLockManager;
public:
	RCEngine() : m_tab_ext(NULL), m_log_output(NULL), m_control_output(NULL), m_dev_output(NULL),
				 m_tab_sequencer(-1), m_resourceManager(NULL), m_engine_slot(0),
				 m_loader_main_heapsize(0), m_loader_compressed_heapsize(0) {}
	~RCEngine();

	int Init(const char* tab_ext, uint engine_slot);


	BHErrorCode CreateTable(const char *table, TABLE *from);
	void DropTable(const char *table);
	void RenameTable(const char* from, const char* to);

	std::vector<AttrInfo> GetTableAttributesInfo(const char* table_path, struct st_table_share* table_share);
	void UpdateAndStoreColumnComment(TABLE* table, int field_id, Field* source_field, int source_field_id,
			CHARSET_INFO *cs);
	void GetTableIterator(const char* table_path, RCTableImpl::Iterator& iter_begin, RCTableImpl::Iterator& iter_end,
			JustATable*& table, const std::vector<bool>& , THD *thd, ConnectionInfo *conn);
    RCTablePtr GetTableShared(const std::string& table_path, struct st_table_share* table_share);
    BHEngineReturnValues RunLoader(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, BHError & bherror);
    BHEngineReturnValues ExternalLoad(THD *thd, sql_exchange *ex, TABLE_LIST *table_list, BHError & bherror);
    void TerminateLoader(THD *thd);
    void Commit(THD *thd, bool all);
    void Rollback(THD *thd, bool all, bool force_error_message = false);
    ConnectionInfo* GetThreadParams(const THD& thd);
    void ClearThreadParams(THD& thd);
    int HandleSelect(THD *thd, LEX *lex, select_result *& result_output, ulong setup_tables_done_option, int & res, int & optimize_after_bh, int & bh_free_join, int with_insert = false);
    static AttributeType GetCorrespondingType(const Field& field);
    static ATI GetCorrespondingATI(Field& field);
	static AttributeType GetCorrespondingType(const enum_field_types& eft);
    static bool IsBHTable(TABLE *table);
    int WriteRow(THD *thd, TABLE *table, Field **field, uchar *buf,RCTable *&);
    int UpdateRow(THD *thd, TABLE *table, _uint64 row_id, Field **field,RCTable *&);
    int DeleteRow(THD *thd, TABLE *table, _uint64 row_id, RCTable *&);
    int DeleteAllRows(THD *thd, TABLE *table, RCTable *&);
	BHError PrivateLoad(THD* thd, const std::string& table_path, struct st_table_share* table_share, bool in_parallel = false);
	void ReplaceTable(const std::string& table_path, RCTablePtr table);
    //int DropColumn(THD* thd, TABLE* table, Field** fields, byte* buf, RCTable *&tab);

	static AttributeTypeInfo GetAttrTypeInfo(const Field & field) throw (UnsupportedDataTypeRCException);
	
	inline GlobalDataCache* getGlobalDataCache() const { return &GlobalDataCache::GetGlobalDataCache(); }
	inline ResourceManager* getResourceManager() const { return m_resourceManager; }
	RCTablePtr GetTableForWrite(const char* table_path, struct st_table_share* table_share);

public:
	const char *m_tab_ext;

protected:

	class DatadirMigrator
	{
	public:
		DatadirMigrator(const std::string& datadir) : dv(datadir) {}
		void DoMigrationIfNeeded();

	private:
		void	Upgrade();
		void	Downgrade();

	private:
			DatadirVersion dv;
	};

private:

	void CeateThreadParams(THD& thd);
    int SetUpCacheFolder(const std::string & cachefolder_path);
    void RemoveTemporaryFiles(std::string cachefolder_path);
    ChannelOut *m_log_output;
    ChannelOut *m_control_output;
    ChannelOut *m_dev_output;
    int m_tab_sequencer;
	//DataCache *m_globalCache;
	ResourceManager *m_resourceManager;
	
    uint m_engine_slot;
    std::map<std::string,std::map<std::string,boost::shared_ptr<RCTable> > > m_tables;
    size_t m_loader_main_heapsize;
    size_t m_loader_compressed_heapsize;
    IBMutex tables_mutex;
    int Execute(THD* thd, LEX* lex, select_result* result_output, SELECT_LEX_UNIT* unit_for_union = NULL);
	BHError PrivateLoadInternal(THD* thd, RCTable* tab, const std::string& table_path, struct st_table_share* table_share, bool refresh = true);

private:
    //static void SendResults(JustATable & results, THD *thd, select_result *res, List<Item> & fields, ConnectionInfo *conn);
    static bool AreConvertible(RCDataType& rcitem, enum_field_types my_type, uint length = 0);

public:
    static int Convert(int & is_null, Field *field, void *out_ptr, RCDataType& rcitem, std::vector<uchar>* blob_buf);
    //static int Convert(int & is_null, my_decimal *value, RCItem *rcitem, int output_scale = -1);
    static int Convert(int & is_null, my_decimal *value, RCDataType& rcitem, int output_scale = -1);
    static int Convert(int & is_null, int64 & value, RCDataType& rcitem, enum_field_types f_type);
    static int Convert(int & is_null, double & value, RCDataType& rcitem);
    static int Convert(int & is_null, String *value, RCDataType& rcitem, enum_field_types f_type);

private:
    static bool IsBHRoute(THD *thd, TABLE_LIST *table_list, SELECT_LEX *selects_list, int & in_case_of_failure_can_go_to_mysql, int with_insert);
    static const char *GetFilename(SELECT_LEX *selects_list, int & is_dumpfile);
    static std::auto_ptr<IOParameters> CreateIOParameters(const std::string & path);
    static std::auto_ptr<IOParameters> CreateIOParameters(THD *thd, TABLE *table);

public:
    static void GetNames(const char *table_path, char *db_buf, char *tab_buf, size_t buf_size);
    static std::pair<std::string, std::string> GetDatabaseAndTableNamesFromFullPath(const std::string& full_path);
    static void ComputeTimeZoneDiffInMinutes(THD *thd, short  & sign, short  & minutes);
	static std::string GetTablePath(THD *thd, TABLE *table);
	static BHError GetIOParameters(IOParameters& io_params, THD& thd, sql_exchange& ex, TABLE *table = 0, bool for_exporter = false);
	static BHError GetRejectFileIOParameters(THD& thd, IOParameters& io_params);
	bool IsRouteMySqlLoader(THD* thd);
	static void insert_th_func(void* f) {
		(*(boost::function0<void>*)(f))();
		return;
	}
};

class ResultSender 
{
public:
	ResultSender(THD* thd, select_result* res, List<Item>& fields);
	virtual ~ResultSender();

	void Send(TempTable* t);
	void Send(TempTable::RecordIterator& iter);
	void SetLimits(_int64* _offset, _int64* _limit) { offset = _offset; limit = _limit; }

	virtual void CleanUp();
	virtual void SendEof();

	void Finalize(TempTable* result);
	_int64 SentRows() const	{return rows_sent;}

protected:
	THD* thd;
	select_result* res;
	std::map<int,Item*> items_backup;
	uint* buf_lens;
	List<Item>& fields;
	bool is_initialized;
	_int64* offset;
	_int64* limit;
	_int64 rows_sent;

	virtual void Init(TempTable* t);
	virtual void SendRecord(TempTable::RecordIterator& iter);
};

class ResultExportSender : public ResultSender
{
public:
	ResultExportSender(THD* thd, select_result* result, List<Item>& fields);

	virtual void CleanUp() {}
	virtual void SendEof();

protected:
	virtual void Init(TempTable* t);
	virtual void SendRecord(TempTable::RecordIterator& iter);

	select_bh_export* export_res;
	std::auto_ptr<DataExporter> rcde;
	boost::shared_ptr<LargeBuffer> rcbuffer;
};


enum bh_var_name
{
	BH_TIMEOUT = 0,
	BH_DATAFORMAT,
	BH_PIPEMODE,
	BH_NULL,
	BH_THROTTLE,
	BH_IBEXPRESSIONS,
	BH_PARALLEL_AGGR,
	BH_REJECT_FILE_PATH,
	BH_ABORT_ON_COUNT,
	BH_ABORT_ON_THRESHOLD,
	BH_VAR_LIMIT // KEEP THIS LAST
};

static std::string bh_var_name_strings[] = {
	"BH_TIMEOUT",
	"BH_DATAFORMAT",
	"BH_PIPEMODE",
	"BH_NULL",
	"BH_THROTTLE",
	"BH_IBEXPRESSIONS",
	"BH_PARALLEL_AGGR",
	"BH_REJECT_FILE_PATH",
	"BH_ABORT_ON_COUNT",
	"BH_ABORT_ON_THRESHOLD"
};

std::string get_parameter_name(enum bh_var_name vn);

int get_parameter(THD* thd, enum bh_var_name vn, longlong &result, std::string &s_result);

// return 0 on success
// 1 if parameter was not specified
// 2 if was specified but with wrong type
int get_parameter(THD* thd, enum bh_var_name vn, double& value);
int get_parameter(THD* thd, enum bh_var_name vn, int64& value);
int get_parameter(THD* thd, enum bh_var_name vn, std::string& value);


bool parameter_equals(THD* thd, enum bh_var_name vn, longlong value);
int has_lookup(const LEX_STRING &comment);

//extern RCEngine* rceng;// RCBase engine used by MySQL

#endif

#endif // RCENGINE_H

