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

#include <boost/algorithm/string.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/foreach.hpp>

#include "edition/local.h"
#include "edition/loader/LoaderFactory.h"
#include "RCEngine.h"
#include "system/FileOut.h"
#include "system/StdOut.h"
#include "system/MemoryManagement/Initializer.h"
#include "system/BHToolkit.h"
#include "loader/Loader.h"
#include "core/tools.h"
#include "core/TempTable.h"
#include "system/RCException.h"
#include "system/IBLicense.h"
#include "edition/core/Transaction.h"
#include "edition/core/GlobalDataCache.h"
#include "TransactionManager.h"
#include "edition/system/ResourceManager.h"
#include "domaininject/DomainInjectionsDictionary.h"

#ifndef __BH_COMMUNITY__
#include "edition/core/Query_ent.h"
#include "edition/system/Compactor.h"
#endif
#ifdef test
#undef test
#endif

using namespace std;
using namespace boost;
using namespace boost::filesystem;

const int		CURRENT_DATA_VERSION = 3;
const int 		DATADIR_VERSION_IN_4_0_7 = 3;
const string	CURRENT_SERVER_VERSION = "4.0.7";


#ifdef PROFILE_LOCK_WAITING
LockProfiler lock_profiler;
#endif

int has_lookup(const LEX_STRING &comment)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	char *str = comment.str;
	uint length = (uint)comment.length;
	if(length == 0)
		return 1;

	char lookup_str[] = "LOOKUP";
	uint const lookup_len = (const uint) strlen(lookup_str);

	uint idx = 0;
	while((str[0] == ' ' || str[0] == '\t' || str[0] == '\n') && idx < length) {
		str++;
		idx++;
	}
	if(length - idx < lookup_len)
		return 1;

	uint const buf_size = 128;
	char buf[buf_size] = { 0 };
	//strncpy_s(buf, 128, str, lookup_len);
	std::strncpy(buf, str, lookup_len);
	if(strcasecmp(buf, "LOOKUP") == 0)
		return 0;
	else
		return 1;
#endif
}

bool parameter_equals(THD* thd, enum bh_var_name vn, longlong value)
{
	longlong param = 0;
	string s_res;

	if(!get_parameter(thd, vn, param, s_res))
		if(param == value)
			return true;

	return false;
}

int RCEngine::Init(const char* tab_ext, uint engine_slot)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	m_tab_ext = tab_ext;
	m_engine_slot = engine_slot;
	BHASSERT( infobright_data_dir.length(), "data dir not set" );
	BHASSERT( process_type != ProcessType::INVALID, "process type not set" );
	m_log_output = new FileOut(infobright_data_dir + "brighthouse.log");
	rclog.addOutput(m_log_output);
	m_control_output = ( ( process_type != ProcessType::EMBEDDED ) || !log_error_file_ptr || !log_error_file_ptr[0] ) ? static_cast<ChannelOut*>( new StdOut() ) : static_cast<ChannelOut*>( new FileOut( log_error_file_ptr ) );
	rccontrol.addOutput(m_control_output);

	// check datadir version and update the number if necessary
	try {
		DatadirMigrator dm(infobright_data_dir);
		dm.DoMigrationIfNeeded();
	} catch (RCException& e) {
		sql_print_error(e.what(), 0);
		rclog << lock << e.what() << unlock;
		return 1;
	}

	try {
		Configuration::LoadSettings();
	} catch (const Configuration::Exception& e) {
		sql_print_error("Unable to load configuration");
		sql_print_error(e.what(), 0);
		rclog << lock << e.what() << " Configuration::Exception" << __FILE__ << __LINE__ << unlock;
		return 1;
	}
    ConfigureRCControl();
    if(Configuration::GetProperty(Configuration::InternalMessages)){
        m_dev_output = new FileOut(infobright_data_dir + "development.log");
        rcdev.addOutput(m_dev_output);
        rcdev.setOn();
    } else {
        rcdev.setOff();
    }
    DefaultPushDown = Configuration::GetProperty(Configuration::PushDown);
#ifdef __BH_EVALUATION__
	try {
		IBLicense* license = IBLicense::GetLicense();
		license->SetProductDir(infobright_home_dir);
		license->SetDataDir(infobright_data_dir);
		string file_name = Configuration::GetProperty(Configuration::LicenseFile);
		if (!license->SetKeyFile(file_name))
			return 1;
		license->Load();
		if (license->IsExpired(true)) {
                    sql_print_error(EXPIRATION_MESSAGE);
                    rclog << lock << EXPIRATION_MESSAGE << unlock;
   		    return 1;
		}
	}
	catch (RCException& e) {
            sql_print_error("Error: License verification failed!");
            sql_print_error(e.what(), 0);
            rclog << lock << e.what() << unlock;
	    return 1;
	}
#endif
    int control_level = Configuration::GetProperty(Configuration::ControlMessages);
    string conferror;
    int loadconf = ConfMan.LoadConfig(conferror);
    if (control_level >= 4) {
			if (loadconf)
				sql_print_warning("%s. Default values will be used.", conferror.c_str());
		string advconf = ConfMan.PrintConfigurations();
		rclog << lock << advconf << unlock;
    }
    ConfMan.PublishElementsToShowVar();

	GlobalDataCache::GetGlobalDataCache().Init();
    srand(unsigned(time(NULL)));

    DomainInjectionsDictionary::Instance().SetDataDir(infobright_data_dir);

    size_t main_size = size_t(Configuration::GetProperty(Configuration::ServerMainHeapSize)) << 20;
    // ServerCompressedHeapSize is a ratio of main_size,
    // For instance, ServerCompressedHeapSize = main_size *0.1 (i.e. 1/10 of ServerMainHeapSize)
    size_t compressed_size = size_t(main_size * ConfMan.GetValueFloat("brighthouse/ServerCompressedHeapSize", conferror));
    m_loader_main_heapsize = size_t(Configuration::GetProperty(Configuration::LoaderMainHeapSize)) << 20;
    m_loader_compressed_heapsize = 128 << 20;
    string hugefiledir = ConfMan.GetValueString("brighthouse/HugefileDir", conferror);
    int hugefilesize = ConfMan.GetValueInt("brighthouse/HugefileSize", conferror);
    if(hugefiledir.empty())
        MemoryManagerInitializer::Instance(compressed_size, main_size, compressed_size / 10, main_size / 10);
    else
        MemoryManagerInitializer::Instance(compressed_size, main_size, compressed_size / 10, main_size / 10, hugefiledir,hugefilesize);

    the_filter_block_owner = new TheFilterBlockOwner();

	string p = infobright_data_dir + "brighthouse.seq";
    if(! DoesFileExist(p) ) {
		m_tab_sequencer = 0;
		ofstream seq_file(p.c_str());
		if(seq_file)
			seq_file << m_tab_sequencer;
		if(!seq_file) {
			m_tab_sequencer = -1;
			rclog << lock << "Brighthouse: Fatal error: can not create or write the table sequencer file" << unlock;
		}
		seq_file.close();
	} else {
		ifstream seq_file(p.c_str());
		if(seq_file)
			seq_file >> m_tab_sequencer;
		if(!seq_file) {
			m_tab_sequencer = -1;
			rclog << lock << "Brighthouse: Fatal error: can not read the sequence number from the table sequencer file"
					<< unlock;
		}
		seq_file.close();
	}

    int sync_bufs_flag = ConfMan.GetValueInt("brighthouse/sync_buffers", conferror);
    if (sync_bufs_flag > 0)
    	sync_buffers = true;
    else
    	sync_buffers = false;

    if(InitRSIManager(infobright_data_dir) != 0)
    	return 1;

	CachingLevel = ConfMan.GetValueInt("brighthouse/CachingLevel", conferror);
	if(CachingLevel == 2)
		CachingLevel = 1;

	try {
		string cachefolder_path = Configuration::GetProperty(Configuration::CacheFolder);
		trim(cachefolder_path);
		trim_if(cachefolder_path, boost::is_any_of("\""));
		if (SetUpCacheFolder(cachefolder_path) != 0)
			return 1;
		RemoveTemporaryFiles(cachefolder_path);
	}
	catch (const Configuration::Exception&) {
		sql_print_error("'CacheFolder' not defined.");
		rclog << lock << "Error: 'CacheFolder' not defined." << unlock;
		return 1;
	}
	m_resourceManager = new ResourceManager();

#	ifdef FUNCTIONS_EXECUTION_TIMES
	fet = new FunctionsExecutionTimes();
#	endif
	rclog << lock << "Brighthouse engine started. " << unlock;
	rccontrol.lock(0) << "Brighthouse engine started, ";
	if(rsi_manager)
		rccontrol << "KNs: ok";
	else
		rccontrol << "KNs: none";
	rccontrol << unlock;

#if !defined(__BH_COMMUNITY__)
    ParallScanThreadParams::no_DPs_per_thread_for_filter = ConfMan.GetValueInt("brighthouse/parallelscan/noDPsAtOnce", conferror);
    ParallScanThreadParams::min_no_DPs_per_thread = ConfMan.GetValueInt("brighthouse/parallelscan/minDPsPerThread", conferror);

	RMon.StartCpuMon();
	int throttlelimit = ConfMan.GetValueInt("brighthouse/throttle/limit", conferror);
	int throttlescheduler =  ConfMan.GetValueInt("brighthouse/throttle/scheduler", conferror);
	if (throttlelimit > 0)
		QThrottler.EnableThrottle(throttlelimit, throttlescheduler>0, control_level >= 5);
	unsigned int max_thr_per_scan = ConfMan.GetValueInt("brighthouse/parallelscan/maxthreads", conferror);
//	if(max_thr_per_scan )
		RMon.SetMaxThreadsForScan(max_thr_per_scan);
	unsigned int max_thr_per_aggr = ConfMan.GetValueInt("brighthouse/parallelaggr/maxthreads", conferror);
//	if(max_thr_per_scan )
		RMon.SetMaxThreadsForAggr(max_thr_per_aggr);
	Compactor::Instance();
#endif
	return 0;
#endif
}

RCEngine::~RCEngine()
{

	rsi_manager.reset();

//	for(map<string,map<string, boost::shared_ptr<RCTable> > >::const_iterator db_iter = m_tables.begin(); db_iter!=m_tables.end(); db_iter++)
//		for(map<string,RCTable*>::const_iterator tab_iter = db_iter->second.begin(); tab_iter!=db_iter->second.end(); tab_iter++)
//			tab_iter->second.reset();
/**************************************************
#ifdef __GNUC__
	pthread_mutex_destroy(&m_sync_tables);
	pthread_mutex_destroy(&m_sync_threadparams);
#else
	DeleteCriticalSection(&m_sync_tables);
	DeleteCriticalSection(&m_sync_threadparams);
#endif
*****************************************************/

	m_tables.clear();	
	
	GlobalDataCache::GetGlobalDataCache().ReleaseAll();

	if(rccontrol.isOn())
		MemoryManagerInitializer::deinit(true);
	else
		MemoryManagerInitializer::deinit(false);

#ifdef FUNCTIONS_EXECUTION_TIMES
	fet->PrintToRcdev();
	//delete fet;
	//fet = NULL;
#endif
	if (m_control_output) {
		m_control_output->close();
		delete m_control_output;
		m_control_output = NULL;
	}
	rclog << lock << "Brighthouse engine shutdown." << unlock;

	if(m_log_output) {
		m_log_output->close();
		delete m_log_output;
		m_log_output = NULL;
	}
	if(m_dev_output) {
		m_dev_output->close();
		delete m_dev_output;
		m_dev_output = NULL;
	}

	delete m_resourceManager;
	delete the_filter_block_owner;
}


BHErrorCode RCEngine::CreateTable(const char *table, TABLE *from)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return BHERROR_UNKNOWN;
#else
	if(table == 0 || from == 0)
		return BHERROR_UNKNOWN;

	if(m_tab_sequencer == -1) {
		rclog << lock << "Brighthouse: Fatal error: can not create tables, the sequencer file did not work" << unlock;
		return BHERROR_UNKNOWN;
	};
	if(m_tab_sequencer == INT_MAX32) {
		rclog << lock
				<< "Brighthouse: Can not create more tables in this version please ask the support team for an update"
				<< unlock;
		return BHERROR_UNKNOWN;
	};
	string p = infobright_data_dir + "brighthouse.seq";
	ofstream seq_file(p.c_str());
	if(seq_file)
		seq_file << (m_tab_sequencer + 1);
	if(!seq_file) {
		m_tab_sequencer = -1;
		rclog << lock << "Brighthouse: Fatal error: can not write the table sequencer file" << unlock;
		seq_file.close();
		return BHERROR_UNKNOWN;
	}
	seq_file.close();
	size_t path_len = strlen(table) + strlen(m_tab_ext) + 1;
	char* tab_path = new char[path_len];

	/*strcpy_s(tab_path, path_len, table);
	 strcat_s(tab_path, path_len, m_tab_ext);*/
	strcpy(tab_path, table);
	strcat(tab_path, m_tab_ext);

	char db_name[256] = { 0 }, tab_name[256] = { 0 };
	GetNames(table, db_name, tab_name, 256);

	vector<DTCollation> charsets;
	if(from->s) {
		for(uint i = 0; i < from->s->fields; i++) {
			const Field_str* fstr = dynamic_cast<const Field_str*>(from->s->field[i]);
			if(fstr)
				charsets.push_back(DTCollation(fstr->charset(), fstr->derivation()));
			else
				charsets.push_back(DTCollation());
		}
	}

	shared_ptr<RCTable> tab = boost::shared_ptr<RCTable> (new RCTable(tab_path, charsets, tab_name, m_tab_sequencer++, true));
	delete[] tab_path;
	
	int fid = 0;
	Field* field = from->field[fid];
	try {
		while(field != 0) {
			AttributeTypeInfo ati = GetAttrTypeInfo(*field);
			tab->AddAttribute(const_cast<char*> (field->field_name), ati, ati.Precision(), ati.Scale(), ati.Params(), ati.GetCollation());
			field = from->field[++fid];
		}
	} catch (UnsupportedDataTypeRCException&) {
		return BHERROR_UNSUPPORTED_DATATYPE;
	} catch (...) {
		return BHERROR_UNKNOWN;
	}
	tab->Save();
	tab->CommitSwitch(tab->CommitSaveSession(0)); // TO DO: provide transaction id
	
	tables_mutex.Lock();
	m_tables[db_name][tab_name] = tab;
	tables_mutex.Unlock();

	return BHERROR_SUCCESS;
#endif
	// TO DO: logging ?
}


/*static string GetDecompositionDef(const string& comm)
{
	int beg = comm.find("DECOMPOSE");
	if (beg == string::npos) return "";
	int p = beg + 9;
	while (p<comm.length() && isspace(comm.at(p)))
		p++;
	if (p==comm.length() || comm.at(p++)!='=')
		throw UnsupportedDataTypeRCException("Wrong definition of a column decomposition.");
	while (p<comm.length() && isspace(comm.at(p)))
		p++;
	beg = p;
	int count = 0;
	if (p<comm.length() && comm.at(p)=='{') {			// definition inside {..}
		int open = 1;
		p++;
		beg = p;
		while (p<comm.length() && open) {
			if (comm.at(p)=='{')
				open++;
			if (comm.at(p)=='}')
				open--;
			p++;
		}
		if (open)
			throw UnsupportedDataTypeRCException("Wrong definition of a column decomposition.");
		count = p-1-beg;
	}
	else {												// definition without {..}
		while (p<comm.length() && !isspace(comm.at(p)))
			p++;
		count = p-beg;
	}
	if (count==0)
		throw UnsupportedDataTypeRCException("Wrong definition of a column decomposition.");
	return comm.substr(beg, count);
}*/

ATI RCEngine::GetAttrTypeInfo(const Field& field) throw(UnsupportedDataTypeRCException)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ATI(RC_INT, true);
#else
	bool is_lookup = false;
	if(has_lookup(field.comment) == 0)
		is_lookup = true;

	bool for_insert = false;
	string c = string(field.comment.str, field.comment.length);
	if(int(c.find("for_insert")) != string::npos)
		for_insert = true;
	//string dom_inj_decomp = GetDecompositionDef(c);
	//if (dom_inj_decomp.length()==0)
	//	dom_inj_decomp = "HALVE";

	switch(field.type()) {
		case MYSQL_TYPE_SHORT:
		case MYSQL_TYPE_TINY:
		case MYSQL_TYPE_INT24:
		case MYSQL_TYPE_LONG:
		case MYSQL_TYPE_FLOAT:
		case MYSQL_TYPE_DOUBLE:
		case MYSQL_TYPE_LONGLONG:
			if(field.flags & UNSIGNED_FLAG)
				throw UnsupportedDataTypeRCException("UNSIGNED data types are not supported.");
		case MYSQL_TYPE_YEAR:
		case MYSQL_TYPE_TIMESTAMP:
		case MYSQL_TYPE_DATETIME:
		case MYSQL_TYPE_DATE:
		case MYSQL_TYPE_NEWDATE:
			return ATI(RCEngine::GetCorrespondingType(field), field.null_bit == 0, (ushort)field.field_length, 0,
					is_lookup, for_insert);
		case MYSQL_TYPE_TIME:
			return ATI(RC_TIME, field.null_bit == 0, 0, 0, is_lookup, for_insert);
			/*case MYSQL_TYPE_YEAR :
			 return ATI(RC_YEAR, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup);*/
		case MYSQL_TYPE_STRING:
		case MYSQL_TYPE_VARCHAR: {
			if(field.field_length > 65535)
				throw UnsupportedDataTypeRCException();
			if(const Field_str* fstr = dynamic_cast<const Field_string*>(&field))
			{
				if(fstr->charset() != &my_charset_bin)
					return ATI(RC_STRING, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert, DTCollation(fstr->charset(), fstr->derivation()));
				return ATI(RC_BYTE, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert, DTCollation(fstr->charset(), fstr->derivation()));
			}
			else if(const Field_str* fvstr = dynamic_cast<const Field_varstring*>(&field))
			{
				if(fvstr->charset() != &my_charset_bin)
					return ATI(RC_VARCHAR, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert, DTCollation(fvstr->charset(), fvstr->derivation()));
				return ATI(RC_VARBYTE, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert, DTCollation(fvstr->charset(), fvstr->derivation()));

			}
			throw UnsupportedDataTypeRCException();
		}
		case MYSQL_TYPE_NEWDECIMAL :
		{
			if(field.flags & UNSIGNED_FLAG)
			throw UnsupportedDataTypeRCException("UNSIGNED data types are not supported.");
			const Field_new_decimal* fnd = ((const Field_new_decimal*)&field);
			if(/*fnd->precision > 0 && */fnd->precision <= 18 /*&& fnd->dec >= 0*/&& fnd->dec <= fnd->precision)
			return ATI(RC_NUM, field.null_bit == 0, fnd->precision, fnd->dec, is_lookup, for_insert);
			throw UnsupportedDataTypeRCException();
		}
		case MYSQL_TYPE_BLOB :
		if(const Field_str* fstr = dynamic_cast<const Field_str*>(&field))
		{
			if(const Field_blob* fblo = dynamic_cast<const Field_blob*>(fstr))
			{
				if(fblo->charset() != &my_charset_bin)
				{//TINYTEXT, MEDIUMTEXT, TEXT, LONGTEXT
					if(field.field_length> 65535)
					throw UnsupportedDataTypeRCException();
					return ATI(RC_VARCHAR, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert, DTCollation());
				}
				else
				{
					switch(field.field_length)
					{
						if(field.field_length> 65535)
						throw UnsupportedDataTypeRCException();
						case 255 :
						case 65535 :
						//TINYBLOB, BLOB
						return ATI(RC_VARBYTE, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert, DTCollation());
						case 16777215 :
						case 4294967295 :
						//MEDIUMBLOB, LONGBLOB
						return ATI(RC_BIN, field.null_bit == 0, (ushort)field.field_length, 0, is_lookup, for_insert);
						default:
						throw UnsupportedDataTypeRCException();
					}
				}
			}
		}
		default :
		//MYSQL_TYPE_LONG_BLOB
		//MYSQL_TYPE_TINY_BLOB
		//MYSQL_TYPE_MEDIUM_BLOB
		//MYSQL_TYPE_NULL
		//MYSQL_TYPE_BIT
		//MYSQL_TYPE_GEOMETRY
		//MYSQL_TYPE_ENUM
		//MYSQL_TYPE_SET
		//MYSQL_TYPE_VAR_STRING
		throw UnsupportedDataTypeRCException();
	}
	throw;
#endif
}

void RCEngine::Commit(THD* thd, bool all)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	if(all || !(thd->options & OPTION_NOT_AUTOCOMMIT)) {
		Transaction* trs = GetThreadParams(*thd)->GetTransaction();
		trs->Commit(thd);
		thd->server_status &= ~SERVER_STATUS_IN_TRANS;
		if (trs->ReadyForEnd())
			GetThreadParams(*thd)->EndTransaction();
	}
#endif
}

void RCEngine::Rollback(THD* thd, bool all, bool force_error_message)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	force_error_message = force_error_message || (!all && (thd->options & OPTION_NOT_AUTOCOMMIT));
	Transaction* trs = GetThreadParams(*thd)->GetTransaction();
	trs->Rollback(thd, force_error_message);
	thd->server_status &= ~SERVER_STATUS_IN_TRANS;
	thd->transaction_rollback_request = false;
	if (trs->ReadyForEnd())
		GetThreadParams(*thd)->EndTransaction();
#endif
}

void RCEngine::DropTable(const char *table)
{
	IBGuard drop_rename_mutex_guard(drop_rename_mutex);
	trs_mngr.DropTable(table);
	char db_name[256] = { 0 }, tab_name[256] = { 0 };
	GetNames(table, db_name, tab_name, 256);
	//pthread_mutex_lock(&m_sync_tables);
    tables_mutex.Lock();
	RCTablePtr tab = m_tables[db_name][tab_name];
	if(tab) {
		m_tables[db_name].erase(tab_name);
		table_lock_manager.UnRegisterTable(table);
	}
    tables_mutex.Unlock();

	if(!tab) {
		string tab_path = table;
		tab_path += m_tab_ext;

		try {
			tab = RCTablePtr(new RCTable(tab_path.c_str(), vector<DTCollation>(), 0, RCTableImpl::OpenMode::FOR_DROP));	// Version: cannot drop inconsistent tables
//			tab = RCTablePtr(new RCTable(tab_path.c_str(), -1, 0, true, true));		// Version: can drop inconsistent tables
		} catch (NoTableFolderRCException&) {
			return;
		} catch (DatabaseRCException&) {
			throw;
		}
	}
	int id = tab->GetID();
	uint no_attrs = tab->NoAttrs();
	//tab->Rollback(0, true);
	tab->Drop();
	GlobalDataCache::GetGlobalDataCache().ReleasePacks(id);
	GlobalDataCache::GetGlobalDataCache().ReleaseDPNs(id);
	GlobalDataCache::GetGlobalDataCache().DropObject(FilterCoordinate(id));
	for(uint a = 0; a < no_attrs; a++)
		GlobalDataCache::GetGlobalDataCache().DropObject(FTreeCoordinate(id, a));
	DomainInjectionsDictionary::Instance().remove_rule(db_name, tab_name);

	//ReleaseAllLocks(table);
	// TO DO: logging ?
}


void RCEngine::GetTableIterator(const char* table_path, RCTableImpl::Iterator& iter_begin, RCTableImpl::Iterator& iter_end,
			JustATable*& table, const std::vector<bool>& attrs , THD *thd, ConnectionInfo *conn)
{
	table = GetThreadParams(*thd)->GetTransaction()->GetTable(table_path, NULL);
	if(((RCTable*)table)->NeedRefresh()) {
		GetThreadParams(*thd)->GetTransaction()->ApplyPendingChanges(thd, table_path, 0, (RCTable*&)table);
		table = GetThreadParams(*thd)->GetTransaction()->GetTable(table_path, NULL);
	}
	iter_begin = ((RCTable*)table)->Begin(attrs);
	iter_end = ((RCTable*)table)->End();
}

RCTablePtr RCEngine::GetTableShared(const string& table_path, struct st_table_share* table_share)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCTablePtr();
#else
	char db_name[256] = { 0 }, tab_name[256] = { 0 };
	GetNames(table_path.c_str(), db_name, tab_name, 256);
    IBGuard tables_mutex_guard(tables_mutex);
	RCTablePtr tab = m_tables[db_name][tab_name];
	if(!tab) {
		vector<DTCollation> charsets;
		if(table_share) {
			for(uint i = 0; i < table_share->fields; i++) {
				const Field_str* fstr = dynamic_cast<const Field_str*>(table_share->field[i]);
				if(fstr)
					charsets.push_back(DTCollation(fstr->charset(), fstr->derivation()));
				else
					charsets.push_back(DTCollation());
			}
		}

		string file_path = table_path + m_tab_ext;
		tab = RCTablePtr(new RCTable(file_path.c_str(), charsets, 0, RCTableImpl::OpenMode::FORCE));
		m_tables[db_name][tab_name] = tab;
		// rollback if there was no commit
		if(tab->GetSaveSessionId() != 0xFFFFFFFF) {
			tab->Rollback();
			rclog << lock << "WARNING: Rollback after uncommitted transaction on table " << db_name << '.'
				<< tab_name << unlock;
		}
	}
	return tab;
#endif
}

std::vector<AttrInfo> RCEngine::GetTableAttributesInfo(const char* table_path, struct st_table_share* table_share)
{
	IBGuard global_mutex_guard(global_mutex);
	RCTable* tab = 0;
	if(ConnectionInfoOnTLS.IsValid())
		tab = ConnectionInfoOnTLS.Get().GetTransaction()->GetTable(table_path, table_share);
	else
		tab = GetTableShared(table_path, table_share).get();
	rccontrol << "Table " << table_path << " (" << tab->GetID() << ") accessed by MySQL engine." << endl;
	return tab->GetAttributesInfo();
}

void RCEngine::UpdateAndStoreColumnComment(TABLE* table, int field_id, Field* source_field, int source_field_id,
		CHARSET_INFO *cs)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	if(source_field->orig_table->s->db_type() == rcbase_hton) //do not use table (cont. default values)
	{
		char buf_size[256] = { 0 };
		char buf_ratio[256] = { 0 };
		uint buf_size_count = 0;
		uint buf_ratio_count = 0;
		_int64 sum_c = 0, sum_u = 0;

		std::vector<AttrInfo> attr_info = GetTableAttributesInfo(source_field->orig_table->s->path.str, source_field->orig_table->s);

		bool is_unique = false;
		if(source_field_id < (int)attr_info.size()) {
			is_unique = attr_info[source_field_id].actually_unique;
			sum_c += attr_info[source_field_id].comp_size;
			sum_u += attr_info[source_field_id].uncomp_size;
		}

		double d_comp = int(sum_c/104857.6)/10.0; // 1 MB = 2^20 bytes
		if(d_comp < 0.1)
			d_comp = 0.1;
		double ratio = (sum_c > 0 ? sum_u / double(sum_c) : 0);
		if(ratio>1000)
			ratio = 999.99;

		buf_size_count = snprintf(buf_size, 256, "Size[MB]: %.1f", d_comp);
		if(is_unique)
			buf_ratio_count = snprintf(buf_ratio, 256, "; Ratio: %.2f; unique", ratio);
		else
			buf_ratio_count = snprintf(buf_ratio, 256, "; Ratio: %.2f", ratio);

		uint len = uint(source_field->comment.length) + buf_size_count + buf_ratio_count + 3;
		//	char* full_comment = new char(len);  !!!! DO NOT USE NEW !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
		char* full_comment = (char*)my_malloc(len, MYF(0));
		for(uint i = 0; i < len; i++)
			full_comment[i]=' ';
		full_comment[len-1]=0;

		char* pos = full_comment + source_field->comment.length;
		if(source_field->comment.length) {
			memcpy(full_comment, source_field->comment.str, source_field->comment.length);
			*pos++ = ';';
			*pos++ = ' ';
		}

		//memcpy_s(pos, buf_size_count, buf_size, buf_size_count);
		memcpy(pos, buf_size, buf_size_count);
		pos += buf_size_count;
		//memcpy_s(pos, buf_ratio_count, buf_ratio, buf_ratio_count);
		memcpy(pos, buf_ratio, buf_ratio_count);
		pos[buf_ratio_count] = 0;

		table->field[field_id]->store(full_comment, len-1, cs);
		//	delete [] full_comment;	!!!! DO NOT USE DELETE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		my_free(full_comment, MYF(0));
		//	table->field[field_id]->store("abcde", 5, cs);
	} else {
		table->field[field_id]->store(source_field->comment.str, uint(source_field->comment.length), cs);
	}
#endif
}

ConnectionInfo* RCEngine::GetThreadParams(const THD& thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(thd.ha_data[m_engine_slot].ha_ptr != NULL)
		return (ConnectionInfo*)thd.ha_data[m_engine_slot].ha_ptr;
	if(thd.ha_data[m_engine_slot].ha_ptr == NULL)
		CeateThreadParams(const_cast<THD&>(thd));
	return (ConnectionInfo*)thd.ha_data[m_engine_slot].ha_ptr;
#endif
}

void RCEngine::CeateThreadParams(THD& thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(thd.ha_data[m_engine_slot].ha_ptr == 0);
	ConnectionInfo *ci = new ConnectionInfo(&thd);
	thd.ha_data[m_engine_slot].ha_ptr = ci;
	ConnectionInfoOnTLS.Set(*static_cast<ConnectionInfo*>(thd.ha_data[m_engine_slot].ha_ptr));
#endif
}

void RCEngine::ClearThreadParams(THD& thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	ConnectionInfoOnTLS.Release();
	delete (ConnectionInfo*)thd.ha_data[m_engine_slot].ha_ptr;
	thd.ha_data[m_engine_slot].ha_ptr = NULL;
#endif
}

//bool RCEngine::HasAnyLockForTable(const THD& thd, const string& table_path)
//{
//	IBGuard guard(threadparams_mutex);
//	ThreadParamsMap::iterator iter	= m_ThreadParamsMap.begin();
//	ThreadParamsMap::iterator end	= m_ThreadParamsMap.end();
//
//	for(; iter != end; ++iter) {
//		if(thd.thread_id != iter->second->GetThreadID() && iter->second->HasAnyLockForTable(table_path))
//			return true;
//	}
//	return false;
//}

//void RCEngine::ReleaseTableLock(const THD& thd, const TableLockPtr table_lock)
//{
//	IBGuard guard(threadparams_mutex);
//	ThreadParamsMap::iterator iter	= m_ThreadParamsMap.begin();
//	ThreadParamsMap::iterator end	= m_ThreadParamsMap.end();
//
//	for(; iter != end; ++iter) {
//		if(thd.thread_id == iter->second->GetThreadID())
//			iter->second->ReleaseTableLock(table_lock);
//	}
//}
//
//void RCEngine::ReleaseAllWriteLocks(const THD& thd, const string& table_path)
//{
//	IBGuard guard(threadparams_mutex);
//	GetThreadParams(thd)->ReleaseAllWriteLocks(table_path);
//}

int RCEngine::SetUpCacheFolder(const string& cachefolder_path)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if (!exists(cachefolder_path)) {
		rclog << lock << "Warning, Cachefolder " << cachefolder_path << " does not exist. Trying to create it." << unlock;
		try {
	        	CreateDir( cachefolder_path.c_str() );
		} catch(DatabaseRCException& e) {
			sql_print_error("Brighthouse: Can not create folder %s. Make sure the parent folders exist and there is proper permission to create this folder on target device. Or create this folder with proper permission before starting Infobright server.", cachefolder_path.c_str());
			rclog << lock << e.what() << " DatabaseRCException" << unlock;
			return 1;
		}
	}
    
	if (! IsReadWriteAllowed(cachefolder_path.c_str())) {
		sql_print_error("Brighthouse: Can not access cache folder %s.", cachefolder_path.c_str());
		rclog << lock << "Error: Can not access cache folder " << cachefolder_path.c_str() << '.' << unlock;
		return 1;
	}
	return 0;
#endif
}

void RCEngine::RemoveTemporaryFiles(string cachefolder_path)
{
	ClearDirectory(cachefolder_path.c_str());
    /**************************************
	string shell_command = "rm -f ";
	shell_command += cachefolder_path;
	shell_command += "/*.bh_tmp";
	system(shell_command.c_str());
    **************************************/
}

bool RCEngine::IsRouteMySqlLoader(THD* thd)
{
	longlong load_route = -1;
	string s_res;

	if(!get_parameter(thd, BH_DATAFORMAT, load_route, s_res) && iequals(trim_copy(s_res), "MYSQL"))
		return true;
	return false;
}

void RCEngine::TerminateLoader(THD *thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	BHASSERT(thd != 0, "'thd' should not be null");
	ConnectionInfo* ci = (ConnectionInfo*) thd->ha_data[m_engine_slot].ha_ptr;
#ifdef __WIN__
#define PROCESS_ID   pi.hProcess
#else
#define PROCESS_ID   pi
#endif
	if((ci != 0) && (ci->PROCESS_ID > 0)) {
        IBProcess ib_process(ci->PROCESS_ID);
        ib_process.Terminate();
        //int return_code;
        //ib_process.Wait(return_code, 1);
	}
#endif
}

void RCEngine::ReplaceTable(const std::string& table_path, RCTablePtr table)
{
	char db_name[256] = {0};
	char tab_name[256] = {0};
	GetNames(table_path.c_str(), db_name, tab_name, 256);
	IBGuard guard(tables_mutex);
	RCTablePtr old_tab = m_tables[db_name][tab_name];
	if (!!old_tab)
		old_tab->MakeObsolete();
	m_tables[db_name][tab_name] = table;
}

std::string get_parameter_name(enum bh_var_name vn)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(vn >= 0 && vn <= BH_VAR_LIMIT );
	return bh_var_name_strings[vn];
}

int get_parameter(THD* thd, enum bh_var_name vn, double& value)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	string var_data = get_parameter_name(vn);
	user_var_entry* m_entry;
	if((m_entry = (user_var_entry*)hash_search(&thd->user_vars, (uchar*)var_data.c_str(), (uint)var_data.size())) && m_entry->value != NULL) {
		if(m_entry->value != NULL) {
			if(m_entry->type == INT_RESULT) {
				value = (double)*(longlong*) m_entry->value;
			} else if(m_entry->type == DECIMAL_RESULT) {
				my_decimal v = *(my_decimal*)m_entry->value;
				my_decimal2double(E_DEC_FATAL_ERROR, &v, &value);
			} else
				return 2;
		}
	} else
		return 1;
	return 0;
#endif
}

int get_parameter(THD* thd, enum bh_var_name vn, int64& value)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	string var_data = get_parameter_name(vn);
	user_var_entry* m_entry;
	if((m_entry = (user_var_entry*)hash_search(&thd->user_vars, (uchar*)var_data.c_str(), (uint)var_data.size())) && m_entry->value != NULL) {
		if(m_entry->type == INT_RESULT) {
			if(m_entry->value != NULL)
				value = (int64)*(longlong*)m_entry->value;
		} else
			return 2;
	} else
		return 1;
	return 0;
#endif
}

int get_parameter(THD* thd, enum bh_var_name vn, std::string& value)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	string var_data = get_parameter_name(vn);
	user_var_entry* m_entry;
	if((m_entry = (user_var_entry*)hash_search(&thd->user_vars, (uchar*)var_data.c_str(), (uint)var_data.size())) && m_entry->value != NULL) {
		if(m_entry->type == STRING_RESULT) {
			value = string(m_entry->value);
		} else
			return 2;
	} else
		return 1;
	return 0;
#endif
}


int get_parameter(THD* thd, enum bh_var_name vn, longlong &result, string& s_result)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	user_var_entry* m_entry;
	string var_data = get_parameter_name(vn);

	if(m_entry = (user_var_entry*)hash_search(&thd->user_vars, (uchar*)var_data.c_str(), (uint)var_data.size())) {
		if(m_entry->type == DECIMAL_RESULT) {
			switch(vn) {
			case BH_ABORT_ON_THRESHOLD: {
				double dv;
				my_decimal v = *(my_decimal*)m_entry->value;
				my_decimal2double(E_DEC_FATAL_ERROR, &v, &dv);
				result = *(longlong*)&dv;
				break;
			}
			default:
				result = -1;
				break;
			}
			return 0;
		} else if(m_entry->type == INT_RESULT) {
			switch(vn) {
				case BH_TIMEOUT:
				case BH_THROTTLE:
				case BH_IBEXPRESSIONS:
				case BH_PARALLEL_AGGR:
				case BH_ABORT_ON_COUNT:
						result = *(longlong*) m_entry->value;
						break;
				default:
					result = -1;
					break;
			}
			return 0;
		} else if(m_entry->type == STRING_RESULT) {
			result = -1;
			if(m_entry->value == NULL)
				return 1;
			var_data = string(m_entry->value);
			if(vn == BH_DATAFORMAT || vn == BH_REJECT_FILE_PATH) {
				s_result = var_data;
			} else if(vn == BH_PIPEMODE) {
				to_upper(var_data);
				if(var_data == "SERVER")
					result = 1;
				if(var_data == "CLIENT")
					result = 0;
			} else if(vn == BH_NULL) {
				s_result = var_data;
			}
			return 0;
		} else {
			result = 0;
			return 2;
		}
	} else
		return 1;
#endif
}

void RCEngine::DatadirMigrator::DoMigrationIfNeeded()
{
	if(dv.GetDataVersion() == CURRENT_DATA_VERSION && dv.GetServerVersion() != InfobrightServerVersion())
		dv.UpdateTo(CURRENT_DATA_VERSION, string(InfobrightServerVersion())); // Just update server version
	else if(dv.GetDataVersion() < CURRENT_DATA_VERSION)
		Upgrade();
	else if(dv.GetDataVersion() > CURRENT_DATA_VERSION)
		Downgrade();
}

void RCEngine::DatadirMigrator::Downgrade()
{
	BHASSERT(dv.GetDataVersion() > CURRENT_DATA_VERSION, "An attempt to downgrade to newer version!");
	std::string str = "You are attempting to downgrade to " + string(InfobrightServerVersion()) + " from " + dv.GetServerVersion() + ". Due to data incompatibilities, the server is unable to start.";
	str += " Please contact your Infobright Support representative, or review the " + dv.GetServerVersion() + " documentation for specific downgrade instructions.";
	throw RCException(str);
}






