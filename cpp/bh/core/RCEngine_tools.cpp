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
#include <boost/filesystem.hpp>
#include "Query.h"
#include "compilation_tools.h"
#include "core/RCEngine.h"
#include "common/DataFormat.h"
#include "system/LargeBuffer.h"
#include "edition/local.h"
#include "edition/core/Transaction.h"

using namespace std;
using namespace boost;

bool RCEngine::IsBHRoute(THD * thd, TABLE_LIST* table_list, SELECT_LEX* selects_list, int &in_case_of_failure_can_go_to_mysql, int with_insert)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	in_case_of_failure_can_go_to_mysql = true;

	if(!table_list)
		return false;
	if(with_insert)
		table_list = table_list->next_global; //we skip one
	for(TABLE_LIST *tl = table_list; tl; tl = tl->next_global) //SZN:we go through tables
	{
		if(!tl->derived && !tl->view && !IsBHTable(tl->table))
			return false;//In this list we have
		//all views, derived tables and their sources, so anyway we walk through all the source tables
		//even though we seem to reject the control of views
	}

	//then we check the parameter of file format.
	//if it is MYSQL_format AND we write to a file, it is a MYSQL route.
	int is_dump;
	const char * file = GetFilename(selects_list, is_dump);

	if (file) {//it writes to a file
		longlong param = 0;
		string s_res;
		if(!get_parameter(thd, BH_DATAFORMAT, param, s_res)) {

			if(boost::iequals(boost::trim_copy(s_res), "MYSQL"))
				return false;

			DataFormatPtr df = DataFormat::GetDataFormat(s_res);
			if(!df) {  //parameter is UNKNOWN VALUE
				my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: Unknown value of BH_DATAFORMAT parameter", MYF(0));
				return true;
			} else if(!df->CanExport()) {
				my_message(ER_SYNTAX_ERROR, (string("Brighthouse specific error: Export in '") + df->GetName() + ("' format is not supported.")).c_str() , MYF(0));
				return true;
			} else
				in_case_of_failure_can_go_to_mysql = false;   //in case of failure
							//it cannot go to MYSQL - it writes to a file,
							//but the file format is not MYSQL
		} else   //param not set - we assume it is (deprecated: MYSQL) TXT_VARIABLE
			return true;
	}

	return true;
#endif
}

bool RCEngine::IsBHTable(TABLE* table)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return table && table->s->db_type() == rcbase_hton;	// table->db_type is always NULL
#endif
}

const char* RCEngine::GetFilename(SELECT_LEX* selects_list, int &is_dumpfile)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	//if the function returns a filename <> NULL
	//additionally is_dumpfile indicates whether it was 'select into OUTFILE' or maybe 'select into DUMPFILE'
	//if the function returns NULL it was a regular 'select'
	//don't look into is_dumpfile in this case
	if (selects_list->parent_lex->exchange)
	{
		is_dumpfile = selects_list->parent_lex->exchange->dumpfile;
		return selects_list->parent_lex->exchange->file_name;
	}
	return 0;
#endif
}

auto_ptr<IOParameters> RCEngine::CreateIOParameters(const string& path)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return auto_ptr<IOParameters>();
#else
	if(path.empty())
		return auto_ptr<IOParameters>(new IOParameters());

	string data_dir;
	string data_path;
	string home_dir = infobright_home_dir;

	if(IsAbsolutePath(path)) {
		data_dir = "";
		data_path = path;
	} else {
		data_dir = infobright_data_dir;
		char db_name[256] = {0}, tab_name[256] = {0};
		GetNames(path.c_str(), db_name, tab_name, 256);
		data_path += db_name;
		data_path += '/';
		data_path += tab_name;
	}

	auto_ptr<IOParameters> iop(new IOParameters(data_dir, data_path, home_dir));
	iop->SetNoColumns(ConnectionInfoOnTLS.Get().GetTransaction()->GetTable(path, 0)->NoAttrs());
	char fname[FN_REFLEN + sizeof("Index.xml")];
	get_charsets_dir(fname);
	iop->SetCharsetsDir(string(fname));

	vector<uchar> columns_collations;
	vector<DTCollation> collations = ConnectionInfoOnTLS.Get().GetTransaction()->GetTable(path, 0)->GetCharsets();
	vector<DTCollation>::iterator iter = collations.begin();
	vector<DTCollation>::iterator end = collations.end();
	for(; iter != end; ++iter )
		columns_collations.push_back(iter->collation->number);

	iop->SetColumnsCollations(columns_collations);
	ConnectionInfoOnTLS.Get().GetTransaction()->GetTable(path, 0)->FillDecompositions(iop->GetDecompositions());

	iop->SetATIs(ConnectionInfoOnTLS.Get().GetTransaction()->GetTable(path, 0)->GetATIs());

	return iop;
#endif
}

auto_ptr<IOParameters> RCEngine::CreateIOParameters(THD *thd, TABLE *table)
{
	if(table == NULL)
		return CreateIOParameters("");

	return CreateIOParameters(GetTablePath(thd, table));
}

string RCEngine::GetTablePath(THD *thd, TABLE *table)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return string();
#else
	BHASSERT(table != NULL, "table pointer shouldn't be null here");
	return table->s->normalized_path.str;
#endif
}

//  compute time zone difference between client and server
void RCEngine::ComputeTimeZoneDiffInMinutes(THD* thd, short& sign, short& minutes)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	if(stricmp(thd->variables.time_zone->get_name()->ptr(), "System") == 0) {
		// System time zone
		sign = 1;
		minutes = NULL_VALUE_SH;
		return;
	}
	TIME client_zone, utc;
	utc.year = 1970;
	utc.month = 1;
	utc.day = 1;
	utc.hour = 0;
	utc.minute = 0;
	utc.second = 0;
	utc.second_part = 0;
	utc.neg = 0;
	utc.time_type = MYSQL_TIMESTAMP_DATETIME;
	thd->variables.time_zone->gmt_sec_to_TIME(&client_zone, (my_time_t)0); // check if client time zone is set
	longlong secs;
	long msecs;
	sign = 1;
	minutes = 0;
	if(calc_time_diff(&utc, &client_zone, 1, &secs, &msecs))
		sign = -1;
	minutes = (short)(secs/60);
#endif
}

BHError RCEngine::GetRejectFileIOParameters(THD& thd, IOParameters& io_params)
{
	string reject_file;
	int64 abort_on_count = 0;
	double abort_on_threshold = 0;

	int ret = get_parameter(&thd, BH_REJECT_FILE_PATH, reject_file);
	if(get_parameter(&thd, BH_REJECT_FILE_PATH, reject_file) == 2)
		return BHError(BHERROR_WRONG_PARAMETER, "Wrong value of BH_REJECT_FILE_PATH parameter.");

	if(get_parameter(&thd, BH_ABORT_ON_COUNT, abort_on_count) == 2)
		return BHError(BHERROR_WRONG_PARAMETER, "Wrong value of BH_ABORT_ON_COUNT parameter.");

	if(get_parameter(&thd, BH_ABORT_ON_THRESHOLD, abort_on_threshold) == 2)
		return BHError(BHERROR_WRONG_PARAMETER, "Wrong value of BH_ABORT_ON_THRESHOLD parameter.");

	if(abort_on_count != 0 && abort_on_threshold != 0)
		return BHError(BHERROR_WRONG_PARAMETER, "BH_ABORT_ON_COUNT and BH_ABORT_ON_THRESHOLD parameters are mutualy exclusive.");

	if(!(abort_on_threshold >= 0.0 && abort_on_threshold < 1.0))
		return BHError(BHERROR_WRONG_PARAMETER, "BH_ABORT_ON_THRESHOLD parameter value must be in range (0,1).");

	if((abort_on_count != 0 || abort_on_threshold != 0) && reject_file.empty())
		return BHError(BHERROR_WRONG_PARAMETER, "BH_ABORT_ON_COUNT or BH_ABORT_ON_THRESHOLD can by only specified with BH_REJECT_FILE_PATH parameter.");

	if(!reject_file.empty() && filesystem::exists(reject_file))
		return BHError(BHERROR_WRONG_PARAMETER, "Can not create the reject file, the file already exists.");

	io_params.SetRejectFile(reject_file, abort_on_count, abort_on_threshold);
	return BHERROR_SUCCESS;
}

BHError RCEngine::GetIOParameters(IOParameters& io_params, THD& thd, sql_exchange& ex, TABLE* table, bool for_exporter)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return BHError(BHERROR_UNKNOWN);
#else
	String *field_term = ex.field_term, *escaped = ex.escaped;
	String *line_term = ex.line_term;
	String *enclosed = ex.enclosed;
	String *line_start = ex.line_start;
	bool opt_enclosed = ex.opt_enclosed;
	ulong skip_lines = ex.skip_lines;
	CHARSET_INFO* cs = ex.cs;
	bool local_load = for_exporter ? false : (bool)(thd.lex)->local_file;
	uint value_list_elements = (thd.lex)->value_list.elements;
	thr_lock_type lock_option = (thd.lex)->lock_option;	

	int io_mode = -1;
	char name[FN_REFLEN];
	char *tdb = 0;
	if(table) {
		tdb = table->s->db.str ? table->s->db.str : thd.db;
	} else
		tdb = thd.db;

	io_params = IOParameters(*RCEngine::CreateIOParameters(&thd, table));
	short sign, minutes;
	ComputeTimeZoneDiffInMinutes(&thd, sign, minutes);
	io_params.SetTimeZone(sign, minutes);

#ifdef DONT_ALLOW_FULL_LOAD_DATA_PATHS
	ex->file_name+=dirname_length(ex->file_name);
#endif

	longlong param = 0;
	string s_res;
	if(DataFormat::GetNoFormats() > 1) {
		if(!get_parameter(&thd, BH_DATAFORMAT, param, s_res)) {			
			DataFormatPtr df = DataFormat::GetDataFormat(s_res);			
			if(!df)
				return BHError(BHERROR_WRONG_PARAMETER, "Unknown value of BH_DATAFORMAT parameter.");
			else 
				io_mode = df->GetId();									
		} else
			io_mode = DataFormat::GetDataFormat(0)->GetId();
	} else
		io_mode = DataFormat::GetDataFormat(0)->GetId();

	if(!get_parameter(&thd, BH_NULL, param, s_res))
		io_params.SetNullsStr(s_res);

#ifdef _MSC_VER
	if(IsPipe(ex.file_name)) {
		strcpy(name, ex.file_name);
		if(!get_parameter(&thd, BH_PIPEMODE, param, s_res))
			io_params.SetOutput(io_mode, ex.file_name, (int) param); // BH_PIPEMODE == 0 - pipe work as a client otherwise as a server
		else
			io_params.SetOutput(io_mode, ex.file_name);
	} else
#endif
	{
		if(!dirname_length(ex.file_name)) {
			strxnmov(name, FN_REFLEN - 1, mysql_real_data_home, tdb, NullS);
			(void) fn_format(name, ex.file_name, name, "", MY_RELATIVE_PATH | MY_UNPACK_FILENAME);
		} else if ( ! local_load )
			(void) fn_format(name, ex.file_name, mysql_real_data_home, "", MY_RELATIVE_PATH | MY_UNPACK_FILENAME);
		else
			strcpy(name, ex.file_name);
		io_params.SetOutput(io_mode, name);
	}
	if(!get_parameter(&thd, BH_TIMEOUT, param, s_res) && param != -1)
		io_params.SetParameter(TIMEOUT, (int) (param));
	
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(escaped);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(field_term);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(line_term);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(enclosed);
	
	if(escaped->length() > 1)
		return BHError(BHERROR_WRONG_PARAMETER, "Multicharacter escape string not supported.");

	if(enclosed->length() > 1 && (enclosed->length() != 4 || stricmp(enclosed->ptr(), "NULL") != 0))
		return BHError(BHERROR_WRONG_PARAMETER, "Multicharacter enclose string not supported.");

	if(!for_exporter) {
		BHError bherr = GetRejectFileIOParameters(thd, io_params);
		if(bherr.ErrorCode() != BHERROR_SUCCESS)
			return bherr;
	}

	if(Configuration::GetProperty(Configuration::UseMySQLImportExportDefaults)) {

		io_params.SetEscapeCharacter(*escaped->c_ptr());
		io_params.SetDelimiter(field_term->c_ptr());
		io_params.SetLineTerminator(line_term->c_ptr());
		if(enclosed->length() == 4 && stricmp(enclosed->ptr(), "NULL") == 0)
			io_params.SetParameter(STRING_QUALIFIER, '\0');
		else
			io_params.SetParameter(STRING_QUALIFIER, *enclosed->ptr());

	} else {

		if(escaped->alloced_length() != 0)
			io_params.SetEscapeCharacter(*escaped->c_ptr());

		if(field_term->alloced_length() != 0)
			io_params.SetDelimiter(field_term->c_ptr());

		if(line_term->alloced_length() != 0)
			io_params.SetLineTerminator(line_term->c_ptr());

		if(enclosed->length()) {
			if(enclosed->length() == 4 && stricmp(enclosed->ptr(), "NULL") == 0)
				io_params.SetParameter(STRING_QUALIFIER, '\0');
			else
				io_params.SetParameter(STRING_QUALIFIER, *enclosed->ptr());
		}
	}


	if(io_params.EscapeCharacter() != 0 && io_params.Delimiter().find(io_params.EscapeCharacter()) != string::npos)
		return BHError(BHERROR_WRONG_PARAMETER, "Field terminator containing the escape character not supported.");

	if(io_params.EscapeCharacter() != 0 && io_params.StringQualifier() != 0 && io_params.EscapeCharacter() == io_params.StringQualifier())
		return BHError(BHERROR_WRONG_PARAMETER, "The same enclose and escape characters not supported.");

	bool unsupported_syntax = false;
	if(cs != 0)
		io_params.SetParameter(CHARSET_INFO_NUMBER, (int) (cs->number));
	else if(!for_exporter)
		io_params.SetParameter(CHARSET_INFO_NUMBER, (int) (thd.variables.collation_database->number)); //default charset

	if(skip_lines != 0) {
		unsupported_syntax = true;
		io_params.SetParameter(SKIP_LINES, (_int64) skip_lines);
	}

	if(line_start != 0 && line_start->length() != 0) {
		unsupported_syntax = true;
		io_params.SetParameter(LINE_STARTER, string(line_start->ptr()));
	}

	if(local_load && ((thd.lex)->sql_command == SQLCOM_LOAD)) {
		io_params.SetParameter(LOCAL_LOAD, (int) local_load);
	}

	if(value_list_elements != 0){
		unsupported_syntax = true;
		io_params.SetParameter(VALUE_LIST_ELEMENTS, (_int64) value_list_elements);
	}

	if(lock_option!=TL_WRITE_DEFAULT && lock_option != TL_READ && lock_option!=-1){
		unsupported_syntax = true;
		io_params.SetParameter(LOCK_OPTION, (int) lock_option);
	}

	if(opt_enclosed) {
		//unsupported_syntax = true;
		io_params.SetParameter(OPTIONALLY_ENCLOSED, 1);
	}
	
		
	if(unsupported_syntax)
		push_warning( &thd, MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, "Query contains syntax that is unsupported in Infobright and which will be ignored." );
	return BHERROR_SUCCESS;
#endif
}

void RCEngine::GetNames(const char* table_path, char* db_buf, char* tab_buf, size_t buf_size)
{
	size_t path_len = strlen(table_path);
	unsigned last = -1, last_but_one = -1;
	for(unsigned i = 0; i < path_len; i++) {
		if(table_path[i]=='/' || table_path[i]=='\\') {
			last_but_one = last;
			last = i+1;
		}
	}
	if(last_but_one==-1 || last-last_but_one > buf_size-1 || path_len-last>buf_size-2)
		return;

	std::strncpy(db_buf, table_path + last_but_one, last-last_but_one-1);
	std::strncpy(tab_buf, table_path + last, path_len-last);
}

std::pair<std::string, std::string> RCEngine::GetDatabaseAndTableNamesFromFullPath(const string& full_path)
{
	string path =  MakeDirectoryPathOsSpecific(to_lower_copy(full_path));
	size_t pos = path.rfind(".bht");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pos != string::npos && pos > 1);

	size_t beg_of_table_name = path.rfind(DIR_SEPARATOR_STRING, pos - 1);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(beg_of_table_name != string::npos);

	size_t beg_of_db_name = path.rfind(DIR_SEPARATOR_STRING, beg_of_table_name - 1);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(beg_of_db_name != string::npos);

	string db_name(path.c_str() + beg_of_db_name + 1, beg_of_table_name - beg_of_db_name - 1);
	string table_name(path.c_str() + beg_of_table_name + 1, pos - beg_of_table_name - 1);

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!db_name.empty());
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!table_name.empty());

	return make_pair(db_name, table_name);
}
