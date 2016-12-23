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

#ifndef _SYSTEM_IOPARAMS_H_
#define _SYSTEM_IOPARAMS_H_

//////////////////////////////////////////////////////////////////////////////////////////
///////////////////  IOParameters:   /////////////////////////////////////////////////////
//
// A class representing output description
//

#include "common/CommonDefinitions.h"
#include "system/IBFileSystem.h"
#include "core/RCAttrTypeInfo.h"
#include "common/bhassert.h"

#ifndef __GNUC__
#define PATH_MAX  _MAX_PATH
#endif

class IBStream;

class IOParameters
{
public:
	IOParameters();
	IOParameters(const IOParameters& io_params);
	IOParameters(std::string base_path, std::string table_name);

	IOParameters(std::string base_path, std::string table_name, std::string install_path="")
		: base_path(base_path), table_name(table_name), install_path(install_path)
	{
		Init();
	}

    IOParameters& operator= (const IOParameters& rhs)
    {
        Copy (rhs);
        return *this;
    }

    void SetNoColumns(uint no_columns);

    void SetDelimiter(const std::string& delimiter)
    {
    	this->delimiter = delimiter;
    }
    void SetLineTerminator(const std::string& lineTerminator_)
    {
    	line_terminator = lineTerminator_;
    }

	void SetParameter(Parameter param, int value)
	{
		switch(param)
		{
		case STRING_QUALIFIER	: string_qualifier = (char)value; break;
		case TIMEOUT			: timeout = value; break;
		case PIPEMODE			: pipe_mode = value; break;
		case CHARSET_INFO_NUMBER: charset_info_number = value; break;	
		case LOCAL_LOAD			: local_load = value; break;
		case OPTIONALLY_ENCLOSED: opt_enclosed = value; break;
		case LOCK_OPTION		: lock_option = value; break;
		default:
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(0 && "unexpected value"); break;
		}
	}

	void SetParameter(Parameter param, const std::string& value)
	{
		switch(param)
		{
			case LINE_STARTER			: line_starter = value; break;
			case LINE_TERMINATOR		: line_terminator = value; break;
			case CHARSETS_INDEX_FILE	: charsets_dir = value; break;
			default:
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(0 && "unexpected value"); break;
		}
	}

	void SetParameter(Parameter param, _int64 value)
	{
		switch(param)
		{
			case SKIP_LINES			: skip_lines = value; break;
			case VALUE_LIST_ELEMENTS: value_list_elements = value; break;
			default:
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(0 && "unexpected value"); break;
		}
	}

	void SetColumnsCollations(const std::vector<uchar>& columns_collations)
	{
		this->columns_collations = columns_collations;
	}

	void SetTimeZone(short _sign, short _minute) { sign = _sign; minute = _minute; }
	void GetTimeZone(short& _sign, short& _minute) const { _sign = sign; _minute = minute; }

	void SetEscapeCharacter(char value)
	{
		this->escape_character = value;
	}

	void SetCharsetsDir(const std::string& value)
	{
		this->charsets_dir = value;
	}

	void SetNullsStr(const std::string& null_str)
	{
		this->null_str = null_str;
	}

	void SetOutput(int _mode, const char *fname, int _pipe_mode_tmp = -1)		// mode: 0 - standard console-style output with header
		// fname: NULL for console
		// These settings will work until the next change
	{
		curr_output_mode=_mode;
		if(fname)
			strcpy(output_path, fname);
		else
			output_path[0]='\0';
		pipe_mode_tmp = _pipe_mode_tmp != -1 ? _pipe_mode_tmp : pipe_mode;
	}

	void SetOutputPath(const std::string& path)
	{
		strcpy(output_path, path.c_str());
	}

	void SetNoOutliers(int col, int64 no_outliers);

	const std::vector<int64>& Outliers() const { return no_outliers; }

	//int Mode() const { return curr_output_mode; }
	int			GetNoColumns() const	{ return no_columns; }
	EDF 		GetEDF() const;
	const char*	Path() const			{ return output_path; }
	int			PipeMode() const		{ return pipe_mode_tmp; }
	int			Timeout() const			{ return timeout; }
	std::string	NullsStr() const		{ return null_str; }
	const std::string&	Delimiter() const	{ return delimiter; }
	const std::string&	LineTerminator() const	{ return line_terminator; }
	char		StringQualifier() const { return string_qualifier; }
	char		EscapeCharacter() const { return escape_character; }
	std::string	BasePath() 				{ return base_path; }
	std::string	InstallPath() 				{ return install_path; }
	std::string	TableName() const		{ return table_name; }
	std::string	TablePath() const
	{
		return MakeDirectoryPathOsSpecific(base_path) += TableName();
	}
	std::string	LineStarter() const		{ return line_starter; }
	_int64		SkipLines() const		{ return skip_lines; }
	const std::string& CharsetsDir() const	{ return charsets_dir; }
	const std::vector<uchar>& ColumnsCollations() const	{ return columns_collations; }
	std::vector<std::string>& GetDecompositions() { return columns_decompositions; }
	int			CharsetInfoNumber() const { return charset_info_number;	}
	int			LocalLoad() const		{ return local_load; }
	int			LockOption() const		{ return lock_option; }	
	int			OptionallyEnclosed()	{ return opt_enclosed; }

	uint GetPackrowSize() const	{ return packrow_size; }
	void SetPackrowSize(uint packrow_size) { this->packrow_size = packrow_size; }
	void SetRejectFile(std::string const& path, _int64 abortOnCount, double long abortOnThreshold) {
		reject_file = path;
		abort_on_count = abortOnCount;
		abort_on_threshold = abortOnThreshold;
	}
	std::string GetRejectFile() const { return (reject_file); }
	int64 GetAbortOnCount() const { return (abort_on_count); }
	double GetAbortOnThreshold() const { return (abort_on_threshold); }

	void SetATIs(const std::vector<ATI>& atis) { this->atis = atis; }
	std::vector<ATI> 		GetATIs() const { return atis; }
	const std::vector<ATI>& ATIs() const {return atis; }
	/* Output format:
		0. An ushort(2 byte) value specifying len of output buffer len
		1. A pointer to a null-terminated string that specifies the database's path
		2. A pointer to a null-terminated string that specifies the name of the table
		3. A pointer to a null-terminated string that specifies output/input file/pipe
		3. A pointer to a null-terminated string that specifies line starter string
		3. A pointer to a null-terminated string that specifies line terminator string
		4. 1 character that specifies output/input mode
		5. 1/x character that specifies delimiter
		6. 1 character that specifies string qualifier
		6. 1 character that specifies if enclosing is optional
		6. 1 character that specifies escape character
		7. 1 character that specifies pipe's mode
		7. 1 character that specifies if load using LOCAL operand
		7. 1 character that specifies lock option
		8. 4 byte integer that specifies timeout parameter. This should be in seconds
		8. 4 byte integer that specifies character set info number. It is the 'number' field of CHARSET_INFO struct
		8. 8 byte integer that specifies number of skipped lines
		9. 2 byte integer that specifies sign of time zone difference client/server
		10. 2 byte integer that specifies time zone difference in minutes
		11. NULL-terminated string - name of an object use for interprocess synchronization by RSI_MANAGER
	*/
	void PutParameters(IBStream& stream);
	void GetParameters(IBStream& stream);

	ushort GetParamsBufferLen()
	{
		return cur_len;
	}

	static ushort GetParamsBufferLen(char* src)
	{
		return *(ushort*)src;
	}

private:
	uint no_columns;
	std::vector<ATI> atis;
	char curr_output_mode;		// I/O file format - see RCTable::SaveTable parameters
	char output_path[1024];		// input or output file name, set by "interface..."
	int timeout;
	std::string delimiter;
	int pipe_mode_tmp;
	int pipe_mode;				// 0 - client, <> 0 - server
	char string_qualifier;
	char escape_character;
	char opt_enclosed;
	std::string line_starter;
	std::string line_terminator;
	std::string charsets_dir;
	int charset_info_number;
	std::vector<uchar> columns_collations;
	std::vector<std::string> columns_decompositions;
	std::vector<int64>	no_outliers;
	_int64 skip_lines;
	_int64 value_list_elements;
	int local_load;
	int lock_option;

	// TimeZone
	short sign;
	short minute;

	std::string base_path;
	std::string table_name;

	std::string null_str;

	uint packrow_size;
	std::string reject_file;
	int64 abort_on_count;
	double abort_on_threshold;

	ushort cur_len;
	std::string install_path;


private:
	void Init()
	{
		no_columns = 0;
		output_path[0]='\0';
		curr_output_mode=0;
		delimiter = std::string(DEFAULT_DELIMITER);
		string_qualifier = DEFAULT_STRING_QUALIFIER;
		opt_enclosed = 0;
		timeout = DEFAULT_PIPE_TIMEOUT;
		pipe_mode_tmp = pipe_mode = DEFAULT_PIPE_MODE;
		cur_len = 0;
		escape_character = 0;
		sign = 1;
		minute = 0;
		line_starter = std::string("");
		line_terminator = std::string(DEFAULT_LINE_TERMINATOR);
		charset_info_number = 0;
		skip_lines = 0;
		value_list_elements = 0;
		local_load = 0;
		lock_option = -1;
		atis.clear();
		packrow_size = MAX_PACK_ROW_SIZE;
		no_outliers.clear();
		abort_on_count = 0;
		abort_on_threshold = 0;
	}

    void Copy (const IOParameters& rhs)
    {
    	no_columns = rhs.no_columns;
    	atis = rhs.atis;
	    curr_output_mode = rhs.curr_output_mode;
	    memcpy (output_path, rhs.output_path, sizeof(output_path));
	    timeout = rhs.timeout;
	    delimiter = rhs.delimiter;
	    string_qualifier = rhs.string_qualifier;
		opt_enclosed = rhs.opt_enclosed;
	    escape_character = rhs. escape_character;
	    pipe_mode_tmp = rhs.pipe_mode_tmp;
	    pipe_mode = rhs.pipe_mode;
	    base_path = rhs.base_path;
	    table_name = rhs.table_name;
	    null_str = rhs.null_str;
	    cur_len = rhs.cur_len;
	    sign = rhs.sign;
	    minute = rhs.minute;
		line_starter = rhs.line_starter;
		line_terminator = rhs.line_terminator;
		charsets_dir = rhs.charsets_dir;
		charset_info_number = rhs.charset_info_number;
		columns_collations = rhs.columns_collations;
		columns_decompositions = rhs.columns_decompositions;
		no_outliers = rhs.no_outliers;
		skip_lines = rhs.skip_lines;
		value_list_elements = rhs.value_list_elements;
		local_load = rhs.local_load;
		lock_option = rhs.lock_option;
		packrow_size = rhs.packrow_size;
		install_path = rhs.install_path;
		reject_file = rhs.reject_file;
		abort_on_count = rhs.abort_on_count;
		abort_on_threshold = rhs.abort_on_threshold;
    }

	//static EDF GetEDF(int edf);
};

#endif //_SYSTEM_IOPARAMS_H_

