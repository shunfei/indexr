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

#ifndef _BHERROR_EXCEPTION_H
#define _BHERROR_EXCEPTION_H

#include <stdexcept>

#include "common/CommonDefinitions.h"

enum BHErrorCode
{
	BHERROR_SUCCESS = 0,
	BHERROR_CANNOT_OPEN_FILE_OR_PIPE = 1,
	BHERROR_DATA_ERROR = 2,
	BHERROR_SYNTAX_ERROR = 3,
	BHERROR_CANNOT_CONNECT_TO_THE_DATABES = 4,
	BHERROR_UNKNOWN = 5,
	BHERROR_WRONG_PARAMETER = 6,
	BHERROR_DATACONVERSION = 7,
	BHERROR_UNSUPPORTED_DATATYPE = 8,
	BHERROR_OUT_OF_MEMORY = 9,
	BHERROR_KILLED = 10,
	BHERROR_BAD_CONFIGURATION = 11,
	BHERROR_BHLOADER_UNAVAILABLE = 12,
	BHERROR_MYSQLLOADER_UNAVAILABLE = 13,
	BHERROR_UNAVAILABLE_EXPORTFORMAT = 14,
	BHERROR_UNSUPPORTED_SYNTAX = 15,
	BHERROR_REMOTE_LOAD_ON_LOOKUP = 16,
	BHERROR_REMOTE_LOAD_REQUIRE_CHARSET_CONVERTION = 17,
	BHERROR_UNKNOWN_DECOMPOSITION = 18,
	BHERROR_WRONG_NUMBEROF_PARAMETERS = 19,
	BHERROR_INTERNAL_EXCEPTION = 20,
	BHLOADER_ERROR_TERMINATED_ABNORMALLY = 21,
	BHERROR_LICENSE_VERIFICATION_FAILED = 22,
	BHERROR_DATA_CORRUPTION,
	BHERROR_SYSERROR,
	BHERROR_ERROR_LIMIT  //keep it always the last error message
};

enum BHReturnCode
{
	BHRC_SUCCESS = 0,
	BHRC_FAILD = 1,
	BHRC_OUT_OF_RANGE_VALUE = 2,
	BHRC_VALUE_TRUNCATED = 3
};

static std::string error_messages[] = {
	"Success.",
	"Cannot open file or pipe.",
	"Wrong data or column definition.",
	"Syntax error.",
	"Cannot connect to the database",
	"Unknown error.",
	"Wrong parameter.",
	"Data conversion error.",
	"Unsupported data type.",
	"Out of memory/disk space.",
	"Load statement terminated.",
	"Bad configuration file.",
	"Invalid option. Please use set @bh_dataformat = 'mysql';",
	"Invalid option. Please use set @bh_dataformat = 'txt_variable';",
	"Invalid option. Please set @bh_dataformat to a supported format.",
	"Unsupported load syntax.",
	"Loading data in 'infobright' format to columns defined as 'LOOKUP' is not supported.",
	"Loading data in 'infobright' format to columns with nonbinary collations is not supported.",
	"Unknown decomposition.",
	"Wrong number of parameters",
	"Internal exception",
	"Error in Load process. Possible cause: loader process was killed, unexpected internal error inside loader process, loader binary does exist, or permission issue of loader binary etc.",
	"License verification failed",
	"Inconsistent data found",
	"System Error."
};

class BHReturn
{
public:
	static bool IsError(BHReturnCode bhrc)
	{
		return bhrc == BHRC_FAILD;
	}

	static bool IsWarning(BHReturnCode bhrc)
	{
		return bhrc == BHRC_OUT_OF_RANGE_VALUE || bhrc == BHRC_VALUE_TRUNCATED;
	}
};

class BHError
{
public:
	BHError(BHErrorCode bh_error_code = BHERROR_SUCCESS)
		: error_code(bh_error_code), is_message_set(false)
	{
	}

	BHError(BHErrorCode bh_error_code, std::string message)
		: error_code(bh_error_code), message(message), is_message_set(true)
	{
	}

	BHError(const BHError& bhe)
		: error_code(bhe.error_code), message(bhe.message), is_message_set(bhe.is_message_set)
	{
	}

	operator BHErrorCode()
	{
		return error_code;
	}

	BHErrorCode ErrorCode()
	{
		return error_code;
	}

	bool operator==(BHErrorCode bhec)
	{
		return bhec == error_code;
	}

	const std::string& Message()
	{
		if(is_message_set)
			return message;
		return error_messages[error_code];
	}

	static const std::string& BHErrorMessage(BHErrorCode bh_error_code)
	{
		return error_messages[bh_error_code];
	}

private:
	BHErrorCode error_code;
	std::string message;
	bool is_message_set;
};

class RCException : public std::runtime_error
{
public:
	RCException(std::string const& msg) throw() : std::runtime_error(msg) {}
};

// internal error
class InternalRCException : public RCException
{
public:
	InternalRCException(std::string const& msg) throw() : RCException(msg) {}
	InternalRCException(BHError bherror) throw() : RCException(bherror.Message()) {}
};

// the system lacks memory or cannot use disk cache
class OutOfMemoryRCException : public RCException
{
public:
	OutOfMemoryRCException() throw() : RCException("Insufficient memory/disk space") {}
	OutOfMemoryRCException(std::string const& msg) throw() : RCException(msg) {}
};

class KilledRCException : public RCException
{
public:
	KilledRCException() throw() : RCException("Process killed") {}
	KilledRCException(std::string const& msg) throw() : RCException(msg) {}
};

// there are problems with system operations,
// e.g. i/o operations on database files, system functions etc.
class SystemRCException : public RCException
{
public:
	SystemRCException(std::string const& msg) throw() : RCException(msg) {}
};

// database is corrupted
class DatabaseRCException : public RCException
{
public:
	DatabaseRCException(std::string const& msg) throw() : RCException(msg) {}
};

// database is corrupted
class NoTableFolderRCException : public DatabaseRCException
{
public:
	NoTableFolderRCException(std::string const& dir) throw() : DatabaseRCException(std::string("Table folder ")+dir+" does not exist") {}
};

// user command syntax error
class SyntaxRCException : public RCException
{
public:
	SyntaxRCException(std::string const& msg) throw() : RCException(msg) {}
};

// a user command has the inappropriate semantics
// e.g. table or attribute do not exist
class DBObjectRCException : public RCException
{
public:
	DBObjectRCException(std::string const& msg) throw() : RCException(msg) {}
};

// user command is too complex (not implemented yet)
class NotImplementedRCException : public RCException
{
public:
	NotImplementedRCException(std::string const& msg) throw() : RCException(msg) {}
};

// a user tries to load a table with open session
class SessionOpenRCException : public RCException
{
public:
	SessionOpenRCException(std::string const& msg) throw() : RCException(msg) {}
};

// import file does not exists or can not be open
class FileRCException : public RCException
{
public:
	FileRCException(std::string const& msg) throw() : RCException(msg) {}
	FileRCException(BHError bherror) throw() 		: RCException(bherror.Message()) {}
};

// the system lacks memory or cannot use disk cache
class TempFileRCException : public RCException
{
public:
	TempFileRCException() throw() : RCException("Error when reading/writing to a temporary file") {}
	TempFileRCException(std::string const& msg) throw() : RCException(msg) {}
};

// wrong format of import file
class FormatRCException : public RCException
{
public:
	unsigned long long m_row_no;
	unsigned int m_field_no;

	FormatRCException(BHError bherror, unsigned long long row_no, unsigned int field_no) throw()
		: RCException(bherror.Message()), m_row_no(row_no), m_field_no(field_no)
	{}
	FormatRCException(std::string const& msg) throw() : RCException(msg), m_row_no(-1), m_field_no(-1)  {}
};

// buffer overrun (e.g. in a compression routine)
class BufOverRCException : public InternalRCException
{
public:
	BufOverRCException(std::string const& msg) throw() : InternalRCException(msg) {}

};

class DataTypeConversionRCException : public RCException
{
public:
	_int64 value;		// converted value
	AttributeType type; // type to which value is converted
	DataTypeConversionRCException(std::string const& msg, _int64 val = NULL_VALUE_64,
			AttributeType t = RC_UNKNOWN) throw()
		: RCException(msg), value(val), type(t)
	{
	}

	DataTypeConversionRCException(BHError bherror = BHERROR_DATACONVERSION, _int64 val = NULL_VALUE_64,
			AttributeType t = RC_UNKNOWN) throw()
		: RCException(bherror.Message()), value(val), type(t)
	{
	}
};

class UnsupportedDataTypeRCException : public RCException
{
public:
	UnsupportedDataTypeRCException(std::string const& msg) throw() : RCException(msg) {}

	UnsupportedDataTypeRCException(BHError bherror = BHERROR_UNSUPPORTED_DATATYPE) throw()
		: RCException(bherror.Message())
	{
	}
};

#endif  //_BHERROR_EXCEPTION_H

