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
#ifndef _LOADER_H_
#define _LOADER_H_

#include "system/IOParameters.h"
#include "system/RCException.h"
#include "system/RCSystem.h"
#include "system/IBFile.h"
#include "common/bhassert.h"

#define RS_CREATOR            1
#define RS_USER               2
#define TMP_FOLDER            "/tmp/"
#define LOADER_MESSAGE_SIZE   512
#define BHLOADER_WARNINGS_EXTENSION    ".bhl_warnings"

enum LOADER_TYPE { LOADER_BH = 0, LOADER_MYSQL = 1 };

class Buffer;

struct LoaderParams
{
	BHErrorCode   return_code;
	_int64        no_records;
	_int64        no_copied;
	_int64        no_deleted;
	int           no_warnings;
	ulong         transaction_id;
	size_t        main_heap_size;
	size_t        compressed_heap_size;
	uint		  packrow_size;
	char          error_message[LOADER_MESSAGE_SIZE];
};

class Loader
{
public:
	Loader();
	virtual ~Loader();

	virtual bool Proceed(PROCESS& load_process)      { return false;};
	virtual bool Proceed(Buffer& buffer, std::vector<DTCollation> charsets) { return false;};

	BHErrorCode& ReturnCode() { return params.return_code; }
	_int64& NoRecords()       { return params.no_records; }
	_int64& NoCopied()        { return params.no_copied; }
	_int64& NoDeleted()       { return params.no_deleted; }
	int& NoWarnings()         { return params.no_warnings; }
	char* ErrorMessage()      { return params.error_message; }
	ulong& TransactionID()    { return params.transaction_id; }
	size_t& MainHeapSize()    { return params.main_heap_size; }
	uint&   PackrowSize()	  { return params.packrow_size; }
	size_t& CompressedHeapSize()  { return params.compressed_heap_size; }
	//operator IOParameters*()  {return & iop; }
	//char* IOParamsBuffer()    {return (char*) (& iop); }
    IOParameters& GetIOParameters() { return iop; }
    std::string GetPathToWarnings() { return GetSystemTempPath()+shared_obj_name+BHLOADER_WARNINGS_EXTENSION; }
    void SetTableId(int id)   { table_id = id; }


public:
	void Init(std::string bhload_app, IOParameters* io_params);
	void Init(std::string shared_obj_name, std::string shared_obj_path);
    void DeInit(bool cleanup = false);
    void FlushParameters();
    void ReLoadParameters();

	std::string bhload_app;
protected:

    LoaderParams params;
	IOParameters  iop;
    std::string shared_obj_name;
	IBFile shared_file;
	bool mode;			// false - server, true - client
	int rs_owner;
	int table_id;

//STATIC
public:
	static int getSaveThreadNumber() { return LoaderSaveThreadNumber; }
	static void setSaveThreadNumber( int no ) { LoaderSaveThreadNumber = no; }

protected:
	static int LoaderSaveThreadNumber;
};

#endif
