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

#ifdef __GNUC__
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#endif
#include <cstdlib>
#include <ctime>
#include <string>
#include <fstream>

#include "edition/local.h"
#include "system/RCSystem.h"
#include "BHLoader.h"
#include "BHLoaderApp.h"
#include "common/bhassert.h"
#include "system/MemoryManagement/Initializer.h"
#include "loader/RCTable_load.h"
#include "core/tools.h"
#include "system/IBFileSystem.h"
#include "system/ib_system.h"
#include "core/Notifier.h"
#include "loader/RemoteLoadHelper.h"
#include "core/TransactionBase.h"
#include "edition/core/Transaction.h"
#include "core/RCEngine.h"
#include <boost/format.hpp>

using namespace std;
using namespace bh;
using namespace boost;

ofstream warnings_out;

BHLoader::~BHLoader()
{
}

bool BHLoader::Proceed(PROCESS& load_process)
{
	BHASSERT(!mode, "'mode' should be false");
	Notifier notifier(Notifier::E_Loader, NULL);

	std::string shared_obj = GetSystemTempPath();
	shared_obj += shared_obj_name;
    std::string report_file_name = shared_obj + BHLOADER_REPORT_EXTENSTION;

    stringstream syserr;
	ReturnCode() = BHERROR_UNKNOWN;

	try {
		shared_file.OpenCreate(shared_obj.c_str());
	} catch(DatabaseRCException& e)	{
		string err_msg= "Unable to start Loader. Trying to create a memory mapped file but it says - ";
		err_msg+= e.what();
		throw InternalRCException(err_msg.c_str());
	}

	RemoteLoadHelper remote_load_helper(iop);

#ifndef RCLOAD_STANDALONE
	{
		TemporalValueReplacement<ProcessType::enum_t> t(process_type, ProcessType::BHLOADER);
		FlushParameters(); // write loader parameters to file
		ReturnCode() = (BHErrorCode)LoadData(shared_obj_name, shared_obj);
		ReLoadParameters();  // read loader results from file
		return (ReturnCode() == BHERROR_SUCCESS);
	}
#endif
	try {
		FunctionExecutor fe(boost::bind(&BHLoader::FlushParameters, this), boost::bind(&BHLoader::ReLoadParameters, this));
		IBProcess ibpr;
		int return_code = -128;
		
		ibpr.Start(bhload_app.c_str(), shared_obj_name.c_str(), shared_obj.c_str());
		// must, to make 'mysql> kill process' work.
		load_process = ibpr.GetProcess();

		try {
			remote_load_helper(ibpr);
		} catch(std::exception const&) {
			ibpr.Wait(return_code);
			throw;
    }

		ibpr.Wait(return_code);
	} catch (IBSysException& e) {
		ReturnCode() = BHERROR_SYSERROR;
		rclog << lock << str(format("System error(%1%) on bhloader execution: %2%.") % e.GetErrorCode() % e.what()) << unlock;
	} catch (RCException& e) {
		if(ReturnCode() == BHERROR_SUCCESS)
			ReturnCode() = BHERROR_UNKNOWN;
		if(*ErrorMessage() == 0)
			strcpy(ErrorMessage(), e.what());
	} catch(...) {
		shared_file.Close();
		RemoveFile(report_file_name.c_str(), false);
		RemoveFile(GetPathToWarnings().c_str(), false);
		RemoveFile(shared_obj.c_str(), false);
		throw;
	}

	std::ifstream is( report_file_name.c_str() );
	if ( is ) {
		 std::string line;
		 while(std::getline(is, line).good())
				rclog << lock << line << unlock;
		 is.close(); 
	}
	RemoveFile(report_file_name.c_str(), false);

	if (ReturnCode() == BHERROR_SUCCESS)
		ConnectionInfoOnTLS.Get().GetTransaction()->AddOutliers(table_id, iop.Outliers());

	DeInit(true);

	return (ReturnCode() == BHERROR_SUCCESS);
}

int LoadData(string shared_obj_name, string shared_obj_path)
{
	BHErrorCode bhl_error_code;
	BHLoader bhl;

	try
	{
		bhl.Init(shared_obj_name, shared_obj_path);
		auto_ptr<RCTableLoad> rct;
		try {
            string table_path = bhl.GetIOParameters().TablePath();
            table_path += ".bht";

			vector<DTCollation> colls;
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
			for(int i = 0; i < bhl.GetIOParameters().ColumnsCollations().size(); i++) {
				DTCollation dtcol;
				dtcol.set(get_charset_IB((char*)bhl.GetIOParameters().CharsetsDir().c_str(), bhl.GetIOParameters().ColumnsCollations()[i], 0));
				colls.push_back(dtcol);
			}
#endif
           	rct = RCTableLoadAutoPtr(new RCTableLoad(table_path.c_str(), 0, colls));
		} catch(DatabaseRCException&) {
			bhl.ReturnCode() = BHERROR_DATA_ERROR;
			#ifndef PURE_LIBRARY
			if(process_type == ProcessType::BHLOADER) {
				free_charsets();
				my_once_free();
			}
			#endif
			return BHERROR_DATA_ERROR;
		}

		unsigned long transaction_id = bhl.TransactionID();
		#ifdef FUNCTIONS_EXECUTION_TIMES
		rcdev.setOn();
		#else
		rcdev.setOff();
		#endif

		string err_msg;
		BufferingLevel = ConfMan.GetValueInt("brighthouse/BufferingLevel", err_msg);

		if (rct->GetSaveSessionId() != 0xFFFFFFFF && rct->GetSaveSessionId() != transaction_id)	{
			bhl.ReturnCode() = BHERROR_CANNOT_CONNECT_TO_THE_DATABES;
			strcpy(bhl.ErrorMessage(), "Unable to start load data session.");
		} else {
			rct->SetWriteMode(transaction_id);
			try {
				rct->LoadData(bhl.GetIOParameters());
				bhl.ReturnCode() = BHERROR_SUCCESS;
				bhl.NoCopied() = bhl.NoRecords() = rct->NoRecordsLoaded();
				bhl.NoDeleted() = 0;
				bhl.PackrowSize() = bhl.GetIOParameters().GetPackrowSize();
				if (rct->NoRecordsRejected() > 0 && !!warnings_out)
					warnings_out << rct->NoRecordsRejected() << " rows rejected during load." << std::endl;
			} catch(FormatRCException& rce) {
				bhl.ReturnCode() = BHERROR_DATA_ERROR;
				if (rce.m_row_no != -1 && rce.m_field_no != -1) {
					stringstream err_msg;
					err_msg << rce.what() << " Row: " << rce.m_row_no << ", field: " << rce.m_field_no << '.';
					strcpy(bhl.ErrorMessage(), err_msg.str().c_str());
				}
				else if (rce.m_row_no != -1 && rce.m_field_no == -1) {
					stringstream err_msg;
					err_msg << rce.what() << " Row: " << rce.m_row_no << '.';
					strcpy(bhl.ErrorMessage(), err_msg.str().c_str());
				} else
					strcpy(bhl.ErrorMessage(), rce.what());
			} catch(FileRCException& rce) {
				strcpy(bhl.ErrorMessage(), rce.what());
				bhl.ReturnCode() = BHERROR_CANNOT_OPEN_FILE_OR_PIPE;
			} catch(RCException& rce) {
				bhl.ReturnCode() = BHERROR_UNKNOWN;
				strcpy(bhl.ErrorMessage(), rce.what());
			} catch(...) {
				bhl.ReturnCode() = BHERROR_UNKNOWN;
			}
		}

		#ifdef FUNCTIONS_EXECUTION_TIMES
		fet->PrintToRcdev();
		#endif
		bhl_error_code = bhl.ReturnCode();
	} catch (...) {
		bhl_error_code = BHERROR_UNKNOWN;
	}

	#ifndef PURE_LIBRARY
	if(process_type == ProcessType::BHLOADER) {
		free_charsets();
		my_once_free();
	}
	#endif
    bhl.FlushParameters();
	return bhl_error_code;
}
