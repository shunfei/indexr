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

#include "system/MemoryManagement/Initializer.h"
#include "core/RCTableImpl.h"
#include "system/BHToolkit.h"
#include "system/IOParameters.h"
#include "system/FileOut.h"
#include "core/WinTools.h"
#include "core/tools.h"
#include "loader/BHLoader.h"
#include "loader/BHLoaderApp.h"
#include "core/RCEngine.h"
#include "edition/core/GlobalDataCache.h"

extern std::ofstream warnings_out;

int main(int argc, char *argv[])
{
	my_thread_global_init();
	my_init_time();

	if (argc < 3)
		return BHERROR_WRONG_NUMBEROF_PARAMETERS;

#	ifdef FUNCTIONS_EXECUTION_TIMES
	ChannelOut *dev_output = NULL;
	dev_output = new FileOut(infobright_data_dir + "development.log");
	rcdev.addOutput(dev_output);
	rcdev.setOn();
#	endif

	std::string report_file_name = argv[2]; // argv[2] is an full path to some file now
	report_file_name += BHLOADER_REPORT_EXTENSTION;
	FileOut fo(report_file_name.c_str());

	std::string warnings_file = argv[2];
	warnings_file += BHLOADER_WARNINGS_EXTENSION;

	int r = BHERROR_SUCCESS;
	try {
		rclog.addOutput(&fo);
		rclog.setTimeStamp(false);
	} catch (...) {
		r = BHERROR_UNKNOWN;
		return r;
	}

	BHLoader bhl;
	try {
		bhl.Init(argv[1], argv[2]);
		MemoryManagerInitializer::Instance(bhl.CompressedHeapSize(), bhl.MainHeapSize(), bhl.CompressedHeapSize() / 10,
				bhl.MainHeapSize() / 10);
		the_filter_block_owner = new TheFilterBlockOwner();
	} catch (InternalRCException&) {
		r = BHERROR_INTERNAL_EXCEPTION;
		rclog << lock << "bhloader: Internal exception." << __FILE__ << __LINE__ << unlock;
	} catch (OutOfMemoryRCException&) {
		r = BHERROR_OUT_OF_MEMORY;
		rclog << lock << "bhloader: Could not allocate memory." << __FILE__ << __LINE__ << unlock;
	} catch (...){
		r = BHERROR_UNKNOWN;
		rclog << lock << "bhloader: Unknown error." << __FILE__ << __LINE__ << unlock;
	}
	
	if (r != BHERROR_SUCCESS) goto cleanup;
	GlobalDataCache::GetGlobalDataCache().Init();
	
	InitRSIManager();
	CachingLevel = 1;

#	ifdef FUNCTIONS_EXECUTION_TIMES
	fet = new FunctionsExecutionTimes();
#	endif

	/**********
	 int status= 0;
	 SharedMemOut server_out(bhl.RCLogMessage(), BHLoader::s_rclog_message_maxsize, bhl.EventName(), argv[2], status);
	 if (!status) return BHERROR_UNKNOWN;
	 SharedMemOut *pserver_out = &server_out;
	 rclog.addOutput(&server_out);
	 **********/
	warnings_out.open(warnings_file.c_str());
	if (!warnings_out.good()) {
		rclog << lock << "bhloader : Unable to open warnings file, " << warnings_file << __FILE__ << __LINE__ << unlock;
		r = BHERROR_CANNOT_OPEN_FILE_OR_PIPE;
		goto cleanup;
	}

	process_type = ProcessType::BHLOADER;
	r = LoadData(argv[1], argv[2]);

cleanup:
	rsi_manager.reset();
	GlobalDataCache::GetGlobalDataCache().ReleaseAll();
	MemoryManagerInitializer::deinit();
	bhl.DeInit(false);
	warnings_out.close();
	fo.close();

#	ifdef FUNCTIONS_EXECUTION_TIMES
	if (dev_output) {
		dev_output->close();
		delete dev_output;
		dev_output = NULL;
	}
	delete fet;
#	endif

	return r;
}

