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

#include "core/RCEngine.h"
#include "edition/core/Transaction.h"
#include "core/RCTableImpl.h"

using namespace std;

BHEngineReturnValues RCEngine::RunLoader(THD* thd, sql_exchange* ex, TABLE_LIST* table_list, BHError& bherror)
{
	return ExternalLoad(thd, ex, table_list, bherror);
}

BHEngineReturnValues RCEngine::ExternalLoad(THD* thd, sql_exchange* ex,	TABLE_LIST* table_list, BHError& bherror)
{
	BHEngineReturnValues ret = LD_Successed;

	char name[FN_REFLEN];
	TABLE* table;
	int error=0;
	String* field_term = ex->field_term;
	int transactional_table = 0;
	boost::shared_array<char> BHLoader_path(new char[4096]);
	ConnectionInfo* tp = NULL;

	COPY_INFO info;

	try {
		// GA
			size_t len = 0;
			dirname_part(BHLoader_path.get(), my_progname, &len);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(BHLoader_path[len] == 0);

		// 	dirname_part(BHLoader_path.get(), my_progname);

		strcat(BHLoader_path.get(), BHLoaderAppName);
		uint counter = 0;
		open_tables(thd, &table_list, &counter, MYSQL_LOCK_IGNORE_FLUSH);
		if(counter != 1 || !RCEngine::IsBHTable(table_list->table))
			return LD_Continue;

		//if(field_term->length() > 1) {
		//	my_message(ER_WRONG_FIELD_TERMINATORS, ER(ER_WRONG_FIELD_TERMINATORS), MYF(0));
		//	bherror = BHError(BHERROR_SYNTAX_ERROR, "Wrong field terminator.");
		//	return LD_Failed;
		//}

		if(open_and_lock_tables(thd, table_list)) {
			bherror = BHError(BHERROR_UNKNOWN, "Could not open or lock required tables.");
			return LD_Failed;
		}
		auto_ptr<Loader> bhl = LoaderFactory::CreateLoader(LOADER_BH);

		table = table_list->table;
		transactional_table = table->file->has_transactions();

		IOParameters iop;
		BHError bherr;
		if((bherror = RCEngine::GetIOParameters(iop, *thd, *ex, table)) != BHERROR_SUCCESS)
			throw LD_Failed;

		string table_path = GetTablePath(thd, table);
		// already checked that this is a BH table
		tp = GetThreadParams(*thd);
		int tab_id = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
		if(rsi_manager)
			rsi_manager->UpdateDefForTable(tab_id);

		table->copy_blobs=0;
		thd->cuted_fields=0L;

		bhl->Init(BHLoader_path.get(), &iop);
		bhl->MainHeapSize() = this->m_loader_main_heapsize;
		bhl->CompressedHeapSize() = this->m_loader_compressed_heapsize;

		uint transaction_id = tp->GetTransaction()->GetID();
		bhl->TransactionID() = transaction_id;
		bhl->SetTableId(tab_id);
		
		uint lask_packrow = JustATable::PackIndex(tp->GetTransaction()->GetTable(table_path, table->s)->NoObj());
		
		ulong warnings = 0;

#ifdef __WIN__
        if(bhl->Proceed(tp->pi.hProcess) && !tp->killed())
#else
        if(bhl->Proceed(tp->pi) && !tp->killed())
#endif
		{
        	if((_int64) bhl->NoRecords() > 0) {
				int tid = tp->GetTransaction()->GetTable(table_path, table->s)->GetID();
				tp->GetTransaction()->ReleasePackRow(tid, lask_packrow);
				for(int a = 0; a < tp->GetTransaction()->GetTable(table_path, table->s)->NoAttrs(); a++) {
					tp->GetTransaction()->DropLocalObject(SpliceCoordinate(tid, a, lask_packrow / DEFAULT_SPLICE_SIZE ));
				}
				tp->GetTransaction()->RefreshTable(table_path.c_str()); // thread id used temp to store transaction id ...
			}
            std::ifstream warnings_in( bhl->GetPathToWarnings().c_str() );
            if ( warnings_in.good() ) {
            	std::string line;
            	while(std::getline(warnings_in, line).good()) {
            		push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, ER_UNKNOWN_ERROR, line.c_str());
            		warnings++;
            	}
            	warnings_in.close();
            }
            RemoveFile(bhl->GetPathToWarnings().c_str(), false);
		} else {
            RemoveFile(bhl->GetPathToWarnings().c_str(), false);
			if(tp->killed()) {
				bherror = BHError(BHERROR_KILLED, error_messages[BHERROR_KILLED]);
				thd->send_kill_message();
			}
			else if(*bhl->ErrorMessage() != 0)
				bherror = BHError(bhl->ReturnCode(), bhl->ErrorMessage());
			else
				bherror = BHError(bhl->ReturnCode());
			error = 1; //rollback required
			throw LD_Failed;
		}
				
		// We must invalidate the table in query cache before binlog writing and
		// ha_autocommit_...
		query_cache_invalidate3(thd, table_list, 0);

		info.records = ha_rows((_int64)bhl->NoRecords());
		info.copied  = ha_rows((_int64)bhl->NoCopied());
		info.deleted = ha_rows((_int64)bhl->NoDeleted());

		sprintf(name, ER(ER_LOAD_INFO), (ulong) info.records, (ulong) info.deleted,
				(ulong) (info.records - info.copied), (ulong) warnings);
		my_ok(thd, info.copied+info.deleted, 0L, name);
		//send_ok(thd,info.copied+info.deleted,0L,name);
	} catch(BHEngineReturnValues& bherv) {
		ret = bherv;
	}

	if(thd->transaction.stmt.modified_non_trans_table)
		thd->transaction.all.modified_non_trans_table= TRUE;

	if(transactional_table)
		error = ha_autocommit_or_rollback(thd, error);




	if(thd->lock) {
		mysql_unlock_tables(thd, thd->lock);
		thd->lock=0;
	}
	thd->abort_on_warning = 0;
	return ret;
}
