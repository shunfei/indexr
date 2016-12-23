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

#include "edition/vc/VirtualColumn.h"
#include "system/Configuration.h"
#include "core/RCEngine.h"
#include "core/compilation_tools.h"
#include "common/mysql_gate.h"
#include "ha_rcengine.h"

void BH_UpdateAndStoreColumnComment(TABLE* table, int field_id, Field* source_field, int source_field_id, CHARSET_INFO *cs)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	try {
		rceng->UpdateAndStoreColumnComment(table, field_id, source_field, source_field_id, cs);
	}
	catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
#endif
}

namespace
{
bool AtLeastOneIBTableInvolved(LEX *lex)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	for(TABLE_LIST *table_list = lex->query_tables; table_list; table_list = table_list->next_global) {
		TABLE* table = table_list->table;
		if(RCEngine::IsBHTable(table))
			return TRUE;
	}
	return FALSE;
#endif
}

bool ForbiddenMySQLQueryPath(LEX* lex)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return (Configuration::GetProperty(Configuration::AllowMySQLQueryPath) == 0 ||
		(lex->select_lex.options & SELECT_ROUGHLY));
#endif
}

}

bool BH_SetStatementAllowed(THD *thd, LEX *lex)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	if (AtLeastOneIBTableInvolved(lex))
	{
		if (ForbiddenMySQLQueryPath(lex)) {
			my_message(BHERROR_UNKNOWN,
				"Queries inside SET statements are not supported by the Infobright Optimizer. \
Enable the MySQL Query Path in the brighthouse.ini file to execute the query with reduced performance.", MYF(0));
			return false;
		}
		else
			push_warning(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, "SET statement not supported by the Infobright Optimizer. The query executed by MySQL engine.");
	}
	return true;
#endif
}

int BH_HandleSelect(THD *thd, LEX *lex, select_result *&result, ulong setup_tables_done_option, int& res, int& optimize_after_bh, int& bh_free_join, int with_insert)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	int ret = RCBASE_QUERY_ROUTE;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( MysqlExpression::get_instance_count() == 0 );
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( VirtualColumn::get_instance_count() == 0 );
	try {
		// handle_select_ret is introduced here because in case of some exceptions (e.g. thrown from ForbiddenMySQLQueryPath)
		// we want to return RCBASE_QUERY_ROUTE
		int handle_select_ret =
			rceng->HandleSelect(thd, lex, result, setup_tables_done_option, res, optimize_after_bh, bh_free_join, with_insert);
		if(handle_select_ret == RETURN_QUERY_TO_MYSQL_ROUTE && AtLeastOneIBTableInvolved(lex) && ForbiddenMySQLQueryPath(lex)) {
			my_message(BHERROR_UNKNOWN,
					"The query includes syntax that is not supported by the Infobright Optimizer. \
Either restructure the query with supported syntax, or enable the MySQL Query Path in the brighthouse.ini file \
to execute the query with reduced performance.", MYF(0));
			//rceng->Rollback(thd, true, true);
			handle_select_ret = RCBASE_QUERY_ROUTE;
		}
		ret = handle_select_ret;
	}
	catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( VirtualColumn::get_instance_count() == 0 );
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( MysqlExpression::get_instance_count() == 0 );
	return ret;
#endif
}

int BH_LoadData(THD *thd, sql_exchange *ex, TABLE_LIST *table_list,	char* errmsg, int len, int &errcode)
{
	BHError bh_error;
	int ret = BH_LD_Failed;
	try	{
		ret = rceng->RunLoader(thd, ex, table_list, bh_error);
		if(ret == BH_LD_Failed)
			rclog << lock << "Error: "<< bh_error.Message().c_str() << unlock;
	} catch (std::exception& e)	{
		bh_error = BHError(BHERROR_UNKNOWN, e.what());
		rclog << lock << "Error: "<< bh_error.Message().c_str() << unlock;
	} catch (...) {
		bh_error = BHError(BHERROR_UNKNOWN, "An unknown system exception error caught.");
		rclog << lock << "Error: "<< bh_error.Message().c_str() << unlock;
	}
	strncpy(errmsg, bh_error.Message().c_str(), len - 1);
	errmsg[len-1] = '\0';
	errcode = (int)bh_error.ErrorCode();
	return ret;
}

void BH_TerminateLoadBH(THD* thd)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	try {
		rceng->TerminateLoader(thd);
	} catch (std::exception& e) {
		my_message(BHERROR_UNKNOWN, e.what(), MYF(0));
		rclog << lock << "Error: " << e.what() << unlock;
	} catch (...) {
		my_message(BHERROR_UNKNOWN, "An unknown system exception error caught.", MYF(0));
		rclog << lock << "Error: An unknown system exception error caught." << unlock;
	}
#endif
}

// Depricated, will be removed later
bool BH_ThrottleQuery()
{
	return false;
};

bool BH_ThrottleQuery(THD* thd, const char *sql, uint length)
{
//#if !defined(__BH_COMMUNITY__)
//	return QThrottler.Throttle(thd, sql, length);
//#endif
	return false;
}

void BH_UnThrottleQuery()
{
//#if !defined(__BH_COMMUNITY__)
	//QThrottler.UnThrottle();
//#endif
}
