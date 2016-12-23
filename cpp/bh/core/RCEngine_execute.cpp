/* Copyright (C) 2000 MySQL AB, portions Copyright (C) 2005-2008 Infobright Inc.

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

#include "Query.h"
#include "compilation_tools.h"
#include "exporter/export2file.h"
#include "core/RCEngine.h"
#include "edition/vc/VirtualColumn.h"
#include "edition/core/Transaction.h"

using namespace std;

int optimize_select(THD *thd, Item ***rref_pointer_array,
					 TABLE_LIST *tables, uint wild_num, List<Item> &fields,
					 COND *conds, uint og_num,  ORDER *order, ORDER *group,
					 Item *having, ORDER *proc_param, ulong select_options,
					 select_result *result, SELECT_LEX_UNIT *unit,
					 SELECT_LEX *select_lex, int& optimize_after_bh, int& free_join);

/*
Handles a single query in the Brighthouse engine.
If an error appears during query preparation/optimization
query structures are cleaned up and the function returns information about the error through 'res'.
If the query can not be compiled by Brighthouse engine
RETURN_QUERY_TO_MYSQL_ROUTE is returned and MySQL engine continues query execution.
*/
int RCEngine::HandleSelect(THD* thd, LEX* lex, select_result*& result, ulong setup_tables_done_option, int& res, int& optimize_after_bh, int& bh_free_join, int with_insert)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	// check if we have writing access to all Brighthouse tables involved in the query
	// perform load of buffered values
	for(TABLE_LIST* table_list = lex->query_tables; table_list; table_list = table_list->next_global) {
		TABLE* table = table_list->table;
		if(IsBHTable(table)) {
			string table_path = GetTablePath(thd, table);
			GetThreadParams(*thd)->GetTransaction()->ApplyPendingChanges(thd, table_path, table->s);
			//if(rsi_manager)								
			//	rsi_manager->UpdateDef();
				//rsi_manager->UpdateDef(table_path);
		}
	}

	int in_case_of_failure_can_go_to_mysql;
	optimize_after_bh = FALSE;
	bh_free_join = 0;

	SELECT_LEX_UNIT* unit 		= NULL;
	register SELECT_LEX* select_lex = NULL;
	select_export* se 		= NULL;

	thd->variables.engine_condition_pushdown = DefaultPushDown;
	if(!IsBHRoute(thd, lex->query_tables, &lex->select_lex, in_case_of_failure_can_go_to_mysql, with_insert)) {
		//res = open_and_lock_tables(thd, lex->query_tables, 2);
		res = open_and_lock_tables_derived(thd, lex->query_tables, TRUE, 2);
		if(res)
			return RCBASE_QUERY_ROUTE;
		else
			return RETURN_QUERY_TO_MYSQL_ROUTE;
	}

	//at this point all tables are in RCBase engine, so we can proceed with the query
	//and we know that if the result goes to the file, the BH_DATAFORMAT is one of BH formats
	int route = RCBASE_QUERY_ROUTE;
	SELECT_LEX *save_current_select = lex->current_select;
	List<st_select_lex_unit> derived_optimized;		// collection to remember derived tables that are optimized
	if(thd->fill_derived_tables() && lex->derived_tables) {
		// Derived tables are processed completely in the function open_and_lock_tables(...).
		// To avoid execution of derived tables in open_and_lock_tables(...)
		// the function mysql_derived_filling(..) optimizing and executing derived tables is passed over,
		// then optimization of derived tables must go here.
		res = FALSE;
		int free_join = FALSE;
		lex->thd->derived_tables_processing = TRUE;
		for(SELECT_LEX *sl = lex->all_selects_list; sl; sl = sl->next_select_in_list()) //for all selects
			for(TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local) //for all tables
				if(cursor->table && cursor->derived) { //data source (view or FROM subselect)
					// optimize derived table
					SELECT_LEX *first_select = cursor->derived->first_select();
					if(first_select->next_select() && first_select->next_select()->linkage == UNION_TYPE) { //?? only if union
						if(cursor->derived->describe || cursor->derived->item) { //??called for explain
																				//OR there is subselect(?)
							route = RETURN_QUERY_TO_MYSQL_ROUTE;
							goto ret_derived;
						}
						if(!cursor->derived->executed || cursor->derived->uncacheable) { //??not already executed (not materialized?)
																				//OR not cacheable (meaning not yet in cache, i.e. not materialized
																				//it seems to boil down to NOT MATERIALIZED(?)
							res = cursor->derived->optimize_for_brighthouse(); //===exec()
							derived_optimized.push_back(cursor->derived);
						}
					} else { //??not union
						cursor->derived->set_limit(first_select);
						if(cursor->derived->select_limit_cnt == HA_POS_ERROR)
							first_select->options &= ~OPTION_FOUND_ROWS;
						lex->current_select = first_select;
						int optimize_derived_after_bh = FALSE;
						res = optimize_select(thd, &first_select->ref_pointer_array, //===mysql_select
							(TABLE_LIST*) first_select->table_list.first,
							first_select->with_wild,
							first_select->item_list, first_select->where,
							(first_select->order_list.elements+
							first_select->group_list.elements),
							(ORDER *) first_select->order_list.first,
							(ORDER *) first_select->group_list.first,
							first_select->having, (ORDER*) NULL,
							ulong(first_select->options | thd->options |
							SELECT_NO_UNLOCK),
							cursor->derived_result, cursor->derived, first_select, optimize_derived_after_bh, free_join);
						if(optimize_derived_after_bh)
							derived_optimized.push_back(cursor->derived);
					}
					lex->current_select = save_current_select;
					if(!res && free_join)   //no error &
						route = RETURN_QUERY_TO_MYSQL_ROUTE;
					if(res || route == RETURN_QUERY_TO_MYSQL_ROUTE)
						goto ret_derived;
				}
		lex->thd->derived_tables_processing = FALSE;
	}

	se = dynamic_cast<select_export*>(result);
	if(se != NULL)
		result = new select_bh_export(se);
	// prepare, optimize and execute the main query
	select_lex = &lex->select_lex;
 	unit = &lex->unit;
	if(select_lex->next_select()) {  //it is union
		if(!(res = unit->prepare(thd, result, SELECT_NO_UNLOCK | setup_tables_done_option))) {
			// similar to mysql_union(...) from sql_union.cpp

			select_create* sc = dynamic_cast<select_create*>(result);
			if(sc && sc->create_table->table && sc->create_table->table->db_stat != 0) {
				my_error(ER_TABLE_EXISTS_ERROR, MYF(0), sc->create_table->table_name);
				res = 1;
			} else if(unit->describe || unit->item)   //explain or sth was already computed - go to mysql
				route = RETURN_QUERY_TO_MYSQL_ROUTE;
			else {
				int old_executed = unit->executed;
				res = unit->optimize_for_brighthouse();   //====exec()
				optimize_after_bh = TRUE;
				if(!res) {
					// Brighthouse query engine entry point
					try {
						route = rceng->Execute(unit->thd, unit->thd->lex, result, unit);
						if(route == RETURN_QUERY_TO_MYSQL_ROUTE)
							if (in_case_of_failure_can_go_to_mysql)
								unit->executed = old_executed;
							else {
								rclog << lock << "Error: Query syntax not implemented in Brighthouse, can export only to MySQL format (set BH_DATAFORMAT to 'MYSQL')." << unlock;
								my_message(ER_SYNTAX_ERROR, "Query syntax not implemented in Brighthouse, can export only to MySQL format (set BH_DATAFORMAT to 'MYSQL').", MYF(0));
								throw ReturnMeToMySQLWithError();
							}
					} catch (ReturnMeToMySQLWithError&) {
						route = RCBASE_QUERY_ROUTE;
						res = TRUE;
					}
				}
			}
		}
		if(res || route == RCBASE_QUERY_ROUTE) {
			res |= (int)unit->cleanup();
			optimize_after_bh = FALSE;
		}
	} else {
		unit->set_limit(unit->global_parameters); //the fragment of original handle_select(...)
													//(until the first part of optimization)
													//used for non-union select
		
		//'options' of mysql_select will be set in JOIN, as far as JOIN for
		//every PS/SP execution new, we will not need reset this flag if
		//setup_tables_done_option changed for next rexecution
		
		int err;
		err = optimize_select(thd, &select_lex->ref_pointer_array,
			(TABLE_LIST*) select_lex->table_list.first,
			select_lex->with_wild, select_lex->item_list,
			select_lex->where,
			select_lex->order_list.elements +
			select_lex->group_list.elements,
			(ORDER*) select_lex->order_list.first,
			(ORDER*) select_lex->group_list.first,
			select_lex->having,
			(ORDER*) lex->proc_list.first,
			ulong(select_lex->options | thd->options | setup_tables_done_option),
			result, unit, select_lex, optimize_after_bh, bh_free_join);
		// RCBase query engine entry point
		if(!err) {
			try {
				route = Execute(thd, lex, result);
				if(route==RETURN_QUERY_TO_MYSQL_ROUTE && !in_case_of_failure_can_go_to_mysql) {
					rclog << lock << "Error: Query syntax not implemented in Brighthouse, can export only to MySQL format (set BH_DATAFORMAT to 'MYSQL')." << unlock;
					my_message(ER_SYNTAX_ERROR, "Query syntax not implemented in Brighthouse, can export only to MySQL format (set BH_DATAFORMAT to 'MYSQL').", MYF(0));
					throw ReturnMeToMySQLWithError();
				}
			} catch (ReturnMeToMySQLWithError&) {
				route = RCBASE_QUERY_ROUTE;
				err = TRUE;
			}
		}
		if(bh_free_join) { //there was a join created in an upper function
			//so an upper function will do the cleanup
			if(err || route == RCBASE_QUERY_ROUTE) {
				thd->proc_info = "end";
				err |= (int)select_lex->cleanup();
				optimize_after_bh = FALSE;
				bh_free_join = 0;
			}
			res = (err || thd->is_error());
		} else
			res = select_lex->join->error;
	}
	if(select_lex->join && Query::IsLOJ(select_lex->join->join_list))
		optimize_after_bh = 2; // optimize partially (part=4), since part of LOJ optimization was already done
	res |= (int)thd->is_error();                     //the ending of original handle_select(...) */
	if(unlikely(res)) {
		// If we had a another error reported earlier then this will be ignored //
		result->send_error(ER_UNKNOWN_ERROR, ER(ER_UNKNOWN_ERROR));
		result->abort();
	}
	if(se != NULL) {
		// free the bh export object,
		// restore the original mysql export object
		// and prepare if it is expected to be prepared
		if (!select_lex->next_select() && select_lex->join!=0 && select_lex->join->result==result) {
			select_lex->join->result = se;
			if (((select_bh_export*)result)->IsPrepared())
				se->prepare(select_lex->join->fields_list, unit);
		}
		delete result;
		result = se;
	}
ret_derived:
	// if the query is redirected to MySQL engine
	// optimization of derived tables must be completed
	// and derived tables must be filled
	if(!res && route == RETURN_QUERY_TO_MYSQL_ROUTE)
		if (thd->fill_derived_tables() &&
			mysql_handle_derived(thd->lex, &mysql_derived_filling, &derived_optimized))
			res = TRUE;
	if(res) {
		// if an error appears during query preparation/optimization
		// derived tables must be cleaned up
		for(SELECT_LEX *sl = lex->all_selects_list; sl; sl = sl->next_select_in_list())
			for(TABLE_LIST *cursor = sl->get_table_list(); cursor; cursor = cursor->next_local)
				if(cursor->table && cursor->derived) {
					lex->thd->derived_tables_processing = TRUE;
					cursor->derived->cleanup();
					lex->current_select = save_current_select;
				}
	}
	lex->thd->derived_tables_processing = FALSE;
	return route;
#endif
}

/*
Prepares and optimizes a single select for Brighthouse engine
*/
int optimize_select(THD *thd, Item ***rref_pointer_array,
					 TABLE_LIST *tables, uint wild_num, List<Item> &fields,
					 COND *conds, uint og_num,  ORDER *order, ORDER *group,
					 Item *having, ORDER *proc_param, ulong select_options,
					 select_result *result, SELECT_LEX_UNIT *unit,
					 SELECT_LEX *select_lex, int& optimize_after_bh, int& free_join)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	//copied from sql_select.cpp from the beginning of mysql_select(...)
	int err;
	free_join= 1;

	select_lex->context.resolve_in_select_list= TRUE;
	JOIN* join;
	if (select_lex->join != 0) {
		join = select_lex->join;
		//is it single SELECT in derived table, called in derived table
		//creation
		if(select_lex->linkage != DERIVED_TABLE_TYPE || (select_options & SELECT_DESCRIBE)) {
			if(select_lex->linkage != GLOBAL_OPTIONS_TYPE) {
				//here is EXPLAIN of subselect or derived table
				if(join->change_result(result))
					return TRUE;
			} else {
				if(err = join->prepare(rref_pointer_array, tables, wild_num, conds, og_num, order,
					group, having, proc_param, select_lex, unit)) {

					return err;
				}
			}
		}
		free_join = 0;
		join->select_options = select_options;
	} else {
		if(!(join = new JOIN(thd, fields, select_options, result)))
			return TRUE;
		thd_proc_info(thd, "init");
		thd->used_tables = 0; // Updated by setup_fields
		if(err= join->prepare(rref_pointer_array, tables, wild_num, conds, og_num, order, group,
			having, proc_param, select_lex, unit)) {
			return err;
		}
	}
	optimize_after_bh = TRUE;
	if((err = join->optimize(1))) //OK, so here is the difference, there is
				//only the first part of the optimization done
		return err;					// 1
	//until HERE this was the copy of thebeginning of mysql_select(...)
	return FALSE;
#endif
}

#ifndef PURE_LIBRARY

int st_select_lex_unit::optimize_for_brighthouse()
{

	//copied from sql_union.cpp from the beginning of st_select_lex_unit::exec()
	SELECT_LEX *lex_select_save= thd->lex->current_select;
	SELECT_LEX *select_cursor=first_select();

	if (executed && !uncacheable && !describe)
		return FALSE;
	executed= 1;

	if (uncacheable || !item || !item->assigned() || describe)
	{
		if (item)
			item->reset_value_registration();
		if (optimized && item)
		{
			if (item->assigned())
			{
				item->assigned(0); // We will reinit & rexecute unit
				item->reset();
				table->file->ha_delete_all_rows();
			}
			// re-enabling indexes for next subselect iteration 
			if (union_distinct && table->file->ha_enable_indexes(HA_KEY_SWITCH_ALL))
				assert(0);
		}
		for (SELECT_LEX *sl= select_cursor; sl; sl= sl->next_select())
		{
			thd->lex->current_select= sl;

			if (optimized)
				saved_error= sl->join->reinit();
			else
			{
				set_limit(sl);
				if (sl == global_parameters || describe)
				{
					offset_limit_cnt= 0;
					//We can't use LIMIT at this stage if we are using ORDER BY for the
					//whole query
					if (sl->order_list.first || describe)
						select_limit_cnt= HA_POS_ERROR;
				}

				//When using braces, SQL_CALC_FOUND_ROWS affects the whole query:
				//we don't calculate found_rows() per union part.
				//Otherwise, SQL_CALC_FOUND_ROWS should be done on all sub parts.
				sl->join->select_options=
					(select_limit_cnt == HA_POS_ERROR || sl->braces) ?
					sl->options & ~OPTION_FOUND_ROWS : sl->options | found_rows_for_union;
				saved_error= sl->join->optimize(1);
			}

			//HERE ends the code from bool st_select_lex_unit::exec()
			if (saved_error)
			{
				thd->lex->current_select= lex_select_save;
				return saved_error;
			}
		}
	}
	optimized= 1;
	thd->lex->current_select= lex_select_save;
	return FALSE;
}
#endif

#ifndef PURE_LIBRARY
int st_select_lex_unit::optimize_after_brighthouse()
{

	int res;
	SELECT_LEX *lex_select_save= thd->lex->current_select;
	for (SELECT_LEX *sl= first_select(); sl; sl= sl->next_select())
	{
		thd->lex->current_select= sl;
		if (res= sl->join->optimize(2))
		{
			thd->lex->current_select= lex_select_save;
			return res;
		}
	}
	executed = 0;
	thd->lex->current_select= lex_select_save;
	return FALSE;

}
#endif

int handle_exceptions( THD*, ConnectionInfo* , bool with_error = false);

int RCEngine::Execute(THD* thd, LEX* lex, select_result* result_output, SELECT_LEX_UNIT* unit_for_union)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(thd->lex == lex);
	SELECT_LEX* selects_list = &lex->select_lex;
	SELECT_LEX* last_distinct = NULL;
	if (unit_for_union!=NULL) last_distinct = unit_for_union->union_distinct;

	int is_dumpfile = 0;
	const char * export_file_name = GetFilename(selects_list, is_dumpfile);
	if (is_dumpfile) {
		push_warning(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, "Dumpfile not implemented in Brighthouse, executed by MySQL engine.");
		return RETURN_QUERY_TO_MYSQL_ROUTE;
	}

	ConnectionInfo* cur_connection = GetThreadParams(*thd);
	boost::shared_ptr<Query> query(new Query(cur_connection));
	CompiledQuery cqu;
	cur_connection->ResetDisplay();		// switch display on
	query->SetRoughQuery(selects_list->options & SELECT_ROUGHLY);	// instead of ROUGH_QUERY flag

	try {
		if (!query->Compile(&cqu, selects_list, last_distinct)) {
			push_warning(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, "Query syntax not implemented in Brighthouse, executed by MySQL engine.");
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		}
	} catch(RCException const &x) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << unlock;
		my_message(ER_UNKNOWN_ERROR, (string("Brighthouse compile specific error: ")+x.what()).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	}

	TempTable* result = NULL;
	boost::scoped_ptr<ResultSender> sender;

	FunctionExecutor lock_and_unlock_pack_info(boost::bind(&Query::LockPackInfoForUse, query), boost::bind(&Query::UnlockPackInfoFromUse, query));

	try	{
		RCTablePtr rct;
		if(lex->sql_command == SQLCOM_INSERT_SELECT &&
				RCEngine::IsBHTable(((Query_tables_list*)lex)->query_tables->table)) {
			string table_path = RCEngine::GetTablePath(thd, ((Query_tables_list*)lex)->query_tables->table);
			rct = cur_connection->GetTransaction()->GetTableShared(table_path, ((Query_tables_list*)lex)->query_tables->table->s);
		}
		cur_connection->ResetStatistics();
		if(unit_for_union != NULL)	{
			int res = result_output->prepare(unit_for_union->item_list, unit_for_union);
			if(res) {
				rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << "Unsupported UNION" << unlock;
				my_message(ER_UNKNOWN_ERROR, "Brighthouse: unsupported UNION", MYF(0));
				throw ReturnMeToMySQLWithError();
			}
			if(export_file_name)
				sender.reset(new ResultExportSender(unit_for_union->thd, result_output, unit_for_union->item_list));
			else
				sender.reset(new ResultSender(unit_for_union->thd, result_output, unit_for_union->item_list));
		} else {
			if(export_file_name)
				sender.reset(new ResultExportSender(selects_list->join->thd, result_output, selects_list->item_list));
			else
				sender.reset(new ResultSender(selects_list->join->thd, result_output, selects_list->item_list));
		}

#ifndef __BH_COMMUNITY__
		// inform the transaction which columns are used in the query
		for(std::map<int, std::vector<int> >::const_iterator it = cqu.GetTableIds().begin(); it != cqu.GetTableIds().end(); ++it) {
			cur_connection->GetTransaction()->TableLimitedToColumns(it->first, cqu.GetColumnsPerTable(it->first), thd->spcont!=NULL && thd->spcont->is_function());
		}
#endif
		query->FindLastJoinsForTables(cqu);
		result = query->Preexecute(cqu, sender.get());
		BHASSERT( result != NULL, "Query execution returned no result object" );
		if(query->IsRoughQuery())
			result->RoughMaterialize(false, sender.get());
		else
			result->Materialize(false, sender.get());

		sender->Finalize(result);

		if(rct) {
			//in this case if this is an insert to RCTable from select based on the same table
			//RCTable object for this table can't be deleted in TempTable destructor
			//It will be deleted in RefreshTables method that will be called on commit
			result->RemoveFromManagedList(rct.get());
			query->RemoveFromManagedList(rct);
			rct.reset();
		}
		sender.reset();
		rccontrol.lock(cur_connection->GetThreadID()) << "Total data packs actually loaded (approx.): " << cur_connection->GetPackCount() << unlock;
		rccontrol.lock(cur_connection->GetThreadID()) << "----------------------------------------------------------------------------" << unlock;
		result = 0;
	} catch ( ... ) {
		bool with_error = false;
		if(sender)
			if(sender->SentRows() > 0 ) {
				sender->CleanUp();
				with_error = true;
			} else
				sender->CleanUp();
		return ( handle_exceptions( thd, cur_connection, with_error ));
	}
	return RCBASE_QUERY_ROUTE;
#endif
}

int handle_exceptions( THD* thd, ConnectionInfo* cur_connection, bool with_error )
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	try {
		throw;
	} catch (NotImplementedRCException const &x) {
		//here we can produce a warning with the information kept in the text field of the exception
		rccontrol.lock(cur_connection->GetThreadID()) << "Switched to MySQL: " << x.what() << unlock;
		push_warning(thd, MYSQL_ERROR::WARN_LEVEL_NOTE, ER_UNKNOWN_ERROR, (string("Brighthouse: ") + x.what() + " Query executed by MySQL engine.").c_str());
		if (with_error) {
			std::string msg(x.what());
			msg.append(" Can't switch to MySQL execution path");
			throw InternalRCException(msg);
		}
		return RETURN_QUERY_TO_MYSQL_ROUTE;
	} catch (OutOfMemoryRCException const &x) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << unlock;
		my_message(BHERROR_OUT_OF_MEMORY, (string("Brighthouse out of resources error: ")+x.what()).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	} catch (DataTypeConversionRCException const &x) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << unlock;
		my_message(BHERROR_DATACONVERSION, (string("Brighthouse specific error: ")+x.what()).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	} catch (DBObjectRCException const &x) {  //the subselect had more than one row in a comparison without ANY or ALL
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << unlock;
		my_message(ER_SYNTAX_ERROR, (string("Brighthouse specific error: ")+x.what()).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	} catch (KilledRCException const &) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Stopped by user. " << unlock;
		my_message(ER_UNKNOWN_ERROR, (string("Stopped by user.")).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	} catch (FileRCException const &e) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << e.what() << unlock;
		my_message(BHERROR_CANNOT_OPEN_FILE_OR_PIPE, (string("Brighthouse specific error: ")+ e.what()).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	} catch (RCException const &x) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: " << x.what() << unlock;
		my_message(ER_UNKNOWN_ERROR, (string("Brighthouse other specific error: ")+x.what()).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	} catch (std::bad_alloc const &) {
		rccontrol.lock(cur_connection->GetThreadID()) << "Error: std::bad_alloc caught" << unlock;
		my_message(ER_UNKNOWN_ERROR, (string("Brighthouse out of memory error")).c_str(), MYF(0));
		throw ReturnMeToMySQLWithError();
	}
	return RETURN_QUERY_TO_MYSQL_ROUTE;
#endif
}
