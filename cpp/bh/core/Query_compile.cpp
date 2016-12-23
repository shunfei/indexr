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
#include "Query.h"
#include "MysqlExpression.h"
#include "compilation_tools.h"
#include "common/mysql_gate.h"
#include "edition/core/Transaction.h"
#include "edition/local.h"
#include <algorithm>


using namespace std;

int JudgeErrors(SELECT_LEX* sl)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(!sl->join) {
		return RETURN_QUERY_TO_MYSQL_ROUTE;
	}

	if(sl->join && sl->join->procedure) {
		my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: No PROCEDURE syntax supported", MYF(0));
		throw ReturnMeToMySQLWithError();
	}

	if(sl->offset_limit)
		if(sl->offset_limit->type() != Item::INT_ITEM /*|| sl->offset_limit->val_int()*/) {
			//my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: Only OFFSET=0 supported in LIMIT clause", MYF(0));
			my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: Only numerical OFFSET supported", MYF(0));
			throw ReturnMeToMySQLWithError();
		}

	if(sl->select_limit)
		if(sl->select_limit->type() != Item::INT_ITEM) {
			my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: Only numerical LIMIT supported", MYF(0));
			throw ReturnMeToMySQLWithError();
		}
	if(sl->olap == ROLLUP_TYPE) {
		/*my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: WITH ROLLUP not supported", MYF(0));
		 throw ReturnMeToMySQLWithError();*/
		return RETURN_QUERY_TO_MYSQL_ROUTE;
	}

	return RCBASE_QUERY_ROUTE;
#endif
}

void SetLimit(SELECT_LEX* sl, SELECT_LEX* gsl, _int64& offset_value, _int64& limit_value)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	if(sl->select_limit && (!gsl || sl->select_limit != gsl->select_limit)) {
		limit_value = sl->select_limit->val_int();
		if(limit_value == UINT_MAX) {/* this value seems to be sometimes automatically set by MYSQL to UINT_MAX*/
			limit_value = -1; //no limit set
			offset_value = -1;
		};
	};

	if(limit_value)
		offset_value = 0;

	if(sl->offset_limit && (!gsl || sl->offset_limit != gsl->offset_limit))
		offset_value = sl->offset_limit->val_int();
#endif

}

// Used in Query::Compile() to break compilation in the middle and make cleanup before returning
class CompilationError {};

int Query::FieldUnmysterify(Item* item, const char*& database_name, const char*& table_name,
		const char*& table_alias, const char*& table_path, const TABLE*& table_ptr, const char*& field_name, const char*& field_alias)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	table_alias = EMPTY_TABLE_CONST_INDICATOR;
	database_name = NULL;
	table_name = NULL;
	table_path = NULL;
	table_ptr = NULL;
	field_name = NULL;
	field_alias = NULL;

	item = UnRef(item);

	Item_field* ifield;
	switch(static_cast<int>(item->type())) {
		case Item_bhfield::BHFIELD_ITEM:
			ifield = dynamic_cast<Item_bhfield *>(item)->OriginalItem();
			if(IsAggregationItem(ifield)) {
				Item_sum* is = (Item_sum*)ifield;
				if (is->arg_count > 1)
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				Item* tmp_item = UnRef(is->args[0]);
				if(tmp_item->type() == Item::FIELD_ITEM)
					ifield = (Item_field*)tmp_item;
				else if ( static_cast<int>( tmp_item->type() ) == static_cast<int>( Item_bhfield::BHFIELD_ITEM ) )
					ifield = dynamic_cast<Item_bhfield *>(tmp_item)->OriginalItem();
				else {
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				}
			}
			break;
		case Item::FIELD_ITEM:  // regular select
			ifield = (Item_field*)item;
			break;

		case Item::SUM_FUNC_ITEM: // min(k), max(k), count(), avg(k), sum
		{
			Item_sum* is = (Item_sum*)item;
			if (is->arg_count > 1) 
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			Item* tmp_item = UnRef(is->args[0]);
			if(tmp_item->type() == Item::FIELD_ITEM)
				ifield = (Item_field*)tmp_item;
			else if ( static_cast<int>( tmp_item->type() ) == static_cast<int>( Item_bhfield::BHFIELD_ITEM ) ) /* *CAUTION* comparision of enumerators from different enums */
				ifield = dynamic_cast<Item_bhfield *>(tmp_item)->OriginalItem();
			else
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			break;
		}
		case Item::FUNC_ITEM: // complex expressions
			//if(WrapMysqlExpression(item, &not_a_table_column) == WrapStatus::SUCCESS)
				return RCBASE_QUERY_ROUTE;
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		default:
			//if(WrapMysqlExpression(item, &not_a_table_column) == WrapStatus::SUCCESS)
				return RCBASE_QUERY_ROUTE;
			return RETURN_QUERY_TO_MYSQL_ROUTE;
	};

	/*
	 * By MW. Related to bug 1073.
	 *
	 * For views, 'table_alias' must be created as a concatenation
	 * of original view(s) name(s) and the table name
	 * - currently it's just the table name, which leads to ambiguity and errors
	 * when the same table is used in another place in the query.
	 * Note that there can be several nested views.
	 *
	 * To retrieve name(s) of the view(s) from which the current 'ifield' comes
	 * you may try the following expression:
	 *
	 *     ifield->cached_table->table_list->belong_to_view->alias  or  ...->table_name
	 * or  ifield->cached_table->table_list->referencing_view->alias  or  ...->table_name
	 *
	 * Here, 'belong_to_view' and 'referencing_view' are different
	 * if there are nested views.
	 *
	 * Probably ifield->cached_table could be also used to find
	 * 'database_name', 'table_name' and 'table_path'
	 * in a simpler way than currently.
	 */

	Field* f = ifield->result_field;

	ASSERT_MYSQL_STRING(f->table->s->db);
	ASSERT_MYSQL_STRING(f->table->s->table_name);
	ASSERT_MYSQL_STRING(f->table->s->path);

	table_ptr = f->table;
	table_alias = ifield->table_name;
	database_name = f->table->s->db.str;
	table_name = GetTableName(ifield);
	table_path = f->table->s->path.str;
	field_name = f->field_name;
	field_alias = ifield->name;

	return RCBASE_QUERY_ROUTE;
#endif
}

bool Query::FieldUnmysterify(Item* item, TabID& tab, AttrID& col)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	Item_field* ifield;
	if(item->type() == Item_bhfield::get_bhitem_type()) {
		ifield = dynamic_cast<Item_bhfield *>(item)->OriginalItem();
		if(IsAggregationItem(ifield)) {
			Item_sum* is = (Item_sum*)ifield;
			if(is->arg_count > 1)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			Item* tmp_item = UnRef(is->args[0]);
			if(tmp_item->type() == Item::FIELD_ITEM)
				ifield = (Item_field*)tmp_item;
			else if(tmp_item->type() == Item_bhfield::get_bhitem_type())
				ifield = dynamic_cast<Item_bhfield *>(tmp_item)->OriginalItem();
			else if(tmp_item->type() == Item::FUNC_ITEM) {
				Item_bhfield * bhi = dynamic_cast<Item_bhfield *>(item);
				tab.n = bhi->varID[0].tab;
				col.n = bhi->varID[0].tab;
				return true;
			} else
				return false;
		}
	} else if(item->type() == Item::SUM_FUNC_ITEM) { // min(k), max(k), count(), avg(k), sum
		Item_sum* is = (Item_sum*)item;
		if(is->arg_count > 1)
			return false;
		Item* tmp_item = UnRef(is->args[0]);
		if(tmp_item->type() == Item::FIELD_ITEM)
			ifield = (Item_field*)tmp_item;
		else if(tmp_item->type() == Item_bhfield::get_bhitem_type())
			ifield = dynamic_cast<Item_bhfield *>(tmp_item)->OriginalItem();
		else
			return false;
	} else if(item->type() == Item::FIELD_ITEM)
		ifield = (Item_field*)item;
	else
		return false;

	if(!ifield->table_name) {
		return false;
		// union results have no name, but refer to the temp table
		//if(field_alias2num.find(TabIDColAlias((*table_alias2index)[ifield->table_name], ifield->name)) == field_alias2num.end())
		//	return false;
		//col.n = field_alias2num[TabIDColAlias((*table_alias2index)[ifield->table_name], ifield->name)];
		//tab.n = NULL_VALUE_32;
		//return true;
	}
	const char* table_name = GetTableName(ifield);
	string ext_alias = string(table_name ? table_name : "") + string(":") + string(ifield->table_name);
	multimap<string, pair<int,TABLE*> >::iterator it = table_alias2index_ptr.lower_bound(ext_alias);
	multimap<string, pair<int,TABLE*> >::iterator it_end = table_alias2index_ptr.upper_bound(ext_alias);
	if(it == table_alias2index_ptr.end())
		return false;
	for(; it != it_end; it++) {
		TABLE* mysql_table = it->second.second;
		tab = TabID(it->second.first);
		if(ifield->field->table != mysql_table)
			continue;
		if(mysql_table->derived_select_number == 0) {
			// Physical table in FROM - RCTable
			int field_num;
			for(field_num = 0; mysql_table->field[field_num]; field_num++)
				if(mysql_table->field[field_num]->field_name == ifield->result_field->field_name)
					break;
			if(!mysql_table->field[field_num])
				continue;
			col = AttrID(field_num);
			return true;
		} else {
			// subselect in FROM - TempTable
			if(field_alias2num.find(TabIDColAlias(tab.n, ifield->result_field->field_name)) == field_alias2num.end())
				continue;
			col.n = field_alias2num[TabIDColAlias(tab.n, ifield->result_field->field_name)];
			return true;
		}
	}
	return false;
#endif
}

int Query::AddJoins(List<TABLE_LIST>& join, TabID& tmp_table, std::vector<TabID>& left_tables, std::vector<TabID>& right_tables, bool in_subquery, bool& first_table /*= true*/, bool for_subq_in_where /*false*/)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	// on first call first_table = true. It indicates if it is the first table to be added
	// is_left is true iff it is nested left join which needs to be flatten (all tables regardless of
	//                                                        their join type need to be left-joined)
	TABLE_LIST* join_ptr;
	List_iterator<TABLE_LIST> li(join);
	vector<TABLE_LIST*> reversed;

	if(!join.elements)
		return RETURN_QUERY_TO_MYSQL_ROUTE; // no tables in table list in this select
	//if the table list was empty altogether, we wouldn't even enter Compilation(...)
	//it must be sth. like `select 1 from t1 union select 2` and we are in the second
	//select in the union

	while(join_ptr = li++)
		reversed.push_back(join_ptr);
	size_t size = reversed.size();
	for(unsigned int i = 0; i < size; i++) {
		join_ptr = reversed[size - i - 1];
		if(join_ptr->nested_join) {
			vector<TabID> local_left, local_right;
			if(!AddJoins(join_ptr->nested_join->join_list, tmp_table, local_left, local_right, in_subquery, first_table, for_subq_in_where))
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			JoinType join_type = GetJoinTypeAndCheckExpr(join_ptr->outer_join, join_ptr->on_expr);
			CondID cond_id;
			if(!BuildCondsIfPossible(join_ptr->on_expr, cond_id, tmp_table, join_type))
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			left_tables.insert(left_tables.end(), right_tables.begin(), right_tables.end());
			local_left.insert(local_left.end(), local_right.begin(), local_right.end());
			if(join_ptr->outer_join)
				right_tables = local_left;
			else
				left_tables.insert(left_tables.end(), local_left.begin(), local_left.end());
			if(join_ptr->on_expr && join_ptr->outer_join) {
				cq->LeftJoinOn(tmp_table, left_tables, right_tables, cond_id);
				left_tables.insert(left_tables.end(), right_tables.begin(), right_tables.end());
				right_tables.clear();
			} else if(join_ptr->on_expr && !join_ptr->outer_join)
				cq->InnerJoinOn(tmp_table, left_tables, right_tables, cond_id);
		} else {
			assert(join_ptr->table && "We require that the table is defined if it is not a nested join");
			const char* database_name = 0;
			const char* table_name = 0;
			const char* table_alias = 0;
			const char* table_path = 0;
			TabID tab(0);
			if(join_ptr->derived) {
				if(!Compile(cq, join_ptr->derived->first_select(), join_ptr->derived->union_distinct, &tab))
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				table_alias = join_ptr->alias;
			} else {
				if(!TableUnmysterify(join_ptr, database_name, table_name, table_alias, table_path))
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				int tab_num = path2num[table_path]; //number of a table on a list in `this` QUERY object
				int id = t[tab_num]->GetID();
				cq->TableAlias(tab, TabID(tab_num), table_name, id);
			}
			string ext_alias = string(table_name ? table_name : "") + string(":") + string(table_alias);
			table_alias2index_ptr.insert(make_pair(ext_alias, make_pair(tab.n, join_ptr->table)));
			if(first_table) {
				left_tables.push_back(tab);
				assert(!join_ptr->on_expr && "It is not possible to join the first table with the LEFT direction");
				cq->TmpTable(tmp_table, tab, for_subq_in_where);
				first_table = false;
			} else {
				cq->Join(tmp_table, tab);
				JoinType join_type = GetJoinTypeAndCheckExpr(join_ptr->outer_join, join_ptr->on_expr);
				//if(join_type == JO_LEFT && join_ptr->on_expr && dynamic_cast<Item_cond_or*>(join_ptr->on_expr))
				//	return RETURN_QUERY_TO_MYSQL_ROUTE;
				CondID cond_id;
				if(!BuildCondsIfPossible(join_ptr->on_expr, cond_id, tmp_table, join_type))
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				if(join_ptr->on_expr && join_ptr->outer_join) {
					right_tables.push_back(tab);
					cq->LeftJoinOn(tmp_table, left_tables, right_tables, cond_id);
					left_tables.push_back(tab);
					right_tables.clear();
				} else if(join_ptr->on_expr && !join_ptr->outer_join) {
					right_tables.push_back(tab);
					cq->InnerJoinOn(tmp_table, left_tables, right_tables, cond_id);
					left_tables.push_back(tab);
					right_tables.clear();
				} else
					left_tables.push_back(tab);
				//if(join_ptr->on_expr)
				//	cq->SetLOJOuterDim(tmp_table, tab, i);
			}
		}
	}
	return RCBASE_QUERY_ROUTE;
#endif
}

int Query::AddFields(List<Item>& fields, TabID const& tmp_table, bool const group_by_clause, int& num_of_added_fields, bool ignore_minmax, bool& aggregation_used)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	List_iterator_fast<Item> li(fields);
	Item* item;
	int added = 0;
	item = li++;
	while(item) {
		WrapStatus::wrap_status_t ws;
		ColOperation oper;
		bool distinct;
		if(!OperationUnmysterify(item, oper, distinct, group_by_clause))
			return RETURN_QUERY_TO_MYSQL_ROUTE;

		if(IsAggregationItem(item))
			aggregation_used = true;

		// in case of transformed subquery sometimes we need to revert back transformation to MIN/MAX
		if(ignore_minmax && (oper == MIN || oper == MAX))
			oper = LISTING;

		// select PHYSICAL COLUMN or AGGREGATION over PHYSICAL COLUMN
		if((IsFieldItem(item) || IsAggregationOverFieldItem(item)) && IsLocalColumn(item, tmp_table))
			AddColumnForPhysColumn(item, tmp_table, oper, distinct, false, item->name);
		//REF to FIELD_ITEM
		else if (item->type() == Item::REF_ITEM ) {
			item = UnRef(item);
			continue;
		}
		//			if ((UnRef(item)->type() == Item_bhfield::BHFIELD_ITEM || UnRef(item)->type() == Item_bhfield::FIELD_ITEM ) && IsLocalColumn(UnRef(item), tmp_table) )
		//				AddColumnForPhysColumn(UnRef(item), tmp_table, oper, distinct, false, false);
		//			else {
		//				//
		//			}
		else if (IsAggregationItem(item) && (((Item_sum*)item)->args[0])->type() == Item::REF_ITEM
			&& (UnRef(((Item_sum*)item)->args[0])->type() == Item_bhfield::get_bhitem_type() || (UnRef(((Item_sum*)item)->args[0])->type() == Item_bhfield::FIELD_ITEM))
			&& IsLocalColumn(UnRef(((Item_sum*)item)->args[0]), tmp_table))
			//AGGR on REF to FIELD_ITEM
			AddColumnForPhysColumn(UnRef(((Item_sum*)item)->args[0]), tmp_table, oper, distinct, false, item->name);
		else if(IsAggregationItem(item)) {
			// select AGGREGATION over EXPRESSION
			Item_sum* item_sum = (Item_sum*)item;
			if(item_sum->arg_count > 1  || HasAggregation(item_sum->args[0]))
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			if(IsCountStar(item_sum)) { // count(*) doesn't need any virtual column
				AttrID at;
				cq->AddColumn(at, tmp_table, CQTerm(), oper, item_sum->name, false);
				field_alias2num[TabIDColAlias(tmp_table.n, item_sum->name)] = at.n;
			} else {
				MysqlExpression* expr;
				ws = WrapMysqlExpression(item_sum->args[0], tmp_table, expr, false, false);
				if(ws == WrapStatus::FAILURE)
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				AddColumnForMysqlExpression(expr, tmp_table, ignore_minmax ? item_sum->args[0]->name : item_sum->name, oper, distinct);
			}
		} else if(item->type() == Item::SUBSELECT_ITEM) {
			CQTerm term;
			AttrID at;
			if (Item2CQTerm(item, term, tmp_table, /*group_by_clause ? HAVING_FILTER :*/ WHERE_COND) == RETURN_QUERY_TO_MYSQL_ROUTE)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			cq->AddColumn(at, tmp_table, term, LISTING, item->name, distinct);
			field_alias2num[TabIDColAlias(tmp_table.n, item->name)] = at.n;
		} else {
			// select EXPRESSION
			if(HasAggregation(item)) {
				oper = DELAYED;
				aggregation_used = true;
			}
			MysqlExpression* expr(NULL);
			ws = WrapMysqlExpression(item, tmp_table, expr, false, oper == DELAYED);
			if(ws == WrapStatus::FAILURE)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			if(!item->name) {								 												
				Item_func_conv_charset* item_conv = dynamic_cast<Item_func_conv_charset*>(item);
				if(item_conv) {
					Item** ifunc_args = item_conv->arguments();
					AddColumnForMysqlExpression(expr, tmp_table, ifunc_args[0]->name, oper, distinct);					
				} else {
					AddColumnForMysqlExpression(expr, tmp_table, item->name, oper, distinct);
				}
			} else
				AddColumnForMysqlExpression(expr, tmp_table, item->name, oper, distinct);
		}
		added++;
		item = li++;
	}
	num_of_added_fields = added;
	return RCBASE_QUERY_ROUTE;
#endif
}

int Query::AddGroupByFields(ORDER* group_by, const TabID& tmp_table)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	for(; group_by; group_by = group_by->next) {
		if(!group_by->asc) {
			my_message(ER_SYNTAX_ERROR,
				"Brighthouse specific error: Using DESC after GROUP BY clause not allowed. Use ORDER BY to order the result",
				MYF(0));
			throw ReturnMeToMySQLWithError();
		}

		Item* item = *(group_by->item);
		item = UnRef(item);
		// group by PHYSICAL COLUMN
		if((IsFieldItem(item) || (IsAggregationItem(item) && IsFieldItem(((Item_sum*)item)->args[0]))) && IsLocalColumn(item, tmp_table))
			AddColumnForPhysColumn(item, tmp_table, GROUP_BY, false, true);
		else if (item->type() == Item::SUBSELECT_ITEM) {
			CQTerm term;
			AttrID at;
			if (Item2CQTerm(item, term, tmp_table, WHERE_COND) == RETURN_QUERY_TO_MYSQL_ROUTE)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			cq->AddColumn(at, tmp_table, term, GROUP_BY, 0);
			//			field_alias2num[TabIDColAlias(tmp_table.n, item->name)] = at.n;
		} else { // group by COMPLEX EXPRESSION
			MysqlExpression* expr = 0;
			if(WrapStatus::FAILURE == WrapMysqlExpression(item, tmp_table, expr, true, true))
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			AddColumnForMysqlExpression(expr, tmp_table, item->name, GROUP_BY, false, true);
		}
	}
	return RCBASE_QUERY_ROUTE;
#endif
}

int Query::AddOrderByFields(ORDER* order_by, TabID const& tmp_table, int const group_by_clause)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	for(; order_by; order_by = order_by->next) {
		std::pair<int,int> vc;
		Item* item = *(order_by->item);
		CQTerm my_term;
		int result;
		// at first we need to check if we don't have non-deterministic expression (e.g., rand())
		// in such case we should order by output column in TempTable
		if(!IsFieldItem(item) && !IsAggregationItem(item) && !IsDeterministic(item) && 
			item->type() != Item::SUBSELECT_ITEM) {
			MysqlExpression* expr = NULL;
			WrapStatus::wrap_status_t ws = WrapMysqlExpression(item, tmp_table, expr, false, false);
			if(ws == WrapStatus::FAILURE)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!expr->IsDeterministic());
			int col_num = AddColumnForMysqlExpression(expr, tmp_table, NULL, LISTING, false, true);
			vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
			if(vc.first == NULL_VALUE_32) {
				vc.first = tmp_table.n;
				cq->CreateVirtualColumn(vc.second, tmp_table, tmp_table, AttrID(col_num));
				phys2virt.insert(make_pair(std::make_pair(tmp_table.n, -col_num - 1), vc));
			}
			cq->Add_Order(tmp_table, AttrID(vc.second), !(order_by->asc));
			continue;
		}
		if(group_by_clause) {
			if(item->type() == Item::FUNC_ITEM) {
				MysqlExpression* expr = NULL;
				bool delayed = false;
				if(HasAggregation(item)) {
					delayed = true;
				}

				WrapStatus::wrap_status_t ws = WrapMysqlExpression(item, tmp_table, expr, false, delayed);
				if(ws == WrapStatus::FAILURE)
					return RETURN_QUERY_TO_MYSQL_ROUTE;
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(expr->IsDeterministic());
				int col_num = AddColumnForMysqlExpression(expr, tmp_table, NULL, delayed ? DELAYED : LISTING, false, true);
				vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
				if(vc.first == NULL_VALUE_32) {
					vc.first = tmp_table.n;
					cq->CreateVirtualColumn(vc.second, tmp_table, tmp_table, AttrID(col_num));
					phys2virt.insert(make_pair(std::make_pair(tmp_table.n, -col_num - 1), vc));
				}
				cq->Add_Order(tmp_table, AttrID(vc.second), !(order_by->asc));
				continue;
			// we can reuse transformation done in case of HAVING
			//result = Item2CQTerm(item, my_term, tmp_table, HAVING_COND);
			} else {
				AttrID at;
				result = Item2CQTerm(item, my_term, tmp_table, HAVING_COND);
				if(item->type() == Item::SUBSELECT_ITEM) {
					// create a materialized column with subsel results for the ordering
					cq->AddColumn(at, tmp_table, my_term, DELAYED, NULL, false);
					vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, at);
					if(vc.first == NULL_VALUE_32) {
						vc.first = tmp_table.n;
						cq->CreateVirtualColumn(vc.second, tmp_table, tmp_table, at);
						phys2virt.insert(make_pair(std::pair<int,int>(tmp_table.n, at.n), vc));
					}
				} else //a naked column
					vc.second = my_term.vc_id;
				//cq->Add_Order(tmp_table, AttrID(vc.second), !(order_by->asc));
			}

		} else {
			result = Item2CQTerm(item, my_term, tmp_table, WHERE_COND);
			vc.second = my_term.vc_id;
		}
		if(result != RCBASE_QUERY_ROUTE)
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		cq->Add_Order(tmp_table, AttrID(vc.second), !(order_by->asc));
	}
	return RCBASE_QUERY_ROUTE;
#endif
}

int Query::AddGlobalOrderByFields(SQL_LIST* global_order, const TabID& tmp_table, int max_col)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(!global_order)
		return RCBASE_QUERY_ROUTE;

	ORDER* order_by;
	for(uint i = 0; i < global_order->elements; i++) {
		order_by = (i == 0 ? (ORDER*)(global_order->first) : order_by->next);
		// the way to traverse 'global_order' list maybe is not very orthodox, but it works

		int col_num = NULL_VALUE_32;
		if((*(order_by->item))->type() == Item::INT_ITEM) {
			col_num = int((*(order_by->item))->val_int());
			if(col_num < 1 || col_num > max_col)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			col_num--;
			col_num = -col_num - 1; // make it negative as are columns in TempTable
		} else {
			Item* item = *(order_by->item);
			if(!item->name)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			bool found = false;
			for(map<TabIDColAlias,int>::iterator it = field_alias2num.begin(); it != field_alias2num.end(); ++it) {
				if(tmp_table.n == (*it).first.first && stricmp((*it).first.second.c_str(), item->name) == 0) {
					col_num = (*it).second;
					found = true;
					break;
				}
			}
			if(!found)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
		}
		int attr;
		cq->CreateVirtualColumn(attr, tmp_table, tmp_table, AttrID(col_num));
		phys2virt.insert(make_pair(make_pair(tmp_table.n, col_num), make_pair(tmp_table.n, attr)));
		cq->Add_Order(tmp_table, AttrID(attr), !order_by->asc);
	}

	return RCBASE_QUERY_ROUTE;
#endif
}

Query::WrapStatus::wrap_status_t Query::WrapMysqlExpression(Item* item, const TabID& tmp_table, MysqlExpression*& expr, bool in_where, bool aggr_used)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return WrapStatus::FAILURE;
#else
	// Check if the expression doesn't contain any strange items that we don't want to see.
	// By the way, collect references to all Item_field objects.
	set<Item*> ifields;
	MysqlExpression::Item2VarID item2varid;
	if(!MysqlExpression::SanityAggregationCheck(item, ifields))
		return WrapStatus::FAILURE;

	//this large "if" can be removed to use common code, but many small "ifs" must be created then
	if(in_where) {
		// create a map:  [Item_field pointer] -> VarID
		for(set<Item*>::iterator it = ifields.begin(); it != ifields.end(); it++) {
			if(IsAggregationItem(*it)) {
				// a few checkings for aggregations
				Item_sum* aggregation = (Item_sum*)*it;
				if(aggregation->arg_count > 1)
					return WrapStatus::FAILURE;
				if(IsCountStar(aggregation))  // count(*) doesn't need any virtual column
					return WrapStatus::FAILURE;
			}
			AttrID col, at;
			TabID tab;
			// find [tab] and [col] which identify column in BH
			if(!FieldUnmysterify(*it, tab, col))
				return WrapStatus::FAILURE;
			if(!cq->ExistsInTempTable(tab, tmp_table)) {
				bool is_group_by;
				TabID params_table = cq->FindSourceOfParameter(tab, tmp_table, is_group_by);
				ColOperation oper;
				bool distinct;
				if(!OperationUnmysterify(*it, oper, distinct, true))
					return WrapStatus::FAILURE;
				if(is_group_by && !IsParameterFromWhere(params_table)) {
					col.n = AddColumnForPhysColumn(*it, params_table, oper, distinct, true);
					item2varid[*it] = VarID(params_table.n, col.n);
				} else
					item2varid[*it] = VarID(tab.n, col.n);
			} else {
				// aggregation in WHERE not possible unless it is a parameter
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!IsAggregationItem(*it));
				item2varid[*it] = VarID(tab.n, col.n);
			}
		}
	} else { // !in_where
		bool is_const = true;
		WrapStatus::wrap_status_t ws;
		AttrID at, vc;
		for(set<Item*>::iterator it = ifields.begin(); it != ifields.end(); it++) {
			if(IsAggregationItem(*it)) {
				is_const = false;
				Item_sum* aggregation = (Item_sum*)*it;
				if(aggregation->arg_count > 1)
					return WrapStatus::FAILURE;

				if(IsCountStar(aggregation)) { // count(*) doesn't need any virtual column
					at.n = GetAddColumnId(AttrID(NULL_VALUE_32), tmp_table, COUNT, false);
					if(at.n == NULL_VALUE_32) // doesn't exist yet
						cq->AddColumn(at, tmp_table, CQTerm(), COUNT, NULL, false);
				} else {
					ColOperation oper;
					bool distinct;
					if(!OperationUnmysterify(aggregation, oper, distinct, true))
						 return WrapStatus::FAILURE;
					AttrID col;
					TabID tab;
					if(IsFieldItem(aggregation->args[0]) && FieldUnmysterify(aggregation, tab, col) && cq->ExistsInTempTable(tab, tmp_table)) {
						// PHYSICAL COLUMN
						at.n = AddColumnForPhysColumn(aggregation->args[0], tmp_table, oper, distinct, true);
					} else {
						// EXPRESSION
						ws = WrapMysqlExpression(aggregation->args[0], tmp_table, expr, in_where, false);
						if(ws == WrapStatus::FAILURE)
							return ws;
						at.n = AddColumnForMysqlExpression(expr, tmp_table, aggregation->name, oper, distinct, true);
					}
				}
				item2varid[*it] = VarID(tmp_table.n, at.n);
			} else if(IsFieldItem(*it)) {
				is_const = false;
				AttrID col;
				TabID tab;
				if(!FieldUnmysterify(*it, tab, col))
					return WrapStatus::FAILURE;
				if(!cq->ExistsInTempTable(tab, tmp_table)) {
					bool is_group_by;
					TabID params_table = cq->FindSourceOfParameter(tab, tmp_table, is_group_by);
					ColOperation oper;
					bool distinct;
					if(!OperationUnmysterify(*it, oper, distinct, true))
						return WrapStatus::FAILURE;
					if(is_group_by && !IsParameterFromWhere(params_table)) {
						col.n = AddColumnForPhysColumn(*it, params_table, oper, distinct, true);
						item2varid[*it] = VarID(params_table.n, col.n);
					} else
						item2varid[*it] = VarID(tab.n, col.n);
				} else if(aggr_used) {
					ColOperation oper;
					bool distinct;
					if(!OperationUnmysterify(*it, oper, distinct, true))
						return WrapStatus::FAILURE;
					at.n = AddColumnForPhysColumn(*it, tmp_table, oper, distinct, true);
					item2varid[*it] = VarID(tmp_table.n, at.n);
				} else {
					item2varid[*it] = VarID(tab.n, col.n);
				}
			} else
				assert(0); // unknown item type?
		}
	}
	gc_expressions.push_back(expr = new MysqlExpression(item, item2varid, use_IBExpressions));
	return WrapStatus::SUCCESS;
#endif
}

int Query::AddColumnForPhysColumn(Item* item, const TabID& tmp_table, const ColOperation oper, const bool distinct, bool group_by, const char* alias)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	std::pair<int,int> vc;
	AttrID col, at;
	TabID tab;
	if(!FieldUnmysterify(item, tab, col))
		return NULL_VALUE_32;
	if(tab.n == NULL_VALUE_32)
		tab = tmp_table; 	//table name not contained in item - must be the result temp_table

	assert(cq->ExistsInTempTable(tab, tmp_table));
	if(item->type() == Item_bhfield::get_bhitem_type() && IsAggregationItem(dynamic_cast<Item_bhfield *>(item)->OriginalItem()))
		return ((Item_bhfield*)item)->varID[0].col;

	vc = VirtualColumnAlreadyExists(tmp_table, tab, col);
	if(vc.first == NULL_VALUE_32) {
		vc.first = tmp_table.n;
		cq->CreateVirtualColumn(vc.second, tmp_table, tab, col);
		phys2virt.insert(make_pair(std::make_pair(tab.n, col.n), vc));
	} else {
		int attr = GetAddColumnId(AttrID(vc.second), tmp_table, oper, distinct);
		if(attr != NULL_VALUE_32) {
			if(group_by) // do not add column - not needed duplicate
				return attr;
			//vc.n = col_to_vc[attr];
		} else if(group_by && oper == GROUP_BY && (attr = GetAddColumnId(AttrID(vc.second), tmp_table, LISTING, distinct)) != NULL_VALUE_32) {
			// modify existing column
			CQChangeAddColumnLIST2GROUP_BY(tmp_table, attr);
			return attr;
		} else if(group_by && oper == LISTING && (attr = GetAddColumnId(AttrID(vc.second), tmp_table, GROUP_BY, distinct)) != NULL_VALUE_32) {
			// don;t add unnecessary column to select list
			return attr;
		}
	}
	if(!item->name && item->type() == Item::SUM_FUNC_ITEM)
		cq->AddColumn(at, tmp_table, CQTerm(vc.second), oper, group_by ? NULL : ((Item_field*)(((Item_sum*)item)->args[0]))->name, distinct);
	else
		cq->AddColumn(at, tmp_table, CQTerm(vc.second), oper, group_by ? NULL : item->name, distinct);
	if(!group_by && item->name)
		field_alias2num[TabIDColAlias(tmp_table.n, alias ? alias : item->name)] = at.n;
	return at.n;
#endif
}

int Query::AddColumnForMysqlExpression(MysqlExpression* mysql_expression,
		const TabID& tmp_table, const char* alias, const ColOperation oper, const bool distinct, bool group_by /*= false*/)
{
	AttrID at, vc;
	vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
	if(vc.n == NULL_VALUE_32) {
		cq->CreateVirtualColumn(vc, tmp_table, mysql_expression, (oper == DELAYED ? tmp_table : TabID(NULL_VALUE_32)));
		tab_id2expression.insert(make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
	} else {
		mysql_expression->RemoveUnusedVarID();
		int attr = GetAddColumnId(vc, tmp_table, oper, distinct);
		if(attr != NULL_VALUE_32) {
			if(group_by) // do not add column - not needed duplicate
				return attr;
			//vc.n = col_to_vc[attr];
		} else if(group_by && oper == GROUP_BY && (attr = GetAddColumnId(vc, tmp_table, LISTING, distinct)) != NULL_VALUE_32) {
			// modify existing column
			CQChangeAddColumnLIST2GROUP_BY(tmp_table, attr);
			return attr;
		} else if(group_by && oper == LISTING && (attr = GetAddColumnId(vc, tmp_table, GROUP_BY, distinct)) != NULL_VALUE_32) {
			// don;t add unnecessary column to select list
			return attr;
		}
	}

	//if (parametrized)
	//	cq->AddColumn(at, tmp_table, CQTerm(vc.n), DELAYED, group_by ? NULL : alias, distinct);
	//else
		cq->AddColumn(at, tmp_table, CQTerm(vc.n), oper, group_by ? NULL : alias, distinct);
	if(!group_by && alias)
		field_alias2num[TabIDColAlias(tmp_table.n, alias)] = at.n;
	return at.n;
}

bool Query::IsLocalColumn(Item* item, const TabID& tmp_table)
{
	assert(IsFieldItem(item) || IsAggregationItem(item));
	AttrID col;
	TabID tab;
	if(!FieldUnmysterify(item, tab, col))
		return false;
	return cq->ExistsInTempTable(tab, tmp_table);
}

int Query::Compile(CompiledQuery* compiled_query, SELECT_LEX* selects_list, SELECT_LEX* last_distinct,
		TabID* res_tab, bool ignore_limit, Item* left_expr_for_subselect, Operator* oper_for_subselect, 
		bool ignore_minmax, bool for_subq_in_where)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	MEASURE_FET("Query::Compile(...)");
	// at this point all tables are in RCBase engine, so we can proceed with the query

	/*Item_func
	 |
	 --Item_int_func  <-  arguments are kept in an array accessible through arguments()
	 |
	 --Item_bool_func
	 |   |
	 |   ---Item_cond  <- arguments are kept in a list accessible through argument_list()
	 |   |     |
	 |   |     ---Item_cond_and   <- when negated OR of negated items is created
	 |   |     |
	 |   |     ---Item_cond_or    <- when negated AND of negated items is created
	 |   |     |
	 |   |     ---Item_cond_xor
	 |   |
	 |   ---Item_equal  <- arguments are kept in a list accessible through argument_list()
	 |   |                 + const_item (accessible through get_const() )
	 |   |           (multiple equality)
	 |   |
	 |   ---Item_func_not
	 |   |            (???)
	 |   |
	 |   ---Item func_isnull     <- when negated IS NOT NULL is created
	 |
	 --Item_func_opt_neg  <-  arguments are kept in an array accessible through arguments(), if negated
	 |   |                     this information is kept additionally (in a field named 'negated')
	 |   |
	 |   |
	 |   ---Item_func_in
	 |   |
	 |   |
	 |   ---Item_func_between
	 |
	 |
	 --Item_bool_func2
	 |
	 |
	 ---Item_bool_rowready_func2  <-arguments are kept in an array accessible through arguments(), if negated
	 |                          an object of a corresponding class is created
	 |                           (e.q. ~Item_func_lt => Item_func_ge)
	 |
	 ----Item_func_eq
	 |
	 |
	 ----Item_func_ne
	 |
	 |
	 ----Item_func_ge
	 |
	 |
	 ----Item_func_le
	 |
	 |
	 ----Item_func_gt
	 |
	 |
	 ----Item_func_lt
	 |
	 |
	 ----Item_func_equal   <- This is mystery so far

	 There are 3 equality functions:
	 Item_equal -> multiple equality (many fields and optional additional constant value)
	 Item_func_equal -> ???
	 Item_func_eq -> pairwise equality
	 */

	bool union_all = (last_distinct == NULL);
	TabID prev_result;

	SQL_LIST* global_order = NULL;
	int col_count = 0;
	_int64 global_limit_value = -1;
	_int64 global_offset_value = -1;

	// local copy of current this->cq, to be restored on exit
	CompiledQuery* _cq = this->cq;
	this->cq = compiled_query;

	if(selects_list != selects_list->join->unit->global_parameters) { //only in case of unions this is set
		SetLimit(selects_list->join->unit->global_parameters, 0, global_offset_value, (_int64&)global_limit_value);
		global_order = &(selects_list->join->unit->global_parameters->order_list);
	};

	for(SELECT_LEX* sl = selects_list; sl; sl = sl->next_select()) {
		_int64 limit_value = -1;
		_int64 offset_value = -1;

		if(!JudgeErrors(sl))
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		SetLimit(sl, sl==selects_list ? 0 : sl->join->unit->global_parameters, offset_value, limit_value);

		List<Item>* fields = &sl->join->fields_list;
		COND *conds = sl->join->conds;
		ORDER *order = sl->join->order;

		//if (order) global_order = 0;   //we want to zero global order (which seems to be always present)
		//if we find a local order by clause
		//  The above is not necessary since global_order is set only in case of real UNIONs

		ORDER *group = sl->join->group_list;
		COND *having = sl->join->having;
		List<TABLE_LIST> *join_list = sl->join->join_list;
		bool zero_result = sl->join->zero_result_cause != NULL;

		Item* field_for_subselect;
		Item* cond_to_reinsert= NULL;
		List<Item>* list_to_reinsert= NULL;

		// keep local copies of class fields to be changed
		//map<string,int>* _table_alias2index = this->table_alias2index;
		//map<string,TABLE *>* _table_alias2ptr = this->table_alias2ptr;

		// structures that will be used throughout compilation
		//map<string, int> local_table_alias2index = *outer_table_alias2index;
		//map<string, TABLE *> local_table_alias2ptr = *outer_table_alias2ptr;

		// assign pointers to above structures onto class fields
		//this->table_alias2index = &local_table_alias2index;
		//this->table_alias2ptr = &local_table_alias2ptr;

		TabID tmp_table;

		try {
			// partial optimization of LOJ conditions, JOIN::optimize(part=3)
			// necessary due to already done basic transformation of conditions
			// see comments in sql_select.cc:JOIN::optimize()
			if(IsLOJ(join_list))
				sl->join->optimize(3);

			if(left_expr_for_subselect)
				if(!ClearSubselectTransformation(*oper_for_subselect, field_for_subselect, conds, having,
						cond_to_reinsert, list_to_reinsert, left_expr_for_subselect))
					throw CompilationError();

			if(having && !group) //we cannot handle the case of a having without a group by
				throw CompilationError();

			TABLE_LIST *tables = sl->leaf_tables ? sl->leaf_tables : (TABLE_LIST*)sl->table_list.first;
			for(TABLE_LIST* table_ptr = tables; table_ptr; table_ptr = table_ptr->next_leaf) {
				if(!table_ptr->derived) {
					if(!RCEngine::IsBHTable(table_ptr->table))
						throw CompilationError();
					string path = TablePath(table_ptr);
					if(path2num.find(path)==path2num.end()) {
						path2num[path] = no_tab;
						Add(m_conn->GetTransaction()->GetTable(path, table_ptr->table->s));
					}
				}
			}
			vector<TabID> left_tables, right_tables;
			bool first_table = true;
			if(!AddJoins(*join_list, tmp_table, left_tables, right_tables, (res_tab != NULL && res_tab->n != 0), first_table, for_subq_in_where))
				throw CompilationError();

			List<Item> field_list_for_subselect;
			if(left_expr_for_subselect && field_for_subselect) {
				field_list_for_subselect.push_back(field_for_subselect);
				fields = &field_list_for_subselect;
			}
			bool aggr_used = false;
			if(!AddFields(*fields, tmp_table, group != NULL, col_count, ignore_minmax, aggr_used ))
				throw CompilationError();

			if(!AddGroupByFields(group, tmp_table))
				throw CompilationError();

			if(!AddOrderByFields(order, tmp_table, group != NULL || sl->join->select_distinct || aggr_used))
				throw CompilationError();
			CondID cond_id;
			if(!BuildConditions(conds, cond_id, cq, tmp_table, WHERE_COND, zero_result))
				throw CompilationError();

			cq->AddConds(tmp_table, cond_id, WHERE_COND);

			cond_id = CondID();
			if(!BuildConditions(having, cond_id, cq, tmp_table, HAVING_COND))
				throw CompilationError();
			cq->AddConds(tmp_table, cond_id, HAVING_COND);

			cq->ApplyConds(tmp_table);
		}
		catch(...) {
			// restore original values of class fields (necessary if this method is called recursively)
			this->cq = _cq;
			//this->table_alias2index = _table_alias2index;
			//this->table_alias2ptr = _table_alias2ptr;
			if(cond_to_reinsert && list_to_reinsert)
				list_to_reinsert->push_back(cond_to_reinsert);
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		}

		// restore original values of class fields (necessary if this method is called recursively)
		//this->table_alias2index = _table_alias2index;
		//this->table_alias2ptr = _table_alias2ptr;

		if(sl->join->select_distinct)
			cq->Mode(tmp_table, TM_DISTINCT);
		if(!ignore_limit && limit_value >= 0)
			cq->Mode(tmp_table, TM_TOP, offset_value, limit_value);

		if(sl == selects_list) {
			prev_result = tmp_table;
			if(global_order && !selects_list->next_select()) { // trivial union with one select and ext. order by
				tmp_table = TabID();
				cq->Union(prev_result, prev_result, tmp_table, true);
			}
		} else
			cq->Union(prev_result, prev_result, tmp_table, union_all);
		if(sl == last_distinct)
			union_all = true;
		if(cond_to_reinsert && list_to_reinsert)
			list_to_reinsert->push_back(cond_to_reinsert);
	}

	cq->BuildTableIDStepsMap();

	if(!AddGlobalOrderByFields(global_order, prev_result, col_count))
		return RETURN_QUERY_TO_MYSQL_ROUTE;

	if(!ignore_limit && global_limit_value >= 0)
		cq->Mode(prev_result, TM_TOP, global_offset_value, global_limit_value);

	if(res_tab != NULL)
		*res_tab = prev_result;
	else
		cq->Result(prev_result);
	this->cq = _cq;
	return RCBASE_QUERY_ROUTE;
#endif
}

JoinType Query::GetJoinTypeAndCheckExpr(uint outer_join, Item* on_expr)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return JO_INNER;
#else
	if(outer_join)
		BHASSERT(on_expr != 0, "on_expr shouldn't be null when outer_join != 0");

	JoinType join_type;

	if((outer_join & JOIN_TYPE_LEFT) && (outer_join & JOIN_TYPE_RIGHT))
		join_type = JO_FULL;
	else if(outer_join & JOIN_TYPE_LEFT)
		join_type = JO_LEFT;
	else if(outer_join & JOIN_TYPE_RIGHT)
		join_type = JO_RIGHT;
	else
		join_type = JO_INNER;

	return join_type;
#endif
}

void Query::FindLastJoinsForTables(CompiledQuery& qu)
{
	std::map<int, int> alias2table;
	std::map<int, int>::const_iterator iter;
	last_join = new int[no_tab];
	for (int i = 0; i < no_tab; i++)
		last_join[i] = -1;
	for(int i = 0; i < qu.NoSteps(); i++) {
		CompiledQuery::CQStep step = qu.Step(i);
		switch(step.type) {
			case CompiledQuery::TABLE_ALIAS:
				alias2table[step.t1.n] = step.t2.n;
				break;
			case CompiledQuery::TMP_TABLE:
				iter = alias2table.find(step.tables1[0].n);
				if (iter != alias2table.end())
					last_join[iter->second] = i;
				break;
			case CompiledQuery::JOIN_T:
				iter = alias2table.find(step.t2.n);
				if (iter != alias2table.end())
					last_join[iter->second] = i;
				break;
			default:
				break;
		}
	}
}

bool Query::IsLOJ(List<TABLE_LIST>* join)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	TABLE_LIST* join_ptr;
	List_iterator<TABLE_LIST> li(*join);
	while(join_ptr = li++) {
		JoinType join_type = GetJoinTypeAndCheckExpr(join_ptr->outer_join, join_ptr->on_expr);
		if(join_ptr->on_expr && (join_type == JO_LEFT || join_type == JO_RIGHT))
			return true;
	}
	return false;
#endif
}

