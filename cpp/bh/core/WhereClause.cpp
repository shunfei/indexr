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

#include <boost/bind.hpp>
#include "Query.h"
#include "compilation_tools.h"
#include "CQTerm.h"
#include "ValueSet.h"
#include "CompiledQuery.h"
#include "MysqlExpression.h"

using namespace std;

int Query::Item2CQTerm(Item* an_arg, CQTerm& term, const TabID& tmp_table, CondType filter_type, bool negative, Item* left_expr_for_subselect, Operator* oper_for_subselect) {
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	an_arg = UnRef(an_arg);
	if(an_arg->type() == Item::SUBSELECT_ITEM) {
		Item_subselect* item_subs = dynamic_cast<Item_subselect*>(an_arg);
		assert(item_subs && "The cast to (Item_subselect*) was unsuccessful");

		bool ignore_limit = false;
		if(dynamic_cast<Item_maxmin_subselect*>(item_subs) != NULL || dynamic_cast<Item_in_subselect*>(item_subs) != NULL)
			ignore_limit = true;
		TabID subselect;
		st_select_lex_unit* select_unit = item_subs->get_unit();

		// needs to check if we can relay on subquery transformation to min/max
		bool ignore_minmax = (dynamic_cast<Item_maxmin_subselect*>(item_subs)==NULL
			&& dynamic_cast<Item_in_subselect*>(item_subs)==NULL
			&& negative
			&& item_subs->substype()==Item_subselect::SINGLEROW_SUBS);
		subqueries_in_where.push_back(make_pair(tmp_table, item_subs->place() != IN_HAVING && filter_type != HAVING_COND));

		// we need to make a copy of global map with table aliases so that subquery contains
		// aliases of outer queries and itself but not "parallel" subqueries.
		// Once subquery is compiled we can get rid of its aliases since they are not needed any longer
		// and stay with aliases of outer query only
		std::multimap<std::string,std::pair<int,TABLE*> > outer_map_copy = table_alias2index_ptr;
		int res = Compile(cq, select_unit->first_select(), select_unit->union_distinct, &subselect, ignore_limit, 
			left_expr_for_subselect, oper_for_subselect, ignore_minmax, true);
		// restore outer query aliases
		table_alias2index_ptr = outer_map_copy;

		subqueries_in_where.pop_back();
		if(res == RCBASE_QUERY_ROUTE) {
			AttrID at, vc;
			vc.n = VirtualColumnAlreadyExists(tmp_table, subselect);
			if(vc.n == NULL_VALUE_32) {
				cq->CreateVirtualColumn(vc, tmp_table, subselect, filter_type == HAVING_COND ? true : false);
				tab_id2subselect.insert(make_pair(tmp_table, std::make_pair(vc.n, subselect)));
			}
			if(oper_for_subselect) {
				if(dynamic_cast<Item_maxmin_subselect*>(item_subs) != NULL || dynamic_cast<Item_in_subselect*>(item_subs) != NULL) {
					if(negative) {
						MarkWithAll(*oper_for_subselect);
						if(dynamic_cast<Item_in_subselect*>(item_subs) != NULL && *oper_for_subselect == O_IN)
							*oper_for_subselect = O_EQ_ALL;
					} else {
						MarkWithAny(*oper_for_subselect);
						if(dynamic_cast<Item_allany_subselect*>(item_subs) != NULL && dynamic_cast<Item_allany_subselect*>(item_subs)->all == 1)
							*oper_for_subselect = O_EQ_ALL;
					}
				} else {
					if(negative) {
						//if(item_subs->substype() != Item_subselect::SINGLEROW_SUBS)
						//	return RETURN_QUERY_TO_MYSQL_ROUTE;
						MarkWithAll(*oper_for_subselect);
					} else
						UnmarkAllAny(*oper_for_subselect);
				}
			}
			term = CQTerm(vc.n);
		}
		return res;
	}

	if(filter_type == HAVING_COND) {
		ColOperation oper;
		bool distinct;
		if(!OperationUnmysterify(an_arg, oper, distinct, true)) //is_having_clause may be true only in case group by clause was present
			return RETURN_QUERY_TO_MYSQL_ROUTE;

		std::pair<int,int> phys_vc;
		AttrID col, at, vc;
		TabID tab;
		if((IsFieldItem(an_arg) || IsAggregationOverFieldItem(an_arg))&& !FieldUnmysterify(an_arg, tab, col))
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		if(IsAggregationItem(an_arg) && HasAggregation(((Item_sum*)an_arg)->args[0]))
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		if((IsFieldItem(an_arg) || IsAggregationOverFieldItem(an_arg)) && cq->ExistsInTempTable(tab, tmp_table)) {
			int col_num = AddColumnForPhysColumn(an_arg, tmp_table, oper, distinct, true);
			phys_vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
			if(phys_vc.first == NULL_VALUE_32) {
				phys_vc.first = tmp_table.n;
				cq->CreateVirtualColumn(phys_vc.second, tmp_table, tmp_table, AttrID(col_num));
				phys2virt.insert(make_pair(std::pair<int,int>(tmp_table.n, -col_num - 1), phys_vc));
			}
			vc.n = phys_vc.second;
		} else if(IsCountStar(an_arg)) {
			at.n = GetAddColumnId(AttrID(NULL_VALUE_32), tmp_table, COUNT, false);
			if(at.n == NULL_VALUE_32) // doesn't exist yet
				cq->AddColumn(at, tmp_table, CQTerm(), COUNT, NULL, false);
			phys_vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, at);
			if(phys_vc.first == NULL_VALUE_32) {
				phys_vc.first = tmp_table.n;
				cq->CreateVirtualColumn(phys_vc.second, tmp_table, tmp_table, at);
				phys2virt.insert(make_pair(std::pair<int,int>(tmp_table.n, at.n), phys_vc));
			}
			vc.n = phys_vc.second;
		} else if (an_arg->type() == Item::VARBIN_ITEM) {
			String str;
			an_arg->val_str(&str); //sets null_value
			if(!an_arg->null_value) {
				if(an_arg->max_length <= 8) {
					Item* int_item = new Item_int((ulonglong)an_arg->val_int());
					MysqlExpression* mysql_expression = NULL;
					MysqlExpression::Item2VarID item2varid;
					gc_expressions.push_back(mysql_expression = new MysqlExpression(int_item, item2varid, use_IBExpressions));
					vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
					if(vc.n == NULL_VALUE_32) {
						cq->CreateVirtualColumn(vc, tmp_table, mysql_expression);
						tab_id2expression.insert(make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
					}
				} else
					return RETURN_QUERY_TO_MYSQL_ROUTE; //too large binary to be treated as BIGINT
			} else {
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			}
		} else {
			MysqlExpression* expr;
			MysqlExpression::SetOfVars vars;
			if(WrapMysqlExpression(an_arg, tmp_table, expr, false, true) == WrapStatus::FAILURE)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			if(IsConstExpr(expr->GetVars(), tmp_table)) {
				vc.n = VirtualColumnAlreadyExists(tmp_table, expr);
				if(vc.n == NULL_VALUE_32) {
					cq->CreateVirtualColumn(vc, tmp_table, expr, tmp_table);
					tab_id2expression.insert( make_pair(tmp_table, std::make_pair(vc.n, expr)) );
				}
			} else if(IsAggregationItem(an_arg)) {
				assert(expr->GetItem()->type() == Item_bhfield::get_bhitem_type());
				int col_num = ((Item_bhfield*)expr->GetItem())->varID[((Item_bhfield*)expr->GetItem())->varID.size()-1].col;
				phys_vc = VirtualColumnAlreadyExists(tmp_table, tmp_table, AttrID(-col_num - 1));
				if(phys_vc.first == NULL_VALUE_32) {
					phys_vc.first = tmp_table.n;
					cq->CreateVirtualColumn(phys_vc.second, tmp_table, tmp_table, AttrID(col_num));
					phys2virt.insert(make_pair(std::pair<int,int>(tmp_table.n, -col_num - 1), phys_vc));
				}
				vc.n = phys_vc.second;
			} else {
//				int col_num = AddColumnForMysqlExpression(expr, tmp_table, NULL, DELAYED, distinct, true);
				vc.n = VirtualColumnAlreadyExists(tmp_table, expr);
				if(vc.n == NULL_VALUE_32) {
					cq->CreateVirtualColumn(vc, tmp_table, expr, tmp_table);
					tab_id2expression.insert( make_pair(tmp_table, std::make_pair(vc.n, expr)) );
				}
			}
		}
		term = CQTerm(vc.n);
		return RCBASE_QUERY_ROUTE;
	} else {
		//WHERE FILTER

		std::pair<int,int> phys_vc;
		AttrID vc;
		AttrID col;
		TabID tab;
		if(IsFieldItem(an_arg) && !FieldUnmysterify(an_arg, tab, col))
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		if(IsFieldItem(an_arg) && cq->ExistsInTempTable(tab, tmp_table)) {
			phys_vc = VirtualColumnAlreadyExists(tmp_table, tab, col);
			if(phys_vc.first == NULL_VALUE_32) {
				phys_vc.first = tmp_table.n;
				cq->CreateVirtualColumn(phys_vc.second, tmp_table, tab, col);
				phys2virt.insert(make_pair(std::pair<int,int>(tab.n, col.n), phys_vc));
			}
			vc.n = phys_vc.second;
		} else if(an_arg->type() == Item::VARBIN_ITEM) {
			String str;
			an_arg->val_str(&str); //sets null_value
			if(!an_arg->null_value) {
				if(an_arg->max_length <= 8) {
					Item* int_item= new Item_int((ulonglong)an_arg->val_int());
					MysqlExpression* mysql_expression = NULL;
					MysqlExpression::Item2VarID item2varid;
					gc_expressions.push_back( mysql_expression = new MysqlExpression(int_item, item2varid, use_IBExpressions) );
					vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
					if(vc.n == NULL_VALUE_32) {
						cq->CreateVirtualColumn(vc, tmp_table, mysql_expression);
						tab_id2expression.insert(make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
					}
				} else
					return RETURN_QUERY_TO_MYSQL_ROUTE; //too large binary to be treated as BIGINT
			} else {
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			}
		} else {
			MysqlExpression* expr;
			WrapStatus::wrap_status_t ws = WrapMysqlExpression(an_arg, tmp_table, expr, true, false);
			if(ws != WrapStatus::SUCCESS)
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			vc.n = VirtualColumnAlreadyExists(tmp_table, expr);
			if(vc.n == NULL_VALUE_32) {
				cq->CreateVirtualColumn(vc, tmp_table, expr);
				tab_id2expression.insert( make_pair(tmp_table, std::make_pair(vc.n, expr)) );
			}
		}
		term = CQTerm(vc.n);
		return RCBASE_QUERY_ROUTE;
	}
	return RETURN_QUERY_TO_MYSQL_ROUTE;
#endif
}

CondID Query::ConditionNumberFromMultipleEquality(Item_equal* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter, bool is_or_subtree)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return CondID(0);
#else
	Item_equal_iterator li(*conds);

	CQTerm zero_term, first_term, next_term;
	Item_field* ifield;
	Item* const_item = conds->get_const();
	if(const_item) {
		if(!Item2CQTerm(const_item, zero_term, tmp_table, filter_type))
			return CondID(-1);
	} else {
		 ifield = li++;
		 if(!Item2CQTerm(ifield, zero_term, tmp_table, filter_type))
			 return CondID(-1);
	}
	ifield = li++;
	if(!Item2CQTerm(ifield, first_term, tmp_table, filter_type))
		return CondID(-1);
	CondID filter;
	if(!and_me_filter)
		cq->CreateConds(filter, tmp_table, first_term, O_EQ, zero_term, CQTerm(), is_or_subtree || filter_type == HAVING_COND);
	else
		cq->And(*and_me_filter, tmp_table, first_term,O_EQ, zero_term);
	while(ifield = li++) {
		if(!Item2CQTerm(ifield, next_term, tmp_table, filter_type))
			 return CondID(-1);
		if(!and_me_filter)
			cq->And(filter, tmp_table, next_term, O_EQ, zero_term);
		else
			cq->And(*and_me_filter, tmp_table, next_term, O_EQ, zero_term);
	}
	if(and_me_filter)
		filter = *and_me_filter;
	return filter;
#endif
}

Item* Query::FindOutAboutNot(Item* it, bool& is_there_not)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	is_there_not = false;

	/*First we try to take care of IN and BETWEEN*/
	Item_func_opt_neg* it_maybe_neg = dynamic_cast<Item_func_opt_neg *>(it);
	if(it_maybe_neg) {
		is_there_not = it_maybe_neg->negated;
		return it;
	}

	/*Then we remove negation in case of LIKE (or sth. else)*/
	if( ((Item_func*)it)->functype() == Item_func::NOT_FUNC ) {
		Item * arg = UnRef(((Item_func *)it)->arguments()[0]);
		//OK, in case of subselects (and possibly other stuff) NOT is handled by other statements,
		//So we narrow the choice of cases in which we handle NOT down to LIKE
		if(arg->type() == Item::FUNC_ITEM && ((Item_func*)arg)->functype() == Item_func::LIKE_FUNC) {
			is_there_not = true;
			return arg;
		}
	}
	return it;
#endif
}

CondID Query::ConditionNumberFromComparison(Item* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter, bool is_or_subtree, bool negative)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return CondID(0);
#else
	CondID filter;
	Operator op;  /*{	O_EQ, O_NOT_EQ, O_LESS, O_MORE, O_LESS_EQ, O_MORE_EQ,
			O_IS_NULL, O_NOT_NULL, O_BETWEEN, O_LIKE, O_IN, O_ESCAPE etc...};*/
	char like_esc;
	Item_in_optimizer* in_opt = NULL;	// set if IN expression with subselect

	ExtractOperatorType(conds, op, negative, like_esc);
	Item_func* cond_func = (Item_func*)conds;
	if(op == O_MULT_EQUAL_FUNC)
		return ConditionNumberFromMultipleEquality((Item_equal *) conds, tmp_table, filter_type, and_me_filter, is_or_subtree);
	else if(op == O_NOT_FUNC) {
		if(cond_func->arg_count != 1 || dynamic_cast<Item_in_optimizer*>(cond_func->arguments()[0]) == NULL)
			return CondID(-2);
		return ConditionNumberFromComparison(cond_func->arguments()[0], tmp_table, filter_type, and_me_filter, is_or_subtree, true);
	} else if(op == O_NOT_ALL_FUNC) {
		if(cond_func->arg_count != 1)
			return CondID(-1);
		return ConditionNumberFromComparison(cond_func->arguments()[0], tmp_table, filter_type, and_me_filter, is_or_subtree, dynamic_cast<Item_func_nop_all*>(cond_func) == NULL);
	} else if(op == O_UNKNOWN_FUNC) {
		in_opt = dynamic_cast<Item_in_optimizer*>(cond_func);
		if(in_opt == NULL || cond_func->arg_count != 2 || in_opt->arguments()[0]->cols() != 1)
			return CondID(-2);
		op = O_IN;
	} else if(op == O_ERROR)
		return CondID(-2);   //unknown function type

	if((cond_func->arg_count < 0) || (cond_func->arg_count > 3 && op != O_IN && op!= O_NOT_IN))
		return CondID(-1); // argument count error

	CQTerm terms[3];
	vector<MysqlExpression*> exprs;
	vector<int> vcs;

	Item** args = cond_func->arguments();
	for(uint i = 0; i < cond_func->arg_count; i++) {
		Item* an_arg = UnRef(args[i]);
		if((op == O_IN || op == O_NOT_IN) && i > 0) {
			if(i==1 && in_opt) {
				if(!Item2CQTerm(an_arg, terms[i], tmp_table, filter_type, negative, *in_opt->get_cache(), &op))
					return CondID(-1);
				if(negative)
					switch(op) {
						case O_EQ:		op = O_NOT_EQ; break;
						case O_EQ_ALL:	op = O_NOT_IN; break;
						case O_EQ_ANY:	op = O_NOT_EQ_ANY; break;
						case O_NOT_EQ:	op = O_EQ; break;
						case O_NOT_EQ_ALL:	op = O_EQ_ALL; break;
						case O_NOT_EQ_ANY:	op = O_EQ_ANY; break;
						case O_LESS_EQ:	op = O_MORE; break;
						case O_LESS_EQ_ALL:	op = O_MORE_ALL; break;
						case O_LESS_EQ_ANY:	op = O_MORE_ANY; break;
						case O_MORE_EQ:	op = O_LESS; break;
						case O_MORE_EQ_ALL:	op = O_LESS_ALL; break;
						case O_MORE_EQ_ANY:	op = O_LESS_ANY; break;
						case O_MORE:	op = O_LESS_EQ; break;
						case O_MORE_ALL:	op = O_LESS_EQ_ALL; break;
						case O_MORE_ANY:	op = O_LESS_EQ_ANY; break;
						case O_LESS:	op = O_MORE_EQ; break;
						case O_LESS_ALL:	op = O_MORE_EQ_ALL; break;
						case O_LESS_ANY:	op = O_MORE_EQ_ANY; break;
						case O_LIKE:	op = O_NOT_LIKE; break;
						case O_IN:		op = O_NOT_IN; break;
						case O_NOT_LIKE:op = O_LIKE; break;
						case O_NOT_IN:	op = O_IN; break;
						default:
							return CondID(-1);
					}
			} else {
				CQTerm t;
				if(!Item2CQTerm(an_arg, t, tmp_table, filter_type, an_arg->type() == Item::SUBSELECT_ITEM ? negative : false, NULL, &op))
					return CondID(-1);
				vcs.push_back(t.vc_id);
			}
		} else {
			if(!Item2CQTerm(an_arg, terms[i], tmp_table, filter_type, an_arg->type() == Item::SUBSELECT_ITEM ? negative : false, NULL, &op))
				return CondID(-1);
			if((op == O_LIKE || op == O_NOT_LIKE) &&
				!(an_arg->field_type() == MYSQL_TYPE_VARCHAR || an_arg->field_type() == MYSQL_TYPE_STRING || an_arg->field_type() == MYSQL_TYPE_VAR_STRING || an_arg->field_type() == MYSQL_TYPE_BLOB)) {
				my_message(ER_SYNTAX_ERROR, "Brighthouse specific error: Both operator and operand must be string data types for LIKE comparison", MYF(0));
				throw ReturnMeToMySQLWithError();
			}
		}
	}

	if((op == O_IN || op == O_NOT_IN) && !in_opt) {
		AttrID vc;
		vc.n = VirtualColumnAlreadyExists(tmp_table, vcs, AttrID(terms[0].vc_id));
		if(vc.n == NULL_VALUE_32) {
			cq->CreateVirtualColumn(vc, tmp_table, vcs, AttrID(terms[0].vc_id));
			tab_id2inset.insert(make_pair(tmp_table, std::make_pair(vc.n, std::make_pair(vcs, AttrID(terms[0].vc_id)))));
		}
		terms[1] = CQTerm(vc.n);
	}

	if(!and_me_filter)
		cq->CreateConds(filter, tmp_table, terms[0], op, terms[1], terms[2], is_or_subtree || filter_type == HAVING_COND, like_esc);
	else {
		if(is_or_subtree)
			cq->Or(*and_me_filter, tmp_table, terms[0], op, terms[1], terms[2]);
		else
			cq->And(*and_me_filter, tmp_table, terms[0], op, terms[1], terms[2]);
		filter = *and_me_filter;
	}
	return filter;
#endif
}

CondID Query::ConditionNumberFromNaked(COND* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter, bool is_or_subtree)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return CondID(0);
#else
	CondID filter;
	CQTerm naked_col;
	if(!Item2CQTerm(conds, naked_col, tmp_table, filter_type, conds->type() == Item::SUBSELECT_ITEM ? (and_me_filter!=NULL) : false))
		return CondID(-1);

	bool is_string = conds->result_type() == STRING_RESULT;
	MysqlExpression::Item2VarID item2varid;
	AttrID vc;
	Item* zero_item;
	if(is_string)
		zero_item = new Item_empty_string("", 0, conds->collation.collation);
	else
		zero_item= new Item_int((longlong)0);

	MysqlExpression* mysql_expression = NULL;
	gc_expressions.push_back( mysql_expression = new MysqlExpression(zero_item, item2varid, use_IBExpressions) );
	// TODO: where mysql_expression & zero_item is destroyed ???
	vc.n = VirtualColumnAlreadyExists(tmp_table, mysql_expression);
	if(vc.n == NULL_VALUE_32) {
		cq->CreateVirtualColumn(vc, tmp_table, mysql_expression);
		tab_id2expression.insert(make_pair(tmp_table, std::make_pair(vc.n, mysql_expression)));
	}
	if(!and_me_filter)
		cq->CreateConds(filter, tmp_table, naked_col, O_NOT_EQ, CQTerm(vc.n), CQTerm(), 
		is_or_subtree || filter_type == HAVING_COND);
	else {
		if(is_or_subtree)
			cq->Or(*and_me_filter, tmp_table, naked_col, O_NOT_EQ, CQTerm(vc.n), CQTerm());
		else
			cq->And(*and_me_filter, tmp_table, naked_col, O_NOT_EQ, CQTerm(vc.n), CQTerm());
		filter = *and_me_filter;
	}
	return filter;
#endif
}

struct ItemFieldCompare {

	bool operator()(Item_field* const &f1, Item_field* const& f2) const  {
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
		return f1->field < f2->field;
#endif
	}
};

CondID Query::ConditionNumber(COND* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter, bool is_or_subtree)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return CondID(0);
#else
	//we know, that conds != 0
	//returns -1 on error
	//        >=0 is a created filter number
	conds = UnRef(conds);
	Item::Type cond_type = conds->type();
	CondID cond_id;
	if(cond_type == Item::COND_ITEM) {
		Item_cond* cond_cond = (Item_cond*) conds;
		Item_func::Functype func_type = cond_cond->functype();
		switch(func_type) {
			case Item_func::COND_AND_FUNC: {
				List_iterator_fast<Item> li(*(cond_cond->argument_list()));
				Item* item;
				auto_ptr<CondID> and_cond;
				while(item = li++) {
					CondID res = ConditionNumber(item, tmp_table, filter_type, and_cond.get(),
					// if there is no and_cond the filter has to be created and info
					// about tree/no tree like descriptor has to be passed recursively down
					// once filter is created we pass 'false' to indicate AND
					and_cond.get() ? false : is_or_subtree);
					if(res.IsInvalid())
						return res;
					if(!and_cond.get())
						and_cond = auto_ptr<CondID>(new CondID(res.n));
				}
				cond_id.n = and_cond->n;
				if(and_me_filter && is_or_subtree)
					cq->Or(*and_me_filter, tmp_table, cond_id);
				else if(and_me_filter && !is_or_subtree)
					cq->And(*and_me_filter, tmp_table, cond_id);
				break;
			}
			case Item_func::COND_OR_FUNC: {
				List_iterator_fast<Item> li(*(cond_cond->argument_list()));
				Item* item;
				// rewriting a=1 or a=2 or b=3 or c=4 into a IN (1,2) or b IN (3,4)
				// step1: collect all constants for every column
				// a -> 1,2
				// b -> 3,4
				map<Item_field*, set<int>, ItemFieldCompare> value_map;
				map<Item*, bool> is_transformed;
				CondID* or_cond = NULL;
				while(item = li++) {
					is_transformed[item] = false;
					Item_func_eq* item_func = dynamic_cast<Item_func_eq*>(item);
					if(!item_func)
						continue;
					Item** args = item_func->arguments();
					BHASSERT_WITH_NO_PERFORMANCE_IMPACT(item_func->arg_count == 2);
					Item* first_arg = UnRef(args[0]);
					Item* sec_arg = UnRef(args[1]);
					if(IsConstItem(first_arg) && IsFieldItem(sec_arg))
						swap(first_arg, sec_arg);
					if(IsFieldItem(first_arg) && IsConstItem(sec_arg)) {
						is_transformed[item] = true;
						CQTerm t;
						if(!Item2CQTerm(sec_arg, t, tmp_table, filter_type))
							return CondID(-1);
						value_map[static_cast<Item_field*>(first_arg)].insert(t.vc_id);
					}
				}
				bool create_or_subtree = is_or_subtree;
				li.rewind();
				// generate non-transformed conditions
				while(item = li++) {
					if(is_transformed[item] == true)
						continue;
					create_or_subtree = true;
					CondID res = ConditionNumber(item, tmp_table, filter_type, or_cond, true /*OR_SUBTREE*/);
					if(res.IsInvalid()) 
						return res;
					if(!or_cond)
						or_cond = new CondID(res.n);
				}
				create_or_subtree = create_or_subtree || (value_map.size() > 1);
				// generate re-written INs
				// one IN for every element of value_map 
				map<Item_field*, set<int>, ItemFieldCompare>::iterator it;
				size_t i = value_map.size() - 1;
				for(it = value_map.begin(); it != value_map.end(); ++it, --i) {
					try {
						CQTerm terms[2];
						if(!Item2CQTerm(it->first, terms[0], tmp_table, filter_type))
							return CondID(-1);
						AttrID vc;
						vector<int> vcv(it->second.begin(), it->second.end());
						CondID c_id;
						if(vcv.size() > 1) {
							cq->CreateVirtualColumn(vc, tmp_table, vcv, AttrID(terms[0].vc_id));
							terms[1] = CQTerm(vc.n);
							if(!or_cond) {
								if(and_me_filter && !create_or_subtree) {
									cq->And(*and_me_filter, tmp_table, terms[0], O_IN, terms[1], CQTerm());
									c_id = *and_me_filter;
									and_me_filter = NULL;
								} else 
									cq->CreateConds(c_id, tmp_table, terms[0], O_IN, terms[1], CQTerm(), create_or_subtree || filter_type == HAVING_COND);
								or_cond = new CondID(c_id.n);
							} else {
								cq->Or(*or_cond, tmp_table, terms[0], O_IN, terms[1], CQTerm());
								c_id = *or_cond;
							}
						} else {
							terms[1] = CQTerm(vcv[0]);
							if(!or_cond) {
								if(and_me_filter && !create_or_subtree) {
									cq->And(*and_me_filter, tmp_table, terms[0], O_EQ, terms[1], CQTerm());
									c_id = *and_me_filter;
									and_me_filter = NULL;
								} else 
									cq->CreateConds(c_id, tmp_table, terms[0], O_EQ, terms[1], CQTerm(), create_or_subtree || filter_type == HAVING_COND);
								or_cond = new CondID(c_id.n);
							} else {
								cq->Or(*or_cond, tmp_table, terms[0], O_EQ, terms[1], CQTerm());
								c_id = *or_cond;
							}
						}
						if(c_id.IsInvalid()) {//to deal with deleting dynamical variables (new_and_me_filter) properly
							//in case of failure
							delete or_cond;
							return c_id;
						}
					} catch(...) {//to deal with deleting dynamical variables (new_and_me_filter) properly in case of an exception
						delete or_cond;
						throw;
					}
				}
				cond_id.n = or_cond->n;
				delete or_cond;
				if(and_me_filter)
					cq->And(*and_me_filter, tmp_table, cond_id);
				else if(filter_type != HAVING_COND && create_or_subtree && !is_or_subtree)
					cq->CreateConds(cond_id, tmp_table, cond_id, create_or_subtree || filter_type == HAVING_COND);
				break;
			}
			case Item_func::COND_XOR_FUNC:   //we don't handle xor as yet
			default:
				return CondID(-1);    //unknown function type
		} // end switch()
	} else if(cond_type == Item::FUNC_ITEM) {
		Item_func* cond_func = (Item_func*)conds;
		Item_func::Functype func_type = cond_func->functype();
		Item* arg = NULL;
		if(cond_func->arg_count == 1)
			arg = cond_func->arguments()[0];
		if(func_type == Item_func::NOT_FUNC && arg != NULL
			&& arg->type() == Item::SUBSELECT_ITEM && ((Item_subselect*)arg)->substype() == Item_subselect::EXISTS_SUBS) {

			CQTerm term;
			if(!Item2CQTerm(arg, term, tmp_table, filter_type))
				return CondID(-1);
			if(!and_me_filter)
				cq->CreateConds(cond_id, tmp_table, term, O_NOT_EXISTS, CQTerm(), CQTerm(), is_or_subtree || filter_type == HAVING_COND);
			else {
				if(is_or_subtree)
					cq->Or(*and_me_filter, tmp_table, term, O_NOT_EXISTS, CQTerm(), CQTerm());
				else
					cq->And(*and_me_filter, tmp_table, term, O_NOT_EXISTS, CQTerm(), CQTerm());
				cond_id = *and_me_filter;
			}
		} else if(func_type == Item_func::COND_XOR_FUNC) {
			return CondID(-1);
		} else {
			CondID val = ConditionNumberFromComparison(cond_func, tmp_table, filter_type, and_me_filter, is_or_subtree);
			if(val.n == -2)
				val = ConditionNumberFromNaked(conds, tmp_table, filter_type, and_me_filter, is_or_subtree);
			return val;
		}
	} else if(cond_type == Item::SUBSELECT_ITEM && ((Item_subselect*)conds)->substype() == Item_subselect::EXISTS_SUBS) {
		CQTerm term;
		if(!Item2CQTerm(conds, term, tmp_table, filter_type))
			return CondID(-1);
		if(!and_me_filter) {
			cq->CreateConds(cond_id, tmp_table, term, O_EXISTS,  CQTerm(), CQTerm(), is_or_subtree || filter_type == HAVING_COND);
		} else {
			if(is_or_subtree)
				cq->Or(*and_me_filter, tmp_table, term, O_EXISTS, CQTerm(), CQTerm());
			else
				cq->And(*and_me_filter, tmp_table, term, O_EXISTS, CQTerm(), CQTerm());
			cond_id = *and_me_filter;
		}
	} else if(cond_type == Item::FIELD_ITEM ||
			  cond_type == Item::SUM_FUNC_ITEM ||			  
			  cond_type == Item::SUBSELECT_ITEM ||
			  cond_type == Item::INT_ITEM ||
			  cond_type == Item::STRING_ITEM ||
			  cond_type == Item::NULL_ITEM ||
			  cond_type == Item::REAL_ITEM ||
			  cond_type == Item::DECIMAL_ITEM) {
		return ConditionNumberFromNaked( conds, tmp_table, filter_type, and_me_filter, is_or_subtree);
	}
	return cond_id;
#endif
}

int Query::BuildConditions(COND* conds, CondID& cond_id, CompiledQuery* cq, const TabID& tmp_table,
		CondType filter_type, bool is_zero_result, JoinType join_type)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	conds = UnRef(conds);
	PrintItemTree("BuildFiler(), item tree passed in 'conds':", conds);
	if(is_zero_result) {
		CondID fi;
		cq->CreateConds(fi, tmp_table, CQTerm(), O_FALSE, CQTerm(), CQTerm(), false);
		return RCBASE_QUERY_ROUTE;
	}
	if(!conds)
		return RCBASE_QUERY_ROUTE;   //No Conditions - no filters. OK

	// keep local copies of class fields to be changed
	CompiledQuery* _cq = this->cq;

	// copy method arguments to class fields
	this->cq = cq;

	CondID res = ConditionNumber(conds, tmp_table, filter_type);
	if(res.IsInvalid())
		return RETURN_QUERY_TO_MYSQL_ROUTE;

	if(filter_type == HAVING_COND) {
		cq->CreateConds(res, tmp_table, res, false);
	}

	// restore original values of class fields (may be necessary if this method is called recursively)
	this->cq = _cq;
	if(res.IsInvalid())
		return RETURN_QUERY_TO_MYSQL_ROUTE;
	cond_id = res;
	return RCBASE_QUERY_ROUTE;
#endif
}

bool Query::ClearSubselectTransformation(Operator& oper_for_subselect, Item*& field_for_subselect, COND*& conds, Item*& having, Item*& cond_to_reinsert, List<Item>*& list_to_reinsert, Item* left_expr_for_subselect)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	cond_to_reinsert = NULL;
	list_to_reinsert = NULL;
	Item* cond_removed;
	if(having && (having->type()==Item::COND_ITEM
		|| (having->type()==Item::FUNC_ITEM
			&& ((Item_func*)having)->functype()!=Item_func::ISNOTNULLTEST_FUNC
			&& (((Item_func*)having)->functype()!=Item_func::TRIG_COND_FUNC
				|| ((Item_func*)having)->arguments()[0]->type()!=Item::FUNC_ITEM
				|| ((Item_func*)((Item_func*)having)->arguments()[0])->functype()!=Item_func::ISNOTNULLTEST_FUNC)))) {

		if(having->type() == Item::COND_ITEM) {
			Item_cond* having_cond = (Item_cond*)having;
			// if the condition is a complex formula it must be AND
			if(having_cond->functype() != Item_func::COND_AND_FUNC) 
				return false;
			//the extra condition is in the last argument
			if(having_cond->argument_list()->elements < 2) 
				return false;
			List_iterator<Item> li(*(having_cond->argument_list()));
			while(li++ != NULL) 
				cond_to_reinsert = *li.ref();
			li.rewind();
			while(*li.ref() != cond_to_reinsert) 
				li++;
			li.remove();
			list_to_reinsert = having_cond->argument_list();
			cond_removed = cond_to_reinsert;
		} else {
			// if no complex boolean formula the original condition was empty
			cond_removed = having;
			having = NULL;
		}
		cond_removed = UnRef(cond_removed);
		// check if the extra condition was wrapped into trigger
		if(cond_removed->type() == Item::FUNC_ITEM && 
			((Item_func*)cond_removed)->functype() == Item_func::TRIG_COND_FUNC) {

			cond_removed = ((Item_func*)cond_removed)->arguments()[0];
			cond_removed = UnRef(cond_removed);
		}
		// check if the extra condition is a comparison
		if(cond_removed->type() != Item::FUNC_ITEM || ((Item_func*)cond_removed)->arg_count != 2)
			return false;
		// the right side of equality is the field of the original subselect
		if(dynamic_cast<Item_ref_null_helper*>(((Item_func*)cond_removed)->arguments()[1]) == NULL)
			return false;
		field_for_subselect = NULL;
	} else if(!having || (having->type()==Item::FUNC_ITEM
				&& (((Item_func*)having)->functype()==Item_func::ISNOTNULLTEST_FUNC
					|| ((Item_func*)having)->functype()==Item_func::TRIG_COND_FUNC))) {
		if(!conds) 
			return false;
		if(conds->type()==Item::COND_ITEM && ((Item_cond*)conds)->functype()==Item_func::COND_AND_FUNC) {
			// if the condition is a conjunctive formula
			// the extra condition should be in the last argument
			if(((Item_cond*) conds)->argument_list()->elements < 2) 
				return false;
			List_iterator<Item> li(*(((Item_cond*) conds)->argument_list()));
			while (li++ != NULL) cond_to_reinsert = *li.ref();
			li.rewind();
			while (*li.ref()!=cond_to_reinsert) li++;
			li.remove();
			list_to_reinsert = ((Item_cond*) conds)->argument_list();
			cond_removed = cond_to_reinsert;
		} else {
			// if no conjunctive formula the original condition was empty
			cond_removed = conds;
			conds = NULL;
		}
		if(cond_removed->type() == Item::FUNC_ITEM && 
			((Item_func*)cond_removed)->functype() == Item_func::TRIG_COND_FUNC) {
			// Condition was wrapped into trigger
			cond_removed = (Item_cond*) ((Item_func*)cond_removed)->arguments()[0];
		}
		if(cond_removed->type() == Item::COND_ITEM && 
			((Item_func*)cond_removed)->functype() == Item_func::COND_OR_FUNC) {
			// if the subselect field could have null values
			// equality condition was OR-ed with IS NULL condition
			Item_cond *cond_cond = (Item_cond*) cond_removed;
			List_iterator_fast<Item> li(*(cond_cond->argument_list()));
			cond_removed = li++;
			if(cond_removed==NULL) 
				return false;
			if(li++ == NULL) 
				return false;
			if(li++ != NULL) 
				return false;
			// the original having was empty
			having = NULL;
		}
		// check if the extra condition is a comparison
		if (cond_removed->type() != Item::FUNC_ITEM || ((Item_func*)cond_removed)->arg_count != 2)
			return false;
		// the right side of equality is the field of the original subselect
		field_for_subselect = ((Item_func*)cond_removed)->arguments()[1];
	} else 
		return false;
	// the left side of equality should be the left side of the original expression with subselect
	Item* left_ref = ((Item_func*)cond_removed)->arguments()[0];
	if(dynamic_cast<Item_int_with_ref*>(left_ref) != NULL)
		left_ref = ((Item_int_with_ref*)left_ref)->real_item();
	if(left_ref->type()!=Item::REF_ITEM
		|| ((Item_ref*)left_ref)->ref_type()!=Item_ref::DIRECT_REF
		|| ((Item_ref*)left_ref)->real_item()!=left_expr_for_subselect)
		return false;
	// set the operation type
	switch (((Item_func*)cond_removed)->functype()) {
		case Item_func::EQ_FUNC:
			oper_for_subselect = O_IN; /*Operator::O_IN;*/
			break;
		case Item_func::NE_FUNC:
			oper_for_subselect = O_NOT_EQ;
			break;
		case Item_func::LT_FUNC:
			oper_for_subselect = O_LESS;
			break;
		case Item_func::LE_FUNC:
			oper_for_subselect = O_LESS_EQ;
			break;
		case Item_func::GT_FUNC:
			oper_for_subselect = O_MORE;
			break;
		case Item_func::GE_FUNC:
			oper_for_subselect = O_MORE_EQ;
			break;
		default:
			return false;
	}
	return true;
#endif
}

int Query::PrefixCheck(COND* conds)
//used for verification if all tables involved in a condition were already seen
//used only in assert macro
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	conds = UnRef(conds);
	if(!conds)
		return RCBASE_QUERY_ROUTE;
	Item::Type cond_type = conds->type();
	CondID filter;
	switch(cond_type) {
		case Item::COND_ITEM: {
			Item_cond* cond_cond = (Item_cond*)conds;
			Item_func::Functype func_type = cond_cond->functype();
			switch(func_type) {
				case Item_func::COND_AND_FUNC:
				case Item_func::COND_OR_FUNC: {
		//			List_iterator_fast<Item_equal> list_equal(((Item_cond_and*)cond_cond)->cond_equal.current_level);
					List_iterator_fast<Item> li(*(cond_cond->argument_list()));
					Item* item;
					while((item = li++) /*|| (item = list_equal++)*/) {
						int ret = PrefixCheck(item);
						if(!ret)
							return RETURN_QUERY_TO_MYSQL_ROUTE;
						if(ret == TABLE_YET_UNSEEN_INVOLVED)
							return TABLE_YET_UNSEEN_INVOLVED;
						//for RETURN_TO_RCBASE_ROUTE the next item is evaluated
					}
					break;
				}
				case Item_func::COND_XOR_FUNC:   //we don't handle xor as yet
				default:
					return -1;    //unknown function type
			}
			break;
		}
		case Item::FUNC_ITEM: {
			Item_func* cond_func = (Item_func*)conds;
			Item_func::Functype func_type = cond_func->functype();
			switch (func_type) {
				case Item_func::BETWEEN:
				case Item_func::LIKE_FUNC:
				case Item_func::ISNULL_FUNC:
				case Item_func::ISNOTNULL_FUNC:
				case Item_func::IN_FUNC:
				case Item_func::EQ_FUNC:            // =
				case Item_func::NE_FUNC:            // <>
				case Item_func::LE_FUNC:            // <=
				case Item_func::GE_FUNC:            // >=
				case Item_func::GT_FUNC:            // >
				case Item_func::LT_FUNC: {          // <
					for(int i = 0; (unsigned)i < cond_func->arg_count; i++) {
						Item** args = cond_func->arguments();
						Item* an_arg = UnRef(args[i]);
						int ret = PrefixCheck(an_arg);
						if(!ret)
							return RETURN_QUERY_TO_MYSQL_ROUTE;
						if(ret == TABLE_YET_UNSEEN_INVOLVED)
							return TABLE_YET_UNSEEN_INVOLVED;
					}
					break;
				}
				case Item_func::MULT_EQUAL_FUNC: {
					Item_equal_iterator li(*(Item_equal *)conds);
					Item_field* ifield;
					while(ifield = li++) {
						int ret = PrefixCheck(ifield);
						if(!ret)
							return RETURN_QUERY_TO_MYSQL_ROUTE;
						if(ret == TABLE_YET_UNSEEN_INVOLVED)
							return TABLE_YET_UNSEEN_INVOLVED;
					}
					break;
				}
				default:
					return RETURN_QUERY_TO_MYSQL_ROUTE;   //unknown function type
			}
			break;
		}
		case Item::FIELD_ITEM:  // regular select
		case Item::SUM_FUNC_ITEM: { // min(k), max(k), count(), avg(k), sum
			const char* field_alias = NULL;
			const char* table_alias = NULL;
			const char* database_name = NULL;
			const char* table_name = NULL;
			const char* table_path = NULL;
			const TABLE* table_ptr = NULL;
			const char* field_name = NULL;
			if(!FieldUnmysterify(conds, database_name, table_name, table_alias, table_path, table_ptr, field_name, field_alias))
				return RETURN_QUERY_TO_MYSQL_ROUTE;
			BHASSERT(table_alias != EMPTY_TABLE_CONST_INDICATOR, "unexpected table alias");
			string ext_alias = string(table_name ? table_name : "") + string(":") + string(table_alias);
			if(table_alias2index_ptr.lower_bound(ext_alias) == table_alias2index_ptr.end())
				return TABLE_YET_UNSEEN_INVOLVED;
			else
				return RCBASE_QUERY_ROUTE;
			break;
		}
	}
	return RCBASE_QUERY_ROUTE;
#endif
}

//#include "sql_select.h"

int Query::BuildCondsIfPossible(COND* conds, CondID& cond_id, const TabID& tmp_table, JoinType join_type)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	conds = UnRef(conds);
	if(conds) {
		CondType filter_type = (join_type == JO_LEFT ? ON_LEFT_FILTER : (join_type == JO_RIGHT ? ON_RIGHT_FILTER : ON_INNER_FILTER));
		// in case of Right join MySQL changes order of tables. Right must be switched back to left!
		if(filter_type == ON_RIGHT_FILTER)
			filter_type = ON_LEFT_FILTER;
		assert(PrefixCheck(conds) != TABLE_YET_UNSEEN_INVOLVED && "Table not yet seen was involved in this condition");

		bool zero_result = conds->type() == Item::INT_ITEM && !conds->val_bool();
		if(!BuildConditions(conds, cond_id, cq, tmp_table, filter_type, zero_result, join_type))
			return RETURN_QUERY_TO_MYSQL_ROUTE;
		conds = 0;
	}
	return RCBASE_QUERY_ROUTE;
#endif
}
