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

#include "common/CommonDefinitions.h"
#include "edition/local.h"
#include "Query.h"
#include "RoughMultiIndex.h"
#include "vc/ConstColumn.h"
#include "vc/ConstExpressionColumn.h"
#include "vc/TypeCastColumn.h"
#include "vc/SingleColumn.h"
#include "vc/InSetColumn.h"
#include "MysqlExpression.h"
#include "compilation_tools.h"
#include "RCEngine.h"

using namespace std;

Query::Query(ConnectionInfo *conn_info)
	:	cq(NULL), t(NULL), no_tab(0), capacity(5), last_join(NULL), notifier(Notifier::E_Query, conn_info),
	 	use_IBExpressions(true), parallel_aggr(true), m_conn(conn_info)
{
	t = new RCTable*[capacity];
	no_tab = 0;
	rough_query = false;
	InitUserParams(conn_info);
}

Query::~Query()
{
	for(expressions_t::iterator it = gc_expressions.begin(), end =	gc_expressions.end(); it != end; ++it)
		delete *it;
	for(int i = (int)ta.size() - 1; i >= 0; i--)
		ta[i].reset();
	ta.clear();
	delete[] last_join;
	delete [] t;
}

void Query::Add(RCTable* tab)
{
	if(no_tab == capacity)
	{
		capacity *= 2;
		RCTable** new_t = new RCTable*[capacity];
		for(int i = 0; i < no_tab; i++)
			new_t[i] = t[i];
		delete[] t;
		t = new_t;
	}
	t[no_tab] = tab;
	no_tab++;
}

void Query::RemoveFromManagedList(const RCTablePtr tab)
{
	ta.erase(std::remove(ta.begin(), ta.end(), tab), ta.end());
}

RCTable* Query::Table(int table_num) const
{
	if (table_num < 0 || table_num >= no_tab) return NULL;
	return t[table_num];
}

void Query::LockPackInfoForUse()
{
	for (int i = 0; i < no_tab; i++)
		t[i]->LockPackInfoForUse();
};

void Query::UnlockPackInfoFromUse()
{
	for (int i = 0; i < no_tab; i++)
		t[i]->UnlockPackInfoFromUse();
};

bool Query::IsCountStar(Item* item_sum)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	Item_sum_count* is = dynamic_cast<Item_sum_count*>(item_sum);
	if (is)
		return ( is->sum_func() == Item_sum::COUNT_FUNC )
			&& ( ( is->args[0]->type() == Item::INT_ITEM)
					|| is->const_item()
					|| ( ( is->args[0]->type() == Item::FUNC_ITEM ) && ( is->args[0]->const_item() ) ) );
	else
		return false;
#endif
}

bool Query::IsAggregationItem(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return item->type() == Item::SUM_FUNC_ITEM;
#endif
}

bool Query::IsDeterministic(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	switch(static_cast<int>(item->type())) {
		case Item::FUNC_ITEM: {
			Item_func* ifunc = static_cast<Item_func*>(item);

			if((dynamic_cast<Item_func_rand*>(ifunc)  || dynamic_cast<Item_func_last_insert_id*>(ifunc)
					|| dynamic_cast<Item_func_get_system_var*>(ifunc) || dynamic_cast<Item_func_is_free_lock*>(ifunc)
					|| dynamic_cast<Item_func_is_used_lock*>(ifunc) || dynamic_cast<Item_func_row_count*>(ifunc)
					|| dynamic_cast<Item_func_sp*>(ifunc)
/*					// disputable functions start here - should they be nondeterministic?
					|| dynamic_cast<Item_func_weekday*>(ifunc)
					|| dynamic_cast<Item_func_unix_timestamp*>(ifunc)
					|| dynamic_cast<Item_func_time_to_sec*>(ifunc) ||dynamic_cast<Item_date*>(ifunc)
					|| dynamic_cast<Item_func_curtime*>(ifunc) ||dynamic_cast<Item_func_now*>(ifunc)
					|| dynamic_cast<Item_func_microsecond*>(ifunc) ||dynamic_cast<Item_func_last_day*>(ifunc)
*/					// end of disputable functions
			))
				return false;

			Item** args = ifunc->arguments();
			bool is_determ = true;
			for(uint i = 0; i < ifunc->argument_count(); i++)
				is_determ = is_determ && IsDeterministic(args[i]);
			return is_determ;
		}

		case Item::COND_ITEM: {
			Item_cond* cond = static_cast <Item_cond*> (item);
			List_iterator<Item> li(*cond->argument_list());
			Item* arg;
			bool is_determ = true;
			while((arg = li++))
				is_determ = is_determ && IsDeterministic(arg);
			return is_determ;
		}

		case Item::REF_ITEM: {
	    	Item_ref* iref = dynamic_cast<Item_ref*>(item);
	    	Item* arg = *(iref->ref);
    		return IsDeterministic(arg);
		}

		default:
			return true;
	}
#endif
}

bool Query::HasAggregation(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	bool has = false;
	Item* i = UnRef(item);
	if(i->type() == Item::SUM_FUNC_ITEM)
		has = true;
	else if(i->type() == Item::FUNC_ITEM) {
		Item_func* f = static_cast<Item_func*>(i);
		Item** args = f->arguments();
		int const arg_count(f->argument_count());
		for(int arg = 0; (arg < arg_count) && !has; ++ arg )
			has = HasAggregation(args[arg]);
	} else if(i->type() == Item::COND_ITEM) {
		Item_cond* c = static_cast<Item_cond*>(i);
		List<Item>* args = c->argument_list();
		List_iterator_fast<Item> li(*args);
		Item* item;
		while(!has && (item = li++))
			has = HasAggregation(item);
	} else if(i->type() == Item_bhfield::get_bhitem_type())
		if(static_cast<Item_bhfield*>(i)->IsAggregation())
			has = true;
	return has;
#endif
}

int Query::VirtualColumnAlreadyExists(const TabID& tmp_table, MysqlExpression* expression)
{
	int exists = NULL_VALUE_32;
	for(tab_id2expression_t::const_iterator it = tab_id2expression.lower_bound(tmp_table), end = tab_id2expression.upper_bound(tmp_table);
		it != end; ++it) {
		if(*(it->second.second) == *expression) {
			exists = it->second.first;
			break;
		}
	}
	return exists;
}

int Query::VirtualColumnAlreadyExists(const TabID& tmp_table, const TabID& subselect)
{
	int exists = NULL_VALUE_32;
	for(multimap<TabID, std::pair<int, TabID> >::const_iterator it = tab_id2subselect.lower_bound(tmp_table), end = tab_id2subselect.upper_bound(tmp_table);
		it != end; ++it) {

		if(it->second.second == subselect) {
			exists = it->second.first;
			break;
		}
	}
	return exists;
}

int Query::VirtualColumnAlreadyExists(const TabID& tmp_table, const vector<int>& vcs, const AttrID& at)
{
	int exists = NULL_VALUE_32;
	for(tab_id2inset_t::const_iterator it = tab_id2inset.lower_bound(tmp_table), end = tab_id2inset.upper_bound(tmp_table);
		it != end; ++it) {

		if(it->second.second.second.n == at.n) {
			std::set<int> s1, s2;
			s1.insert(it->second.second.first.begin(), it->second.second.first.end());
			s2.insert(vcs.begin(), vcs.end());
			if(s1 == s2) {
				exists = it->second.first;
				break;
			}
		}
	}
	return exists;
}


std::pair<int,int> Query::VirtualColumnAlreadyExists(const TabID& tmp_table, const TabID& tab, const AttrID& at)
{
	for(multimap<pair<int,int>, std::pair<int, int> >::const_iterator it = phys2virt.lower_bound(make_pair(tab.n, at.n)), end = phys2virt.upper_bound(make_pair(tab.n, at.n));
		it != end; ++it) {

			if(it->second.first == tmp_table.n) {
				return it->second;
			}
	}
	return std::make_pair(NULL_VALUE_32, NULL_VALUE_32);
}

bool Query::IsFieldItem(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return (item->type() == Item::FIELD_ITEM || item->type() == Item_bhfield::get_bhitem_type());
#endif

}

bool Query::IsAggregationOverFieldItem(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return IsAggregationItem(item) && IsFieldItem(((Item_sum*)item)->args[0]);
#endif
}

bool Query::IsConstItem(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	bool res = item->type() == Item::INT_ITEM ||
		item->type() == Item::NULL_ITEM ||
		item->type() == Item::INT_ITEM ||
		item->type() == Item::VARBIN_ITEM ||
		item->type() == Item::STRING_ITEM ||
		item->type() == Item::DECIMAL_ITEM ||
		item->type() == Item::REAL_ITEM;
	return res;
#endif
}

const string Query::GetItemName(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return string();
#else
//	static const char* nameOf[] = {
//		"FIELD_ITEM", "FUNC_ITEM", "SUM_FUNC_ITEM", "STRING_ITEM", "INT_ITEM",
//		"REAL_ITEM", "NULL_ITEM", "VARBIN_ITEM", "COPY_STR_ITEM", "FIELD_AVG_ITEM",
//		"DEFAULT_VALUE_ITEM", "PROC_ITEM", "COND_ITEM", "REF_ITEM", "FIELD_STD_ITEM",
//		"FIELD_VARIANCE_ITEM", "INSERT_VALUE_ITEM", "SUBSELECT_ITEM", "ROW_ITEM",
//		"CACHE_ITEM", "TYPE_HOLDER", "PARAM_ITEM", "TRIGGER_FIELD_ITEM", "DECIMAL_ITEM",
//		"XPATH_NODESET", "XPATH_NODESET_CMP", "VIEW_FIXER_ITEM" };
	static const char* sumNameOf[] =  {
			"COUNT_FUNC", "COUNT_DISTINCT_FUNC", "SUM_FUNC", "SUM_DISTINCT_FUNC", "AVG_FUNC",
			"AVG_DISTINCT_FUNC", "MIN_FUNC", "MAX_FUNC", "STD_FUNC",
			"VARIANCE_FUNC", "SUM_BIT_FUNC", "UDF_SUM_FUNC", "GROUP_CONCAT_FUNC" };
	char buf[256] = {0};
	switch(static_cast<int>(item->type())) {
		case Item::FUNC_ITEM: {
			Item_func* func = static_cast <Item_func*> (item);
			return func->func_name();
		}
		case Item::COND_ITEM: {
			Item_cond* cond = static_cast <Item_cond*> (item);
			return cond->func_name();
		}
		case Item::SUM_FUNC_ITEM: {
			Item_sum* sum_func = static_cast <Item_sum*> (item);
			uint index = sum_func->sum_func();
			if (index >= sizeof(sumNameOf)/sizeof(*sumNameOf))
				return "UNKNOWN SUM_FUNC_ITEM";
			else
				return sumNameOf[index];
		}
		case Item::REF_ITEM: {
			Item_ref* ref = static_cast<Item_ref*> (item);
			Item* real = ref->real_item();
			if(ref != real)
				return GetItemName(real);
			return "REF_ITEM";
		}
		case Item::NULL_ITEM:
			return "NULL";
		case Item::INT_ITEM: {
			Item_int_with_ref* int_ref = dynamic_cast<Item_int_with_ref*>(item);
			String s(buf, 256, NULL);
			if(!int_ref) {
				String* ps = item->val_str(&s);
				return ps ? ps->c_ptr_safe() : "NULL";
			}
			// else item is an instance of Item_int_with_ref, not Item_int
			return GetItemName(int_ref->real_item());
		}
		case Item::STRING_ITEM: {
			String s(buf, 256, NULL);
			String* ps = item->val_str(&s);
			return ps ? ps->c_ptr_safe() : "NULL";
		}
		case Item::SUBSELECT_ITEM:
			return "SUBSELECT";
		case Item::REAL_ITEM:
			return "REAL";
		case Item::DECIMAL_ITEM:
			return "DECIMAL";
		case Item_bhfield::BHFIELD_ITEM :
			Item_bhfield* bh = static_cast<Item_bhfield*>(item);
			int cur_var_id = bhitems_cur_var_ids[bh]++;
			if(cur_var_id >= bh->varID.size())
				cur_var_id = 0;
			sprintf(buf, "BH_FIELD(T:%d,A:%d)", bh->varID[cur_var_id].tab, bh->varID[cur_var_id].col);
			return buf;
		}
	return "UNKNOWN";
#endif
}

int Query::GetAddColumnId(const AttrID& vc, const TabID& tmp_table, const ColOperation oper, const bool distinct)
{
	for(int i = 0; i < cq->NoSteps(); i++) {
		CompiledQuery::CQStep* step = &cq->Step(i);
		if(step->type == CompiledQuery::ADD_COLUMN && step->t1 == tmp_table && step->e1.vc_id == vc.n && step->cop == oper && step->n1 == (distinct ? 1 : 0)) {
			return step->a1.n;
		}
	}
	return NULL_VALUE_32;
}

void Query::CQChangeAddColumnLIST2GROUP_BY(const TabID& tmp_table, int attr)
{
	for(int i = 0; i < cq->NoSteps(); i++) {
		CompiledQuery::CQStep* step = &cq->Step(i);
		if(step->type == CompiledQuery::ADD_COLUMN && step->t1 == tmp_table && step->a1.n == attr && step->cop == LISTING) {
			step->cop = GROUP_BY;
			cq->AddGroupByStep(*step);
			return;
		}
	}
}

void Query::MarkWithAny(Operator& op)
{
	switch(op) {
		case O_EQ:
			op = O_EQ_ANY;
			break;
		case O_NOT_EQ:
			op = O_NOT_EQ_ANY;
			break;
		case O_LESS:
			op = O_LESS_ANY;
			break;
		case O_MORE:
			op = O_MORE_ANY;
			break;
		case O_LESS_EQ:
			op = O_LESS_EQ_ANY;
			break;
		case O_MORE_EQ:
			op = O_MORE_EQ_ANY;
			break;
		default:
			// ANY can't be added to any other operator 
			break;
	}
}

void Query::MarkWithAll(Operator& op)
{
	switch(op) {
		case O_EQ:
			op = O_EQ_ALL;
			break;
		case O_NOT_EQ:
			op = O_NOT_EQ_ALL;
			break;
		case O_LESS:
			op = O_LESS_ALL;
			break;
		case O_MORE:
			op = O_MORE_ALL;
			break;
		case O_LESS_EQ:
			op = O_LESS_EQ_ALL;
			break;
		case O_MORE_EQ:
			op = O_MORE_EQ_ALL;
			break;
		default:
			// ALL can't be added to any other operator 
			break;
	}
}

bool Query::IsAllAny(Operator& op)
{
	return (op == O_EQ_ALL ||
		op == O_EQ_ANY ||
		op == O_NOT_EQ_ALL ||
		op == O_NOT_EQ_ANY ||
		op == O_LESS_ALL ||
		op == O_LESS_ANY ||
		op == O_MORE_ALL ||
		op == O_MORE_ANY ||
		op == O_LESS_EQ_ALL ||
		op == O_LESS_EQ_ANY ||
		op == O_MORE_EQ_ALL ||
		op == O_MORE_EQ_ANY);
}

void Query::UnmarkAllAny(Operator& op)
{
	switch(op) {
		case O_EQ_ALL:
		case O_EQ_ANY:
			op = O_EQ;
			break;
		case O_NOT_EQ_ALL:
		case O_NOT_EQ_ANY:
			op = O_NOT_EQ;
			break;
		case O_LESS_ALL:
		case O_LESS_ANY:
			op = O_LESS;
			break;
		case O_MORE_ALL:
		case O_MORE_ANY:
			op = O_MORE;
			break;
		case O_LESS_EQ_ALL:
		case O_LESS_EQ_ANY:
			op = O_LESS_EQ;
			break;
		case O_MORE_EQ_ALL:
		case O_MORE_EQ_ANY:
			op = O_MORE_EQ;
			break;
		default:
			// ALL/ANY can't be removed from any other operator 
			break;
	}
}

void Query::ExtractOperatorType(Item*& conds, Operator& op, bool& negative, char& like_esc)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	bool is_there_not;
	like_esc = '\\';
	conds = UnRef(conds);
	conds = FindOutAboutNot(conds, is_there_not);   //it UnRefs the conds if there is need

	Item_func* cond_func = (Item_func*)conds;
	switch(cond_func->functype()) {
		case Item_func::BETWEEN:
			op = is_there_not ? O_NOT_BETWEEN : O_BETWEEN;
			break;
		case Item_func::LIKE_FUNC:
			op = is_there_not ? O_NOT_LIKE : O_LIKE;
			like_esc = ((Item_func_like*)cond_func)->escape; 
			break;
		case Item_func::ISNULL_FUNC:
			op = O_IS_NULL;
			break;
		case Item_func::ISNOTNULL_FUNC:
			op = O_NOT_NULL;
			break;
		case Item_func::IN_FUNC:
			op = is_there_not ? O_NOT_IN : O_IN;
			break;
		case Item_func::EQ_FUNC:			// =
			op = negative ? O_NOT_EQ : O_EQ;
			break;
		case Item_func::NE_FUNC:			// <>
			op = negative ? O_EQ : O_NOT_EQ;
			break;
		case Item_func::LE_FUNC:            // <=
			op = negative ? O_MORE : O_LESS_EQ;
			break;
		case Item_func::GE_FUNC:            // >=
			op = negative ? O_LESS : O_MORE_EQ;
			break;
		case Item_func::GT_FUNC:            // >
			op = negative ? O_LESS_EQ : O_MORE;
			break;
		case Item_func::LT_FUNC:            // <
			op = negative ? O_MORE_EQ : O_LESS;
			break;
		case Item_func::MULT_EQUAL_FUNC:
			op = O_MULT_EQUAL_FUNC;
			break;
		case Item_func::NOT_FUNC:
			op = O_NOT_FUNC;
			break;
		case Item_func::NOT_ALL_FUNC: {
			Item_func* cond_func = (Item_func*)conds;
			negative = dynamic_cast<Item_func_nop_all*>(cond_func) == NULL;
			ExtractOperatorType(cond_func->arguments()[0], op, negative, like_esc);
			if(dynamic_cast<Item_func_nop_all*>(cond_func))
				MarkWithAny(op);
			else if(dynamic_cast<Item_func_not_all*>(cond_func))
				MarkWithAll(op);
			conds = cond_func->arguments()[0];
			break;
		}
		case Item_func::UNKNOWN_FUNC:
			op = O_UNKNOWN_FUNC;
			break;
		default:
			op = O_ERROR;   //unknown function type
			break;
	}
#endif
}

VirtualColumn* Query::CreateColumnFromExpression(Query::expressions_t const& exprs, TempTable* temp_table,
		int temp_table_alias, MultiIndex* mind)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return NULL;
#else
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(exprs.size() > 0);
	VirtualColumn* vc = NULL;
	if(exprs.size() == 1) {
		bool is_const_expr = VirtualColumn::IsConstExpression(exprs[0], temp_table_alias, &temp_table->GetAliases());
		if(exprs[0]->IsDeterministic() && (exprs[0]->GetVars().size() == 0)) {
			ColumnType type(exprs[0]->EvalType());
			vc = new ConstColumn(exprs[0]->Evaluate(), type, true);
		} else if(is_const_expr && exprs[0]->IsDeterministic() && (!exprs[0]->IsIBExpression() || !use_IBExpressions) ){		
			if(IsFieldItem(exprs[0]->GetItem())) {
				// a special case when a naked column is a parameter
				// without this column type would be a seen by mysql, not IB.
				// e.g. timestamp would be string 19
				TabID tab;
				AttrID col;
				tab.n = exprs[0]->GetVars().begin()->tab;
				col.n = exprs[0]->GetVars().begin()->col;
				col.n = col.n < 0 ? -col.n -1 : col.n;
				ColumnType ct = ta[-tab.n-1]->GetColumnType(col.n);
				vc = new ConstExpressionColumn(exprs[0], ct, temp_table, temp_table_alias, mind);
			} else
				vc = new ConstExpressionColumn(exprs[0], temp_table, temp_table_alias, mind);
		} else if(use_IBExpressions && exprs[0]->IsIBExpression()) {
			vc = BuildIBExpression(exprs[0]->GetItem(), ColumnType(exprs[0]->EvalType()), temp_table, 
				temp_table_alias, mind, false, true);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(vc);
		} else {
			if(rccontrol.isOn()) {
				if(static_cast<int>(exprs[0]->GetItem()->type()) == Item::FUNC_ITEM) {
					Item_func* ifunc = static_cast<Item_func*>(exprs[0]->GetItem());
					rccontrol.lock(mind->m_conn->GetThreadID()) << "Unoptimized expression near '" << ifunc->func_name() << "'" << unlock;
				}
			}
			vc = new ExpressionColumn(exprs[0], temp_table, temp_table_alias, mind);
			if(static_cast<ExpressionColumn*>(vc)->GetStringType() ==  MysqlExpression::STRING_TIME && vc->TypeName() != RC_TIME) { //RC_TIME is already as int64
				TypeCastColumn* tcc = new String2DateTimeCastColumn(vc, ColumnType(RC_TIME));				
				temp_table->AddVirtColumn(vc);
				vc = tcc;
			}
		}
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(0);
	}
	MysqlExpression::SetOfVars params = vc->GetParams();
	MysqlExpression::TypOfVars types;
	for(MysqlExpression::SetOfVars::iterator iter = params.begin(); iter != params.end(); iter++) {
		types[*iter] = ta[-(*iter).tab-1]->GetColumnType((*iter).col < 0 ? -(*iter).col-1 : (*iter).col);
	}
	vc->SetParamTypes(&types);
	return vc;
#endif
}

bool Query::IsConstExpr(MysqlExpression::SetOfVars& sv, const TabID& t)
{
	bool res = false;
	for(MysqlExpression::SetOfVars::iterator iter = sv.begin(); iter != sv.end(); iter++) {
		res |= cq->ExistsInTempTable(TabID(iter->tab), t);
	}
	return !res;
}
 
bool Query::IsParameterFromWhere(const TabID& params_table)
{
	for(uint i = 0; i < subqueries_in_where.size(); i++) {
		if(subqueries_in_where[i].first == params_table) 
			return subqueries_in_where[i].second;
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Subquery not properly placed on compilation stack");
	return true;
}

const char* Query::GetTableName(Item_field* ifield)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	char* table_name = NULL;
	if(ifield->cached_table && !ifield->cached_table->view && !ifield->cached_table->derived)
		if(ifield->cached_table->referencing_view)
			table_name = ifield->cached_table->referencing_view->table_name;
		else
			table_name = ifield->cached_table->table_name;
	else if(ifield->result_field->table && ifield->result_field->table->s->table_category != TABLE_CATEGORY_TEMPORARY)
		table_name = ifield->result_field->table->s->table_name.str;
	return table_name;
#endif
}

bool Query::SkipFix2FixConv(Item_func* item_func, ColumnType arg_type)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	bool is_arithm_op = (strcmp(item_func->func_name(), "+") == 0 ||
		strcmp(item_func->func_name(), "-") == 0 ||
		strcmp(item_func->func_name(), "*") == 0 ||
		strcmp(item_func->func_name(), "/") == 0 ||
		strcmp(item_func->func_name(), "%") == 0);
	if(is_arithm_op && item_func->result_type() == DECIMAL_RESULT && arg_type.IsFixed())
		return true;
	return false;
#endif
}

bool Query::IsLogicOperator(Item_func* item_func)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#else
	return (strcmp(item_func->func_name(), "<") == 0 ||
		strcmp(item_func->func_name(), ">") == 0 ||
		strcmp(item_func->func_name(), "<=") == 0 ||
		strcmp(item_func->func_name(), ">=") == 0 ||
		strcmp(item_func->func_name(), "<>") == 0 ||
		strcmp(item_func->func_name(), "=") == 0 ||
		strcmp(item_func->func_name(), " IN ") == 0 ||
		strcmp(item_func->func_name(), "between") == 0);
#endif
}

void Query::GetPrecisionScale(Item* item, int& precision, int& scale, bool max_scale)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return;
#else
	precision = item->decimal_precision();
	scale = item->decimals;
	//if(precision > 19) {
	//	int integers = (precision - scale);
	//	precision = 19;
	//	scale = integers > 19 ? 0 :  19 - integers;
	//}
	if(scale > 18) {
		precision -= (scale - 18);
		scale = 18;
	}
	if(precision > 18)
		precision = 18;	

	Item_func* item_func = dynamic_cast<Item_func*>(item);
	if(max_scale && precision < 18 && item_func && strcmp(item_func->func_name(), "/") == 0) {
		scale += 18 - precision;
		if(scale > 15)
			scale = 15;
		precision = 18;
	}
#endif
}

ColumnType Query::ItemResult2ColumnType(Item* item)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return ColumnType(RC_BIGINT);
#else
	switch(item->result_type()) {
			case INT_RESULT:				
				return ColumnType(RC_BIGINT, AS_MISSED, false, 18,0);
			case REAL_RESULT:
				return ColumnType(RC_REAL, AS_MISSED, false, 18);
			case DECIMAL_RESULT: {
				int precision, scale;
				GetPrecisionScale(item, precision, scale);
				return ColumnType(RC_NUM, AS_MISSED, false, precision, scale);
								 } 
			case STRING_RESULT:
				if(item->field_type() == MYSQL_TYPE_TIMESTAMP)
					return ColumnType(RC_TIMESTAMP);
				if(item->field_type() == MYSQL_TYPE_TIME)
					return ColumnType(RC_TIME);
				if(item->field_type() == MYSQL_TYPE_DATE)
					return ColumnType(RC_DATE);
				if(item->field_type() == MYSQL_TYPE_DATETIME)
					return ColumnType(RC_DATETIME);
				return ColumnType(RC_VARCHAR, AS_MISSED, false, item->max_length, 0, item->collation);
	}
	return ColumnType(RC_BIGINT);
#endif
}
