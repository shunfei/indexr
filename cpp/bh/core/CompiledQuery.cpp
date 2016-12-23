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

#include "CompiledQuery.h"

#include "Query.h"
#include "MysqlExpression.h"
#include "vc/MultiValColumn.h"

using namespace std;

CompiledQuery::CompiledQuery()
{
	no_tabs = 0;
	no_conds = 0;
}

CompiledQuery::CompiledQuery(CompiledQuery const & cq)
{
	this->steps = cq.steps;

	no_tabs = cq.no_tabs;
	no_attrs = cq.no_attrs;
	no_conds = cq.no_conds;
	no_virt_cols = cq.no_virt_cols;
	columns_per_table = cq.columns_per_table;
	id2aliases = cq.id2aliases;
	steps_tmp_tables = cq.steps_tmp_tables;
	steps_group_by_cols = cq.steps_group_by_cols;
	TabIDSteps = cq.TabIDSteps;
}

CompiledQuery::~CompiledQuery()
{
	steps.clear();
}

CompiledQuery& CompiledQuery::operator=(CompiledQuery const &cq)
{
	this->steps = cq.steps;

	no_tabs = cq.no_tabs;
	no_attrs = cq.no_attrs;
	no_conds = cq.no_conds;
	no_virt_cols = cq.no_virt_cols;
	columns_per_table = cq.columns_per_table;
	id2aliases = cq.id2aliases;
	steps_tmp_tables = cq.steps_tmp_tables;
	steps_group_by_cols = cq.steps_group_by_cols;
	TabIDSteps = cq.TabIDSteps;
	return *this;
}

CompiledQuery::CQStep::CQStep(const CompiledQuery::CQStep & s)
	: type( s.type ),
	t1( s.t1 ),
	t2( s.t2 ),
	t3( s.t3 ),
	a1( s.a1 ),
	a2( s.a2 ),
	c1( s.c1 ),
	c2( s.c2 ),
	c3( s.c3 ),
	e1( s.e1 ),
	e2( s.e2 ),
	e3( s.e3 ),
	op( s.op ),
	tmpar( s.tmpar ),
	jt( s.jt ),
	cop( s.cop ),
	alias( NULL ),
	mysql_expr( s.mysql_expr ),
	virt_cols(s.virt_cols),
	tables1(s.tables1),
	tables2(s.tables2),
	n1( s.n1 ),
	n2( s.n2 )
{

	if(s.alias) {
		size_t alias_ct(strlen(s.alias)+1);
		alias = new char[alias_ct];
		strcpy(alias, s.alias);
	}
	else
		alias = NULL;
}

CompiledQuery::CQStep & CompiledQuery::CQStep::operator=(const CompiledQuery::CQStep & s)
{
	if(&s != this) {
		CQStep tmp_step(s);
		swap(tmp_step);
	}
	return *this;
}
void CompiledQuery::CQStep::swap(CQStep& s) {
	if(&s != this) {
		using std::swap;
		swap(type, s.type);
		swap(t1, s.t1);
		swap(t2, s.t2);
		swap(t3, s.t3);
		swap(a1, s.a1);
		swap(a2, s.a2);
		swap(c1, s.c1);
		swap(c2, s.c2);
		swap(c3, s.c3);
		swap(e1, s.e1);
		swap(e2, s.e2);
		swap(e3, s.e3);
		swap(op, s.op);
		swap(tmpar, s.tmpar);
		swap(jt, s.jt);
		swap(cop, s.cop);
		swap(n1, s.n1);
		swap(n2, s.n2);
		swap(mysql_expr, s.mysql_expr);
		swap(virt_cols, s.virt_cols);
		swap(tables1, s.tables1);
		swap(tables2, s.tables2);
		swap(alias, s.alias);
	}
}

void CompiledQuery::CQStep::Print(Query* query)
{
	char buf[512] = "";
	char b1[100], b2[100], b3[100];
	b1[0] = b2[0] = b3[0] = '\0';

	char b_op[20];
	switch(op) {
		case O_EQ:
			strcpy(b_op,"=");
			break;
		case O_EQ_ALL:
			strcpy(b_op,"=ALL");
			break;
		case O_EQ_ANY:
			strcpy(b_op,"=ANY");
			break;
		case O_NOT_EQ:
			strcpy(b_op,"<>");
			break;
		case O_NOT_EQ_ALL:
			strcpy(b_op,"<>ALL");
			break;
		case O_NOT_EQ_ANY:
			strcpy(b_op,"<>ANY");
			break;
		case O_LESS:
			strcpy(b_op,"<");
			break;
		case O_LESS_ALL:
			strcpy(b_op,"<ALL");
			break;
		case O_LESS_ANY:
			strcpy(b_op,"<ANY");
			break;
		case O_MORE:
			strcpy(b_op,">");
			break;
		case O_MORE_ALL:
			strcpy(b_op,">ALL");
			break;
		case O_MORE_ANY:
			strcpy(b_op,">ANY");
			break;
		case O_LESS_EQ:
			strcpy(b_op,"<=");
			break;
		case O_LESS_EQ_ALL:
			strcpy(b_op,"<=ALL");
			break;
		case O_LESS_EQ_ANY:
			strcpy(b_op,"<=ANY");
			break;
		case O_MORE_EQ:
			strcpy(b_op,">=");
			break;
		case O_MORE_EQ_ALL:
			strcpy(b_op,">=ALL");
			break;
		case O_MORE_EQ_ANY:
			strcpy(b_op,">=ANY");
			break;
		case O_IS_NULL:
			strcpy(b_op,"IS NULL");
			break;
		case O_NOT_NULL:
			strcpy(b_op,"IS NOT NULL");
			break;
		case O_BETWEEN:
			strcpy(b_op,"BETWEEN");
			break;
		case O_IN:
			strcpy(b_op,"IN");
			break;
		case O_LIKE:
			strcpy(b_op,"LIKE");
			break;
		case O_ESCAPE:
			strcpy(b_op,"ESCAPE");
			break;
		case O_EXISTS:
			strcpy(b_op,"EXISTS");
			break;
		case O_NOT_LIKE:
			strcpy(b_op,"NOT LIKE");
			break;
		case O_NOT_BETWEEN:
			strcpy(b_op,"NOT BETWEEN");
			break;
		case O_NOT_IN:
			strcpy(b_op,"NOT IN");
			break;
		case O_NOT_EXISTS:
			strcpy(b_op,"NOT EXISTS");
			break;
		case O_FALSE:
			strcpy(b_op, "FALSE");
			break;
		case O_TRUE:
			strcpy(b_op,"TRUE");
			break;
		default:
			strcpy(b_op,"?");
	}

	char b_cop[20]; // enum ColOperation {LIST, COUNT, COUNT_DISTINCT, SUM, MIN, MAX, AVG};

	switch(cop) {
		case LISTING:
			strcpy(b_cop, "LIST");
			break;
		case COUNT:
			strcpy(b_cop, "COUNT");
			break;
		case SUM:
			strcpy(b_cop, "SUM");
			break;
		case MIN:
			strcpy(b_cop, "MIN");
			break;
		case MAX:
			strcpy(b_cop, "MAX");
			break;
		case AVG:
			strcpy(b_cop, "AVG");
			break;
		case STD_POP:
			strcpy(b_cop, "STD_POP");
			break;
		case STD_SAMP:
			strcpy(b_cop, "STD_SAMP");
			break;
		case VAR_POP:
			strcpy(b_cop, "VAR_POP");
			break;
		case VAR_SAMP:
			strcpy(b_cop, "VAR_SAMP");
			break;
		case BIT_AND:
			strcpy(b_cop, "BIT_AND");
			break;
		case BIT_OR:
			strcpy(b_cop, "BIT_OR");
			break;
		case BIT_XOR:
			strcpy(b_cop, "BIT_XOR");
			break;
		case GROUP_CONCAT:
			strcpy(b_cop, "GROUP_CONCAT");
			break;
		case GROUP_BY:
			strcpy(b_cop,"GROUP_BY");
			break;
		case DELAYED:
			strcpy(b_cop,"DELAYED");
			break;
		default:
			strcpy(b_cop,"[no name yet]");
	}

	char b_tmpar[20]; // enum TMParameter	{	TM_DISTINCT, TM_TOP, TM_EXISTS, TM_COUNT }; // Table Mode

	switch(tmpar) {
		case TM_DISTINCT:
			strcpy(b_tmpar,"DISTINCT");
			break;
		case TM_TOP:
			strcpy(b_tmpar,"LIMIT");
			break;
		case TM_EXISTS:
			strcpy(b_tmpar,"EXISTS");
			break;
		default:
			strcpy(b_tmpar,"???");
	}

	char b_jt[20]; // enum JoinType		{	JO_INNER, JO_LEFT, JO_RIGHT, JO_FULL };

	switch(jt) {
		case JO_INNER:
			strcpy(b_jt,"INNER");
			break;
		case JO_LEFT:
			strcpy(b_jt,"LEFT");
			break;
		case JO_RIGHT:
			strcpy(b_jt,"RIGHT");
			break;
		case JO_FULL:
			strcpy(b_jt,"FULL");
			break;
		default:
			strcpy(b_jt,"????");
	}

	switch(type) {
		case TABLE_ALIAS:
			if(alias)
				sprintf(buf,"T:%d = TABLE_ALIAS(T:%d,\"%s\")", N(t1.n), N(t2.n), alias);
			else
				sprintf(buf,"T:%d = TABLE_ALIAS(T:%d)", N(t1.n), N(t2.n));
			break;
		case CREATE_CONDS:
			if(c2.IsNull()) {
				sprintf(buf,"C:%d = CREATE_%sCONDS(T:%d,%s,%s,%s,%s)", N(c1.n),
					n1 == OR_SUBTREE ? "TREE_" : "",
					N(t1.n), 
					e1.ToString(b1, _countof(b1), t1.n),
					b_op,
					e2.ToString(b2, _countof(b2), t1.n),
					e3.ToString(b3, _countof(b3), t1.n)
					//n1 == 0 ? "WHERE" : (n1 == 1 ? "HAVING" : (n1 == 2 ? "ON INNER" : (n1==3 ? "ON LEFT" : "ON RIGHT"))),
					//n2
					);
			} else
				sprintf(buf,"C:%d = CREATE_CONDS(T:%d, C:%d)", N(c1.n), N(t1.n), N(c2.n));
			break;
		case AND_F:
			sprintf(buf,"C:%d.AND(C:%d)", N(c1.n), N(c2.n));
			break;
		case OR_F:
			sprintf(buf,"C:%d.OR(C:%d)", N(c1.n), N(c2.n));
			break;
		//case NOT_F:
		//	sprintf(buf,"F:%d = NOT(F:%d)",N(f2.n),N(f1.n));
		//	break;
		//case COPY_F:
		//	sprintf(buf,"C:%d = COPY(C:%d)", N(c2.n), N(c1.n));
		//	break;
		case AND_DESC:
			sprintf(buf,"C:%d.AND(%s,%s,%s,%s)",
				N(c1.n),
				e1.ToString(b1, _countof(b1), t1.n),
				b_op,
				e2.ToString(b2, _countof(b2), t1.n),
				e3.ToString(b3, _countof(b3), t1.n)
				);
			break;
		case OR_DESC:
			sprintf(buf,"C:%d.OR(%s,%s,%s,%s)",
				N(c1.n),
				e1.ToString(b1, _countof(b1), t1.n),
				b_op,
				e2.ToString(b2, _countof(b2), t1.n),
				e3.ToString(b3, _countof(b3), t1.n)
				);
			break;
		case TMP_TABLE: {
			sprintf(buf, "T:%d = TMP_TABLE(", N(t1.n));
			int i = 0;
			for(; i < tables1.size() - 1; i++)
				sprintf(buf + strlen(buf), "T:%d,", N(tables1[i].n));
			sprintf(buf + strlen(buf), "T:%d)", N(tables1[i].n));
			break;
		}
		case CREATE_VC:
			if(mysql_expr.size() == 1) {
				char s1[200];
				strncpy(s1, query->GetItemName(mysql_expr[0]->GetItem()).c_str(), 199);
				s1[199] = '\0';
				sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,EXPR(\"%s\"))", N(t1.n), N(a1.n), N(t1.n), s1);
			} else if(mysql_expr.size() > 1) {
				char s1[200], s2[200];
				strncpy(s1, query->GetItemName(mysql_expr[0]->GetItem()).c_str(), 199);
				strncpy(s2, query->GetItemName(mysql_expr[mysql_expr.size() - 1]->GetItem()).c_str(), 199);
				s1[199] = '\0';
				s2[199] = '\0';
				sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,EXPR([%s,..,%s](%ld items)))", N(t1.n), N(a1.n), N(t1.n), s1, s2, mysql_expr.size());
			} else if(virt_cols.size() > 1) {
				sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,VIRT_COLS([%d,..,%d](%ld items)))", N(t1.n), N(a1.n), N(t1.n), virt_cols[0], virt_cols[virt_cols.size() - 1], virt_cols.size());
			} else if(a2.n != NULL_VALUE_32)
				sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,PHYS_COL(T:%d,A:%d))", N(t1.n), N(a1.n), N(t1.n), N(t2.n), N(a2.n));
			else
				sprintf(buf, "VC:%d.%d = CREATE_VC(T:%d,SUBQUERY(T:%d))", N(t1.n), N(a1.n), N(t1.n), N(t2.n));
			break;
		case T_MODE:
			sprintf(buf,"T:%d.MODE(%s,%lld,%lld)", N(t1.n), b_tmpar, n1, n2);
			break;
		case JOIN_T:
			// This step exists but should not be displayed
			return;
		case LEFT_JOIN_ON: {
			sprintf(buf, "T:%d.LEFT_JOIN_ON({", N(t1.n));
			int i = 0;
			for(; i < (int)tables1.size() - 1; i++)
				sprintf(buf + strlen(buf), "T:%d,", N(tables1[i].n));
			sprintf(buf + strlen(buf), "T:%d},{", N(tables1[i].n));
			i = 0;
			for(; i < (int)tables2.size() - 1; i++)
				sprintf(buf + strlen(buf), "T:%d,", N(tables2[i].n));
			sprintf(buf + strlen(buf), "T:%d},C:%d)", N(tables2[i].n), N(c1.n));
			break;
		}
		case INNER_JOIN_ON: {
			sprintf(buf, "T:%d.INNER_JOIN_ON({", N(t1.n));
			int i = 0;
			for(; i < tables1.size() - 1; i++)
				sprintf(buf + strlen(buf), "T:%d,", N(tables1[i].n));
			sprintf(buf + strlen(buf), "T:%d},C:%d)", N(tables1[i].n), N(c1.n));
			break;
		}
		case ADD_CONDS:
			sprintf(buf,"T:%d.ADD_CONDS(C:%d,%s)", N(t1.n), N(c1.n), n1 == 0 ? "WHERE" : (n1 == 1 ? "HAVING" : "?!?"));
			break;
		case APPLY_CONDS:
			sprintf(buf,"T:%d.APPLY_CONDS()", N(t1.n));
			break;
		case ADD_COLUMN:
			sprintf(buf,"A:%d = T:%d.ADD_COLUMN(%s,%s,\"%s\",\"%s\")", N(a1.n), N(t1.n),
					e1.ToString(b1, _countof(b1), t1.n), b_cop, (alias) ? alias : "null", n1 ? "DISTINCT" : "ALL");
			break;
		case ADD_ORDER:
			sprintf(buf, "T:%d.ADD_ORDER(VC:%d.%d,%s)", N(t1.n), N(t1.n), N(a1.n), n1 ? "DESC" : "ASC");
			break;
		case UNION:
			sprintf(buf, "T:%d = UNION(T:%d,T:%d,%lld)", N(t1.n), N(t2.n), N(t3.n), n1);
			break;
		case RESULT:
			sprintf(buf, "RESULT(T:%d)", N(t1.n));
			break;
		default:
			sprintf(buf, "Unsupported type of CQStep: %d", type);
	}
	rccontrol << buf << endl;
}

void CompiledQuery::TableAlias(TabID& t_out, const TabID& n, const char *name, int id)
{
	CompiledQuery::CQStep s;
	s.type = TABLE_ALIAS;
	s.t1 = t_out = NextTabID();
	s.t2 = n;
	if(name) {
		s.alias = new char [strlen(name) + 1];
		strcpy(s.alias, name);
		id2aliases[id].push_back(s.t1.n) ;
	}
	steps.push_back(s);
}

void CompiledQuery::TmpTable(TabID& t_out, const TabID& t1, bool for_subq_in_where)
{
	CompiledQuery::CQStep s;
	if(for_subq_in_where)
		s.n1 = 1;
	else
		s.n1 = 0;
	assert(t1.n < 0 && NoTabs() > 0);
	s.type = TMP_TABLE;
	s.t1 = t_out = NextTabID(); // was s.t2!!!
	s.tables1.push_back(t1);
	steps_tmp_tables.push_back(s);
	steps.push_back(s);
}

void CompiledQuery::CreateConds(CondID &c_out, const TabID& t1, CQTerm e1, Operator pr, CQTerm e2, CQTerm e3, bool is_or_subtree, char like_esc)
{
	CompiledQuery::CQStep s;
	s.type = CREATE_CONDS;
	s.c1 = c_out = NextCondID();
	s.t1 = t1;
	s.e1 = e1;
	s.op = pr;
	s.e2 = e2;
	s.e3 = e3;
	s.n1 = is_or_subtree ? OR_SUBTREE : 0;
	s.n2 = like_esc;
	steps.push_back(s);
}

void CompiledQuery::CreateConds(CondID &c_out, const TabID& t1, const CondID& c1, bool is_or_subtree)
{
	CompiledQuery::CQStep s;
	s.type = CREATE_CONDS;
	s.c2 = c1;
	s.c1 = c_out = NextCondID();
	s.t1 = t1;
	s.n1 = is_or_subtree ? OR_SUBTREE : 0;
	steps.push_back(s);
}

void CompiledQuery::And(const CondID& c1, const TabID& t, const CondID& c2)
{
	if(c1.IsNull()) {
		return;
	}
	CompiledQuery::CQStep s;
	s.type = AND_F;
	s.c1 = c1;
	s.t1 = t;
	s.c2 = c2;
	steps.push_back(s);
}

void CompiledQuery::Or(const CondID& c1, const TabID& t, const CondID& c2)
{
	if(c1.IsNull()) {
		return;
	}
	CompiledQuery::CQStep s;
	s.type = OR_F;
	s.c1 = c1;
	s.t1 = t;
	s.c2 = c2;
	steps.push_back(s);
}

void CompiledQuery::And(const CondID& c1, const TabID& t, CQTerm e1, Operator pr, CQTerm e2, CQTerm e3)
{
	CompiledQuery::CQStep s;
	s.type = AND_DESC;
	s.t1 = t;
	s.c1 = c1;
	s.e1 = e1;
	s.op = pr;
	s.e2 = e2;
	s.e3 = e3;
	steps.push_back(s);
}

void CompiledQuery::Or(const CondID& c1, const TabID& t, CQTerm e1, Operator pr, CQTerm e2, CQTerm e3)
{
	CompiledQuery::CQStep s;
	s.type = OR_DESC;
	s.t1 = t;
	s.c1 = c1;
	s.e1 = e1;
	s.op = pr;
	s.e2 = e2;
	s.e3 = e3;
	steps.push_back(s);
}

void CompiledQuery::Mode(const TabID& t1, TMParameter mode, _int64 n1, _int64 n2)
{
	CompiledQuery::CQStep s;
	if(s.t1.n >= 0) {
		size_t const alias_ct(100);
		s.type = STEP_ERROR;
		s.alias = new char [alias_ct];
		strcpy(s.alias,  "T_MODE: can't be applied to RCTable");
		return;
	}
	s.type = T_MODE;
	s.t1 = t1;
	s.tmpar = mode;
	s.n1 = n1;
	s.n2 = n2;
	steps.push_back(s);
}

void CompiledQuery::Join(const TabID& t1, const TabID& t2)
{
	for(int i = 0; i < steps.size(); i++)
		if(steps[i].type == TMP_TABLE && steps[i].t1 == t1) {
			steps[i].tables1.push_back(t2);
			for(int j = 0; j < steps_tmp_tables.size(); j++) {
				if (steps_tmp_tables[j].t1 == t1) {
					steps_tmp_tables[j].tables1.push_back(t2);
					break;
				}
			}
			break;
		}
	CompiledQuery::CQStep s;
	s.type = JOIN_T;
	s.t1 = t1;
	s.t2 = t2;
	steps.push_back(s);
}

void CompiledQuery::LeftJoinOn(const TabID& temp_table, vector<TabID>& left_tables, vector<TabID>& right_tables, const CondID& cond_id)
{
	CompiledQuery::CQStep s;
	s.type = LEFT_JOIN_ON;
	s.t1 = temp_table;
	s.c1 = cond_id;
	s.tables1 = left_tables;
	s.tables2 = right_tables;
	steps.push_back(s);
}

void CompiledQuery::InnerJoinOn(const TabID& temp_table, vector<TabID>& left_tables, vector<TabID>& right_tables, const CondID& cond_id)
{
	CompiledQuery::CQStep s;
	s.type = INNER_JOIN_ON;
	s.t1 = temp_table;
	s.c1 = cond_id;
	s.tables1 = left_tables;
	s.tables1.insert(s.tables1.end(), right_tables.begin(), right_tables.end());
	steps.push_back(s);
}

void CompiledQuery::AddConds(const TabID& t1, const CondID& c1, CondType cond_type)
{
	if(c1.IsNull())
		return;
	CompiledQuery::CQStep s;
	s.type = ADD_CONDS;
	s.t1 = t1;
	s.c1 = c1;
	s.n1 = cond_type;
	steps.push_back(s);
}

void CompiledQuery::ApplyConds(const TabID& t1)
{
	CompiledQuery::CQStep s;
	s.type = APPLY_CONDS;
	s.t1 = t1;
	steps.push_back(s);
}

void CompiledQuery::AddColumn(AttrID &a_out, const TabID& t1, CQTerm e1, ColOperation op, char const alias[], bool distinct)
{
	assert(t1.n < 0 && NoTabs() > 0);
	CompiledQuery::CQStep s;
	s.type = ADD_COLUMN;
	s.a1 = a_out = NextAttrID(t1);
	s.t1 = t1;
	s.e1 = e1;
	s.cop = op;
	if(alias) {
		size_t const alias_ct(strlen(alias) + 1);
		s.alias = new char [alias_ct];
		strcpy(s.alias,  alias);
	} else
		s.alias = NULL;
	s.n1 = distinct ? 1 : 0;
	steps.push_back(s);
	if (op == GROUP_BY)
		steps_group_by_cols.push_back(s);
}


void CompiledQuery::CreateVirtualColumn(AttrID &a_out, const TabID& t1, MysqlExpression* expr, const TabID& src_tab)
{
	assert(t1.n < 0 && NoTabs() > 0);
	CompiledQuery::CQStep s;
	s.type = CREATE_VC;
	s.a1 = a_out = NextVCID(t1);
	s.t1 = t1;
	s.t2 = src_tab;
	s.mysql_expr.push_back(expr);
	steps.push_back(s);
	MysqlExpression::SetOfVars& sov = expr->GetVars();
	for(MysqlExpression::SetOfVars::const_iterator v = sov.begin(); v != sov.end(); ++v)
		columns_per_table[(*v).tab].push_back((*v).col);
}

void CompiledQuery::CreateVirtualColumn(AttrID &a_out, const TabID& t1, const TabID& subquery, bool on_result)
{
	assert(t1.n < 0 && NoTabs() > 0);
	CompiledQuery::CQStep s;
	s.type = CREATE_VC;
	s.a1 = a_out = NextVCID(t1);
	s.t1 = t1;
	s.t2 = subquery;
	s.n1 = on_result ? 1 : 0;
	steps.push_back(s);
}

void CompiledQuery::CreateVirtualColumn(AttrID &a_out, const TabID& t1, vector<int>& vcs, const AttrID& vc_id)
{
	assert(t1.n < 0 && NoTabs() > 0);
	CompiledQuery::CQStep s;
	s.type = CREATE_VC;
	s.a1 = a_out = NextVCID(t1);
	s.a2 = vc_id;
	s.t1 = t1;
	s.virt_cols = vcs;
	steps.push_back(s);
}

void CompiledQuery::CreateVirtualColumn(int &a_out, const TabID& t1, const TabID& table_alias, const AttrID& col_number)
{
	assert(t1.n < 0 && NoTabs() > 0);
	CompiledQuery::CQStep s;
	s.type = CREATE_VC;
	s.a1 = NextVCID(t1);
	a_out = s.a1.n;
	s.a2 = col_number;
	s.t1 = t1;
	s.t2 = table_alias;
	steps.push_back(s);
	columns_per_table[table_alias.n].push_back(col_number.n);
}

void CompiledQuery::Add_Order(const TabID& t1, const AttrID& vc, int d)			// d=1 for descending
{
	CompiledQuery::CQStep s;
	s.type = ADD_ORDER;
	s.t1 = t1;
	s.a1 = vc;
	s.n1 = d;
	steps.push_back(s);
}


void CompiledQuery::Union(TabID &t_out, const TabID& t2, const TabID& t3, int all)
{
	CompiledQuery::CQStep s;
	s.type = UNION;
	if(t_out.n != t2.n)
		s.t1 = t_out = NextTabID();
	else
		s.t1 = t2;
	s.t2 = t2;
	s.t3 = t3;
	s.n1 = all;
	steps.push_back(s);
}

void CompiledQuery::Result(const TabID& t1)
{
	CompiledQuery::CQStep s;
	s.type = RESULT;
	s.t1 = t1;
	steps.push_back(s);
}

void CompiledQuery::Print(Query* query)
{
	uint i;
	for(i = 0; i < steps.size(); i++)
		steps[i].Print(query);
}

bool CompiledQuery::CountColumnOnly(const TabID& table)
{
	CompiledQuery::CQStep step;
	bool count_only = false;
	for(int i = 0; i < NoSteps(); i++) {
		step = Step(i);
		if(step.type == CompiledQuery::ADD_COLUMN && step.t1 == table && step.cop == COUNT && step.e1.IsNull())
			count_only = true;
		if(step.type == CompiledQuery::ADD_COLUMN && step.t1 == table && (step.cop != COUNT || ( step.cop == COUNT && !step.e1.IsNull() )) ) {
			count_only = false;
			break;
		}
	}
	return count_only;
}

bool CompiledQuery::NoAggregationOrderingAndDistinct(int table)
{
	CompiledQuery::CQStep step;
	for(int i = 0; i < NoSteps(); i++) {
		step = Step(i);
		if(step.type == CompiledQuery::ADD_ORDER && step.t1.n == table)
			return false;			// exclude ordering
		if(step.type == CompiledQuery::ADD_COLUMN && step.t1.n == table && step.cop != LISTING)
			return false;			// exclude all kinds of aggregations
		if(step.type == CompiledQuery::T_MODE && step.t1.n == table && step.tmpar == TM_DISTINCT)
			return false;			// exclude DISTINCT
	}
	return true;
}

_int64 CompiledQuery::FindLimit(int table)
{
	CompiledQuery::CQStep step;
	for(int i = 0; i < NoSteps(); i++) {
		step = Step(i);
		if(step.type == CompiledQuery::T_MODE && step.t1.n == table && step.tmpar == TM_TOP)
			return step.n1 + step.n2;			// n1 - omitted, n2 - displayed,
												// i.e. either  ...LIMIT n2;   or  ...LIMIT n1, n2;
	}
	return -1;
}

bool CompiledQuery::FindDistinct(int table)
{
	CompiledQuery::CQStep step;
	for(int i = 0; i < NoSteps(); i++) {
		step = Step(i);
		if(step.type == CompiledQuery::T_MODE && step.t1.n == table && step.tmpar == TM_DISTINCT)
			return true;
	}
	return false;
}

std::set<int> CompiledQuery::GetUsedDims(const TabID& table_id, std::vector<JustATablePtr>& ta)
{
	set<int> result, local;
	CompiledQuery::CQStep step;
	pair<tabstepsmap::iterator, tabstepsmap::iterator> itsteps = TabIDSteps.equal_range(table_id);

	for (tabstepsmap::iterator it = itsteps.first; it != itsteps.second; ++it) {
		step = it->second;
		if(step.type == CompiledQuery::ADD_COLUMN && step.t1 == table_id && step.e1.vc_id != NULL_VALUE_32) {
			VirtualColumn* vc = ((TempTable*)ta[-table_id.n - 1].get())->GetVirtualColumn(step.e1.vc_id);
			if(vc) {
				local = vc->GetDimensions();
				result.insert(local.begin(), local.end());
			} else {
				// VC does not exist since it is created in latter compilation step
				// temporary solution is to skip optimization
				result.clear();
				result.insert(NULL_VALUE_32);
				return result;
			}
		} else {
			bool is_group_by = IsGroupByQuery(table_id);
			if(!is_group_by && step.type == CompiledQuery::ADD_ORDER && step.t1 == table_id && step.a1.n != NULL_VALUE_32) {
				VirtualColumn* vc = ((TempTable*)ta[-table_id.n - 1].get())->GetVirtualColumn(step.a1.n);
				if(vc) {
					local = vc->GetDimensions();
					result.insert(local.begin(), local.end());
				} else {
					// VC does not exist since it is created in latter compilation step
					// temporary solution is to skip optimization
					result.clear();
					result.insert(NULL_VALUE_32);
					return result;
				}
			}
		}
	}
	return result;
}

void CompiledQuery::BuildTableIDStepsMap()
{
	CompiledQuery::CQStep step;
	for(int i = 0; i < NoSteps(); i++) {
		step = Step(i);
		TabIDSteps.insert(pair<TabID, CompiledQuery::CQStep>(step.t1, step));
	}
}

bool CompiledQuery::IsGroupByQuery(const TabID& tab_id)
{
	CompiledQuery::CQStep step;
	for(int i = 0; i < steps_group_by_cols.size(); i++) {
		if(steps_group_by_cols[i].t1 == tab_id)
			return true;
	}
	return false;
}

bool CompiledQuery::ExistsInTempTable(const TabID& tab_id, const TabID& tmp_table)
{
	CompiledQuery::CQStep step;
	if (tab_id == tmp_table)
		return true;
	for(int i = 0; i < steps_tmp_tables.size(); i++) {
		if(steps_tmp_tables[i].t1 == tmp_table) {
			step = steps_tmp_tables[i];
			for(int i = 0; i < step.tables1.size(); i++)
				if(step.tables1[i] == tab_id)
					return true;
		} 
	}
	return false;
}

TabID CompiledQuery::FindSourceOfParameter(const TabID& tab_id, const TabID& tmp_table, bool& is_group_by)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(tmp_table.n < 0);
	is_group_by = false;
	for(int x = tmp_table.n + 1; x < 0; x++) {
		if(IsTempTable(TabID(x)) && ExistsInTempTable(tab_id, TabID(x))) {
			x = FindRootTempTable(x);
			is_group_by = IsGroupByQuery(TabID(x));
			return TabID(x);
		}
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Invalid parameter");
	return TabID(0);
}

bool CompiledQuery::IsTempTable(const TabID& t)
{
	for(int i = 0; i < NoSteps(); i++) {
		if(Step(i).type == CompiledQuery::TMP_TABLE && Step(i).t1 == t)
			return true;
	}
	return false;
}

int CompiledQuery::FindRootTempTable(int tab_id)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(tab_id < 0);
	for(int x = tab_id + 1; x < 0; x++) {
		if(IsTempTable(TabID(x)) && ExistsInTempTable(TabID(tab_id), TabID(x)))
			return FindRootTempTable(x);
	}
	return tab_id;
}

bool CompiledQuery::IsResultTable(const TabID& t)
{
	for(int i = 0; i < NoSteps(); i++) {
		if(Step(i).type == CompiledQuery::RESULT && Step(i).t1 == t) {
			return true;
		}
	}
	return false;
}

bool CompiledQuery::IsOrderedBy(const TabID& t)
{
	for(int i = 0; i < NoSteps(); i++) {
		if(Step(i).type == CompiledQuery::ADD_ORDER && Step(i).t1 == t) {
			return true;
		}
	}
	return false;
}

std::pair<_int64, _int64> CompiledQuery::GetGlobalLimit()
{
	//global limit is the step just before RESULT and for the same table as RESULT

	int i;
	for(i = 0; i < NoSteps(); i++) {
		if(Step(i).type == CompiledQuery::RESULT) {
			break;
		}
	}
	assert(i< NoSteps());
	TabID res = Step(i).t1;
	if(i > 0 && Step(i-1).type == CompiledQuery::T_MODE && Step(i-1).t1 == res) {
		return std::pair<_int64, _int64>(Step(i-1).n1, Step(i-1).n2);
	}
	return std::pair<_int64, _int64>(0,-1);
}

std::set<int> CompiledQuery::GetColumnsPerTable(int id)
{
	std::set<int> cols;
	std::map<int, std::vector<int> >::const_iterator it = GetTableIds().find(id);
	if(it != GetTableIds().end()) {
		for(int a = 0; a < (*it).second.size(); ++a) {
			std::vector<int>& c = GetColumnsPerAlias((*it).second[a]);
			cols.insert(c.begin(), c.end());
		}
	}
	return cols;
}

int CompiledQuery::GetNoDims(const TabID& tab_id)
{
	for(int i = 0; i < NoSteps(); i++) {
		if(Step(i).type == CompiledQuery::TMP_TABLE && Step(i).t1 == tab_id) {
			return int(Step(i).tables1.size());
		}
	}
	return -1;
}

TabID CompiledQuery::GetTableOfCond(const CondID& cond_id)
{
	for(int i = 0; i < NoSteps(); i++) {
		if(Step(i).type == CompiledQuery::CREATE_CONDS && Step(i).c1 == cond_id)
			return Step(i).t1;
	}
	return TabID();
}
