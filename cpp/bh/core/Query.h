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

#ifndef CORE_QUERY_H_INCLUDED
#define CORE_QUERY_H_INCLUDED

#include "system/ConnectionInfo.h"
#include "MysqlExpression.h"
#include "Joiner.h"
#include "Item_bhfield.h"
#include "Notifier.h"

#define RETURN_QUERY_TO_MYSQL_ROUTE 0
#define RCBASE_QUERY_ROUTE 1

class RoughMultiIndex;
class ParameterizedFilter;
class CompiledQuery;
class MysqlExpression;
class ResultSender;
class MIUpdatingIteratorShadow;

class Query
{
public:
	struct Context {
		typedef enum { SELECT, WHERE, HAVING, GROUPBY } context_t;
	};

	struct WrapStatus {
		typedef enum { SUCCESS, FAILURE, OPTIMIZE } wrap_status_t;
	};

public:
	Query(ConnectionInfo *conn_info);
	~Query();

	void Add(RCTable* tab);

	/*! \brief Remove the given RCTable from the list of tables used in a query
	 */
	void RemoveFromManagedList(const RCTablePtr tab);

	int NoTabs() const { return no_tab; };
	RCTable* Table(int table_num) const;
	void Clear() { no_tab = 0; };
	void LockPackInfoForUse();
	void UnlockPackInfoFromUse();

	void SetRoughQuery(bool set_rough = true)						{ rough_query = set_rough; }
	bool IsRoughQuery()												{ return rough_query; }

	struct Distinct	{
		std::map<bool,int> el;
	};
	struct Operation {
		std::map<ColOperation,struct Distinct> el;
	};
	struct FieldAlias {
		std::map<std::string,struct Operation> el;
	};
	struct TableAlias {
		std::map<std::string,struct FieldAlias> el;
	};
	typedef struct TableAlias map_of_fields;
	int Compile(CompiledQuery* compiled_query, SELECT_LEX* selects_list, SELECT_LEX* last_distinct, TabID* res_tab = NULL, bool ignore_limit = false, Item* left_expr_for_subselect = NULL, Operator* oper_for_subselect = NULL, bool ignore_minmax = false, bool for_subq_in_where = false);
	void FindLastJoinsForTables(CompiledQuery& qu);
	TempTable* Preexecute(CompiledQuery& qu, ResultSender* sender, bool display_now = true);
	int BuildConditions(COND* conds, CondID& cond_id, CompiledQuery* cq, const TabID& tmp_table, CondType filter_type, bool is_zero_result = false, JoinType join_type = JO_INNER);

	typedef std::vector<MysqlExpression*> expressions_t;
	std::multimap<std::string,std::pair<int,TABLE*> > table_alias2index_ptr;
	//std::multimap<std::string,int> table_alias2index;
	//std::multimap<std::string,TABLE*> table_alias2ptr;

	bool GetParallelAggr()	{return parallel_aggr;}
private:
	class ColumnData {
	private:
		AttrID atr;
		TabID tab;
		CQTerm term;
		ColOperation oper;
		bool dist;
	public:
		ColumnData(const AttrID& _atr, const TabID& _tab, CQTerm _term, ColOperation _oper, bool _dist)
			: atr(_atr), tab(_tab), term(_term), oper(_oper), dist(_dist)
			{ }
		AttrID atribute() { return atr; }
		TabID table() { return tab; }
		CQTerm cqterm() { return term; }
		ColOperation operation() { return oper; }
		bool distinct() { return dist; }
	};

	CompiledQuery* cq;
	std::vector<std::pair<TabID,bool> > subqueries_in_where;
	typedef std::pair<int,std::string> TabIDColAlias;
	std::map<TabIDColAlias,int> field_alias2num;
	std::map<std::string,unsigned > path2num;
	typedef std::pair<int, MysqlExpression*> virtual_column_meta_t;
	typedef std::multimap<TabID,virtual_column_meta_t> tab_id2expression_t; // all expression based virtual columns for a given table
	tab_id2expression_t tab_id2expression;
	typedef std::pair<std::vector<int>, AttrID>	inset_contents_t;
	typedef std::multimap<TabID, std::pair<int,inset_contents_t> > tab_id2inset_t ;
	tab_id2inset_t tab_id2inset;
	expressions_t gc_expressions;
	std::multimap<std::pair<int,int>, std::pair<int,int> > phys2virt;
	std::multimap<TabID, std::pair<int, TabID> > tab_id2subselect;
	std::map<Item_bhfield*,int> bhitems_cur_var_ids;

	std::vector<JustATablePtr> ta;					// table aliases - sometimes point to TempTables (maybe to the same one), sometimes to RCTables
	RCTable** t;
	int no_tab;
	int capacity;
	int* last_join;

	Notifier notifier;

	bool rough_query;			// set as true to enable rough execution

	bool use_IBExpressions;
	bool parallel_aggr;

	bool FieldUnmysterify(Item* item, TabID& tab, AttrID& col);
	int FieldUnmysterify(Item* item, const char*& database_name, const char*& table_name, const char*& table_alias, 
		const char*& table_path, const TABLE*& table_ptr, const char*& field_name, const char*& field_alias);

	const char* GetTableName(Item_field* ifield);
	int PrefixCheck(COND* conds);

	/*! \brief Checks if exists virtual column defined by physical column:
	 * \param tmp_table - id of TempTable for which VC is searched for
	 * \param tab - table id
	 * \param col - column id
	 * \return table id and number of virtual column if it exists or NULL_VALUE_32 otherwise
	 */
	std::pair<int,int> VirtualColumnAlreadyExists(const TabID& tmp_table, const TabID& tab, const AttrID& col);

	/*! \brief Checks if exists virtual column defined by expression:
	 * \param tmp_table - id of table for which column is supposed to be created
	 * \param expression
	 * \return number of virtual column if it exists or NULL_VALUE_32 otherwise
	 */
	int VirtualColumnAlreadyExists(const TabID& tmp_table, MysqlExpression* expression);

	/*! \brief Checks if exists virtual column defined by subquery
	 * \param tmp_table - id of table for which column is supposed to be created
	 * \param subquery - id of subquery
	 * \return number of virtual column if it exists or NULL_VALUE_32 otherwise
	 */
	int VirtualColumnAlreadyExists(const TabID& tmp_table, const TabID& subquery);

	int VirtualColumnAlreadyExists(const TabID& tmp_table, const std::vector<int>& vcs, const AttrID& at);

	int Item2CQTerm(Item* an_arg, CQTerm& term, const TabID& tmp_table, CondType filter_type, bool negative = false, Item* left_expr_for_subselect = NULL, Operator* oper_for_subselect = NULL);

	//int FilterNotSubselect(Item *conds, const TabID& tmp_table, FilterType filter_type, FilterID *and_me_filter = 0);

	/*! \brief Create filter from field or function that has no condition attached.
	 * \param conds - condition (a field or function).
	 * \param tmp_table - required for Item2CQTerm.
	 * \param filter_type - type of filter context (WHERE, HAVING).
	 * \param and_me_filter - ?
	 * \return filter number (non-negative) or error indication (negative)
	 */
	CondID ConditionNumberFromNaked(COND* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter, bool is_or_subtree = false);
	CondID ConditionNumberFromMultipleEquality(Item_equal* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter = 0, bool is_or_subtree = false);
	CondID ConditionNumberFromComparison(Item* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter = 0, bool is_or_subtree = false, bool negative = false);

	/*! \brief Checks type of operator involved in condition
	 * \param conds - conditions
	 * \param op - output param.: type of operator
	 * \param negative - input information about negation of condition
	 * \param like_esc - output param.: escape character for LIKE operator
	 * \return void
	 */
	void ExtractOperatorType(Item*& conds, Operator& op, bool& negative, char& like_esc);

	/*! \brief Adds ANY modifier to an operator
	 * \param op - operator
	 * \return void
	 */
	static void MarkWithAny(Operator& op);

	/*! \brief Adds ALL modifier to an operator
	* \param op - operator
	* \return void
	*/
	static void MarkWithAll(Operator& op);

	CondID ConditionNumber(COND* conds, const TabID& tmp_table, CondType filter_type, CondID* and_me_filter = 0, bool is_or_subtree = false);
	int BuildCondsIfPossible(COND* conds, CondID& cond_id, const TabID& tmp_table, JoinType join_type);

public:
	/*! \brief Removes ALL/ANY modifier from an operator
	* \param op - operator
	* \return void
	*/
	static void UnmarkAllAny(Operator& op);

	
	/*! \brief Checks if operator is of all/any type
	 * \param op - operator
	 * \return true if yes 
	*/
	static bool IsAllAny(Operator& op);

	/*! \brief Checks if item has a aggregation as one of its arguments.
	 * \param item
	 * \return true if yes
	 */
	static bool HasAggregation(Item* item);

	/*! \brief Checks if an aggregation represents COUNT(*)
	 * \param item_sum - aggregation item
	 * \return true if yes
	 */
	static bool IsCountStar(Item* item_sum);

	/*! \brief Checks if an item is of type FieldItem
	 * \param item
	 * \return true if yes
	 */
	static bool IsFieldItem(Item* item);

	bool IsDeterministic(Item* item);

	/*! \brief Checks if an item is of type Item_sum_func
	 * \param item
	 * \return true if yes
	 */
	static bool IsAggregationItem(Item* item);

	/*! \brief Checks if an item represents a constant
	 * \param item
	 * \return true if yes
	 */
	static bool IsConstItem(Item* item);

	/*! \brief Returns true if an item is aggregation over field item
	 * \param item
	 * \return true if yes
	 */
	static bool IsAggregationOverFieldItem(Item* item);

	/*! \brief Checks if given join contains left or right outer component.
	* \param join - join to be checked
	* \return bool - true if contains left or right outer join
	*/
	static bool IsLOJ(List<TABLE_LIST>* join);

	const std::string GetItemName(Item* item);

	static bool IsSupportedFunction(Item_func* ifunc);

	static void GetPrecisionScale(Item* item, int& precision, int& scale, bool max_scale = true);

	static bool IBExpressionCheck(Item*);

	static bool IsLogicOperator(Item_func* ifunc);

private:

	/*! \brief Creates AddColumn step in compilation by creating, if does not exist, Virtual Column based on PhysicalColumn
	 * \param item - what Item
	 * \param tmp_table - for which TempTable
	 * \param oper - defines type of added column
	 * \param distinct - flag for AddColumn operation
	 * \param group_by - indicates if it is column for group by query
	 * \return column number
	 */
	int AddColumnForPhysColumn(Item* item, const TabID& tmp_table, const ColOperation oper, const bool distinct, bool group_by, const char* alias = NULL);

	/*! \brief Creates AddColumn step in compilation by creating, if does not exist, Virtual Column based on expression
	 * \param mysql_expression - pointer to expression
	 * \param tmp_table - for which TempTable
	 * \param alias - defines alias of added column
	 * \param oper - defines type of added column
	 * \param distinct - flag for AddColumn operation
	 * \param group_by - indicates if it is column for group by query
	 * \return column number
	 */
	int AddColumnForMysqlExpression(MysqlExpression* mysql_expression, const TabID& tmp_table, const char* alias, const ColOperation oper, const bool distinct, bool group_by = false);

	/*! \brief Computes identifier of a column created by AddColumn operation
	 * \param vc - for which virtual column
	 * \param tmp_table -  for which TempTable
	 * \param oper - type of column
	 * \param distinct - modifier of AddColumn
	 * \return column number if it exists or NULL_VALUE_32 otherwise
	 */
	int GetAddColumnId(const AttrID& vc, const TabID& tmp_table, const ColOperation oper, const bool distinct);

	/*! \brief Changes type of AddColumn step in compilation from LISTING to GROUP_BY
	 * \param tmp_table - for which TempTable
	 * \param attr - for which column
	 */
	void CQChangeAddColumnLIST2GROUP_BY(const TabID& tmp_table, int attr);

	/*! \brief Creates MysqlExpression object that wraps expression tree of MySQL not containing aggregations.
	 * All Item_field items are substituted with Item_bhfield items
	 * \param item - root of MySQL expression tree to be wrapped
	 * \param tmp_table - alias of TempTable which is checked if it contains aggregation fields (should not)
	 * \param expr - expression to store the result tree
	 * \param in_aggregation - the expression is used in WHERE (true) or elsewhere (having, select list = false)
	 * \return WrapStatus::SUCCESS on success, WrapStatus::OPTIMIZE if an aggregation was encountered, WrapStatus::FAILURE if
	 * there was any problem with wrapping, e.g., not acceptable type of expression
	 */
	WrapStatus::wrap_status_t WrapMysqlExpression(Item* item, const TabID& tmp_table, MysqlExpression*& expr, bool in_where, bool aggr_used);
//
//	/*! \brief Creates MysqlExpression object that wraps full expression tree of MySQL. All Item_field items are
//	 * substituted with Item_bhfield items. In case of aggregation it is substituted with its whole subtree by a single
//	 * Item_bhfield, however, there is an AddColumn step added to compilation with this aggregation and Item_bhfield refers to the
//	 * column created by this step.
//	 * \param item - root of MySQL expression tree to be wrapped
//	 * \param tmp_table - alias of TempTable to which AddColumn is added in case of aggregation
//	 * \param expr - expression to store the result tree
//	 * \param is_const_or_aggr - optional pointer to bool variable that is set to true if expression has no variables or
//	 * it contains at least one aggregation (false otherwise)
//	 * \return WrapStatus::SUCCESS on success, WrapStatus::FAILURE if there was any problem with wrapping,
//	 *  e.g., not acceptable type of expression
//	 */
//	WrapStatus::wrap_status_t WrapMysqlExpressionWithAggregations(Item *item, const TabID& tmp_table, MysqlExpression*& expr, bool* is_const_or_aggr = NULL);

	/*! \brief Generates AddColumn compilation steps for every field on SELECT list
	 * \param fields - list of fields
	 * \param tmp_table - alias of TempTable for which AddColumn steps are added
	 * \param group_by_clause - indicates whether it is group by query
	 * \param ignore_minmax - indicates if field of typy Min/Max should be transformed to LISTING
	 * \return returns RETURN_QUERY_TO_MYSQL_ROUTE in case of any problem and RCBASE_QUERY_ROUTE otherwise
	 */
	int AddFields(List<Item>& fields, const TabID& tmp_table, const bool group_by_clause, int& num_of_added_fields, bool ignore_minmax, bool& aggr_used);

	/*! \brief Generates AddColumn compilation steps for every field on GROUP BY list
	 * \param fields - pointer to GROUP BY fields
	 * \param tmp_table - alias of TempTable for which AddColumn steps are added
	 * \return returns RETURN_QUERY_TO_MYSQL_ROUTE in case of any problem and RCBASE_QUERY_ROUTE otherwise
	 */
	int AddGroupByFields(ORDER* group_by, const TabID& tmp_table);

	//! is this item representing a column local to the temp table (not a parameter)
	bool IsLocalColumn(Item* item, const TabID& tmp_table) ;
	int AddOrderByFields(ORDER* order_by, TabID const& tmp_table, int const group_by_clause);
	int AddGlobalOrderByFields(SQL_LIST* global_order, const TabID& tmp_table, int max_col);
	int AddJoins(List<TABLE_LIST>& join, TabID& tmp_table, std::vector<TabID>& left_tables, std::vector<TabID>& right_tables, bool in_subquery, bool& first_table, bool for_subq = false);
	static bool ClearSubselectTransformation(Operator& oper_for_subselect, Item*& field_for_subselect, COND*& conds, Item*& having, Item*& cond_removed, List<Item>*& list_to_reinsert, Item* left_expr_for_subselect);

	/*!
	 * \brief Are the variables constant (i.e. they are parameters) in the context of the query represented by TempTable t
	 */
	bool IsConstExpr(MysqlExpression::SetOfVars& vars, const TabID& t);

	ConnectionInfo* m_conn;

	//void EvalCQTerm(bool cplx, CQTerm& term, MysqlExpression::BufOfVars bufs, CQTerm& t2, DataType eval_type, std::map<int,ValueOrNull> & columns);
	//void PrepareCQTerm(CQTerm& term, bool& cplx_val2, MysqlExpression::SetOfVars& val2_vars, std::vector<int>& aliases, std::map<int,ValueOrNull>& columns, DataType& attr_eval_type);

	static JoinType GetJoinTypeAndCheckExpr(uint outer_join, Item* on_expr);

	void DisplayJoinResults(MultiIndex& mind, DimensionVector& all_involved_dims,
						JoinAlgType cur_join_type, bool is_outer, int conditions_used);

	/*! \brief Checks if condition represented by \e it is a negation of other condition
	 * \param it - item
	 * \param is_there_not - output param.: true if negation was found
	 * \return Item* - returns pointer to argument of negation or original item if there was no negation
	 */
	Item* FindOutAboutNot(Item* it, bool& is_there_not);

	VirtualColumn*	CreateColumnFromExpression(Query::expressions_t const& exprs,
			TempTable* temp_table, int temp_table_alias, MultiIndex* mind);

	bool IsParameterFromWhere(const TabID& params_table);

	/*! \brief true if the desc vector contains at least 2 1-dimensional descriptors defined for different dimensions
	 *  e.g. true if contains T.a=1, U.b=7, false if T.a=1, T.b=7
	 *
	 */
	bool DimsWith1dimFilters(Condition& cond, MultiIndex& mind);
	VirtualColumn* BuildIBExpression(Item* item, ColumnType type, TempTable* temp_table, int temp_table_alias, 
		MultiIndex* mind, bool skip_fix2fix_conv, bool toplevel = false);

	VirtualColumn* CreateIBExpFunction(Item_func* item, MultiIndex* mind, std::vector<VirtualColumn*> child_cols, TempTable *t = NULL);

	VirtualColumn* AddTypeCast(const ColumnType& item_type, const ColumnType& result_type, VirtualColumn* col, 
		TempTable* temp_table, bool skip_fix2fix_conv, bool toplevel);
	ColumnType GetArgumentType(Item_func* ifunc, const ColumnType& result_type, uint arg_pos = std::numeric_limits<uint>::max());

	ColumnType ItemResult2ColumnType(Item* item);
	bool SkipFix2FixConv(Item_func* ifunc, ColumnType arg_type);	
	void InitUserParams(ConnectionInfo *conn_info);
};

#endif // not CORE_QUERY_H_INCLUDED
