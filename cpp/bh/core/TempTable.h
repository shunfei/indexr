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


#include <vector>

#include "common/CommonDefinitions.h"
#include "MysqlExpression.h"
#include "Descriptor.h"
#include "ParametrizedFilter.h"
#include "CompiledQuery.h"
#include "CachedBuffer.h"
#include "CQTerm.h"
#include "JustATable.h"
#include "MultiIndex.h"
#include "system/LargeBuffer.h"
#include "exporter/DataExporter.h"
#include "MIUpdatingIterator.h"
#include "PhysicalColumn.h"
#include "PackOrderer.h"

#ifndef _TEMPTABLE_H_
#define _TEMPTABLE_H_

class Filter;
class GroupTable;
class GroupByWrapper;
class DistinctWrapper;

class ResultSender;

struct CQTerm;
class Descriptor;
class SortDescriptor;
struct Attr;
struct DescTreeNode;
enum JoinType;
//////////////////////////////////////////////////////////////////////////////////
//
//  TempTable - for storing a definition or content of a temporary table (view)
//

class TempTable : public JustATable
{
public:
	typedef std::vector<int> aliases_t;
	typedef std::vector<JustATable*> tables_t;
	template<class T>
	class AttrBuffer : public CachedBuffer<T> {
	public:
		explicit AttrBuffer(uint page_size, uint elem_size = 0, ConnectionInfo *conn = NULL);
		~AttrBuffer();

		void GetString(RCBString& s, _int64 idx) {if(constant_value) CachedBuffer<T>::Get(s,0); else CachedBuffer<T>::Get(s,idx);}
		// Warning: read only operator!!!
		T& operator[](_int64 idx) { return constant_value ? CachedBuffer<T>::Get(0) : CachedBuffer<T>::Get(idx); }
		void Set(_int64 idx, T value);
	private:
		bool			constant_value;		// only one value in the whole buffer, stored in buf[0]
	};

	class Attr : public PhysicalColumn
	{
	public:
		void *			buffer; // buffer to values of attribute, if materialized
		_int64			no_obj; // number of objects in the buffer
		_int64			no_materialized; //number of objects already set in the buffer
		uint			page_size; // size of one page of buffered values
		char *			alias;
		ColOperation	mode;
		bool			distinct; // distinct modifier for aggregations
		CQTerm			term;
		int				dim;	// dimension of a column, i.e., index of source table,
								// -1 means there is no corresponding table: constant, count(*)
		int 			orig_precision;
		bool 			not_complete;		//does not contain all the column elements - some functions cannot be computed

		Attr(CQTerm t,
			ColOperation	m,
			bool			distinct = false,
			char *			alias = NULL,
			int				dim = -1,
			AttributeType	type = RC_INT,
			uint			scale = 0,
			uint			precision = 10,
			NullMode		n = NO_NULLS, 
			DTCollation collation = DTCollation(),
			bool			is_unsigned = false
			);
		Attr(const Attr&);
		Attr& operator=(const Attr&);
		int operator==(const Attr &);
		~Attr();
		enum phys_col_t ColType() {return ATTR;}

		//! Use in cases where actual string length is less than declared, before materialization of Attr
		void OverrideStringSize(int size) 	{ ct.OverrideInternalSize(size); ct.SetPrecision(size); }
		void SetPrecision(int prec) 		{ ct.SetPrecision(prec); }
		void SetScale(int sc) 				{ ct.SetScale(sc); }

		void CreateBuffer(_uint64 size, ConnectionInfo *conn = NULL, bool not_completed = false);
		void SetNewPageSize(uint new_page_size);
		void SetValueString(_int64 obj, const RCBString& val);
		RCValueObject GetValue(_int64 obj, bool lookup_to_num = false);
		void GetValueString(_int64 row, RCBString& s)	{ GetValueString(s, row); }
		void GetValueString(RCBString& s, _int64 row);
		void GetNotNullValueString(_int64 row, RCBString& s)	{ GetValueString(s, row); }
		_uint64 ApproxDistinctVals(bool incl_nulls, ::Filter* f, RSValue* rf, bool outer_nulls_possible);	// provide the best upper approximation of number of diff. values (incl. null, if flag set)
		_uint64 ExactDistinctVals(Filter* f)		{ return NULL_VALUE_64; }
		bool IsDistinct(::Filter* f) 				{ return false; }
		ushort MaxStringSize(::Filter* f = NULL);		// maximal byte string length in column
		_int64 RoughMin(::Filter* f = NULL, RSValue* rf = NULL)			{ return MINUS_INF_64; }	// for numerical: best rough approximation of min for a given filter (or global min if filter is NULL)
		_int64 RoughMax(::Filter* f = NULL, RSValue* rf = NULL)			{ return PLUS_INF_64; }		// for numerical: best rough approximation of max for a given filter (or global max if filter is NULL)
		void DisplayAttrStats(::Filter* f)	{}
		bool TryToMerge(Descriptor &d1,Descriptor &d2)	{ return false; }
		PackOntologicalStatus GetPackOntologicalStatus(int pack_no) { return NORMAL; } //not implemented properly yet
		void ApplyFilter(MultiIndex &, _int64 offset, _int64 no_obj);
		void DeleteBuffer();

		bool IsNull(_int64 obj) const;
		void SetNull(_int64 obj);
		void SetMinusInf(_int64 obj);
		void SetPlusInf(_int64 obj);
		_int64 GetValueInt64(_int64 obj) const;
		_int64 GetNotNullValueInt64(_int64 obj) const;
		void SetValueInt64(_int64 obj, _int64 val);
		void InvalidateRow(_int64 obj);
		_int64 GetMinInt64(int pack);
		_int64 GetMaxInt64(int pack);
		RCBString GetMaxString(int pack);
		RCBString GetMinString(int pack);
		_int64 GetNoNulls(int pack);
		bool RoughNullsOnly() const						{return false;}
		_int64 GetSum(int pack, bool &nonnegative);
		uint AttrNo()		 						{ return (uint)-1; }
		RSValue RoughCheck(int pack, Descriptor& d, bool additional_nulls_possible) { return RS_SOME; }
		RSValue RoughCheck(int pack1, int pack2, Descriptor& d) { return RS_SOME; }

		// as far as Attr is not pack oriented the function below should not be called
		void EvaluatePack(MIUpdatingIterator &mit, int dim, Descriptor& desc) { assert(0); }
		RCBString	DecodeValue_S(_int64 code)		{ assert(0); return RCBString(); }	// RCAttr only
		int			EncodeValue_S(RCBString &v)		{ assert(0); return -1; }	// lookup (physical) only
	};
	typedef std::vector<Attr*> attrs_t;

	struct TableMode {
		bool	distinct;
		bool	top;
		bool	exists;
		_int64		param1, param2; // e.g., TOP(5), LIMIT(2,5)
		TableMode(): distinct(false), top(false), exists(false), param1(0), param2(-1) {}
	};

protected:
	TempTable(const TempTable &, bool is_vc_owner, bool for_rough_query = false);
	TempTable(JustATable * const , int alias, Query* q);

	TempTablePtr CreateMaterializedCopy(bool translate_order, bool in_subq);		// move all buffers to a newly created TempTable, make this VC-pointing to it; the new table must be deleted by DeleteMaterializedCopy()
	void DeleteMaterializedCopy(TempTablePtr& t_new);	// delete the external table and remove VC pointers, make this fully materialized
	int mem_scale;
	std::map<PhysicalColumn *, PhysicalColumn *> attr_back_translation;
//	TempTable* subq_template;

public:
	virtual ~TempTable();
	void TranslateBackVCs();
	////////// Query execution (CompiledQuery language implementation)
	void					AddConds(Condition* cond, CondType type);
	void					AddInnerConds(Condition* cond, std::vector<TabID>& dims);
	void					AddLeftConds(Condition* cond, std::vector<TabID>& dims1, std::vector<TabID>& dims2);
	void					SetMode(TMParameter mode, _int64 mode_param1 = 0, _int64 mode_param2 = -1);
	void					JoinT(JustATable * t, int alias, JoinType jt);
	int						AddColumn(CQTerm, ColOperation, char* alias, bool distinct);
	void					AddOrder(VirtualColumn *vc, int direction);
	void					Union(TempTable*, int);
	void					Union(TempTable*, int, ResultSender* sender, _int64& g_offset, _int64& g_limit);
	void					RoughUnion(TempTable*, ResultSender* sender);

	////////// Maintenance and low-level functions

	bool					OrderByAndMaterialize(std::vector<SortDescriptor> &ord, _int64 limit, _int64 offset, ResultSender* sender = NULL); // Sort data contained in ParameterizedFilter by using some attributes (usually specified by AddOrder, but in general - arbitrary)
	void					FillMaterializedBuffers( _int64 local_limit, _int64 local_offset, ResultSender* sender, bool pagewise = false);	// just materialize as SELECT *

	virtual void			RoughMaterialize(bool in_subq = false, ResultSender* sender = NULL, bool lazy = false);
	virtual void			Materialize(bool in_subq = false, ResultSender* sender = NULL, bool lazy = false);

							// just_distinct = 'select distinct' but no 'group by'
							// Set no_obj = no. of groups in result
	void					RoughAggregate(ResultSender* sender);
	void					RoughAggregateCount(DimensionVector &dims, _int64 &min_val, _int64 &max_val, bool group_by_present);
	void					RoughAggregateMinMax(VirtualColumn *vc, _int64 &min_val, _int64 &max_val);
	void					RoughAggregateSum(VirtualColumn *vc, _int64 &min_val, _int64 &max_val, std::vector<Attr*> &group_by_attrs, bool nulls_only, bool distinct_present);
	void 					VerifyAttrsSizes();	// verifies attr[i].field_size basing on the current multiindex contents

	void 					SuspendDisplay()	{ m_conn.SuspendDisplay(); }
	void 					ResumeDisplay()		{ m_conn.ResumeDisplay(); }

	void					LockPackForUse(unsigned attr, unsigned pack_no, ConnectionInfo& conn);
	void					UnlockPackFromUse(unsigned attr, unsigned pack_no) {}

	////////
	_int64					NoObj() { return no_obj; }
	_int64					NoMaterialized() { return no_materialized; }
	void					SetNoObj(_int64 n) { no_obj = n; }
	void					SetNoMaterialized(_int64 n) { no_obj = n; no_materialized = n; }
	TType					TableType() const { return TEMP_TABLE; }	// type of JustATable - TempTable

	uint					NoAttrs() const				{ return (uint)attrs.size(); }
	uint					NoDisplaybleAttrs() const	{ return no_cols; }  // no. of columns with defined alias
	bool					IsDisplayAttr(int i)		{ return attrs[i]->alias != NULL; }

	_int64					GetTable64(_int64 obj, int attr);
	void 					GetTable_S(RCBString& s, _int64 obj, int attr);
	void					GetTableString(RCBString& s, int64 obj, uint attr);
	void					GetTable_B(_int64 obj, int attr, int &size, char *val_buf);
	RCValueObject			GetTable(_int64 obj, uint attr);

	_uint64					ApproxAnswerSize(int attr, Descriptor& d);	// provide the most probable approximation of number of objects matching the condition

	bool					IsNull(_int64 obj, int attr); // return true if the value of attr. is null

	_int64 					RoughMin(int n_a, ::Filter *f = NULL)					{ return MINUS_INF_64; }
	_int64 					RoughMax(int n_a, ::Filter *f = NULL)					{ return PLUS_INF_64; }
	ushort					MaxStringSize(int n_a, ::Filter *f = NULL)				{ if(n_a<0) return GetFieldSize(-n_a - 1);
																					  return GetFieldSize(n_a); }

	uint					NoDimensions() { return filter.mind ? (uint)filter.mind->NoDimensions() : 1; }
	int						GetDimension(TabID alias);
	AttributeType			AttrType(int a) { assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->TypeName(); }
	std::vector<ATI>		GetATIs(bool orig = false);
	int						GetAttrScale(int a)		{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->Type().GetScale(); }
	int						GetAttrSize(int a)		{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->Type().GetDisplaySize(); }
	int						GetFieldSize(int a)		{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->Type().GetInternalSize() ; }
	int						GetNoDigits(int a)		{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->Type().GetPrecision(); }
	const ColumnType&		GetColumnType(int a)	{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->Type();}
	PhysicalColumn* 		GetColumn(int a)		{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]; }
	NullMode				GetAttrNullMode(int a)	{ assert(a >= 0 && (uint)a < NoAttrs()); return attrs[a]->Type().GetNullsMode(); }
	Attr*					GetAttrP(uint a)		{ assert(a < (uint)NoAttrs()); return attrs[a]; }
	Attr*					GetDisplayableAttrP(uint i)		{ assert(displayable_attr); return displayable_attr[i]; }
	void					CreateDisplayableAttrP();
	uint					GetDisplayableAttrIndex(uint attr);
	MultiIndex*				GetMultiIndexP()		{ return filter.mind; }
	void					ClearMultiIndexP()		{ delete filter.mind; filter.mind = NULL; }
	MultiIndex*				GetOutputMultiIndexP()	{ return &output_mind; }
	ParameterizedFilter*	GetFilterP() { return & filter; }
	JustATable*				GetTableP(uint dim)		{ assert(dim<tables.size()); return tables[dim]; }
	int 					NoTables() const		{ return int(tables.size()); }
	aliases_t&				GetAliases()			{ return aliases; }
	tables_t&				GetTables()				{ return tables; }
	bool					IsMaterialized()		{ return materialized; }
	void					SetAsMaterialized()		{ materialized = true; }
	bool					IsFullyMaterialized(); // materialized with order by and limits
	int						CalculatePageSize(_int64 no_obj = -1);	// computes number of output records kept in memory
								// based on no_obj and some upper limit CACHE_SIZE (100MB)
	void					SetPageSize(_int64 new_page_size);
	_int64 					GetPageSize() {assert(attrs[0]); return attrs[0]->page_size; }

	void					DisplayRSI();	// display info about all RSI contained in RCTables from this TempTable

	bool					HasHavingConditions()				{ return having_conds.Size() > 0; }
	bool					CheckHavingConditions(MIIterator &it) { return having_conds[0].tree->root->CheckCondition(it); }
	void					ClearHavingConditions()				{ having_conds.Clear(); }

	void 					SetManageUsedTables()				{ manage_used_tables = true; }
	void 					SetManageUsedTables(bool new_state) { manage_used_tables = new_state; }
	void 					RemoveFromManagedList(const RCTable* rct);
	int						AddVirtColumn(VirtualColumn* vc);
	int						AddVirtColumn(VirtualColumn* vc, int no);
	uint					NoVirtColumns() { return uint(virt_cols.size()); }
	void					ReserveVirtColumns(int no);
	VirtualColumn*			GetVirtualColumn(uint col_num) { assert(col_num < virt_cols.size()); return virt_cols[col_num]; }
	void					SetVCDistinctVals(int dim, _int64 val);			// set dist. vals for all vc of this dimension
	void					ResetVCStatistics();
	void 					ProcessParameters(const MIIterator& mit, const int alias);
	void					RoughProcessParameters(const MIIterator& mit, const int alias);
	Query*					GetQuery() {return query;}
	int						DimInDistinctContext();	// return a dimension number if it is used only in contexts where row repetitions may be omitted, e.g. distinct
	Condition&				GetWhereConds() {return filter.GetConditions();}
	void MoveVC(int colnum, std::vector<VirtualColumn*>& from, std::vector<VirtualColumn*>& to);
	void MoveVC(VirtualColumn* vc, std::vector<VirtualColumn*>& from, std::vector<VirtualColumn*>& to);
	virtual bool			IsForSubq() {return false;}

protected:
	_int64					no_obj;
	uint					no_cols;	// no. of output columns, i.e., with defined alias
	_uint64					buf_index;	// index of first row of TT cached in column buffers
										// e.g., 100 means that all the buffers contain values of objects starting from 101th row of TT
										// size of buffer determines index of last object
	TableMode				mode;		// based on { TM_DISTINCT, TM_TOP, TM_EXISTS }
	attrs_t					attrs;		// vector of output columns, each column contains a buffer with values
	Attr**					displayable_attr;	// just a shortcut: some of attrs
	Condition				having_conds;
	tables_t				tables; // vector of pointers to source tables
	aliases_t				aliases; // vector of aliases of source tables
	std::vector<VirtualColumn*>	virt_cols; // vector of virtual columns defined for TempTable
	std::vector<bool>			virt_cols_for_having;	//is a virt column based on output_mind (true) or source mind (false)
	std::vector<JoinType>	join_types; // vector of types of joins, one less than tables
	ParameterizedFilter		filter;		// multidimensional filter, contains multiindex, can be parametrized
	MultiIndex				output_mind; // one dimensional MultiIndex used for operations on output columns of TempTable
	std::vector<SortDescriptor>	order_by;// indexes of order by columns
	bool					group_by;	// true if there is at least one grouping column
	bool					is_vc_owner; // true if temptable should dealocate virtual columns
	bool					for_rough_query;	// true if this is a temptable copy for rough query execution
	int						no_global_virt_cols; // keeps number of virtual columns. In case for_subq == true, all locally created
							// virtual columns will be removed
	bool					is_sent; // true if result of materialization was sent to MySQL

	////////////////////////////////////////////////////
	// some internal functions for low-level query execution
	static const uint		CACHE_SIZE = 100000000;	// size of memory cache for data in the materialized TempTable
												// everything else is cached on disk (CachedBuffer)
	////////////////////////////////////////////////////
	bool					IsParametrized(); //is the temptable (select) parametrized?

	void					ApplyOffset(_int64 limit, _int64 offset); // apply limit and offset to attr buffers

	bool					materialized;
	bool 					manage_used_tables; // true if destructor should destroy all temptables stored in tables<>
	bool					has_temp_table;

	Query*					query;						//query containing this tempTable
	bool 					lazy;			//materialize on demand, page by page
	_int64					no_materialized; //if lazy - how many are ready
	BHTribool				rough_is_empty; // rough value specifying if there is non-empty result of a query

	////////////////////////////////////////////////////
	// Refactoring: extracted small methods
	bool					CanOrderSources();
	bool					LimitMayBeAppliedToWhere();
	ColumnType				GetUnionType(ColumnType type1, ColumnType type2);
	bool					SubqueryInFrom();
	//void 					ExtractColumnsFromTerm(CQTerm& term, bool& is_complex, DataType& type, std::map<VarID, ValueOrNull>& columns,
	//												MysqlExpression::SetOfVars& vars, MysqlExpression::TypOfVars& types);
public:
	class RecordIterator;
	//virtual RecordIterator	find( ConnectionInfo *conn, _uint64 pos);
	virtual RecordIterator	begin( ConnectionInfo *conn = NULL);
	virtual RecordIterator	end( ConnectionInfo *conn = NULL);

public:
	ConnectionInfo&			m_conn;					// external pointer

	void					Display(std::ostream &out = std::cout); // output to console
	static TempTablePtr		Create(const TempTable &, bool in_subq, bool for_rough_query = false);
	static TempTablePtr		Create(JustATable * const , int alias, Query* q, bool for_subquery = false);
	bool					IsSent() { return is_sent; }
	void					SetIsSent() { is_sent = true; }
	BHTribool				RoughIsEmpty() { return rough_is_empty; }
public:

	class Record
	{
	public:
		Record( RecordIterator& it, _uint64 atRow ) : _it( it ), _atRow( atRow ) {}
		Record( Record const& it ) : _it( it._it ), _atRow( it._atRow ) {}
		virtual ~Record() {}
		RCDataType& operator[]( uint ) const;
		_uint64 get_row_id()
		{
			return ( _atRow );
		}

		uint size() const
		{
			return ( _it.table->NoDisplaybleAttrs() );
		}

	private:
		RecordIterator& _it;
		_uint64 _atRow;
		friend class RecordIterator;
	};

	/*! \brief RecordIterator class for _table records.
	 */
	class RecordIterator
	{
	public:
		TempTable* table;
		_uint64 _currentRNo;
		ConnectionInfo* _conn;
		std::vector<RCDataType*> dataTypes;
		bool is_prepared;

	public:
		RecordIterator( void );
		RecordIterator( const RecordIterator& riter );
		virtual ~RecordIterator();

		RecordIterator& operator = ( const RecordIterator& ri );
		bool operator == ( RecordIterator const& i ) const;
		bool operator != ( RecordIterator const& i ) const;
		RecordIterator& operator ++ ( void );
		Record operator*() {
			if(!is_prepared) {
				PrepareValues();
				is_prepared = true;
			}
			return ( Record( *this, _currentRNo ) );
		}
		_uint64 currentRowNumber() { return _currentRNo; }
		TempTable* Owner() const
			{ return ( table ); }
	private:
		RecordIterator(TempTable*, ConnectionInfo*, _uint64 = 0 );
		void PrepareValues();

	private:
		friend class TempTable;
		friend class Record;
  };

};

class TempTableForSubquery : public TempTable
{
public:
	TempTableForSubquery(JustATable * const t, int alias, Query* q) : TempTable(t,alias,q),
		template_filter(0), is_attr_for_rough(false), rough_materialized(false) {};
	~TempTableForSubquery();

	void CreateTemplateIfNotExists();

	void Materialize(bool in_subq = false, ResultSender* sender = NULL, bool lazy = false);
	void RoughMaterialize(bool in_subq = false, ResultSender* sender = NULL, bool lazy = false);
	void ResetToTemplate(bool rough);
	void SetAttrsForRough();
	void SetAttrsForExact();
	virtual bool			IsForSubq() {return true;}
	//void SetForSubq()			{for_subq = true;}
	//void ResetForSubq()			{for_subq = false;}

private:
	ParameterizedFilter* 	template_filter;
	Condition				template_having_conds;
	TableMode				template_mode;
	std::vector<SortDescriptor>	template_order_by;
	bool 					is_attr_for_rough;
	bool					rough_materialized;
	attrs_t					attrs_for_exact;
	attrs_t					attrs_for_rough;
	attrs_t					template_attrs;
	std::vector<VirtualColumn*> template_virt_cols ;
	std::vector<bool> 		copied_virt_cols;

};

#endif

