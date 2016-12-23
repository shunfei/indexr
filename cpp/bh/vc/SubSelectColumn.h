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


#ifndef SUBSELECTCOLUMN_H_INCLUDED
#define SUBSELECTCOLUMN_H_INCLUDED

#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"
#include "MultiValColumn.h"
#include "core/RoughValue.h"

class MysqlExpression;
class ValueSet;

typedef std::map< VarID, ValueOrNull > param_cache_t;

/*! \brief A column defined by a set of expressions like in IN operator.
 * SubSelectColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in SubSelectColumn object may not exist physically, they can be computed on demand.
 */
class SubSelectColumn : public MultiValColumn
{
public:
	class IteratorImpl : public MultiValColumn::IteratorInterface {
	protected:
		IteratorImpl( TempTable::RecordIterator const& it_, ColumnType const& expected_type_ ) : it(it_), expected_type( expected_type_ ) {}
		virtual ~IteratorImpl() {}
		virtual void DoNext() { ++ it; }
		virtual lazy_value_impl_t DoGetValue() { return lazy_value_impl_t(new LazyValueImpl(*it, expected_type)); }
		virtual bool DoNotEq(IteratorInterface const* it_) const { return it != static_cast<IteratorImpl const*>(it_)->it; }
		virtual impl_t DoClone() const { return impl_t( new IteratorImpl(*this) ); }
		friend class SubSelectColumn;
		TempTable::RecordIterator it;
		ColumnType const& expected_type;
	};
	class LazyValueImpl : public MultiValColumn::LazyValueInterface {
		virtual lazy_value_impl_t DoClone() const {
			return lazy_value_impl_t(new LazyValueImpl(record, expected_type));
		}
		virtual RCBString DoGetString() const {
			return record[0].ToRCString();
		}
		virtual RCNum DoGetRCNum() const {
			if(record[0].GetValueType() == NUMERIC_TYPE || record[0].GetValueType() == DATE_TIME_TYPE)
				return static_cast<RCNum&>(record[0]);
			BHERROR("Bad cast in RCValueObject::RCNum&()");
			return static_cast<RCNum&>(record[0]);
		}
		virtual RCValueObject DoGetValue() const {
			RCValueObject val = record[0];
			if(expected_type.IsString())
				val = val.ToRCString();
			else if(expected_type.IsNumeric() && ATI::IsStringType(val.Type())) {
				RCNum rc;
				RCNum::Parse(*static_cast<RCBString*>(val.Get()), rc, expected_type.GetTypeName());
				val = rc;
			}
			return val;
		}
		virtual _int64 DoGetInt64() const {
			return (static_cast<RCNum&>(record[0])).GetValueInt64();
		}
		virtual bool DoIsNull() const {
			return record[0].IsNull();
		}
		LazyValueImpl( TempTable::Record const& r_, ColumnType const& expected_type_ ) : record(r_), expected_type( expected_type_ ) {}
		virtual ~LazyValueImpl() {}
		TempTable::Record record;
		ColumnType const& expected_type;
		friend class IteratorImpl;
		friend class SubSelectColumn;
	};
	/*! \brief Creates a column representing subquery. Type of this column should be inferred from type of wrapped TempTable.
	 *
	 * \param table - TempTable representing subquery.
	 * \param mind - multindex of query containing subquery.
	 */
	SubSelectColumn(TempTable* subq, MultiIndex* mind, TempTable* outer_query_temp_table, int temp_table_alias);

	//created copies share subq and table
	SubSelectColumn(const SubSelectColumn& c);

	virtual ~SubSelectColumn();

	bool IsMaterialized() const { return subq->IsMaterialized(); }

	/*! \brief Returns true if subquery is correlated, false otherwise
	 * \return bool
	 */
	bool IsCorrelated() const;
	bool IsConst() const { return var_map.size() == 0; }
	virtual bool IsSubSelect() const { return true; }
	bool IsThreadSafe() {return IsConst() && MakeParallelReady(); }
	void PrepareAndFillCache();
	virtual bool IsSetEncoded(AttributeType at, int scale);	// checks whether the set is constant and fixed size equal to the given one
	virtual _int64 GetNotNullValueInt64(const MIIterator& mit)				{ return DoGetValueInt64(mit); }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator& mit) { return DoGetValueString(s, mit); }
	RoughValue RoughGetValue(const MIIterator& mit, SubSelectOptimizationType sot);
	bool CheckExists(MIIterator const& mit);
	BHTribool RoughIsEmpty(const MIIterator& mit, SubSelectOptimizationType sot);

protected:
	uint col_idx; // index of column in table containing result. Doesn't have to be 0 in case of hidden columns.
	boost::shared_ptr<TempTableForSubquery> subq;
	int parent_tt_alias;
	RCValueObject min;
	RCValueObject max;
	bool min_max_uptodate;
	void CalculateMinMax();
	virtual Iterator::impl_t DoBegin(MIIterator const&);
	virtual Iterator::impl_t DoEnd(MIIterator const&);
	virtual BHTribool DoContains(MIIterator const&, RCDataType const&);
	virtual BHTribool DoContains64(const MIIterator& mit, _int64 val);
	virtual BHTribool DoContainsString(const MIIterator& mit, RCBString &val);
	virtual bool DoIsNull(MIIterator const& mit);
	virtual RCValueObject DoGetValue(MIIterator const& mit, bool lookup_to_num);
	virtual _int64 DoNoValues(MIIterator const& mit);
	virtual _int64 DoAtLeastNoDistinctValues(MIIterator const& mit, _int64 const at_least);
	virtual bool DoContainsNull(MIIterator const& mit);
	virtual void DoSetExpectedType(ColumnType const&);
	virtual RCValueObject DoGetSetMin(MIIterator const& mit);
	virtual RCValueObject DoGetSetMax(MIIterator const& mit);
	virtual bool DoIsEmpty(MIIterator const&);
	virtual _int64 DoGetValueInt64(MIIterator const&);
	virtual double DoGetValueDouble(MIIterator const&);
	virtual void DoGetValueString(RCBString& s, MIIterator const& mit);

	virtual bool DoNullsPossible(bool val_nulls_possible)		{ return true; }
	virtual _int64 DoGetMinInt64(const MIIterator& mit) {
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return MINUS_INF_64;
	}
	virtual _int64 DoGetMaxInt64(const MIIterator& mit) {
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return PLUS_INF_64;
	}
	virtual _int64 DoRoughMin()	{
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return MINUS_INF_64;
	}
	virtual _int64 DoRoughMax() {
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return PLUS_INF_64;
	}
	virtual RCBString DoGetMaxString(const MIIterator& mit);
	virtual RCBString DoGetMinString(const MIIterator& mit);

	virtual bool DoIsDistinct() {
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return ( false );
	}

	virtual ushort DoMaxStringSize();		// maximal byte string length in column
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator& mit);
	virtual void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc);

	bool FeedArguments(const MIIterator& mit, bool for_rough);

	const MysqlExpression::bhfields_cache_t& GetBHItems() const { return bhitems; }

	ColumnType expected_type;
	param_cache_t param_cache_for_exact;
	param_cache_t param_cache_for_rough;

private:
	void SetBufs(MysqlExpression::var_buf_t* bufs);
	void RequestEval(const MIIterator& mit, const int tta);
	void PrepareSubqResult(const MIIterator& mit, bool exists_only);
	void RoughPrepareSubqCopy(const MIIterator& mit, SubSelectOptimizationType sot);
	bool MakeParallelReady();
	MysqlExpression::TypOfVars var_types;
	mutable MysqlExpression::var_buf_t var_buf_for_exact;
	mutable MysqlExpression::var_buf_t var_buf_for_rough;

	boost::shared_ptr<ValueSet>	cache;
	int	no_cached_values;
	bool first_eval_for_rough;
//	bool out_of_date_exact;
	bool out_of_date_rough; //important only for old mysql expr
};

#endif /* not SUBSELECTCOLUMN_H_INCLUDED */

