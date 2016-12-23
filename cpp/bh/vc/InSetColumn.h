/* Copyright (C)	2005-2008 Infobright Inc.

	 This program is free software; you can redistribute it and/or modify
	 it under the terms of the GNU General Public License version 2.0 as
	 published by the Free	Software Foundation.

	 This program is distributed in the hope that	it will be useful, but
	 WITHOUT ANY WARRANTY; without even	the implied warranty of
	 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the GNU
	 General Public License version 2.0 for more details.

	 You should have received a	copy of the GNU General Public License
	 version 2.0	along with this	program; if not, write to the Free
	 Software Foundation,	Inc., 59 Temple Place, Suite 330, Boston, MA
	 02111-1307 USA	*/


#ifndef INSETCOLUMN_H_INCLUDED
#define INSETCOLUMN_H_INCLUDED

#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"
#include "MultiValColumn.h"
#include "core/TempTable.h"
#include "core/ValueSet.h"

class MysqlExpression;

/*! \brief A column defined by a set of expressions like in IN operator.
 * InSetColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in InSetColumn object may not exist physically, they can be computed on demand.
 */
class InSetColumn : public MultiValColumn
{
public:
	class IteratorImpl : public MultiValColumn::IteratorInterface {
	protected:
		explicit IteratorImpl(ValueSet::const_iterator const& it_) : it(it_) {}
		virtual ~IteratorImpl() {}
		virtual void DoNext() { ++it; }
		virtual lazy_value_impl_t DoGetValue() { return lazy_value_impl_t(new LazyValueImpl(*it)); }
		virtual bool DoNotEq(IteratorInterface const* it_) const { return it != static_cast<IteratorImpl const*>(it_)->it; }
		virtual impl_t DoClone() const { return impl_t( new IteratorImpl(*this) ); }
		friend class InSetColumn;
		ValueSet::const_iterator it;
	};
	class LazyValueImpl : public MultiValColumn::LazyValueInterface {
		explicit LazyValueImpl(RCDataType const* v) : value(v) {}
	public:
		virtual lazy_value_impl_t DoClone() const {
			return lazy_value_impl_t(new LazyValueImpl(value));
		}
		virtual RCBString DoGetString() const {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCBString const*>(value));
			return *static_cast<RCBString const*>(value);
		}
		virtual RCNum DoGetRCNum() const {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCNum const*>(value));
			return *static_cast<RCNum const*>(value);
		}
		virtual RCValueObject DoGetValue() const {
			return *value;
		}
		virtual _int64 DoGetInt64() const {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<RCNum const*>(value));
			return static_cast<RCNum const*>(value)->Value();
		}
		virtual bool DoIsNull() const {
			return value->IsNull();
		}
		virtual ~LazyValueImpl() {}
	private:
		RCDataType const* value;
		friend class IteratorImpl;
	};
	InSetColumn(const ColumnType & ct, MultiIndex *mind, const columns_t & columns);
	InSetColumn(ColumnType const& ct, MultiIndex* mind, ValueSet& external_valset);		// a special version for sets of constants
	InSetColumn(const InSetColumn& c);

	virtual ~InSetColumn();
	virtual bool IsSetEncoded(AttributeType at, int scale);			// checks whether the set is constant and fixed size equal to the given one
	bool IsConst() const;
	virtual bool IsInSet() const { return true; }
	char *ToString(char p_buf[], size_t buf_ct) const;
	void RequestEval(const MIIterator& mit, const int tta);
	ColumnType& GetExpectedType() {return expected_type;}
	void LockSourcePacks(const MIIterator& mit);
	void LockSourcePacks(const MIIterator& mit, int);
	bool CanCopy() const;
	std::vector<VirtualColumn*> GetChildren()									{return columns;}

	columns_t columns;
protected:
	virtual _int64 DoNoValues(MIIterator const& mit);
	virtual _int64 DoAtLeastNoDistinctValues(const MIIterator& mit, _int64 const at_least);
	virtual bool DoContainsNull(const MIIterator& mit);
	virtual RCValueObject DoGetSetMin(const MIIterator & mit);
	virtual RCValueObject DoGetSetMax(const MIIterator & mit);
	virtual Iterator::impl_t DoBegin(const MIIterator& );
	virtual Iterator::impl_t DoEnd(const MIIterator& );
	virtual BHTribool DoContains(const MIIterator& , const RCDataType& );
	virtual BHTribool DoContains64(const MIIterator& , _int64);				// easy case for integers
	virtual BHTribool DoContainsString(const MIIterator& , RCBString &);	// easy case for encoded strings
	virtual void DoSetExpectedType(const ColumnType& );
	virtual bool DoIsEmpty(const MIIterator& );
	virtual bool DoNullsPossible(bool val_nulls_possible) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return true;
	}
	virtual _int64 DoGetMinInt64(const MIIterator & mit) {
		//BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return MINUS_INF_64;
	}
	virtual _int64 DoGetMaxInt64(const MIIterator & mit) {
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
	virtual RCBString DoGetMaxString(const MIIterator & mit);
	virtual RCBString DoGetMinString(const MIIterator & mit);
	virtual bool DoIsDistinct() {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"To be implemented." );
		return (false);
	}
	virtual ushort DoMaxStringSize();
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator & mit);
	virtual void DoEvaluatePack(MIUpdatingIterator & mit, Descriptor & desc);
	const MysqlExpression::bhfields_cache_t& GetBHItems() const {
		return bhitems;
	}

private:
	mutable bool full_cache;
	mutable ValueSet cache;
	ColumnType expected_type;
	bool is_const;
	char* last_mit;		//to be used for tracing cache creation for mit values
	int last_mit_size;	//to be used for tracing cache creation for mit values
	void PrepareCache(const MIIterator& mit, const _int64& at_least = PLUS_INF_64);
};

#endif /* not INSETCOLUMN_H_INCLUDED */

