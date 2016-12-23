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


#ifndef MULTIVALCOLUMN_H_INCLUDED
#define MULTIVALCOLUMN_H_INCLUDED

#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"
#include "edition/vc/VirtualColumn.h"

class MysqlExpression;

/*! \brief A column defined by a set of expressions like in IN operator.
 * MultiValColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in MultiValColumn object may not exist physically, they can be computed on demand.
 */
class MultiValColumn : public VirtualColumn
{
public:
	typedef std::vector<VirtualColumn*> columns_t;

	class LazyValueInterface {
	public:
		virtual ~LazyValueInterface(){}
		typedef std::auto_ptr<LazyValueInterface> lazy_value_impl_t;
		inline lazy_value_impl_t Clone(void) const { return DoClone(); }
		inline RCBString GetString() const { return DoGetString(); }
		inline RCNum GetRCNum() const { return DoGetRCNum(); }
		inline RCValueObject GetValue() const { return DoGetValue(); }
		inline _int64 GetInt64() const { return DoGetInt64(); }
		inline bool IsNull() const { return DoIsNull(); }
	private:
		virtual lazy_value_impl_t DoClone() const = 0;
		virtual RCBString DoGetString() const = 0;
		virtual RCNum DoGetRCNum() const = 0;
		virtual RCValueObject DoGetValue() const = 0;
		virtual _int64 DoGetInt64() const = 0;
		virtual bool DoIsNull() const = 0;
	};

	class Iterator;
	class LazyValue {
	public:
		typedef std::auto_ptr<LazyValueInterface> lazy_value_impl_t;
		LazyValue( LazyValue const& lv ) : impl(lv.impl.get() ? lv.impl->Clone() : lazy_value_impl_t()) {}
		LazyValue& operator = ( LazyValue const& lv ) {
			if(&lv != this) {
				if(lv.impl.get())
					impl = lv.impl->Clone();
				else
					impl.reset();
			}
			return *this;
		}
		LazyValue* operator->(void) { return this; }
		RCBString GetString() { return impl->GetString(); }
		RCNum GetRCNum() { return impl->GetRCNum(); }
		RCValueObject GetValue() { return impl->GetValue(); }
		_int64 GetInt64() { return impl->GetInt64(); }
		bool IsNull() { return impl->IsNull(); }
	private:
		lazy_value_impl_t impl;
		LazyValue( lazy_value_impl_t impl_ ) : impl( impl_ ) {}
		friend class Iterator;
	};

	class IteratorInterface {
	public:
		virtual ~IteratorInterface() {}
		typedef std::auto_ptr<LazyValueInterface> lazy_value_impl_t;
		typedef std::auto_ptr<MultiValColumn::IteratorInterface> impl_t;
		inline void Next() { DoNext(); }
		inline lazy_value_impl_t GetValue() { return DoGetValue(); }
		inline bool NotEq( IteratorInterface const* it ) const { return DoNotEq(it); }
		impl_t clone() const { return DoClone(); }
	private:
		virtual impl_t DoClone() const = 0;
		virtual void DoNext() = 0;
		virtual lazy_value_impl_t DoGetValue() = 0;
		virtual bool DoNotEq( IteratorInterface const* ) const = 0;
	};

	class Iterator {
	public:
		typedef std::auto_ptr<MultiValColumn::IteratorInterface> impl_t;
		Iterator( Iterator const& it ) : owner(it.owner), impl(it.impl.get() ? it.impl->clone() : impl_t()) {}
		Iterator& operator = ( Iterator const& i ) {
			if( &i != this ) {
				if(i.impl.get())
					impl = i.impl->clone();
				else
					impl.reset();
			}
			return *this;
		}
		Iterator& operator ++(void) {
			impl->Next();
			return *this;
		}
		LazyValue operator*(void) { return LazyValue(impl->GetValue()); }
		LazyValue operator->(void) { return LazyValue(impl->GetValue()); }
		bool operator != ( Iterator const& it ) const {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(owner == it.owner);
			return ( impl->NotEq( it.impl.get() ) );
		}
	private:
		MultiValColumn const* owner;
		impl_t impl;
		Iterator( MultiValColumn const* owner_, impl_t impl_ ) : owner( owner_ ), impl( impl_ ) {}
		friend class MultiValColumn;
	};

	/*! \brief Create a column that represent const value.
	 *
	 * \param ct - type of column.
	 * \param mind - MultiIndex.
	 * \param expressions - a STL container of MysqlExpression*.
	 */
	MultiValColumn(ColumnType const& ct, MultiIndex* mind ) : VirtualColumn( ct, mind ) {}
	MultiValColumn(const MultiValColumn&c) : VirtualColumn(c) {bhitems = c.bhitems;}
	virtual ~MultiValColumn() {}
	virtual bool IsMultival() const { return true; }

	inline BHTribool Contains64(MIIterator const& mit, _int64 val)
	{ return DoContains64(mit, val); }
	inline BHTribool ContainsString(MIIterator const& mit, RCBString &val)
	{ return DoContainsString(mit, val); }
	inline BHTribool Contains(MIIterator const& mit, RCDataType const& val)
	{ return (DoContains(mit, val)); }
	virtual bool IsSetEncoded(AttributeType at, int scale)			{ return false; } // checks whether the set is constant and fixed size equal to the given one
	inline bool IsEmpty(MIIterator const& mit) { return (DoIsEmpty(mit)); }
	inline _int64 NoValues(MIIterator const& mit) { return DoNoValues(mit); }
	inline _int64 AtLeastNoDistinctValues(MIIterator const& mit, _int64 const at_least) { return DoAtLeastNoDistinctValues(mit, at_least); }
	inline bool ContainsNull(const MIIterator& mit) { return DoContainsNull(mit); }
	virtual bool CheckExists(MIIterator const& mit)  { return (DoNoValues(mit) > 0); }
	inline Iterator begin(MIIterator const& mit) { return Iterator(this, DoBegin(mit)); }
	inline Iterator end(MIIterator const& mit) { return Iterator(this, DoEnd(mit)); }
	inline RCValueObject GetSetMin(MIIterator const& mit) { return DoGetSetMin(mit); }
	inline RCValueObject GetSetMax(MIIterator const& mit) { return DoGetSetMax(mit); }
	inline void SetExpectedType(ColumnType const& ct) { DoSetExpectedType(ct); }
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit) { assert(0); return 0; }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &mit)  { assert(0);; }

protected:

	virtual RCValueObject DoGetSetMin(MIIterator const& mit) = 0;
	virtual RCValueObject DoGetSetMax(MIIterator const& mit) = 0;
	virtual _int64 DoNoValues(MIIterator const& mit) = 0;
	virtual _int64 DoAtLeastNoDistinctValues(MIIterator const&, _int64 const at_least) = 0;
	virtual bool DoContainsNull(const MIIterator&) = 0;
	virtual Iterator::impl_t DoBegin(MIIterator const&) = 0;
	virtual Iterator::impl_t DoEnd(MIIterator const&) = 0;
	virtual void DoSetExpectedType(ColumnType const&) = 0;
	virtual _int64 DoGetValueInt64(const MIIterator& mit) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"Invalid call for this type of column." );
		return ( 0 );
	}
	virtual double DoGetValueDouble(const MIIterator& mit) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"Invalid call for this type of column." );
		return ( 0 );
	}
	virtual bool DoIsNull(const MIIterator& mit) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"Invalid call for this type of column." );
		return ( false );
	}
	virtual void DoGetValueString(RCBString& s, const MIIterator& mit) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"Invalid call for this type of column." );
	}
	virtual RCValueObject DoGetValue(const MIIterator& mit, bool lookup_to_num) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"Invalid call for this type of column." );
		return ( RCValueObject() );
	}
	virtual _int64 DoGetNoNulls(const MIIterator& mit, bool val_nulls_possible)		{ return NULL_VALUE_64; }

	virtual bool DoRoughNullsOnly()	const	{return false;} //implement properly when DoRoughMin/Max are implemented non-trivially

	virtual _int64 DoGetSum(const MIIterator& mit, bool &nonnegative) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !"Invalid call for this type of column." );
		nonnegative = false;
		return NULL_VALUE_64;
	}
	virtual _int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind) {
		if(mind->TooManyTuples())
			return PLUS_INF_64;
		return mind->NoTuples();				// default
	}
	virtual BHTribool DoContains64(MIIterator const& mit, _int64 val) = 0;
	virtual BHTribool DoContainsString(MIIterator const& mit, RCBString &val) = 0;
	virtual BHTribool DoContains(MIIterator const&, RCDataType const&) = 0;
	virtual bool DoIsEmpty(MIIterator const&) = 0;

	bool CanCopy() const	{ return false; } // even copies refer to the same TempTable and TempTable cannot be used in parallel due to Attr paging
	bool IsThreadSafe()	 	{ return false; }

	MysqlExpression::bhfields_cache_t bhitems;  //items used in mysqlExpressions

};

#endif /* not MULTIVALCOLUMN_H_INCLUDED */

