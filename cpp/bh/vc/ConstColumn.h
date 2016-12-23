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


#ifndef CONSTCOLUMN_H_INCLUDED
#define CONSTCOLUMN_H_INCLUDED

#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"
#include "edition/vc/VirtualColumn.h"

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * ConstColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in ConstColumn object may not exist physically, they can be computed on demand.
 *
 */
class ConstColumn : public VirtualColumn
{
public:

	/*! \brief Create a column that represent const value.
	 *
	 * \param val - value for const column.
	 * \param ct - type of const column.
	 */
	ConstColumn(ValueOrNull const& val, ColumnType const& c, bool shift_to_UTC = false);
	ConstColumn(const RCValueObject& v, const ColumnType& c);
	ConstColumn(ConstColumn const& cc) : VirtualColumn(cc.ct, cc.mind), value(cc.value) {}
	~ConstColumn() {}

	virtual bool IsConst() const { return true; }

	const MysqlExpression::bhfields_cache_t& GetBHItems() const { static MysqlExpression::bhfields_cache_t const dummy; return dummy; }
	char *ToString(char p_buf[], size_t buf_ct) const;
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit) { return value.Get64(); }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &mit)  { value.GetString(s); }
	bool IsThreadSafe() {return true;}
	bool CanCopy() const {return true;}

protected:
	virtual _int64 DoGetValueInt64(const MIIterator &mit) { return value.IsNull() ? NULL_VALUE_64 : value.Get64(); }
	virtual bool DoIsNull(const MIIterator &mit) { return value.IsNull(); }
	virtual void DoGetValueString(RCBString& s, const MIIterator &mit);
	virtual double DoGetValueDouble(const MIIterator& mit);
	virtual RCValueObject DoGetValue(const MIIterator& mit, bool lookup_to_num);
	virtual _int64 DoGetMinInt64(const MIIterator &mit) { return value.IsNull() || value.sp ? MINUS_INF_64 : value.Get64(); }
	virtual _int64 DoGetMaxInt64(const MIIterator &mit) { return value.IsNull() || value.sp ? PLUS_INF_64  : value.Get64(); }
	virtual _int64 DoRoughMin()	{ return value.IsNull() || value.sp ? MINUS_INF_64 : value.Get64(); }
	virtual _int64 DoRoughMax() { return value.IsNull() || value.sp ? PLUS_INF_64  : value.Get64(); }
	virtual _int64 DoGetMinInt64Exact(const MIIterator &) { return value.IsNull() ? NULL_VALUE_64 : value.Get64(); }
	virtual _int64 DoGetMaxInt64Exact(const MIIterator &) { return value.IsNull() ? NULL_VALUE_64 : value.Get64(); }
	virtual RCBString DoGetMaxString(const MIIterator &mit);
	virtual RCBString DoGetMinString(const MIIterator &mit);
	virtual _int64 DoGetNoNulls(const MIIterator &mit, bool val_nulls_possible) { return value.IsNull() ? mit.GetPackSizeLeft() : (mit.NullsPossibleInPack() ? NULL_VALUE_64 : 0); }
	virtual bool DoRoughNullsOnly()	const				  { return value.IsNull(); }
	virtual bool DoNullsPossible(bool val_nulls_possible) { return value.IsNull(); }
	virtual _int64 DoGetSum(const MIIterator &mit, bool &nonnegative);
	virtual bool DoIsDistinct() { return false; }
	virtual _int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind);
	virtual _int64 GetExactDistVals();
	virtual ushort DoMaxStringSize();		// maximal byte string length in column
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator& mit);
	virtual void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc);
	ValueOrNull value;
};

#endif /* not CONSTCOLUMN_H_INCLUDED */

