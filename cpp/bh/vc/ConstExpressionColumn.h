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


#ifndef CONSTEXPRESSIONCOLUMN_H_INCLUDED
#define CONSTEXPRESSIONCOLUMN_H_INCLUDED

#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"
#include "ExpressionColumn.h"

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * ConstExpressionColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in ConstExpressionColumn object may not exist physically, they can be computed on demand.
 *
 */
class ConstExpressionColumn : public ExpressionColumn
{
public:

	/*! \brief Create a column that represent const value.
	 *
	 * \param val - value for const column.
	 * \param ct - type of const column.
	 */
	ConstExpressionColumn(MysqlExpression* expr, TempTable* temp_table, int temp_table_alias, MultiIndex* mind) :
			ExpressionColumn(expr, temp_table, temp_table_alias, mind) {

		//status = VC_CONST;
		dim = -1;
		if(params.size() == 0)
			last_val = expr->Evaluate();
	}

	ConstExpressionColumn(MysqlExpression* expr, ColumnType forced_ct, TempTable* temp_table, int temp_table_alias, MultiIndex* mind) :
			ExpressionColumn(expr, temp_table, temp_table_alias, mind) {
		// special case when naked column is a parameter
		//status = VC_CONST; Used for naked attributes as parameters
		dim = -1;
//		if(params.size() == 0)
//			last_val = expr->Evaluate();
		ct = forced_ct;
		ct = ct.RemovedLookup();
	}

	ConstExpressionColumn(ConstExpressionColumn const& cc) : ExpressionColumn(NULL, NULL, NULL_VALUE_32, NULL) {
		assert(params.size() == 0 && "cannot copy expressions");
		last_val = cc.last_val;
		ct = cc.ct;
		first_eval = cc.first_eval;

	}

	~ConstExpressionColumn() {
	}

	virtual bool IsConst() const { return true; }
	virtual void RequestEval(const MIIterator& mit, const int tta);
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit) { return last_val.Get64(); }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &mit)  { last_val.GetString(s); }
	virtual RCBString DecodeValue_S(_int64 code);	// lookup (physical) only
	virtual int	EncodeValue_S(RCBString &v)			{ return -1; }
	bool CanCopy() const {return params.size() == 0;}
	bool IsThreadSafe() {return params.size() == 0;}


protected:
	virtual _int64 DoGetValueInt64(const MIIterator &mit) { return last_val.IsNull() ? NULL_VALUE_64 : last_val.Get64(); }
	virtual bool DoIsNull(const MIIterator &mit) { return last_val.IsNull(); }
	virtual void DoGetValueString(RCBString& s, const MIIterator &mit) { last_val.GetString(s); }
	virtual double DoGetValueDouble(const MIIterator& mit);
	virtual RCValueObject DoGetValue(const MIIterator& mit, bool lookup_to_num);
	virtual _int64 DoGetMinInt64(const MIIterator &mit) { return last_val.IsNull() ? MINUS_INF_64 : last_val.Get64(); }
	virtual _int64 DoGetMaxInt64(const MIIterator &mit) { return last_val.IsNull() ? PLUS_INF_64  : last_val.Get64(); }
	virtual _int64 DoRoughMin()	{ return (last_val.IsNull() || last_val.Get64() == NULL_VALUE_64) ? MINUS_INF_64 : last_val.Get64(); }
	virtual _int64 DoRoughMax() { return (last_val.IsNull() || last_val.Get64() == NULL_VALUE_64) ? PLUS_INF_64  : last_val.Get64(); }
	virtual RCBString DoGetMaxString(const MIIterator &mit);
	virtual RCBString DoGetMinString(const MIIterator &mit);
	virtual _int64 DoGetNoNulls(const MIIterator &mit, bool val_nulls_possible) { return last_val.IsNull() ? mit.GetPackSizeLeft() : (mit.NullsPossibleInPack() ? NULL_VALUE_64 : 0); }
	virtual bool DoRoughNullsOnly()	const	{return last_val.IsNull();}
	virtual bool DoNullsPossible(bool val_nulls_possible) { return last_val.IsNull(); }
	virtual _int64 DoGetSum(const MIIterator &mit, bool &nonnegative);
	virtual bool DoIsDistinct() { return (mind->TooManyTuples() || mind->NoTuples() > 1) ? false : (!last_val.IsNull()); }
	virtual _int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind);
	virtual ushort DoMaxStringSize();		// maximal byte string length in column
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator& mit);
	virtual RSValue DoRoughCheck(const MIIterator& it, Descriptor& d);
	virtual void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc);
};

#endif /* not CONSTEXPRESSIONCOLUMN_H_INCLUDED */

