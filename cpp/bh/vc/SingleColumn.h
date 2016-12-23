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


#ifndef SINGLECOLUMN_H_INCLUDED
#define SINGLECOLUMN_H_INCLUDED

#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"
#include "core/PhysicalColumn.h"
#include "edition/vc/VirtualColumn.h"

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * SingleColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in SingleColumn object may not exist physically, they can be computed on demand.
 *
 */
class SingleColumn : public VirtualColumn
{
public:

	/*! \brief Create a virtual column for a given column
	 *
	 * \param col the column to be "wrapped" by VirtualColumn
	 * \param _mind the multiindex to which the SingleColumn is attached.
	 * \param alias the alias number of the source table
	 * \param col_no number of the column in the table, negative if col of TempTable
	 * \param source_table pointer to the table holding \endcode col
	 * \param dim dimension number in the multiindex holding the column.
	 * \e note: not sure if the pointer to the MultiIndex is necessary
	 */
	SingleColumn(PhysicalColumn* col, MultiIndex* mind, int alias, int col_no, JustATable* source_table, int dim);
	SingleColumn(const SingleColumn& col);
	~SingleColumn();
	virtual _uint64 ApproxAnswerSize(Descriptor& d) { return col->ApproxAnswerSize(d); }
	PhysicalColumn* GetPhysical() const { return col; }
	bool IsConst() const {return false;}
	virtual single_col_t IsSingleColumn() const {return col->ColType() == PhysicalColumn::ATTR ? SC_ATTR : SC_RCATTR;}
	virtual char* ToString(char p_buf[], size_t buf_ct) const;
	virtual void TranslateSourceColumns(std::map<PhysicalColumn *, PhysicalColumn *> &);
	virtual void GetNotNullValueString(RCBString& s, const MIIterator& mit) { col->GetNotNullValueString(mit[dim], s); }
	virtual _int64 GetNotNullValueInt64(const MIIterator& mit)				{ return col->GetNotNullValueInt64(mit[dim]); }
	virtual bool IsDistinctInTable()										{ return col->IsDistinct(mind->GetFilter(dim)); }
	bool IsTempTableColumn() const	{ return col->ColType() == PhysicalColumn::ATTR; }
	virtual void GetTextStat(TextStat &s) { col->GetTextStat(s, mind->GetFilter(dim)); }
	bool CanCopy() const			{ return (col->ColType() != PhysicalColumn::ATTR); }	// cannot copy TempTable, as it may be paged
	bool IsThreadSafe()				{ return (col->ColType() != PhysicalColumn::ATTR); }	// TODO: for ATTR check if not materialized and not buffered

protected:

	virtual _int64 DoGetValueInt64(const MIIterator &mit) {
		return col->GetValueInt64(mit[dim]);
	}
	virtual bool DoIsNull(const MIIterator &mit) {
		return col->IsNull(mit[dim]);
	}
	virtual void DoGetValueString(RCBString& s, const MIIterator &mit) {
		col->GetValueString(mit[dim], s);
	}

	virtual double DoGetValueDouble(const MIIterator& mit);
	virtual RCValueObject DoGetValue(const MIIterator& mit, bool lookup_to_num);
	virtual _int64 DoGetMinInt64(const MIIterator &mit);
	virtual _int64 DoGetMaxInt64(const MIIterator &mit);
	virtual _int64 DoGetMinInt64Exact(const MIIterator &mit);
	virtual _int64 DoGetMaxInt64Exact(const MIIterator &mit);
	virtual RCBString DoGetMinString(const MIIterator &);
	virtual RCBString DoGetMaxString(const MIIterator &);
	virtual _int64 DoRoughMin()
		{	return col->RoughMin(mind->GetFilter(dim));	}
	virtual _int64 DoRoughMax()
	{	return col->RoughMax(mind->GetFilter(dim)); }
	virtual double RoughSelectivity();

	virtual _int64 DoGetNoNulls(const MIIterator &mit, bool val_nulls_possible) {
		_int64 res;
		if(mit.GetCurPackrow(dim) >= 0 && !mit.NullsPossibleInPack(dim)) {	// if outer nulls possible - cannot calculate precise result
			if(!val_nulls_possible)
				return 0;
			res = col->GetNoNulls(mit.GetCurPackrow(dim));
			if(res == 0 || mit.WholePack(dim))
				return res;
		}
		return NULL_VALUE_64;
	}

	virtual bool DoRoughNullsOnly() const {
		return col->RoughNullsOnly();
	}

	virtual bool DoNullsPossible(bool val_nulls_possible) {
		if(mind->NullsExist(dim))
			return true;
		if(val_nulls_possible && col->GetNoNulls(-1) != 0)
			return true;
		return false;
	}
	virtual _int64 DoGetSum(const MIIterator &mit, bool &nonnegative);
	virtual _int64 DoGetApproxSum(const MIIterator &mit, bool &nonnegative);

	virtual bool DoIsDistinct() {
		return (col->IsDistinct(mind->GetFilter(dim)) && mind->CanBeDistinct(dim));
	}

	virtual _int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind);
	virtual _int64 GetExactDistVals();
	virtual ushort DoMaxStringSize();		// maximal byte string length in column
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator& mit);
	virtual RSValue DoRoughCheck(const MIIterator& it, Descriptor& d);
	virtual void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc);
	virtual bool TryToMerge(Descriptor &d1,Descriptor &d2);
	virtual RCBString DecodeValue_S(_int64 code)	{ return col->DecodeValue_S(code); }
	virtual int	EncodeValue_S(RCBString &v)			{ return col->EncodeValue_S(v); }
	virtual std::vector<_int64> GetListOfDistinctValues(MIIterator const& mit);
	virtual void DisplayAttrStats();

	const MysqlExpression::bhfields_cache_t& GetBHItems() const {static MysqlExpression::bhfields_cache_t const dummy; return dummy;}
	PhysicalColumn* col; 			//!= NULL if SingleColumn encapsulates a single column only (no expression)
	//	this an easily accessible copy var_map[0].tab->GetColumn(var_map[0].col_ndx)
};

#endif /* not SINGLECOLUMN_H_INCLUDED */

