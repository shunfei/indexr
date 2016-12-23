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


#ifndef EXPRESSIONCOLUMN_H_INCLUDED
#define EXPRESSIONCOLUMN_H_INCLUDED

#include "edition/vc/VirtualColumn.h"
#include "core/MIUpdatingIterator.h"
#include "core/PackGuardian.h"

class TempTable;
class JustATable;

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * ExpressionColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in ExpressionColumn object may not exist physically, they can be computed on demand.
 *
 */
class ExpressionColumn : public VirtualColumn
{
public:
	//the struct below is for precomputing relations between variables, tables and dimensions

	/*! \brief Create a virtual column for given expression
	 *
	 * \param expr expression defining the column
	 * \param temp_table TempTable for which the column is created
	 * \param temp_table_alias alias of TempTable for which the column is created
	 * \param mind the multiindex to which the ExpressionColumn is attached.
	 * \e note: not sure if the connection to the MultiIndex is necessary
	 * \param tables list of tables corresponding to dimensions in \e mind,
	 * ExpressionColumn must access the tables to fetch expression arguments
	 *
	 * The constructor takes from \e expr a list of dimensions/tables providing arguments
	 * for the expression. Therefore the list of multiindex dimensions necessary for evaluation
	 * if computed from \e expr
	 */
	ExpressionColumn(MysqlExpression* expr, TempTable* temp_table, int temp_table_alias, MultiIndex* mind);
	ExpressionColumn(const ExpressionColumn&);

	const MysqlExpression::bhfields_cache_t& GetItems() const;
	~ExpressionColumn();
	void SetParamTypes(MysqlExpression::TypOfVars* types);
	bool IsConst() const {return false;}
	virtual bool IsDeterministic() { return expr->IsDeterministic();}
	virtual _int64 GetNotNullValueInt64(const MIIterator &mit) { return DoGetValueInt64(mit); }
	virtual void GetNotNullValueString(RCBString& s, const MIIterator &mit)  { DoGetValueString(s, mit); }
	MysqlExpression::StringType GetStringType() { return expr->GetStringType();}

	/////////////// Special functions for expressions on lookup //////////////////////
	virtual bool ExactlyOneLookup();		// the column is a deterministic expression on exactly one lookup column, return the coordinates of this column
	virtual VirtualColumnBase::VarMap GetLookupCoordinates();
	virtual void FeedLookupArguments(MILookupIterator& mit);
	void LockSourcePacks(const MIIterator& mit);
	void LockSourcePacks(const MIIterator& mit, int);
	/////////////// Data access //////////////////////

protected:

	virtual _int64 DoGetValueInt64(const MIIterator&);
	virtual bool DoIsNull(const MIIterator&);
	virtual void DoGetValueString(RCBString&, const MIIterator&);
	virtual double DoGetValueDouble(const MIIterator&);
	virtual RCValueObject DoGetValue(const MIIterator&, bool);
	virtual _int64 DoGetMinInt64(const MIIterator &);
	virtual _int64 DoGetMaxInt64(const MIIterator &);
	virtual _int64 DoRoughMin();
	virtual _int64 DoRoughMax();

	virtual RCBString DoGetMaxString(const MIIterator &);
	virtual RCBString DoGetMinString(const MIIterator &);
	virtual _int64 DoGetNoNulls(const MIIterator &, bool val_nulls_possible);
	virtual bool DoRoughNullsOnly()	const												{ return false; }
	virtual bool DoNullsPossible(bool val_nulls_possible) 								{ return true; }
	virtual _int64 DoGetSum(const MIIterator &, bool &nonnegative);
	virtual bool DoIsDistinct();
	virtual _int64 DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind);
	virtual ushort DoMaxStringSize();		// maximal byte string length in column
	virtual PackOntologicalStatus DoGetPackOntologicalStatus(const MIIterator&);
	virtual void DoEvaluatePack(MIUpdatingIterator& mit, Descriptor&);
	const MysqlExpression::bhfields_cache_t& GetBHItems() const {return expr->GetBHItems();}
	MysqlExpression* expr;			//!= NULL if ExpressionColumn encapsulates an expression. Note - a constant is an expression

private:

	/*! \brief Set variable buffers with values for a given iterator.
	 *
	 * \param mit - iterator that new variable values shall be based on.
	 * \return true if new values are exactly the same as previous (already existing.
	 */
	bool FeedArguments(const MIIterator& mit);


	//if ExpressionColumn ExpressionColumn encapsulates an expression these sets are used to interface with MysqlExpression
	MysqlExpression::SetOfVars vars;
	MysqlExpression::TypOfVars var_types;
	mutable MysqlExpression::var_buf_t var_buf;

	//! value for a given row is always the same or not? e.g. currenttime() is not deterministic
	bool deterministic;


};

#endif /* not EXPRESSIONCOLUMN_H_INCLUDED */

