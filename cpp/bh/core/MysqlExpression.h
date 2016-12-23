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

#ifndef _MYSQLEXPRESSION_H_
#define _MYSQLEXPRESSION_H_

#include <set>
#include <map>

#include "util/DestructionDebugger.h"
#include "common/CommonDefinitions.h"
#include "ValueOrNull.h"
#include "Item_bhfield.h"

class Item_bhfield;

// Wrapper for MySQL expression tree
class MysqlExpression : public boost::noncopyable DEBUG_STATEMENT( DEBUG_COMA public DestructionDebugger<MysqlExpression> )
{
public:
	// map which tells how to replace Item_field's with Item_bhfield
	typedef std::map<Item*, VarID>                Item2VarID;
	typedef std::map<VarID, DataType>             TypOfVars; // map of types of variables
	typedef std::map<VarID, ValueOrNull*>         BufOfVars; // map of buffers to pass current values of variables during execution
	typedef std::map<VarID, std::set<Item_bhfield*> >       	bhfields_cache_t;
	typedef std::pair<ValueOrNull, ValueOrNull*>  				value_or_null_info_t;
	typedef std::map<VarID, std::vector<value_or_null_info_t> > 	var_buf_t;
	typedef std::set<VarID>                     				SetOfVars; // set of IDs of variables occuring in expression
	typedef int (MysqlExpression::*ItemProcess) (void* arg);
	typedef enum {STRING_NOTSTRING, STRING_TIME, STRING_NORMAL} StringType;

	MysqlExpression(Item* item, Item2VarID& item2varid, bool use_ibexpr);
	virtual ~MysqlExpression();

	static bool				SanityAggregationCheck(Item* item, std::set<Item*>& aggregations, bool toplevel = true, bool* has_aggregation = NULL);
	static bool				HasAggregation(Item* item);

	virtual MysqlExpression::SetOfVars&		GetVars();
//	virtual void			SetBufs(BufOfVars* bufs)	{ SetBufsOrParams(bufs, true); }
	void					SetBufsOrParams(var_buf_t* bufs);
	virtual DataType		EvalType(TypOfVars* tv = NULL);
	StringType 				GetStringType();
	virtual ValueOrNull		Evaluate();

	//! Use if IBExpression not created from this (already existing): advance the current VarID in bhfileds
	void 					RemoveUnusedVarID()	{RemoveUnusedVarID(item);}
	Item*					GetItem() { return item; }
	const bhfields_cache_t& GetBHItems() { return bhfields_cache;}
	bool					IsDeterministic() { return deterministic; }
	bool					IsIBExpression() { return ib_expression; }
	/*! \brief Tests if other MysqlExpression is same as this one.
	 *
	 * \param other - equality to this MysqlExpression is being questioned.
	 * \return True iff this MysqlExpression and other MysqlExpression are equivalent.
	 */
	bool operator == ( MysqlExpression const& other ) const;

	static ValueOrNull		ItemInt2ValueOrNull(Item* item);
	static ValueOrNull		ItemReal2ValueOrNull(Item* item);
	static ValueOrNull		ItemDecimal2ValueOrNull(Item* item, int dec_scale = -1);
	static ValueOrNull		ItemString2ValueOrNull(Item* item, int max_str_len = -1, AttributeType a_type = RC_STRING);

	static _int64&			AsInt(ValueOrNull::Value& x)				{ return x; }
	static double&			AsReal(ValueOrNull::Value& x)			{ return *(double*)&x; }
	static ValueOrNull::Value&			AsValue(_int64& x)			{ return x; }
	static ValueOrNull::Value&			AsValue(double& x)			{ return *(_int64*)&x; }
	static const _int64&	AsInt(const ValueOrNull::Value& x)		{ return x; }
	static const double&	AsReal(const ValueOrNull::Value& x)		{ return *(double*)&x; }
	static const ValueOrNull::Value&		AsValue(const _int64& x)		{ return x; }
	static const ValueOrNull::Value&		AsValue(const double& x)	{ return *(_int64*)&x; }

private:
	DataType	type;		// type of result of the expression
//	virtual int Proc_SetParams(void* vals)	{ SetBufsOrParams((BufOfVars*)vals, true); return 0; }

	// Implements MysqlExpression::SetBufs() and MysqlExpression::SetParams().
	// Which exactly is indicated by \param alloc - 'true' indicates SetParams().
//	void		SetBufsOrParams(BufOfVars* bufs, bool alloc);

	static bool				HandledResultType(Item* item);
	static bool				HandledFieldType(Item_result type);
	Item_bhfield*			GetBhfieldItem(Item_field*);

	enum	TransformDirection { FORWARD, BACKWARD };

	//! Returns the root of transformed tree. New root may be different than the old one!
	Item*					TransformTree(Item* root, TransformDirection dir);

	//! Checks whether an operation on MySQL decimal returned an error.
	//! If so, throws RCException.
	static void				CheckDecimalError(int err);
	void RemoveUnusedVarID(Item* root);

	Item*		item;
	Item_result	mysql_type;
	uint		decimal_precision, decimal_scale;
	Item2VarID*	item2varid;
	bhfields_cache_t bhfields_cache;
	SetOfVars vars;		//variable IDs in the expression, same as in bhfields_cache; filled in TransformTree
	bool deterministic;
	bool ib_expression;
};

#endif
