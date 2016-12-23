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

#include <assert.h>
#include "core/CompiledQuery.h"
#include "ExpressionColumn.h"
#include "core/MysqlExpression.h"
#include "core/RCAttr.h"

using namespace std;

ExpressionColumn::ExpressionColumn(MysqlExpression* expr, TempTable* temp_table, int temp_table_alias, MultiIndex* mind)
	: VirtualColumn(ColumnType(), mind), expr(expr), deterministic(expr ? expr->IsDeterministic() : true)
{
	const vector<JustATable*>* tables = &temp_table->GetTables();
	const vector<int>* aliases = &temp_table->GetAliases();

	if(expr) {
		vars = expr->GetVars(); 		// get all variables from complex term
		first_eval = true;
		//status = deterministic ? VC_EXPR : VC_EXPR_NONDET;
		// fill types for variables and create buffers for argument values
		int only_dim_number = -2;	// -2 = not used yet
		for(MysqlExpression::SetOfVars::iterator iter = vars.begin(); iter != vars.end(); iter++) {
			vector<int>::const_iterator ndx_it = find(aliases->begin(), aliases->end(), iter->tab);
			if(ndx_it != aliases->end()) {
				int ndx = int(distance(aliases->begin(), ndx_it));

				var_map.push_back(VarMap(*iter,(*tables)[ndx], ndx));
				if(only_dim_number == -2 || only_dim_number == ndx)
					only_dim_number = ndx;
				else
					only_dim_number = -1;		// more than one

				var_types[*iter] = (*tables)[ndx]->GetColumnType(var_map[var_map.size()-1].col_ndx);
				var_buf[*iter] = std::vector<MysqlExpression::value_or_null_info_t>(); //now empty, pointers inserted by SetBufs()
			} else if(iter->tab == temp_table_alias) {
				var_map.push_back(VarMap(*iter, temp_table, 0));
				if(only_dim_number == -2 || only_dim_number == 0)
					only_dim_number = 0;
				else
					only_dim_number = -1;		// more than one

				var_types[*iter] = temp_table->GetColumnType(var_map[var_map.size()-1].col_ndx);
				var_buf[*iter] = std::vector<MysqlExpression::value_or_null_info_t>(); //now empty, pointers inserted by SetBufs()
			} else {
				//parameter
				//parameter type is not available here, must be set later (EvalType())
				params.insert(*iter);
//				param_buf[*iter].second = NULL; //assigned by SetBufs()
			}

		}
		ct = ColumnType(expr->EvalType(&var_types)); //set the column type from expression result type
		expr->SetBufsOrParams(&var_buf);
//		expr->SetBufsOrParams(&param_buf);
		dim = ( only_dim_number >= 0 ? only_dim_number : -1 );

		//if (status == VC_EXPR && var_map.size() == 0 )
		//	status = VC_CONST;
	} else {
		assert(!"unexpected!!");
	}
}

ExpressionColumn::ExpressionColumn(const ExpressionColumn& ec) : VirtualColumn(ec), expr(ec.expr),
	vars(ec.vars), var_types(ec.var_types), var_buf(ec.var_buf), deterministic(ec.deterministic)
{
	var_map = ec.var_map;
}

ExpressionColumn::~ExpressionColumn()
{
	pguard.UnlockAll();
}

void ExpressionColumn::SetParamTypes(MysqlExpression::TypOfVars* types)
{
	expr->EvalType(types);
}


bool ExpressionColumn::FeedArguments(const MIIterator& mit)
{
	bool diff = first_eval;
	if(mit.Type() == MIIterator::MII_LOOKUP) {
		MILookupIterator *mit_lookup = (MILookupIterator*)(&mit);
		FeedLookupArguments(*mit_lookup);
		first_eval = false;
		return true;
	}
	for(vector<VarMap>::const_iterator iter = var_map.begin(); iter != var_map.end(); iter++) {
//		ValueOrNull v = iter->GetTabPtr()->GetComplexValue(mit[iter->dim], iter->col_ndx);
		ValueOrNull v = iter->tabp->GetComplexValue(mit[iter->dim], iter->col_ndx);
		v.MakeStringOwner();
		MysqlExpression::var_buf_t::iterator cache = var_buf.find(iter->var);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( cache != var_buf.end() );
		diff = diff || (v != cache->second.begin()->first);
		if(diff)
			for(std::vector<MysqlExpression::value_or_null_info_t>::iterator val_it = cache->second.begin(); val_it != cache->second.end(); val_it++)
				*((*val_it).second) = (*val_it).first = v;
	}
	first_eval = false;
	return (diff || !deterministic);
}

_int64 ExpressionColumn::DoGetValueInt64(const MIIterator& mit)
{
	if(FeedArguments(mit))
		last_val = expr->Evaluate();
	if(last_val.IsNull())
		return NULL_VALUE_64;
	return last_val.Get64();
}

bool ExpressionColumn::DoIsNull(const MIIterator &mit)
{
	if(FeedArguments(mit))
		last_val = expr->Evaluate();
	return last_val.IsNull();
}

void ExpressionColumn::DoGetValueString(RCBString& s, const MIIterator &mit) {
	if(FeedArguments(mit))
		last_val = expr->Evaluate();
	if(ATI::IsDateTimeType(TypeName())) {
		_int64 tmp;
		RCDateTime vd(last_val.Get64(), TypeName());
		vd.ToInt64(tmp);
		last_val.SetFixed(tmp);
	}
	last_val.GetString(s);
}

double ExpressionColumn::DoGetValueDouble(const MIIterator& mit) 
{
	double val = 0;
	if(FeedArguments(mit))
		last_val = expr->Evaluate();
	if (last_val.IsNull())
		val = NULL_VALUE_D;
	if (ATI::IsIntegerType(TypeName()))
		val = (double) last_val.Get64();
	else if(ATI::IsFixedNumericType(TypeName()))
		val = ((double) last_val.Get64()) / PowOfTen(ct.GetScale());
	else if(ATI::IsRealType(TypeName())) {
		val = last_val.GetDouble();
	} else if(ATI::IsDateTimeType(TypeName())) {
		RCDateTime vd(last_val.Get64(), TypeName());	// 274886765314048  ->  2000-01-01
		_int64 vd_conv = 0;
		vd.ToInt64(vd_conv);			// 2000-01-01  ->  20000101
		val = (double)vd_conv;
	} else if(ATI::IsStringType(TypeName())) {
		char *vs = last_val.GetStringCopy();
		if(vs)
			val = atof(vs);
		delete [] vs;
	} else
		assert(0 && "conversion to double not implemented");
	return val;
}

RCValueObject ExpressionColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num) 
{
	if(ATI::IsStringType((TypeName()))) {
		RCBString s;
		GetValueString(s,mit);
		return s;
	}
	if(ATI::IsIntegerType(TypeName()))
		return RCNum(GetValueInt64(mit), -1, false, TypeName());
	if(ATI::IsDateTimeType(TypeName()))
		return RCDateTime(GetValueInt64(mit), TypeName());
	if(ATI::IsRealType(TypeName()))
		return RCNum(GetValueInt64(mit), 0, true, TypeName());
	if(lookup_to_num || TypeName() == RC_NUM)
		return RCNum(GetValueInt64(mit), Type().GetScale());
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Illegal execution path");
	return RCValueObject();
}

_int64 ExpressionColumn::DoGetSum(const MIIterator &mit, bool &nonnegative)
{
	nonnegative = false;
	return NULL_VALUE_64; //not implemented
}

_int64 ExpressionColumn::DoGetMinInt64(const MIIterator &mit) {
	return MINUS_INF_64; //not implemented
}

_int64 ExpressionColumn::DoGetMaxInt64(const MIIterator &mit) {
	return PLUS_INF_64; //not implemented
}

RCBString ExpressionColumn::DoGetMinString(const MIIterator &mit) {
	return RCBString(); //not implemented
}

RCBString ExpressionColumn::DoGetMaxString(const MIIterator &mit) {
	return RCBString(); //not implemented
}

_int64 ExpressionColumn::DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind)
{
	if(mind->TooManyTuples())
		return PLUS_INF_64;
	return mind->NoTuples();		// default
}

ushort ExpressionColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return ct.GetPrecision();		// default
}

PackOntologicalStatus ExpressionColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	PackOntologicalStatus st = deterministic ? UNIFORM : NORMAL;  //will be used for 0 arguments
	// what about 0 arguments and null only?
	PackOntologicalStatus st_loc;
	for (vector<VarMap>::const_iterator it = var_map.begin(); it != var_map.end() ; it++) {
		// cast to remove const as GetPackOntologicalStatus() is not const
		st_loc = ((PhysicalColumn*)it->GetTabPtr()->GetColumn(it->col_ndx))->GetPackOntologicalStatus(mit.GetCurPackrow(it->dim));
		if(st_loc != UNIFORM && st_loc != NULLS_ONLY)
			return NORMAL;
		// if(NULLS_ONLY) and the expression may not be nontrivial on any null => NULLS_ONLY
	}
	return st;
}

void ExpressionColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& d)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Common path shall be used in case of ExpressionColumn.");
}

_int64 ExpressionColumn::DoRoughMin() {
	if(ATI::IsRealType(TypeName())) {
		double dmin = -(DBL_MAX);
		return *(_int64*)(&dmin);
	}
	return MINUS_INF_64;
}

_int64 ExpressionColumn::DoRoughMax() {
	if(ATI::IsRealType(TypeName())) {
		double dmax = DBL_MAX;
		return *(_int64*)(&dmax);
	}
	return PLUS_INF_64;
}

_int64 ExpressionColumn::DoGetNoNulls(MIIterator const&, bool val_nulls_possible) {
	return NULL_VALUE_64;
}

bool ExpressionColumn::DoIsDistinct() {
	return false;
}

bool ExpressionColumn::ExactlyOneLookup()		// the column is a deterministic expression on exactly one lookup column
{
	if(!deterministic)
		return false;
	vector<VarMap>::const_iterator iter = var_map.begin(); 
	if(iter == var_map.end() || !iter->GetTabPtr()->GetColumnType(iter->col_ndx).IsLookup())
		return false;				// not a lookup
	iter++;
	if(iter != var_map.end())		// more than one column
		return false;
	return true;
}

VirtualColumnBase::VarMap ExpressionColumn::GetLookupCoordinates()
{
	vector<VarMap>::const_iterator iter = var_map.begin(); 
	return *iter;
}

void ExpressionColumn::FeedLookupArguments(MILookupIterator& mit)
{
	vector<VarMap>::const_iterator iter = var_map.begin(); 
	RCAttr *col = (RCAttr*)(iter->GetTabPtr()->GetColumn(iter->col_ndx));
	ValueOrNull v = RCBString();	
	if(mit.IsValid() && mit[0] != NULL_VALUE_64 && mit[0] < col->dic->CountOfUniqueValues())
		v = col->DecodeValue_S(mit[0]);

	MysqlExpression::var_buf_t::iterator cache = var_buf.find(iter->var);
	for(std::vector<MysqlExpression::value_or_null_info_t>::iterator val_it = cache->second.begin(); val_it  != cache->second.end(); val_it ++)
		*((*val_it ).second) = (*val_it ).first = v;

	if(mit.IsValid() && mit[0] != NULL_VALUE_64 && mit[0] >= col->dic->CountOfUniqueValues())
		mit.Invalidate();
}

void ExpressionColumn::LockSourcePacks(const MIIterator& mit)
{
	for (vector<VarMap>::iterator it = var_map.begin(); it != var_map.end() ; it++)
		it->tabp = it->GetTabPtr().get();
	pguard.LockPackrow(mit);		// TODO: do something with a return code, when it becomes nontrivial
}
