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
#include "ConstExpressionColumn.h"
#include "core/MysqlExpression.h"
#include "core/RCAttr.h"

void ConstExpressionColumn::RequestEval(const MIIterator& mit, const int tta)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	first_eval = true;
	//TODO: check if parameters were changed before reeval
	if(expr->GetItem()->type() == Item_bhfield::get_bhitem_type()) {
		// a special case when a naked column is a parameter
		last_val = ((Item_bhfield*) (expr->GetItem()))->GetCurrentValue();
		last_val.MakeStringOwner();
	} else
		last_val = expr->Evaluate();
#endif
}

double ConstExpressionColumn::DoGetValueDouble(const MIIterator& mit) {
	assert(ATI::IsNumericType(TypeName()));
	double val = 0;
	if (last_val.IsNull())
		return NULL_VALUE_D;
	if (ATI::IsIntegerType(TypeName()))
		val = (double) last_val.Get64();
	else if(ATI::IsFixedNumericType(TypeName()))
		val = ((double) last_val.Get64()) / PowOfTen(ct.GetScale());
	else if(ATI::IsRealType(TypeName())) {
		union { double d; _int64 i;} u;
		u.i = last_val.Get64();
		val = u.d;
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

RCValueObject ConstExpressionColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num) {

	if (last_val.null)
		return RCValueObject();

	if(ATI::IsStringType((TypeName()))) {
		RCBString s;
		last_val.GetString(s);
		return s;
	}
	if(ATI::IsIntegerType(TypeName()))
		return RCNum(last_val.Get64(), -1, false, TypeName());
	if(ATI::IsDateTimeType(TypeName()))
		return RCDateTime(last_val.GetDateTime64() , TypeName());
	if(ATI::IsRealType(TypeName()))
		return RCNum(last_val.Get64(), 0, true, TypeName());
	if(lookup_to_num || TypeName() == RC_NUM)
		return RCNum((_int64)last_val.Get64(), Type().GetScale());
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Illegal execution path");
	return RCValueObject();
}

_int64 ConstExpressionColumn::DoGetSum(const MIIterator &mit, bool &nonnegative)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!ATI::IsStringType(TypeName()));
	nonnegative = true;
	if(last_val.IsNull())
		return NULL_VALUE_64;		// note that this is a bit ambiguous: the same is for sum of nulls and for "not implemented"
	if( ATI::IsRealType(TypeName()) ) {
		double res = last_val.GetDouble() * mit.GetPackSizeLeft();
		return *(_int64 *)&res;
	}
	return (last_val.Get64() * mit.GetPackSizeLeft());
}

RCBString ConstExpressionColumn::DoGetMinString(const MIIterator &mit) {
	RCBString s;
	last_val.GetString(s);
	return s;
}

RCBString ConstExpressionColumn::DoGetMaxString(const MIIterator &mit) {
	RCBString s;
	last_val.GetString(s);
	return s;
}

_int64 ConstExpressionColumn::DoGetApproxDistVals(bool incl_nulls, RoughMultiIndex* rough_mind)
{
	return 1;
}

ushort ConstExpressionColumn::DoMaxStringSize()		// maximal byte string length in column
{
	return ct.GetPrecision();
}

PackOntologicalStatus ConstExpressionColumn::DoGetPackOntologicalStatus(const MIIterator &mit)
{
	if (last_val.IsNull())
		return NULLS_ONLY;
	return UNIFORM;
}

void ConstExpressionColumn::DoEvaluatePack(MIUpdatingIterator& mit, Descriptor& desc)
{
	assert(0); 	// comparison of a const with a const should be simplified earlier
}

RSValue ConstExpressionColumn::DoRoughCheck(const MIIterator& mit, Descriptor& d)
{
	return RS_SOME; //not implemented
}

RCBString ConstExpressionColumn::DecodeValue_S(_int64 code)
{
	RCBString s; 
	last_val.GetString(s); 
	return s;
}


