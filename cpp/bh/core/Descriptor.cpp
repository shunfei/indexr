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

#include "Descriptor.h"
#include "vc/ConstColumn.h"
#include "vc/ConstExpressionColumn.h"
#include "vc/ExpressionColumn.h"
#include "vc/TypeCastColumn.h"
#include "vc/InSetColumn.h"
#include "vc/SubSelectColumn.h"
#include "vc/SingleColumn.h"
#include "QueryOperator.h"
#include "ValueOrNull.h"
#include "Query.h"
#include "RoughValue.h"
#include "ParametrizedFilter.h"
#include "ConditionEncoder.h"
#include "system/ConnectionInfo.h"
#include <boost/scoped_ptr.hpp>

using namespace std;

extern VirtualColumn* CreateVCCopy(VirtualColumn* vc);

int SortDescriptor::operator==(const SortDescriptor& sec)
{
	return (dir==sec.dir) && (vc == sec.vc);
}

Descriptor::Descriptor() :
lop(O_AND), sharp(false), encoded(false), done(false), evaluation(0), delayed(false), table(NULL), tree(NULL), left_dims(0), 
right_dims(0), rv(RS_UNKNOWN), like_esc('\\'), desc_t(DT_NOT_KNOWN_YET), collation(DTCollation()), null_after_simplify(false)
{
	op = O_UNKNOWN_FUNC;
}

Descriptor::Descriptor(TempTable* t, int no_dims)		// no_dims is a destination number of dimensions (multiindex may be smaller at this point)
	:	op(O_OR_TREE), lop(O_AND), sharp(false), encoded(false), done(false),
		evaluation(0), delayed(false),
		table(t), tree(NULL), left_dims(no_dims), right_dims(no_dims), rv(RS_UNKNOWN), 
		like_esc('\\'), desc_t(DT_NOT_KNOWN_YET), collation(DTCollation()), null_after_simplify(false)
{ 
	assert(table);
}

Descriptor::~Descriptor()
{
	delete tree;
}

Descriptor::Descriptor(const Descriptor& desc)
{
	attr = desc.attr;
	op = desc.op;
	lop = desc.lop;
	val1 = desc.val1;
	val2 = desc.val2;
	sharp = desc.sharp;
	encoded = desc.encoded;
	done = desc.done;
	evaluation = desc.evaluation;
	desc_t = desc.desc_t;
	delayed = desc.delayed;
	table = desc.table;
	collation = desc.collation;
	tree = NULL;
	if(desc.tree)
		tree = new DescTree(*desc.tree);
	left_dims = desc.left_dims;
	right_dims = desc.right_dims;
	rv = desc.rv;
	like_esc = desc.like_esc; 
	null_after_simplify = desc.null_after_simplify;
}

Descriptor::Descriptor(CQTerm e1, Operator pr, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_escape) : lop(O_AND), sharp(false), 
	encoded(false), done(false), evaluation(0), delayed(false), table(t), tree(NULL), left_dims(no_dims), 
	right_dims(no_dims), rv(RS_UNKNOWN), desc_t(DT_NOT_KNOWN_YET), collation(DTCollation()), null_after_simplify(false)
{
	op = pr;
	attr = e1;
	val1 = e2;
	val2 = e3;
	like_esc = like_escape, 
	CalculateJoinType();
}

Descriptor::Descriptor(DescTree* sec_tree, TempTable* t, int no_dims) : op(O_OR_TREE), lop(O_AND), sharp(false), encoded(false), done(false), 
	evaluation(0), delayed(false), table(t), left_dims(no_dims), right_dims(no_dims), rv(RS_UNKNOWN), like_esc('\\'),
	desc_t(DT_NOT_KNOWN_YET), collation(DTCollation()), null_after_simplify(false)
{
	tree = NULL;
	if(sec_tree)
		tree = new DescTree(*sec_tree);
	CalculateJoinType();
}

Descriptor::Descriptor(TempTable* t, VirtualColumn *v1, Operator pr, VirtualColumn *v2, VirtualColumn *v3) :
lop(O_AND), sharp(false), encoded(false), done(false), evaluation(0), delayed(false), table(t), tree(NULL), left_dims(0), 
right_dims(0), rv(RS_UNKNOWN), like_esc('\\'), desc_t(DT_NOT_KNOWN_YET), collation(DTCollation()), null_after_simplify(false)
{
	attr = CQTerm();
	val1 = CQTerm();
	val2 = CQTerm();
	attr.vc = v1;
	val1.vc = v2;
	val2.vc = v3;
	op = pr;
	CalculateJoinType();
}

void Descriptor::swap(Descriptor& d)
{
	std::swap(op, d.op);
	std::swap(lop, d.lop);
	std::swap(attr, d.attr);
	std::swap(val1, d.val1);
	std::swap(val2, d.val2);
	std::swap(sharp, d.sharp);
	std::swap(encoded, d.encoded);
	std::swap(done, d.done);
	std::swap(evaluation, d.evaluation);
	std::swap(delayed, d.delayed);
	std::swap(rv, d.rv);
	std::swap(like_esc, d.like_esc);
	std::swap(table, d.table);
	std::swap(tree, d.tree);
	std::swap(left_dims, d.left_dims);
	std::swap(right_dims, d.right_dims);
	std::swap(desc_t, d.desc_t);
	std::swap(collation, d.collation);
	std::swap(null_after_simplify, d.null_after_simplify);
}

Descriptor& Descriptor::operator=(const Descriptor& d)
{
	if(&d == this)
		return *this;
	op = d.op;
	lop = d.lop;
	attr = d.attr;
	val1 = d.val1;
	val2 = d.val2;
	sharp = d.sharp;
	encoded = d.encoded;
	done = d.done;
	evaluation = d.evaluation;
	delayed = d.delayed;
	rv = d.rv;
	like_esc = d.like_esc;
	table = d.table;
	delete tree;
	tree = NULL;
	if(d.tree)
		tree = new DescTree(*d.tree);
	left_dims = d.left_dims;
	right_dims = d.right_dims;
	desc_t = d.desc_t;
	collation = d.collation;
	null_after_simplify = d.null_after_simplify;
	return *this;
}

int Descriptor::operator==(const Descriptor& sec) const
{
	return (
		attr == sec.attr	&&
		op == sec.op		&&
		lop == sec.lop		&&
		val1 == sec.val1	&&
		val2 == sec.val2	&&
		sharp == sec.sharp	&&
		encoded == sec.encoded &&
		delayed == sec.delayed &&
		like_esc == sec.like_esc &&
		table == sec.table &&
		left_dims == sec.left_dims && 		//check: dims order in vectors can be different
		right_dims == sec.right_dims &&		//check: dims order in vectors can be different
		collation.collation == sec.collation.collation &&
		collation.derivation == sec.collation.derivation &&
		null_after_simplify == sec.null_after_simplify
		);
}

bool Descriptor::operator<=(const Descriptor& sec)  const
{
	if(*this == sec)
		return true;
	MIIterator dummy_mit;
	if(attr == sec.attr && (!val1.vc || val1.vc->IsConst()) && (!val2.vc || val2.vc->IsConst()) &&
		(!sec.val1.vc || sec.val1.vc->IsConst()) && (!sec.val2.vc || sec.val2.vc->IsConst())) {
		switch(op) {
			case O_EQ: 
				if(sec.op == O_BETWEEN && val1.vc->GetValue(dummy_mit) >= sec.val1.vc->GetValue(dummy_mit) &&
					 val1.vc->GetValue(dummy_mit) <= sec.val2.vc->GetValue(dummy_mit))
					return true;
				if(sec.op == O_IN && static_cast<MultiValColumn*>(sec.val1.vc)->Contains(dummy_mit, *val1.vc->GetValue(dummy_mit).Get()) == true )
					return true;
				if((sec.op == O_LESS || sec.op == O_LESS_EQ || sec.op == O_MORE || sec.op == O_MORE_EQ || sec.op == O_EQ) &&
					RCValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op, '\\'))
					return true;
				break;
			case O_LESS_EQ:
				if((sec.op == O_LESS || sec.op == O_LESS_EQ) &&
					RCValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op, '\\'))
					return true;
				break;
			case O_MORE_EQ:
				if((sec.op == O_MORE || sec.op == O_MORE_EQ) &&
					RCValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op, '\\'))
					return true;
				break;
			case O_LESS:
				if((sec.op == O_LESS || sec.op == O_LESS_EQ) &&
					RCValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), O_LESS_EQ, '\\'))
					return true;
				break;
			case O_MORE:
				if((sec.op == O_MORE || sec.op == O_MORE_EQ) &&
					RCValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), O_MORE_EQ, '\\'))
					return true;
				break;
			case O_BETWEEN:
				if(sec.op == O_BETWEEN && 
					val1.vc->GetValue(dummy_mit) >= sec.val1.vc->GetValue(dummy_mit) &&
					val2.vc->GetValue(dummy_mit) <= sec.val2.vc->GetValue(dummy_mit))
					return true;
				if((sec.op == O_LESS || sec.op == O_LESS_EQ) &&
					RCValueObject::compare(val2.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op, '\\'))
					return true;
				if((sec.op == O_MORE || sec.op == O_MORE_EQ) &&
					RCValueObject::compare(val1.vc->GetValue(dummy_mit), sec.val1.vc->GetValue(dummy_mit), sec.op, '\\'))
					return true;
				break;
			case O_IN: {
				//MultiValColumn* mvc = static_cast<MultiValColumn*>(val1.vc);
				//if( sec.op == O_EQ && mvc->Contains(dummy_mit, *sec.val1.vc->GetValue(dummy_mit).Get()) == true )
				//	return true;
				break;
			}
			default:
				break;
		}
		
	}
	return false;
}

bool Descriptor::operator<(const Descriptor& desc) const
{
	return evaluation < desc.evaluation;
}

bool Descriptor::EqualExceptOuter(const Descriptor& sec)
{
	return (
		attr == sec.attr	&&
		op == sec.op		&&
		lop == sec.lop		&&
		val1 == sec.val1	&&
		val2 == sec.val2	&&
		sharp == sec.sharp	&&
		like_esc == sec.like_esc &&
		table == sec.table  &&
		collation.collation == sec.collation.collation &&
		collation.derivation == sec.collation.derivation && 
		null_after_simplify == sec.null_after_simplify
		);
}

void Descriptor::SwitchSides()		// change "a<b" into "b>a" etc; throw error if not possible (e.g. between)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(op == O_EQ || op == O_NOT_EQ || op == O_LESS || op == O_MORE ||
		op == O_LESS_EQ || op == O_MORE_EQ);
	SwitchOperator(op);
	CQTerm p = attr;
	attr = val1;
	val1 = p;
}

bool Descriptor::IsType_AttrValOrAttrValVal() const		// true if "phys column op val or column between val and val or "
{														// or "phys_column IS NULL/NOT NULL"
	if(attr.vc == NULL || !attr.vc->IsSingleColumn())
		return false;
	return ((val1.vc && val1.vc->IsConst()) ||
			(op == O_IS_NULL || op == O_NOT_NULL))
			&&
			(!val2.vc || (val2.vc && val2.vc->IsConst()));
}

bool Descriptor::IsType_AttrMultiVal() const
{
	return 	op != O_BETWEEN && op != O_NOT_BETWEEN &&
		attr.vc && attr.vc->IsSingleColumn() &&
		val1.vc && val1.vc->IsMultival() &&
		val1.vc->IsConst();
}

bool Descriptor::IsType_Subquery()
{
	return (attr.vc->IsSubSelect() || (val1.vc && val1.vc->IsSubSelect()) || (val2.vc && val2.vc->IsSubSelect()));
}

bool Descriptor::IsType_JoinSimple() const							// true if more than one table involved
{
	assert(desc_t != DT_NOT_KNOWN_YET);		
	return desc_t == DT_SIMPLE_JOIN ;
}

bool Descriptor::IsType_AttrAttr() const						// true if "column op column" from one table
{
	if(attr.vc == NULL || val1.vc == NULL || !attr.vc->IsSingleColumn() || !val1.vc->IsSingleColumn() || val2.vc ||
		IsType_Join())
		return false;

	return true;
}

bool Descriptor::IsType_IBExpression() const		// only columns, constants and IBExpressions
{
	if(attr.vc == NULL)
		return false;
	if((attr.vc->IsSingleColumn() || attr.vc->IsConst() || attr.vc->IsIBExpression()) &&
		(val1.vc == NULL || val1.vc->IsSingleColumn() || val1.vc->IsConst() || val1.vc->IsIBExpression()) &&
		(val2.vc == NULL || val2.vc->IsSingleColumn() || val2.vc->IsConst() || val2.vc->IsIBExpression()))
		return true;
	return false;
}

void Descriptor::CalculateJoinType()
{
	if(IsType_OrTree()) {
		DimensionVector used_dims(right_dims.Size());		// the most current number of dimensions
		tree->DimensionUsed(used_dims);
		if(used_dims.NoDimsUsed() > 1) {
			desc_t = DT_COMPLEX_JOIN;
			left_dims = used_dims;
		} else
			desc_t = DT_NON_JOIN;
		return;
	}
	set<int> tables_a, tables_v1, tables_v2, tables_all;
	if(attr.vc) {
		tables_a = attr.vc->GetDimensions();
		tables_all.insert(tables_a.begin(), tables_a.end());
	}
	if(val1.vc) {
		tables_v1 = val1.vc->GetDimensions();
		tables_all.insert(tables_v1.begin(), tables_v1.end());
	}
	if(val2.vc) {
		tables_v2 = val2.vc->GetDimensions();
		tables_all.insert(tables_v2.begin(), tables_v2.end());
	}
	if(tables_all.size() <= 1)
		desc_t = DT_NON_JOIN;
	else if(tables_a.size() > 1 || tables_v1.size() > 1 || tables_v2.size() > 1 ||
		// 1 BETWEEN a1 AND b1
		(tables_a.size() == 0 && (op == O_BETWEEN || op == O_NOT_BETWEEN)) )
		desc_t = DT_COMPLEX_JOIN;
	else
		desc_t = DT_SIMPLE_JOIN;
}

bool Descriptor::IsType_JoinComplex() const
{
	// Make sure to use CalculateJoinType() before
	assert(desc_t != DT_NOT_KNOWN_YET);		
	return desc_t == DT_COMPLEX_JOIN ;
}

RSValue Descriptor::EvaluateRoughlyPack(const MIIterator& mit)
{
	if(IsType_OrTree())
		return tree->root->EvaluateRoughlyPack(mit);
	RSValue r = RS_SOME;
	if(attr.vc /*&& !attr.vc->IsConst()*/)
		r = attr.vc->RoughCheck(mit, *this);
	if(rv == RS_UNKNOWN)
		rv = r;
	else if(rv == RS_NONE && r != RS_NONE)
		rv = RS_SOME;
	else if(rv == RS_ALL && r != RS_ALL)
		rv = RS_SOME;
	return r;
}

void Descriptor::Simplify(bool in_having)
{
	MEASURE_FET("Descriptor::Simplify(...)");
	static MIIterator const mit(NULL);
	if(op == O_FALSE || op == O_TRUE)
		return;

	if(IsType_OrTree()) {
		BHTribool res = tree->Simplify(in_having);
		if(res == true)
			op = O_TRUE;
		else if(res == false)
			op = O_FALSE;
		else if(!tree->root->desc.IsType_OrTree()) {
			Descriptor new_desc = tree->root->desc;
			new_desc.left_dims = left_dims;
			new_desc.right_dims = right_dims;
			*this = new_desc;
		}
		return;
	}

	bool res = false;

	if(attr.vc && attr.vc->IsConst() &&
	   val1.vc && !val1.vc->IsConst() &&
	   (op == O_EQ || op == O_NOT_EQ || op == O_LESS || op == O_LESS_EQ || op == O_MORE || op == O_MORE_EQ)) {
		SwitchSides();
	}
	if(Query::IsAllAny(op) && dynamic_cast<MultiValColumn*>(val1.vc) == NULL)
		Query::UnmarkAllAny(op);
	if( (attr.vc && (!attr.vc->IsConst() || (in_having && attr.vc->IsParameterized()))) ||
		(val1.vc && (!val1.vc->IsConst() || (in_having && val1.vc->IsParameterized()))) ||
		(val2.vc && (!val2.vc->IsConst() || (in_having && val2.vc->IsParameterized()))) ) {
		return;
	}

	// from here attr, val1 and val2 (if exists) are constants

	VirtualColumn* acc = attr.vc;
	VirtualColumn* v1cc = val1.vc;
	VirtualColumn* v2cc = val2.vc;

	if( op == O_BETWEEN && (v1cc->IsNull(mit) || v2cc->IsNull(mit)) ) {
		op = O_FALSE;
		null_after_simplify = true;
		return;
	} else if( op == O_NOT_BETWEEN && (v1cc->IsNull(mit) || v2cc->IsNull(mit)) ) {
		if(v1cc->IsNull(mit) && v2cc->IsNull(mit)) {
			op = O_FALSE;
			null_after_simplify = true;
			return;
		}
		if(v1cc->IsNull(mit)) {		// a not between null and x  ==>  a > x
			op = O_MORE;
			val1 = val2;
			v1cc = v2cc;
		}
		else 						// a not between x and null  ==>  a < x
			op = O_LESS;
	}
	if(IsSetOperator(op)) {
		null_after_simplify = IsNull_Set(mit, op);
		res = CheckSetCondition(mit, op);
		op = res ? O_TRUE : O_FALSE;
		return;
	}

	switch(op) {
		case O_IS_NULL:
			res = acc->IsNull(mit) ? true : false;
			break;
		case O_NOT_EXISTS:
		case O_EXISTS: {
			MultiValColumn* mvc = static_cast<MultiValColumn*>(attr.vc);
			res = mvc->CheckExists(mit);
			if(op == O_NOT_EXISTS)
				res = !res;
			break;
		}
		case O_NOT_NULL:
			res = acc->IsNull(mit) ? false : true;
			break;
		case O_BETWEEN:
		case O_NOT_BETWEEN: {
			assert(sharp == false);
			if(acc->IsNull(mit)) {
				null_after_simplify = true;
				res = false;
			} else if(ATI::IsTxtType(acc->TypeName()) || ATI::IsTxtType(v1cc->TypeName()) || ATI::IsTxtType(v2cc->TypeName())) {
				RCBString s1, s2, s3;
				acc->GetValueString(s1, mit);
				v1cc->GetValueString(s2, mit);
				v2cc->GetValueString(s3, mit);
				res = (strcmp(s1 , s2) >= 0 && strcmp(s1, s3) <= 0);
			} else {
				RCValueObject rv1 = acc->GetValue(mit);
				RCValueObject rv2 = v1cc->GetValue(mit);
				RCValueObject rv3 = v2cc->GetValue(mit);
				res = RCValueObject::compare(rv1, rv2, O_MORE_EQ, '\\') && RCValueObject::compare(rv1, rv3, O_LESS_EQ, '\\');
			}

			if(op == O_NOT_BETWEEN && !acc->IsNull(mit))
				res = !res;
			break;
		}
		default: {
			RCValueObject rv1 = acc->GetValue(mit);
			RCValueObject rv2 = v1cc->GetValue(mit);
			res = RCValueObject::compare(rv1, rv2, op, like_esc);
			if(res == false && (rv1.IsNull() || rv2.IsNull()))
				null_after_simplify = true;
		}
	}
	op = res ? O_TRUE : O_FALSE;
	return;
}


/**
Appends as string representation of this descriptor's operator
and data values to a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
*/
char* Descriptor::ToString(char buffer[], size_t buffer_size)
{
	/*
	Only the first few of the several steps needed to completely
	refactor this code were needed to resolve the valgrind issue.  The
	next steps in refactoring this code would be done via 'Replace
	Conditional with Polymorphism'.
	i.e. Create the operator objects with a common appendToString()
	member overriden to function as one of the groups below and
	by moving append*ToString() calls into the QueryOperator
	base/derived classes (the operator_object parameter will
	not be needed, etc).
	One last note, the Operator classes would need to be the objects
	that are sentient of the relationships with the objects around them.
	*/
	auto_ptr<const QueryOperator> operator_object(CreateQueryOperator(op));

	switch(operator_object->GetType()) {
		case O_ESCAPE:
		case O_EXISTS:
		case O_NOT_EXISTS:
		{
			size_t offset = strlen(buffer);
			AppendString(buffer, buffer_size, "(exists expr.)", 14, offset);
			break;
		 }
		case O_OR_TREE:
		{
			size_t offset = strlen(buffer);
			AppendString(buffer, buffer_size, "(OR expr.)", 10, offset);
			//if(tree) {
			//	if(!tree->root->left)
			//		tree->root->desc.ToString(buffer, buffer_size);
			//	else {
			//		tree->root->left->desc.ToString(buffer, buffer_size);
			//		tree->root->right->desc.ToString(buffer, buffer_size);
			//	}
			//}
			break;
		}
		case O_FALSE:
		case O_TRUE:
			AppendConstantToString(buffer, buffer_size, operator_object.get());
			break;

		case O_IS_NULL:
		case O_NOT_NULL:
			AppendUnaryOperatorToString(buffer, buffer_size, operator_object.get());
			break;

		case O_EQ:
		case O_EQ_ALL:
		case O_EQ_ANY:
		case O_LESS:
		case O_LESS_ALL:
		case O_LESS_ANY:
		case O_MORE:
		case O_MORE_ALL:
		case O_MORE_ANY:
		case O_LESS_EQ:
		case O_LESS_EQ_ALL:
		case O_LESS_EQ_ANY:
		case O_MORE_EQ:
		case O_MORE_EQ_ALL:
		case O_MORE_EQ_ANY:
		case O_NOT_EQ:
		case O_NOT_EQ_ALL:
		case O_NOT_EQ_ANY:
		case O_IN:
		case O_NOT_IN:
		case O_LIKE:
		case O_NOT_LIKE:
			AppendBinaryOperatorToString(buffer, buffer_size, operator_object.get());
			break;

		case O_BETWEEN:
		case O_NOT_BETWEEN:
			AppendTernaryOperatorToString(buffer, buffer_size, operator_object.get());
			break;
		default:
			break;
	}
	if(!IsInner()) {
		sprintf(buffer + strlen(buffer), " (outer");
		for(int i = 0; i < right_dims.Size(); ++i)
			if(right_dims[i])
				sprintf(buffer + strlen(buffer), " %d", i);
		sprintf(buffer + strlen(buffer), ")");
	}
	return buffer;
}

/**
Creates a query operator of the requested type.

Interim implementation until an operator class hierarchy
is put in place.  This call is only a stop-gap measure
to that effect.

\param The Operator enumeration of the desired type of query operator.
*/
const QueryOperator * Descriptor::CreateQueryOperator(Operator type) const
{
	const char* string_rep[OPERATOR_ENUM_COUNT] = {
		"=",						// O_EQ
		"=ALL",						// O_EQ_ALL
		"=ANY",						// O_EQ_ANY
		"<>",						// O_NOT_EQ
		"<>ALL",					// O_NOT_EQ_ALL
		"<>ANY",					// O_NOT_EQ_ANY
		"<",						// O_LESS
		"<ALL",						// O_LESS_ALL
		"<ANY",						// O_LESS_ANY
		">",						// O_MORE
		">ALL",			            // O_MORE_ALL
		">ANY",				        // O_MORE_ANY
		"<=",						// O_LESS_EQ
		"<=ALL",					// O_LESS_EQ_ALL
		"<=ANY",					// O_LESS_EQ_ANY
		">=",						// O_MORE_EQ
		">=ALL",					// O_MORE_EQ_ALL
		">=ANY",					// O_MORE_EQ_ANY
		"IS NULL",					// O_IS_NULL
		"IS NOT NULL",				// O_NOT_NULL
		"BET.",						// O_BETWEEN
		"NOT BET.",					// O_NOT_BETWEEN
		"LIKE",						// O_LIKE
		"NOT LIKE",					// O_NOT_LIKE
		"IN",						// O_IN
		"NOT IN",					// O_NOT_IN
		"EXISTS",					// O_EXISTS
		"NOT EXISTS",				// O_NOT_EXISTS
		"FALSE",					// O_FALSE
		"TRUE",						// O_TRUE
		"ESCAPE",					// O_ESCAPE
		"OR TREE"					// O_OR_TREE
	};

	return new QueryOperator(type, string_rep[type]);
}

/**
Simple string concatenation into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The string to concatenate.
\param The length of the string to be concatenated.
\param The destination offset within buffer for the concatenation.
*/
void Descriptor::AppendString(char *buffer, size_t buffer_size,
							  const char *string, size_t string_length, size_t offset) const
{
	if((offset + string_length) > buffer_size) {
		throw InternalRCException("Bounds Check in Descriptor::AppendString");
	}
	strcpy(buffer + offset, string);
}

/**
Appends a string representation of a constant operator into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendConstantToString(char buffer[], size_t size,
										const QueryOperator *operator_object) const
{
	const std::string &constant = operator_object->AsString();

	size_t offset = strlen(buffer);

	AppendString(buffer, size, " ", 1, offset++);
	AppendString(buffer, size, constant.data(), constant.length(), offset);
}

/**
Appends a string representation of a unary operator (attr op)
into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendUnaryOperatorToString(char buffer[], size_t size,
											 const QueryOperator *operator_object) const
{
	attr.ToString(buffer, size, 0);
	AppendConstantToString(buffer, size, operator_object);
}

/**
Appends a string representation of a binary operator (attr op val1)
into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendBinaryOperatorToString(char buffer[], size_t size,
											  const QueryOperator* operator_object) const
{
	AppendUnaryOperatorToString(buffer, size, operator_object);
	AppendString(buffer, size, " ", 1, strlen(buffer));
	val1.ToString(buffer, size, 0);
}

/**
Appends a string representation of a special-case ternary
operator into a buffer.

\param The destination buffer.
\param The capacity of the destination buffer.
\param The QueryOperator object managed by this Descriptor object.
*/
void Descriptor::AppendTernaryOperatorToString(char buffer[], size_t size,
											   const QueryOperator* operator_object) const
{
	AppendBinaryOperatorToString(buffer, size, operator_object);
	size_t offset = strlen(buffer);
	AppendString(buffer, size, " AND ", 5, offset);
	val2.ToString(buffer, size, 0);
	if(sharp)
	{
		offset = strlen(buffer);
		AppendString(buffer, size, " (sharp)", 8, offset);
	}
}

//////////////////////////////////////////////////////////////////////////////////////////

void Descriptor::DimensionUsed(DimensionVector &dims)
{
	if(tree)
		tree->DimensionUsed(dims);
	if(attr.vc)
		attr.vc->MarkUsedDims(dims);
	if(val1.vc)
		val1.vc->MarkUsedDims(dims);
	if(val2.vc)
		val2.vc->MarkUsedDims(dims);
}

void Descriptor::LockSourcePacks(const MIIterator& mit)
{
	if(tree)
		tree->root->PrepareToLock(0);
	if(attr.vc)
		attr.vc->LockSourcePacks(mit);
	if(val1.vc)
		val1.vc->LockSourcePacks(mit);
	if(val2.vc)
		val2.vc->LockSourcePacks(mit);
}

#ifdef __BH_COMMUNITY__
void Descriptor::LockSourcePacks(const MIIterator& mit, int th_no)
{
	LockSourcePacks(mit);
}
#endif

void Descriptor::UnlockSourcePacks()
{
	if(tree)
		tree->root->UnlockSourcePacks();
	if(attr.vc)
		attr.vc->UnlockSourcePacks();
	if(val1.vc)
		val1.vc->UnlockSourcePacks();
	if(val2.vc)
		val2.vc->UnlockSourcePacks();
}

void Descriptor::EvaluatePackImpl(MIUpdatingIterator & mit)
{
	MEASURE_FET("Descriptor::EvaluatePackImpl(...)");

	// Check if we can delegate evaluation of descriptor to physical column
    if(encoded)
        attr.vc->EvaluatePack(mit, *this);
	else if(IsType_OrTree()) {
		// Prepare rough values to be stored inside the tree
		tree->root->ClearRoughValues();
		tree->root->EvaluateRoughlyPack(mit);
		tree->root->EvaluatePack(mit);
	}
	else if(RequiresUTFConversions(collation)){
        while(mit.IsValid()){
            if(CheckCondition_UTF(mit) == false)
                mit.ResetCurrent();
            ++mit;
            if(mit.PackrowStarted())
                break;
        }
	} else {
		if(IsType_Subquery() && op != O_OR_TREE) {
			// pack based optimization of corr. subq. by using RoughQuery
			BHTribool res = RoughCheckSubselectCondition(mit, PACK_BASED);
			if(res == false)
				mit.ResetCurrentPack();
			else if(res == BHTRIBOOL_UNKNOWN) {
				//int true_c = 0, false_c = 0, unkn_c = 0;
				while(mit.IsValid()) {
					// row based optimization of corr. subq. by using RoughQuery
					res = RoughCheckSubselectCondition(mit, ROW_BASED);
					//if(res == false)
					//	false_c++;
					//else if(res == true)
					//	true_c++;
					//else
					//	unkn_c++;
					if(res == false)
						mit.ResetCurrent();
					else if(res == BHTRIBOOL_UNKNOWN && CheckCondition(mit) == false)
						mit.ResetCurrent();
					++mit;
					if(mit.PackrowStarted())
						break;
				}
				//cout << "# of skipped subqueries: " << true_c << "/" << false_c << "/" << unkn_c << " -> " << (true_c + false_c) << " / " << (true_c + false_c + unkn_c) << endl;
			}
		} else {
			while(mit.IsValid()){
				if(CheckCondition(mit) == false)
					mit.ResetCurrent();
				++mit;
				if(mit.PackrowStarted())
					break;
			}
		}
	}
}

void Descriptor::EvaluatePack(MIUpdatingIterator& mit)
{
	MEASURE_FET("Descriptor::EvaluatePack(...)");
	LockSourcePacks(mit);
    EvaluatePackImpl(mit);
}

void Descriptor::UpdateVCStatistics()					// Apply all the information from constants etc. to involved VC
{
	MEASURE_FET("Descriptor::UpdateVCStatistics(...)");
	if(attr.vc == NULL)
		return;
	if(op == O_IS_NULL) {
		attr.vc->SetLocalNullsOnly(true);
		return;
	}
	attr.vc->SetLocalNullsPossible(false);
	if((attr.vc->Type().IsNumeric() || attr.vc->Type().IsLookup()) && encoded) {		// if not encoded, we may have an incompatible comparison here (e.g. double between int and int)
		_int64 v1 = NULL_VALUE_64;
		_int64 v2 = NULL_VALUE_64;
		if(op == O_BETWEEN) {
			if(val1.vc)
				v1 = val1.vc->RoughMin();
			if(val2.vc)
				v2 = val2.vc->RoughMax();			
		} else if(op == O_EQ) {
			if(val1.vc) {
				v1 = val1.vc->RoughMin();
				v2 = val1.vc->RoughMax();						
				val1.vc->SetLocalMinMax(v1, v2);			// apply to both sides
			}
		} else if(op == O_LESS || op == O_LESS_EQ) {
			if(val1.vc)
				v2 = val1.vc->RoughMax();						
		} else if(op == O_MORE || op == O_MORE_EQ) {
			if(val1.vc)
				v1 = val1.vc->RoughMin();						
		}
		int v1_scale = val1.vc ? val1.vc->Type().GetScale() : 0;
		int v2_scale = val2.vc ? val2.vc->Type().GetScale() : v1_scale;
		RCNum v1_conv(v1,  v1_scale); 
		RCNum v2_conv(v2,  v2_scale); 
		if(v1 != NULL_VALUE_64 && v1 != PLUS_INF_64 && v1 != MINUS_INF_64)
			v1_conv = v1_conv.ToDecimal(attr.vc->Type().GetScale());
		if(v2 != NULL_VALUE_64 && v2 != PLUS_INF_64 && v2 != MINUS_INF_64)
			v2_conv = v2_conv.ToDecimal(attr.vc->Type().GetScale());
		attr.vc->SetLocalMinMax(v1_conv.Value(), v2_conv.Value());
	}
	if(op == O_IN && val1.vc->IsConst()) {
		MultiValColumn* mv_vc = static_cast<MultiValColumn*>(val1.vc);
		MIDummyIterator mit(1);
		_int64 v = mv_vc->NoValues(mit);
		attr.vc->SetLocalDistVals(v);
	}
	// TODO: all other info, e.g.:
	//       - min/max from sets
	//       - max_len from string equalities and LIKE
	//       - second column stats for attr-attr inequalities
}

bool Descriptor::CheckCondition_UTF(const MIIterator& mit)
{
	MEASURE_FET("Descriptor::CheckCondition_UTF(...)");
	if(op == O_TRUE)
		return true;
	else if(op == O_FALSE)
		return false;

	bool result = true;

	if(encoded && attr.vc->Type().IsLookup())		// encoded to numerical comparison
		return CheckCondition(mit);

	// Assumption: LockSourcePack externally done.
	if(op == O_EQ) {		// fast track for the most common operator
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && val1.vc && RequiresUTFConversions(collation));
		if(attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
			return false;
		RCBString s1, s2;
		attr.vc->GetNotNullValueString(s1, mit);
		val1.vc->GetNotNullValueString(s2, mit);
		return CollationStrCmp(collation, s1, s2) == 0;
	} else if(op == O_NOT_NULL) {
		if(attr.vc->IsNull(mit))
			return false;
	} else if(op == O_IS_NULL) {
		if(!attr.vc->IsNull(mit))
			return false;
	} else if(op == O_EXISTS || op == O_NOT_EXISTS) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<SubSelectColumn*>(attr.vc));
		SubSelectColumn* sub = static_cast<SubSelectColumn*>(attr.vc);
		bool is_nonempty = sub->CheckExists(mit);
		if((op == O_EXISTS && !is_nonempty) || (op == O_NOT_EXISTS && is_nonempty))
			return false;
	} else if(op == O_BETWEEN || op == O_NOT_BETWEEN) {
		if(attr.vc->IsNull(mit))
			return false;
		// need to consider three value logic
		RCBString s1, s2, s3;
		attr.vc->GetNotNullValueString(s1, mit);
		val1.vc->GetValueString(s2, mit);
		val2.vc->GetValueString(s3, mit);
		bool attr_ge_val1 = (sharp ? CollationStrCmp(collation, s1, s2) > 0 : CollationStrCmp(collation, s1, s2) >= 0);
		bool attr_le_val2 = (sharp ? CollationStrCmp(collation, s1, s3) < 0 : CollationStrCmp(collation, s1, s3) <= 0);
		BHTribool val1_res, val2_res;
		if(encoded) {	// Rare case: for encoded conditions treat NULL as +/- inf.
			val1_res = val1.vc->IsNull(mit) ? true : BHTribool(attr_ge_val1);
			val2_res = val2.vc->IsNull(mit) ? true : BHTribool(attr_le_val2);
		} else {
			val1_res = val1.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(attr_ge_val1);
			val2_res = val2.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(attr_le_val2);
		}
		if(op == O_BETWEEN) {
			if(val1_res != true || val2_res != true)
				return false;
		} else {
			if(val1_res != false && val2_res != false)
				return false;
		}
	} else if(IsSetOperator(op)) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && dynamic_cast<MultiValColumn*>(val1.vc));
		return CheckSetCondition_UTF(mit, op);
	} else if(op == O_LIKE || op == O_NOT_LIKE) {
		if(attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
			return false;
		RCBString v, pattern;
		attr.vc->GetNotNullValueString(v, mit);
		val1.vc->GetNotNullValueString(pattern, mit);
		int x = wildcmp(collation, v.val, v.val + v.len, pattern.val, pattern.val + pattern.len, like_esc, '_', '%');
		result = (x == 0 ? true : false);
		if(op == O_LIKE)	
			return result;
		else
			return !result;
	} else if(IsType_OrTree()) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(tree);
		return tree->root->CheckCondition((MIIterator&)mit);
	} else { // all other logical operators: >, >=, <, <=
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && val1.vc);
		if(attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
			return false;
		RCBString s1, s2;
		attr.vc->GetNotNullValueString(s1, mit);
		val1.vc->GetNotNullValueString(s2, mit);
		if(!CollationStrCmp(collation, s1, s2, op))
			return false;
	}
	return result;
}

bool Descriptor::CheckCondition(const MIIterator& mit)
{
	MEASURE_FET("Descriptor::CheckCondition(...)");
	if(op == O_TRUE)
		return true;
	else if(op == O_FALSE)
		return false;

	bool result = true;

	// Assumption: LockSourcePacks externally done.
	if(op == O_EQ) {		// fast track for the most common operator
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && val1.vc);
		// nulls checked in operator ==
		if(!(attr.vc->GetValue(mit) == val1.vc->GetValue(mit)))
			return false;
	} else if(op == O_NOT_NULL) {
		if(attr.vc->IsNull(mit))
			return false;
	} else if(op == O_IS_NULL) {
		if(!attr.vc->IsNull(mit))
			return false;
	} else if(op == O_EXISTS || op == O_NOT_EXISTS) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<SubSelectColumn*>(attr.vc));
		SubSelectColumn* sub = static_cast<SubSelectColumn*>(attr.vc);
		bool is_nonempty = sub->CheckExists(mit);
		if((op == O_EXISTS && !is_nonempty) || (op == O_NOT_EXISTS && is_nonempty))
			return false;
	} else if(op == O_BETWEEN || op == O_NOT_BETWEEN) {
		if(attr.vc->IsNull(mit))
			return false;
		// need to consider three value logic
		BHTribool val1_res, val2_res;
		if(encoded) {
			if(attr.vc->Type().IsString() && !attr.vc->Type().IsLookup()) {
				RCBString val, v1, v2;
				attr.vc->GetNotNullValueString(val, mit);
				val1.vc->GetValueString(v1, mit);
				val2.vc->GetValueString(v2, mit);
				val1_res = v1.IsNull() ? true : (sharp ? BHTribool(val > v1) : BHTribool(val >= v1));
				val2_res = v2.IsNull() ? true : (sharp ? BHTribool(val < v2) : BHTribool(val <= v2));
			} else if (!attr.vc->Type().IsFloat()) {
				_int64 val = attr.vc->GetNotNullValueInt64(mit);
				val1_res = val1.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(val >= val1.vc->GetNotNullValueInt64(mit));
				val2_res = val2.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(val <= val2.vc->GetNotNullValueInt64(mit));
			} else {	// Rare case: for encoded conditions treat NULL as +/- inf.
				RCValueObject rcvo1 = attr.vc->GetValue(mit, false);
				val1_res = val1.vc->IsNull(mit) ? true : (sharp ? BHTribool(rcvo1 > val1.vc->GetValue(mit, false)) : BHTribool(rcvo1 >= val1.vc->GetValue(mit, false)));
				val2_res = val2.vc->IsNull(mit) ? true : (sharp ? BHTribool(rcvo1 < val2.vc->GetValue(mit, false)) : BHTribool(rcvo1 <= val2.vc->GetValue(mit, false)));
			}
		} else {
			RCValueObject rcvo1 = attr.vc->GetValue(mit, false);
			val1_res = val1.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(rcvo1 >= val1.vc->GetValue(mit, false));
			val2_res = val2.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(rcvo1 <= val2.vc->GetValue(mit, false));
		}
		if(op == O_BETWEEN) {
			if(val1_res != true || val2_res != true)
				return false;
		} else {
			if(val1_res != false && val2_res != false)
				return false;
		}
	} else if(IsSetOperator(op)) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && dynamic_cast<MultiValColumn*>(val1.vc));
		return CheckSetCondition(mit, op);
	} else if(IsType_OrTree()) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(tree);
		return tree->root->CheckCondition((MIIterator&)mit);
	} else { // all other logical operators: >, >=, <, <=
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && val1.vc);
		if(attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
			return false;
		if(!RCValueObject::compare(attr.vc->GetValue(mit), val1.vc->GetValue(mit), op, like_esc))
			return false;
	}
	return result;
}

bool Descriptor::IsNull(const MIIterator& mit)
{
	MEASURE_FET("Descriptor::IsNull(...)");
	if(null_after_simplify)
		return true;
	if(op == O_TRUE || op == O_FALSE)
		return false;

	// Assumption: LockSourcePacks externally done.
	if(op == O_EQ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && val1.vc);
		if(attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
			return true;
	} else if(op == O_NOT_NULL || op == O_IS_NULL) {
		return false;
	} else if(op == O_EXISTS || op == O_NOT_EXISTS) {
		return false;
	} else if(op == O_BETWEEN || op == O_NOT_BETWEEN) {
		if(attr.vc->IsNull(mit))
			return true;
		// need to consider three value logic
		BHTribool val1_res, val2_res;
		if(encoded) {
			if(attr.vc->Type().IsString() && !attr.vc->Type().IsLookup()) {
				RCBString val, v1, v2;
				attr.vc->GetNotNullValueString(val, mit);
				val1.vc->GetValueString(v1, mit);
				val2.vc->GetValueString(v2, mit);
				val1_res = v1.IsNull() ? true : (sharp ? BHTribool(val > v1) : BHTribool(val >= v1));
				val2_res = v2.IsNull() ? true : (sharp ? BHTribool(val < v2) : BHTribool(val <= v2));
			} else if (!attr.vc->Type().IsFloat()) {
				_int64 val = attr.vc->GetNotNullValueInt64(mit);
				val1_res = val1.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(val >= val1.vc->GetNotNullValueInt64(mit));
				val2_res = val2.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(val <= val2.vc->GetNotNullValueInt64(mit));
			} else {	// Rare case: for encoded conditions treat NULL as +/- inf.
				RCValueObject rcvo1 = attr.vc->GetValue(mit, false);
				val1_res = val1.vc->IsNull(mit) ? true : (sharp ? BHTribool(rcvo1 > val1.vc->GetValue(mit, false)) : BHTribool(rcvo1 >= val1.vc->GetValue(mit, false)));
				val2_res = val2.vc->IsNull(mit) ? true : (sharp ? BHTribool(rcvo1 < val2.vc->GetValue(mit, false)) : BHTribool(rcvo1 <= val2.vc->GetValue(mit, false)));
			}
		} else {
			RCValueObject rcvo1 = attr.vc->GetValue(mit, false);
			val1_res = val1.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(rcvo1 >= val1.vc->GetValue(mit, false));
			val2_res = val2.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(rcvo1 <= val2.vc->GetValue(mit, false));
		}
		if(BHTribool::And(val1_res, val2_res) == BHTRIBOOL_UNKNOWN)
			return true;
	} else if(IsSetOperator(op)) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && dynamic_cast<MultiValColumn*>(val1.vc));
		return IsNull_Set(mit, op);
	} else if(IsType_OrTree()) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(tree);
		return tree->root->CheckCondition((MIIterator&)mit);
	} else { // all other logical operators: >, >=, <, <=
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && val1.vc);
		if(attr.vc->IsNull(mit) || val1.vc->IsNull(mit))
			return true;
	}
	return false;
}

BHTribool Descriptor::RoughCheckSubselectCondition(MIIterator& mit, SubSelectOptimizationType sot)
{
	if(sot == PACK_BASED)
		return BHTRIBOOL_UNKNOWN; // not implemented 
	MEASURE_FET("Descriptor::RoughCheckSubselectCondition(...)");
	if(op == O_TRUE)
		return true;
	else if(op == O_FALSE)
		return false;

	AttributeType attr_t = attr.vc ? attr.vc->TypeName() : AttributeType();
	AttributeType val1_t = val1.vc ? val1.vc->TypeName() : AttributeType();
	AttributeType val2_t = val2.vc ? val2.vc->TypeName() : AttributeType();
	if((attr.vc && ATI::IsStringType(attr_t)) || (val1.vc && ATI::IsStringType(val1_t)) || (val2.vc && ATI::IsStringType(val2_t)))
		return BHTRIBOOL_UNKNOWN;

	if(op == O_EXISTS || op == O_NOT_EXISTS) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<SubSelectColumn*>(attr.vc));
		SubSelectColumn* sub = static_cast<SubSelectColumn*>(attr.vc);
		BHTribool is_empty = sub->RoughIsEmpty(mit, sot);
		if((op == O_EXISTS && is_empty == true) || (op == O_NOT_EXISTS && is_empty == false))
			return false;
		else if((op == O_EXISTS && is_empty == false) || (op == O_NOT_EXISTS && is_empty == true))
			return true;
		return BHTRIBOOL_UNKNOWN;
	} else if(IsSetOperator(op)) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc && dynamic_cast<MultiValColumn*>(val1.vc));
		return RoughCheckSetSubSelectCondition(mit, op, sot);
	} 
	
	if(op == O_BETWEEN || op == O_NOT_BETWEEN) {
		// TODO: to be implemented
		//if(attr.vc->IsNull(mit))
		//	return false;
		//RCValueObject rcvo1 = attr.vc->GetValue(mit, false);
		//// need to consider three value logic
		//BHTribool val1_res = val1.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(rcvo1 >= val1.vc->GetValue(mit, false));
		//BHTribool val2_res = val2.vc->IsNull(mit) ? BHTRIBOOL_UNKNOWN : BHTribool(rcvo1 <= val2.vc->GetValue(mit, false));
		//if(op == O_BETWEEN) {
		//	if(val1_res != true || val2_res != true)
		//		return false;
		//} else {
		//	if(val1_res != false && val2_res != false)
		//		return false;
		//}
		return BHTRIBOOL_UNKNOWN;
	}

	if(val1.vc->IsSubSelect() && attr.vc->IsSubSelect())
		return BHTRIBOOL_UNKNOWN;  //trivial, can be implemented better


	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dynamic_cast<SubSelectColumn*>(attr.vc) || dynamic_cast<SubSelectColumn*>(val1.vc));
	SubSelectColumn* sub ;
	VirtualColumn* val;
	if(attr.vc->IsSubSelect()) {
		sub = static_cast<SubSelectColumn*>(attr.vc);
		val = val1.vc;
	} else {
		sub = static_cast<SubSelectColumn*>(val1.vc);
		val = attr.vc;
	}
	if(sub->IsMaterialized())
		return BHTRIBOOL_UNKNOWN;
	RoughValue rv = sub->RoughGetValue(mit, sot);
	RCDataTypePtr rv_min, rv_max;
	if(sub->Type().IsDateTime()) {
		rv_min = RCDataTypePtr(new RCDateTime(rv.GetMin(), sub->TypeName()));
		rv_max = RCDataTypePtr(new RCDateTime(rv.GetMax(), sub->TypeName()));
	} else {
		rv_min = RCDataTypePtr(new RCNum(rv.GetMin(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
		rv_max = RCDataTypePtr(new RCNum(rv.GetMax(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
	}
	RCValueObject v = val->GetValue(mit);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(attr.vc);
	// NULLs are checked within operators
	if(op == O_EQ) {		
		if(v < *rv_min || v > *rv_max)
			return false;
		//else if(v == rv_min && v == rv_max)
		//	return true;
	} else if(op == O_NOT_EQ) {		
		if(v == *rv_min && v == *rv_max)
			return false;
		//else if(v < rv_min || v > rv_max)
		//	return true;
	} else if(op == O_MORE_EQ) {		
		if(v < *rv_min)
			return false;
		//else if(v >= rv_max)
		//	return true;
	} else if(op == O_MORE) {		
		if(v <= *rv_min)
			return false;
		//else if(v > rv_max)
		//	return true;
	} else if(op == O_LESS_EQ) {		
		if(v > *rv_max)
			return false;
		//else if(v <= rv_min)
		//	return true;
	} else if(op == O_LESS) {		
		if(v >= *rv_max)
			return false;
		//else if(v < rv_min)
		//	return true;
	}
	return BHTRIBOOL_UNKNOWN;
}

bool Descriptor::CheckSetCondition_UTF(const MIIterator& mit, Operator op)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsSetOperator(op));
	bool result = true;
	MultiValColumn* mvc = static_cast<MultiValColumn*>(val1.vc);
	RCBString s1;
	attr.vc->GetValueString(s1, mit);
	RCValueObject aggr;
	switch(op) {
		case O_EQ_ALL:
			for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
				RCBString s2 = it->GetString();
				//ConvertToBinaryForm(it->GetString(), buf_val1, buf_val1_len, collation.collation, false);
				if(s1.IsNull() || it->IsNull() || CollationStrCmp(collation, s1, s2) != 0)
					return false;
			}
			break;
		case O_IN:
		case O_EQ_ANY:
			if(s1.IsNull())
				result = false;
			else
				result = (mvc->Contains(mit, s1) == true);
			break;
		case O_NOT_IN:
		case O_NOT_EQ_ALL: {
			if((s1.IsNull() && mvc->NoValues(mit) != 0)) {
				result = false;
				break;
			}
			BHTribool res = mvc->Contains(mit, s1);
			res = !res;
			result = (res == true);
			break;
						   }
		case O_NOT_EQ_ANY:
			result = false;
			if(!s1.IsNull()) {
				for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
					//ConvertToBinaryForm(it->GetString(), buf_val1, buf_val1_len, collation.collation, false);
					if(!it->IsNull() && CollationStrCmp(collation, s1, it->GetString()) != 0)
						return true;
				}
			}
			break;
		case O_LESS_ALL:
		case O_LESS_EQ_ALL:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMin(mit);
			if(mvc->NoValues(mit) == 0)
				result = true; // op ALL (empty_set) is TRUE
			else if(s1.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) || !CollationStrCmp(collation, s1, aggr.ToRCString(), op))
				result = false;
			break;
		case O_MORE_ANY:
		case O_MORE_EQ_ANY:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMin(mit);
			if(mvc->NoValues(mit) == 0)
				result = false; // op ANY (empty_set) is FALSE
			else if(s1.IsNull() || aggr.IsNull() || !CollationStrCmp(collation, s1, aggr.ToRCString(), op))
				result = false;
			break;
		case O_LESS_ANY:
		case O_LESS_EQ_ANY:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMax(mit);
			if(mvc->NoValues(mit) == 0)
				result = false; // op ANY (empty_set) is FALSE
			else if(s1.IsNull() || aggr.IsNull() || !CollationStrCmp(collation, s1, aggr.ToRCString(), op))
				result = false;
			break;
		case O_MORE_ALL:
		case O_MORE_EQ_ALL:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMax(mit);
			if(mvc->NoValues(mit) == 0)
				result = true; // op ALL (empty_set) is TRUE
			else if(s1.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) || !CollationStrCmp(collation, s1, aggr.ToRCString(), op))
				result = false;
			break;
		default:
			break;
	}
	return result;
}

bool Descriptor::CheckSetCondition(const MIIterator& mit, Operator op)
{
	MEASURE_FET("Descriptor::CheckSetCondition(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsSetOperator(op));
	bool result = true;
	MultiValColumn* mvc = static_cast<MultiValColumn*>(val1.vc);
	if(encoded) {
		assert(op == O_IN || op == O_NOT_IN);
		if(attr.vc->IsNull(mit)) {
			if(op == O_IN)
				return false;
			if(mvc->NoValues(mit) != 0)
				return false;
			return true;
		}
		BHTribool res;
		if(attr.vc->Type().IsString() && !attr.vc->Type().IsLookup()) {
			RCBString val;
			attr.vc->GetNotNullValueString(val, mit);
			res = mvc->ContainsString(mit, val);
		} else {
			_int64 val = attr.vc->GetNotNullValueInt64(mit);
			res = mvc->Contains64(mit, val);
		}
		if(op == O_NOT_IN)
			res = !res;
		return (res == true);
	}
	RCValueObject val = attr.vc->GetValue(mit);
	RCValueObject aggr;
	switch(op) {
		case O_EQ_ALL:
			for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
				if(val.IsNull() || it->IsNull() || !RCValueObject::compare(val, it->GetValue(), O_EQ, '\\'))
					return false;
			}
			break;
		case O_IN:
		case O_EQ_ANY:
			if(val.IsNull())
				result = false;
			else
				result = (mvc->Contains(mit, *val) == true);
			break;
		case O_NOT_IN:
		case O_NOT_EQ_ALL: {
			if((val.IsNull() && mvc->NoValues(mit) != 0)) {
				result = false;
				break;
			}
			BHTribool res = mvc->Contains(mit, *val);
			res = !res;
			result = (res == true);
			break;
		}
		case O_NOT_EQ_ANY:
			result = false;
			if(!val.IsNull()) {
				for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
					if(!it->IsNull() && RCValueObject::compare(val, it->GetValue(), O_NOT_EQ, '\\'))
						return true;
				}
			}
			break;
		case O_LESS_ALL:
		case O_LESS_EQ_ALL:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMin(mit);
			if(mvc->NoValues(mit) == 0)
				result = true; // op ALL (empty_set) is TRUE
			else if(val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) || !RCValueObject::compare(val, aggr, op, '\\'))
				result = false;
			break;
		case O_MORE_ANY:
		case O_MORE_EQ_ANY:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMin(mit);
			if(mvc->NoValues(mit) == 0)
				result = false; // op ANY (empty_set) is FALSE
			else if(val.IsNull() || aggr.IsNull() || !RCValueObject::compare(val, aggr, op, '\\'))
				result = false;
			break;
		case O_LESS_ANY:
		case O_LESS_EQ_ANY:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMax(mit);
			if(mvc->NoValues(mit) == 0)
				result = false; // op ANY (empty_set) is FALSE
			else if(val.IsNull() || aggr.IsNull() || !RCValueObject::compare(val, aggr, op, '\\'))
				result = false;
			break;
		case O_MORE_ALL:
		case O_MORE_EQ_ALL:
			Query::UnmarkAllAny(op);
			aggr = mvc->GetSetMax(mit);
			if(mvc->NoValues(mit) == 0)
				result = true; // op ALL (empty_set) is TRUE
			else if(val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit) || !RCValueObject::compare(val, aggr, op, '\\'))
				result = false;
			break;
		default:
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(0 && "unexpected operator"); break;
	}
	return result;
}

bool Descriptor::IsNull_Set(const MIIterator& mit, Operator op)
{
	MEASURE_FET("Descriptor::CheckSetCondition(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsSetOperator(op));
	MultiValColumn* mvc = static_cast<MultiValColumn*>(val1.vc);
	if(encoded) {
		assert(op == O_IN || op == O_NOT_IN);
		if(attr.vc->IsNull(mit))
			return true;
		BHTribool res;
		if(attr.vc->Type().IsString() && !attr.vc->Type().IsLookup()) {
			RCBString val;
			attr.vc->GetNotNullValueString(val, mit);
			res = mvc->ContainsString(mit, val);
		} else {
			_int64 val = attr.vc->GetNotNullValueInt64(mit);
			res = mvc->Contains64(mit, val);
		}
		return (res == BHTRIBOOL_UNKNOWN);
	}
	RCValueObject val = attr.vc->GetValue(mit);
	RCValueObject aggr;
	switch(op) {
		case O_EQ_ALL:
		case O_NOT_EQ_ALL:
			if(val.IsNull() || mvc->ContainsNull(mit))
				return true;
			break;
		case O_IN:
		case O_EQ_ANY:
		case O_NOT_IN:
		case O_NOT_EQ_ANY:
			if(val.IsNull())
				return true;
			return (mvc->Contains(mit, *val) == BHTRIBOOL_UNKNOWN);
		case O_LESS_ALL:
		case O_LESS_EQ_ALL:
			aggr = mvc->GetSetMin(mit);
			if(val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit))
				return true;
			break;
		case O_MORE_ANY:
		case O_MORE_EQ_ANY:
			aggr = mvc->GetSetMin(mit);
			if(val.IsNull() || aggr.IsNull())
				return true;
			break;
		case O_LESS_ANY:
		case O_LESS_EQ_ANY:
			aggr = mvc->GetSetMax(mit);
			if(val.IsNull() || aggr.IsNull())
				return true;
			break;
		case O_MORE_ALL:
		case O_MORE_EQ_ALL:
			aggr = mvc->GetSetMax(mit);
			if(val.IsNull() || aggr.IsNull() || mvc->ContainsNull(mit))
				return true;
			break;
		default:
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(0 && "unexpected operator"); break;
	}
	return false;
}

BHTribool Descriptor::RoughCheckSetSubSelectCondition(const MIIterator& mit, Operator op, SubSelectOptimizationType sot)
{
	MEASURE_FET("Descriptor::RoughCheckSetSubselectCondition(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsSetOperator(op));
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(val1.vc->IsSubSelect());
	SubSelectColumn* sub = static_cast<SubSelectColumn*>(val1.vc);
	if(sub->IsMaterialized())
		return BHTRIBOOL_UNKNOWN;
	RoughValue rv = sub->RoughGetValue(mit, sot);
	RCDataTypePtr rv_min, rv_max;
	if(sub->Type().IsDateTime()) {
		rv_min = RCDataTypePtr(new RCDateTime(rv.GetMin(), sub->TypeName()));
		rv_max = RCDataTypePtr(new RCDateTime(rv.GetMax(), sub->TypeName()));
	} else {
		rv_min = RCDataTypePtr(new RCNum(rv.GetMin(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
		rv_max = RCDataTypePtr(new RCNum(rv.GetMax(), sub->Type().GetScale(), sub->Type().IsFloat(), sub->TypeName()));
	}
	RCValueObject v = attr.vc->GetValue(mit);
	BHTribool rough_is_empty = sub->RoughIsEmpty(mit, sot);

	switch(op) {
		case O_EQ_ALL:
			if(rough_is_empty == true)
				return true; // op ALL (empty_set) is TRUE
			else if(rough_is_empty == false && /*v.IsNull() ||*/ (v < *rv_min || v > *rv_max))
				return false;
			//else if(v == rv_min && v == rv_max)
			//	return true;
			break;
		case O_IN:
		case O_EQ_ANY:
			if(rough_is_empty == true)
				return false; // // op ANY (empty_set) is FALSE
			else if(rough_is_empty == false && /*v.IsNull() ||*/ (v < *rv_min || v > *rv_max))
				return false;
			//else if(v == rv_min && v == rv_max)
			//	return true;
			break;
		//case O_NOT_IN:
		//case O_NOT_EQ_ALL: {
		//	if((val.IsNull() && mvc->NoValues(mit) != 0)) {
		//		result = false;
		//		break;
		//	}
		//	BHTribool res = mvc->Contains(mit, *val);
		//	res = !res;
		//	result = (res == true);
		//	break;
		//				   }
		//case O_NOT_EQ_ANY:
		//	result = false;
		//	if(!val.IsNull()) {
		//		for(MultiValColumn::Iterator it = mvc->begin(mit), end = mvc->end(mit); it != end; ++it) {
		//			if(!it->IsNull() && RCValueObject::compare(val, it->GetValue(), O_NOT_EQ))
		//				return true;
		//		}
		//	}
		//	break;
		case O_LESS_ALL:
		case O_LESS_EQ_ALL:
			Query::UnmarkAllAny(op);
			if(rough_is_empty == true)
				return true; // op ALL (empty_set) is TRUE
			else if(rough_is_empty == false && !RCValueObject::compare(v, *rv_max, op, '\\') )
				return false;
			break;
		case O_MORE_ANY:
		case O_MORE_EQ_ANY:
			Query::UnmarkAllAny(op);
			if(rough_is_empty == true)
				return false; // op ANY (empty_set) is FALSE
			else if(rough_is_empty == false && !RCValueObject::compare(v, *rv_min, op, '\\'))
				return false;
			break;
		case O_LESS_ANY:
		case O_LESS_EQ_ANY:
			Query::UnmarkAllAny(op);
			if(rough_is_empty == true)
				return false; // op ANY (empty_set) is FALSE
			else if(rough_is_empty == false && !RCValueObject::compare(v, *rv_max, op, '\\'))
				return false;
			break;
		case O_MORE_ALL:
		case O_MORE_EQ_ALL:
			Query::UnmarkAllAny(op);
			if(rough_is_empty == true)
				return true; // op ALL (empty_set) is TRUE
			else if(rough_is_empty == false && !RCValueObject::compare(v, *rv_min, op, '\\'))
				return false;
			break;
		default:
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT("unexpected operator"); break;
	}
	return BHTRIBOOL_UNKNOWN;
}

void Descriptor::CoerceColumnType(VirtualColumn*& for_typecast)
{
	VirtualColumn* vc = attr.vc;
	TypeCastColumn* tcc = NULL;
	bool tstamp = false;
	if(ATI::IsNumericType(vc->TypeName())) {
		if(ATI::IsTxtType(for_typecast->TypeName()))
			tcc = new String2NumCastColumn(for_typecast, vc->Type());
		else if(ATI::IsDateTimeType(for_typecast->TypeName()))
			tcc = new DateTime2NumCastColumn(for_typecast, vc->Type());
	} else if(ATI::IsDateTimeType(vc->TypeName())) {
		if(ATI::IsTxtType(for_typecast->TypeName()))
			tcc = new String2DateTimeCastColumn(for_typecast, vc->Type());
		else if(ATI::IsNumericType(for_typecast->TypeName()))
			tcc = new Num2DateTimeCastColumn(for_typecast, vc->Type());
		else if(vc->TypeName() == RC_TIMESTAMP && for_typecast->TypeName() != RC_TIMESTAMP)	{
			tcc = new TimeZoneConversionCastColumn(vc);
			tstamp = true;
		}
	} else if(ATI::IsTxtType(vc->TypeName())) {
		if(ATI::IsDateTimeType(for_typecast->TypeName()))
			tcc = new DateTime2VarcharCastColumn(for_typecast, vc->Type());
		else if(ATI::IsNumericType(for_typecast->TypeName()))
			tcc = new Num2VarcharCastColumn(for_typecast, vc->Type());
	} else
		return;

	if(tcc) {
		if(rccontrol.isOn())
			rccontrol.lock(ConnectionInfoOnTLS.Get().GetThreadID()) << "Type conversion for VC:" << (for_typecast == val1.vc ? val1.vc_id : for_typecast == val2.vc ? val2.vc_id : val1.vc_id ) << unlock;
		table->AddVirtColumn(tcc);
		if(!tstamp)
			for_typecast = tcc;
		else 
			attr.vc = tcc;
	}
}

void Descriptor::CoerceColumnTypes() {

	if(val1.vc)
		if(val1.vc->IsMultival()) {
			MultiValColumn* mvc = static_cast<MultiValColumn*>(val1.vc);
			if(val1.vc->IsInSet()) {
				InSetColumn* isc = new InSetColumn(*static_cast<InSetColumn*>(val1.vc));
				for(uint i = 0; i < isc->columns.size(); i++)
					CoerceColumnType(isc->columns[i]);
				table->AddVirtColumn(isc);
				val1.vc = mvc = isc;
			}
			mvc->SetExpectedType(attr.vc->Type());
		} else
			CoerceColumnType(val1.vc);
	if(val2.vc)
		CoerceColumnType(val2.vc);
	CoerceCollation();
}

bool Descriptor::NullMayBeTrue()		// true, if the descriptor may give nontrivial answer if any of involved dimension is null
{
	if(op == O_IS_NULL || op == O_NOT_NULL)
		return true;
	if(IsType_OrTree())
		return tree->NullMayBeTrue();

	// t1.a not between t2.b and t1.b
	if(op == O_NOT_BETWEEN)
		return true;
	// TODO: more precise conditions
	// Examples:
	//    (a is null) = 1
	//    b = ifnull(a, 0)
	//    c > (case when a is null then 10 else 20)
	//    d = (select count(*) from t where t.x = a)
	//    e IN (1, 2, a)
	//    f < ALL (1, 2, a)

	// For now, a simplistic version: any complex case is true.
	if(attr.vc && !attr.vc->IsSingleColumn() && !attr.vc->IsConst())
		return true;
	if(val1.vc && !val1.vc->IsSingleColumn() && !val1.vc->IsConst())
		return true;
	if(val2.vc && !val2.vc->IsSingleColumn() && !val2.vc->IsConst())
		return true;
	return false;
}

bool Descriptor::IsParameterized() const
{
	return ( (attr.vc && attr.vc->IsParameterized()) ||
			 (val1.vc && val1.vc->IsParameterized()) ||
			 (val2.vc && val2.vc->IsParameterized()) ||
			 (tree && tree->IsParameterized()));
}

bool Descriptor::IsDeterministic() const
{
	bool det = true;
	if(attr.vc)
		det = det && attr.vc->IsDeterministic();
	if(val1.vc)
		det = det && val1.vc->IsDeterministic();
	if(val2.vc)
		det = det && val2.vc->IsDeterministic();
	return det;
}

bool Descriptor::WithoutAttrs()
{
	if(IsType_OrTree())
		return tree->WithoutAttrs();
	else
		return 	(!attr.vc || attr.vc->IsSingleColumn() != VirtualColumn::SC_ATTR) &&
				(!val1.vc || val1.vc->IsSingleColumn() != VirtualColumn::SC_ATTR) &&
				(!val2.vc || val2.vc->IsSingleColumn() != VirtualColumn::SC_ATTR) ;
}

bool Descriptor::WithoutTypeCast()
{
	if(IsType_OrTree())
		return tree->WithoutTypeCast();
	else
		return 	(!attr.vc || !attr.vc->IsTypeCastColumn()) &&
		(!val1.vc || !val1.vc->IsTypeCastColumn()) &&
		(!val2.vc || !val2.vc->IsTypeCastColumn()) ;
}

bool Descriptor::IsBHItemsEmpty()
{
	if(IsType_OrTree())
		return tree->IsBHItemsEmpty();
	else
		return 	(!attr.vc || attr.vc->GetBHItems().empty() ) &&
				(!val1.vc || val1.vc->GetBHItems().empty() ) &&
				(!val2.vc || val2.vc->GetBHItems().empty()) ;
}

void Descriptor::CoerceCollation()
{
	collation = attr.vc && attr.vc->Type().IsString() ? attr.vc->GetCollation() : DTCollation();
	if(val1.vc && val1.vc->Type().IsString())
		collation = ResolveCollation(collation, val1.vc->GetCollation());
	if(val2.vc && val2.vc->Type().IsString())
		collation = ResolveCollation(collation, val2.vc->GetCollation());
}

void Descriptor::ClearRoughValues()
{
	if(IsType_OrTree())
		tree->root->ClearRoughValues();
	else
		rv = RS_UNKNOWN;
}

void Descriptor::RoughAccumulate(MIIterator& mit)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsType_OrTree());
	tree->RoughAccumulate(mit);
}

void Descriptor::SimplifyAfterRoughAccumulate()
{
	if(IsType_OrTree()) {
		if(tree->UseRoughAccumulated())
			Simplify(false);
	} else {
		if(rv == RS_NONE)
			op = O_FALSE;
		else if(rv == RS_ALL)
			op = O_TRUE;
	}
}

bool IsSetOperator(Operator op)
{
	return IsSetAnyOperator(op) || IsSetAllOperator(op) || op == O_IN || op == O_NOT_IN;
}

bool IsSetAllOperator(Operator op)
{
	return
		op == O_EQ_ALL ||
		op == O_NOT_EQ_ALL ||
		op == O_LESS_ALL ||
		op == O_MORE_ALL ||
		op == O_LESS_EQ_ALL ||
		op == O_MORE_EQ_ALL;
}

bool IsSimpleEqualityOperator(Operator op)
{
	return
		op == O_EQ ||
		op == O_NOT_EQ ||
		op == O_LESS ||
		op == O_MORE ||
		op == O_LESS_EQ ||
		op == O_MORE_EQ;
}

bool IsSetAnyOperator(Operator op)
{
	return
		op == O_EQ_ANY ||
		op == O_NOT_EQ_ANY ||
		op == O_LESS_ANY ||
		op == O_MORE_ANY ||
		op == O_LESS_EQ_ANY ||
		op == O_MORE_EQ_ANY;
}

bool ISTypeOfEqualOperator(Operator op)
{
	return op == O_EQ || op == O_EQ_ALL || op == O_EQ_ANY;
}

bool ISTypeOfNotEqualOperator(Operator op)
{
	return op == O_NOT_EQ || op == O_NOT_EQ_ALL || op == O_NOT_EQ_ANY;
}

bool ISTypeOfLessOperator(Operator op)
{
	return op == O_LESS || op == O_LESS_ALL || op == O_LESS_ANY;
}

bool ISTypeOfLessEqualOperator(Operator op)
{
	return op == O_LESS_EQ || op == O_LESS_EQ_ALL || op == O_LESS_EQ_ANY;
}

bool ISTypeOfMoreOperator(Operator op)
{
	return op == O_MORE || op == O_MORE_ALL || op == O_MORE_ANY;
}


bool ISTypeOfMoreEqualOperator(Operator op)
{
	return op == O_MORE_EQ || op == O_MORE_EQ_ALL || op == O_MORE_EQ_ANY;
}

//////////////////////////////////////////////////////////////////////////////////


DescTree::DescTree(CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc)
{
	curr = root = new DescTreeNode(e1, op, e2, e3, t, no_dims, like_esc);
}

DescTree::DescTree(DescTree &t)
{
	curr = root = Copy(t.root);
}

DescTreeNode * DescTree::Copy(DescTreeNode * node)
{
	if(!node)
		return NULL;
	DescTreeNode *res = new DescTreeNode(*node);
	if(node->left) {
		res->left = Copy(node->left);
		res->left->parent = res;
	}
	if(node->right) {
		res->right = Copy(node->right);
		res->right->parent = res;
	}
	return res;
}

//void DescTree::Release(DescTreeNode * &node)
//{
//	if(node && node->left)
//		Release(node->left);
//	if(node && node->right)
//		Release(node->right);
//	delete node;
//	node = NULL;
//}

void DescTree::Release()
{
	delete root;
	root = NULL;
	curr = root;
}

// make _lop the root, make current tree the left child, make descriptor the right child
void DescTree::AddDescriptor(LogicalOperator lop, CQTerm e1, Operator op, CQTerm e2, CQTerm e3, TempTable* t, int no_dims, char like_esc)
{
	curr = new DescTreeNode(lop, t, no_dims);
	curr->left = root;
	curr->left->parent = curr;
	curr->right = new DescTreeNode(e1, op, e2, e3, t, no_dims, like_esc);
	curr->right->parent = curr;
	root = curr;
}

// make _lop the root, make current tree the left child, make tree the right child
void DescTree::AddTree(LogicalOperator lop, DescTree *tree, int no_dims)
{
	if (tree->root) {
		curr = new DescTreeNode(lop, tree->root->desc.table, no_dims);
		curr->left = root;
		root->parent = curr;
		curr->right = tree->root;
		tree->root->parent = curr;
		root = curr;
		tree->root = NULL;
		tree->curr = NULL;
	}
}

void DescTree::Display()
{
	Display(root);
}

void DescTree::Display(DescTreeNode * node)
{
	if(node == NULL)
		return;
	if(node->left)
		Display(node->left);
	if(node->right)
		Display(node->right);
	cout << "------------------------" << endl;
	if(node->left)
		cout << (node->desc.lop ? "OR" : "AND") << endl;
	else {
		char buf[50];
		cout << node->desc.attr.ToString(buf, 0) << endl;
		cout << "........" << endl;
		if(node->desc.op)
			cout << node->desc.op << endl;
		else
			cout << "=" << endl;
		cout << "........" << endl;
		cout << node->desc.val1.ToString(buf, 0) << endl;
	}
	cout << "------------------------" << endl;
}

bool DescTree::IsParameterized()
{
	return root->IsParameterized();
}

Descriptor DescTree::ExtractDescriptor()
{
	vector<pair<int, Descriptor> > desc_counts;
	root->CollectDescriptor(desc_counts);
	sort(desc_counts.begin(), desc_counts.end());
	for(int i = int(desc_counts.size() - 1); i >= 0; i--) {
		Descriptor& desc = desc_counts[i].second;
		if(root->CanBeExtracted(desc)) {
			root->ExtractDescriptor(desc, root);
			return desc;
		}
	}
	return Descriptor();
}

void DescTree::MakeSingleColsPrivate(std::vector<VirtualColumn*>& virt_cols)
{
	root->MakeSingleColsPrivate(virt_cols);
}

bool DescriptorEqual(std::pair<int,Descriptor> const& d1, std::pair<int,Descriptor> const& d2) 
{
	return (d1.second == d2.second);
}
////////////////////////////////////////////////////////////////////////////////////////////////////

DescTreeNode::~DescTreeNode() {
	if(left)
		delete left;
	if(right)
		delete right;
}

BHTribool DescTreeNode::Simplify(DescTreeNode*& root, bool in_having)
{
	BHTribool left_res, right_res;
	if(left)
		left_res = left->Simplify(root, in_having);
	if(right)
		right_res = right->Simplify(root, in_having);
	if(desc.op == O_OR_TREE) {
		BHTribool res = (desc.lop == O_AND ? BHTribool::And(left_res, right_res) : BHTribool::Or(left_res, right_res));
		if(res == true) {
			desc.op = O_TRUE;
			delete left;
			left = NULL;
			delete right;
			right = NULL;
		}
		else if(res == false) {
			desc.op = O_FALSE;
			delete left;
			left = NULL;
			delete right;
			right = NULL;
		}
		else if(!left->left && !right->right && desc.lop == O_AND) {
			bool merged = ParameterizedFilter::TryToMerge(left->desc, right->desc);
			if(merged) {
				delete right;
				right = NULL;
				res = ReplaceNode(this, left, root);
			}
		} else if(desc.lop == O_OR) {
			if(left->desc.op == O_FALSE) {
				delete left;
				left = NULL;
				res = ReplaceNode(this, right, root);
			} else if(left->desc.op == O_TRUE) {
				delete right;
				right = NULL;
				res = ReplaceNode(this, left, root);
			} else if(right->desc.op == O_FALSE) {
				delete right;
				right = NULL;
				res = ReplaceNode(this, left, root);
			} else if(right->desc.op == O_TRUE) {
				delete left;
				left = NULL;
				res = ReplaceNode(this, right, root);
			}
		}

		return res;
	}
	desc.Simplify(in_having);
	return desc.op == O_TRUE ? true : (desc.op == O_FALSE ? false : BHTRIBOOL_UNKNOWN);
}

RSValue DescTreeNode::EvaluateRoughlyPack(const MIIterator& mit)
{
	if(desc.op == O_OR_TREE) {
		RSValue left_res = left->EvaluateRoughlyPack(mit);
		RSValue right_res = right->EvaluateRoughlyPack(mit);
		RSValue r;
		if(desc.lop == O_AND)
			r = And(left_res, right_res);
		else
			r = Or(left_res, right_res);
		if(desc.rv == RS_UNKNOWN)
			desc.rv = r;
		else if(desc.rv == RS_NONE && r != RS_NONE)
			desc.rv = RS_SOME;
		else if(desc.rv == RS_ALL && r != RS_ALL)
			desc.rv = RS_SOME;
		return r;
	}
	return desc.EvaluateRoughlyPack(mit);
}

bool DescTreeNode::CheckCondition(MIIterator& mit)
{
	if(left) { // i.e., not a leaf
		assert(right); // if left is not empty so should be right
		if(desc.lop == O_AND) {
			if(!left->CheckCondition(mit))
				return false;
			if(!right->CheckCondition(mit))
				return false;
			return true;
		} else {
			if(left->CheckCondition(mit))
				return true;
			if(right->CheckCondition(mit))
				return true;
			return false;
		}
	} else { // i.e., a leaf
		if(locked >= 0) {
			desc.LockSourcePacks(mit, locked);
			locked = -1;
		}
		if(RequiresUTFConversions(desc.GetCollation())) {
			return desc.CheckCondition_UTF(mit);
		} else
			return desc.CheckCondition(mit);
	}
}

bool DescTreeNode::IsNull(MIIterator& mit)
{
	if(left) { // i.e., not a leaf
		assert(right); // if left is not empty so should be right
		bool left_res = left->CheckCondition(mit);
		bool right_res = right->CheckCondition(mit);
		if(desc.lop == O_AND) {
			if(left_res == true && right_res == false && right->IsNull(mit))
				return true;
			if(left_res == false && right_res == true && left->IsNull(mit))
				return true;
			return false;
		} else {
			if(left_res == false && right_res == false && (right->IsNull(mit) || left->IsNull(mit)))
				return true;
			return false;
		}
	} else { // i.e., a leaf
		if(locked >= 0) {
			desc.LockSourcePacks(mit, locked);
			locked = -1;
		}
		return desc.IsNull(mit);
	}
}

void DescTreeNode::EvaluatePack(MIUpdatingIterator & mit)
{
	int single_dim = mit.SingleFilterDim();
	// general case:
	if(single_dim == -1) {
		while(mit.IsValid()) {
			if(CheckCondition(mit) == false)
				mit.ResetCurrent();
			++mit;
			if(mit.PackrowStarted())
				break;
		}
		return;
	}

	// optimized case
	if(left) { // i.e., not a leaf
		assert(right); // if left is not empty so should be right
		if(desc.lop == O_AND) {
			if(left->desc.rv == RS_NONE || right->desc.rv == RS_NONE) {
				mit.ResetCurrentPack();
				mit.NextPackrow();
				return;
			}
			if(left->desc.rv == RS_ALL && right->desc.rv == RS_ALL) {
				mit.NextPackrow();
				return;
			}
			int pack_start = mit.GetCurPackrow(single_dim);
			if(left->desc.rv != RS_ALL && mit.IsValid())
				left->EvaluatePack(mit);
			if(right->desc.rv != RS_ALL && mit.RewindToPack(pack_start) && mit.IsValid()) 		// otherwise the pack is already empty
				right->EvaluatePack(mit);
			return;
		} else {
			if(left->desc.rv == RS_NONE && right->desc.rv == RS_NONE) {
				mit.ResetCurrentPack();
				mit.NextPackrow();
				return;
			}
			if(left->desc.rv == RS_ALL || right->desc.rv == RS_ALL) {
				mit.NextPackrow();
				return;
			}
			if(left->desc.rv == RS_NONE)
				right->EvaluatePack(mit);
			else if(right->desc.rv == RS_NONE)
				left->EvaluatePack(mit);
			else {
				int pack_start = mit.GetCurPackrow(single_dim);
				boost::scoped_ptr<Filter> f(mit.NewPackFilter(pack_start));
				left->EvaluatePack(mit);
				if(mit.SwapPackFilter(pack_start, f.get())) {		// return true if the pack in the current iterator is not full
					mit.RewindToPack(pack_start);
					right->EvaluatePack(mit);
					mit.OrPackFilter(pack_start, f.get());
				}
			}
			return;
		}
	} else {  // i.e., a leaf
		if(locked >= 0) {
			desc.LockSourcePacks(mit, locked);
			locked = -1;
		}
		desc.EvaluatePackImpl(mit);
	}
}

void DescTreeNode::PrepareToLock(int locked_by)
{
	if(left) { // i.e., not a leaf
		left->PrepareToLock(locked_by);
		right->PrepareToLock(locked_by);
	} else  // i.e., a leaf
		locked = locked_by;
}

void DescTreeNode::UnlockSourcePacks()
{
	if(left) { // i.e., not a leaf
		left->UnlockSourcePacks();
		right->UnlockSourcePacks();
	} else  { // i.e., a leaf
		locked = 0;
		desc.UnlockSourcePacks();
	}
}

bool DescTreeNode::IsParameterized()
{
	bool is_parameterized = desc.IsParameterized();
	if(left)
		is_parameterized = is_parameterized || left->IsParameterized();
	if(right)
		is_parameterized = is_parameterized || right->IsParameterized();
	return is_parameterized;
}

void DescTreeNode::DimensionUsed(DimensionVector &dims)
{
	desc.DimensionUsed(dims);
	if(left)
		left->DimensionUsed(dims);
	if(right)
		right->DimensionUsed(dims);
}

bool DescTreeNode::NullMayBeTrue()
{
	// TODO: revisit the logics below
	if(left && right) {
		if(left->NullMayBeTrue() || right->NullMayBeTrue())
			return true;
		// special case: (a1 = 3) OR true
		// return false only if both sides of OR contain all involved dims
		if(desc.lop == O_OR) {
			DimensionVector dims1(desc.right_dims.Size());
			DimensionVector dims2(desc.right_dims.Size());
			left->DimensionUsed(dims1);
			right->DimensionUsed(dims2);
			if(!(dims1 == dims2))
				return true;
		}
		return false;
	}
	return desc.NullMayBeTrue();
}

void DescTreeNode::EncodeIfPossible(bool for_rough_query, bool additional_nulls)
{
	if(left && right) {
		left->EncodeIfPossible(for_rough_query, additional_nulls);
		right->EncodeIfPossible(for_rough_query, additional_nulls);
	} else
		ConditionEncoder::EncodeIfPossible(desc, for_rough_query, additional_nulls);
}

double DescTreeNode::EvaluateConditionWeight(ParameterizedFilter *p, bool for_or)
{
	if(left && right) {
		bool or_here = (desc.lop == O_OR);	// and: smaller result first; or: bigger result first
		double e1 = left->EvaluateConditionWeight(p, or_here);
		double e2 = right->EvaluateConditionWeight(p, or_here);
 		if(e1 > e2) {
 			DescTreeNode *pp = left;
 			left = right;
 			right = pp;
 		}
		return e1 + e2 + 1;							// +1 is a penalty of logical operation
	} else
		return p->EvaluateConditionNonJoinWeight(desc, for_or);
}

void DescTreeNode::CollectDescriptor(vector<pair<int, Descriptor> >& desc_counts)
{
	if(left && right) {
		left->CollectDescriptor(desc_counts);
		right->CollectDescriptor(desc_counts);
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!left && !right);
		IncreaseDescriptorCount(desc_counts);
	}
}

bool DescTreeNode::CanBeExtracted(Descriptor& searched_desc)
{
	if(left && right) {
		if(desc.lop == O_AND) {
			if(left->CanBeExtracted(searched_desc))
				return true;
			return right->CanBeExtracted(searched_desc);
		} else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(desc.lop == O_OR);
			return (left->CanBeExtracted(searched_desc) && right->CanBeExtracted(searched_desc));
		}
	} else {
		return ((parent && parent->desc.lop == O_AND && desc <= searched_desc) ||
			    (parent && parent->desc.lop == O_OR  && desc == searched_desc));
	}
}

//bool DescTreeNode::IsWidestRange(Descriptor& searched_desc)
//{
//	if(left && right) {
//		if(desc.lop == O_AND) {
//			if(left->IsWidestRange(searched_desc))
//				return true;
//			return right->IsWidestRange(searched_desc);
//		} else {
//			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(desc.lop == O_OR);
//			return (left->IsWidestRange(searched_desc) && right->IsWidestRange(searched_desc));
//		}
//	} else {
//		return (desc < searched_desc);
//	}
//}

void DescTreeNode::ExtractDescriptor(Descriptor& searched_desc, DescTreeNode*& root)
{
	if(left && left->left) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(left->right);
		left->ExtractDescriptor(searched_desc, root);
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(left && right);
		if(/*(desc.lop == O_AND && left->desc <= searched_desc) ||
			(desc.lop == O_OR && */left->desc == searched_desc/*)*/) {
			delete left;
			left = NULL;
			DescTreeNode* old_right = right;
			DescTreeNode* old_parent = parent;
			ReplaceNode(this, right, root);
			// if there is no parent and there is single right node 
			// it means that the whole tree should be empty
			// and extracted descriptor should replace the tree
			// we make this tree trivial with one O_TRUE node
			if(old_right->right)
				old_right->ExtractDescriptor(searched_desc, root);
			else if(!old_parent) {
				old_right->desc = Descriptor();
				old_right->desc.op = O_TRUE;
			}
			return;
		}

	}

	if(right && right->left) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(right->right);
		right->ExtractDescriptor(searched_desc, root);
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(left && right);
		if(/*(desc.lop == O_AND && right->desc <= searched_desc) ||
			(desc.lop == O_OR &&*/ right->desc == searched_desc/*)*/) {
			delete right;
			right = NULL;
			DescTreeNode* old_left = left;
			DescTreeNode* old_parent = parent;
			ReplaceNode(this, left, root);
			// if there is no parent and there is single left node 
			// it means that the whole tree should be empty
			// and extracted descriptor should replace the tree
			// we make this tree trivial with one O_TRUE node
			if(old_left->left)
				old_left->ExtractDescriptor(searched_desc, root);
			else if(!old_parent) {
				old_left->desc = Descriptor();
				old_left->desc.op = O_TRUE;
			}
			return;
		}

	}
}

BHTribool DescTreeNode::ReplaceNode(DescTreeNode* src, DescTreeNode* dst, DescTreeNode*& root)
{
	dst->parent = src->parent;
	if(src->parent) {
		if(src->parent->left == src)
			src->parent->left = dst;
		else
			src->parent->right = dst;
	} else
		root = dst;
	if(src->left == dst || src->right == dst) {
		// src's children are reused - prevent deleting them
		src->left = NULL;
		src->right = NULL;
	}
	delete src;
	if(dst->desc.op == O_FALSE)
		return false;
	if(dst->desc.op == O_TRUE)
		return true;
	return BHTRIBOOL_UNKNOWN;
}

void DescTreeNode::RoughAccumulate(MIIterator& mit)
{
	if(left && right) {
		left->RoughAccumulate(mit);
		right->RoughAccumulate(mit);
	} else {
		if(desc.rv == RS_SOME)
			return;
		desc.EvaluateRoughlyPack(mit);		// updating desc.rv inside
	}
}

bool DescTreeNode::UseRoughAccumulated()
{
	if(left && right) {
		bool res1 = left->UseRoughAccumulated();
		bool res2 = right->UseRoughAccumulated();
		return (res1 || res2);
	} else {
		bool res = false;
		if(desc.rv == RS_NONE) {
			desc.op = O_FALSE;
			res = true;
		} else if(desc.rv == RS_ALL) {
			desc.op = O_TRUE;
			res = true;
		}
		return res;
	}
}

void DescTreeNode::ClearRoughValues()
{
	if(left && right) {
		left->ClearRoughValues();
		right->ClearRoughValues();
	} else
		desc.ClearRoughValues();
}

void DescTreeNode::MakeSingleColsPrivate(std::vector<VirtualColumn*>& virt_cols)
{
	if(left && right) {
		left->MakeSingleColsPrivate(virt_cols);
		right->MakeSingleColsPrivate(virt_cols);
	} else {
		if(desc.attr.vc && desc.attr.vc->IsSingleColumn()) {
			int i = 0;
			for(; i < virt_cols.size(); i++)
				if(virt_cols[i] == desc.attr.vc)
					break;
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i < virt_cols.size());
			desc.attr.vc = CreateVCCopy(desc.attr.vc);
			desc.attr.is_vc_owner = true;
			virt_cols[i] = desc.attr.vc;
		}
		if(desc.val1.vc && desc.val1.vc->IsSingleColumn()) {
			int i = 0;
			for(; i < virt_cols.size(); i++)
				if(virt_cols[i] == desc.val1.vc)
					break;
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i < virt_cols.size());
			desc.val1.vc = CreateVCCopy(desc.val1.vc);
			desc.val1.is_vc_owner = true;
			virt_cols[i] = desc.val1.vc;
		}
		if(desc.val2.vc && desc.val2.vc->IsSingleColumn()) {
			int i = 0;
			for(; i < virt_cols.size(); i++)
				if(virt_cols[i] == desc.val2.vc)
					break;
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i < virt_cols.size());
			desc.val2.vc = CreateVCCopy(desc.val2.vc);
			desc.val2.is_vc_owner = true;
			virt_cols[i] = desc.val2.vc;
		}
	}
}

bool DescTreeNode::IsBHItemsEmpty()
{
	if(left && right)
		return left->IsBHItemsEmpty() && right->IsBHItemsEmpty();
	else
		return desc.IsBHItemsEmpty();
}

bool DescTreeNode::WithoutAttrs()
{
	if(left && right)
		return left->WithoutAttrs() && right->WithoutAttrs();
	else
		return desc.WithoutAttrs();
}

bool DescTreeNode::WithoutTypeCast()
{
	if(left && right)
		return left->WithoutTypeCast() && right->WithoutTypeCast();
	else
		return desc.WithoutTypeCast();
}

void DescTreeNode::IncreaseDescriptorCount(vector<pair<int, Descriptor> > &desc_counts)
{
	vector<pair<int, Descriptor> >::iterator p;
	p = find_if(desc_counts.begin(), desc_counts.end(), boost::bind(&DescriptorEqual, make_pair(0, boost::ref(desc)), _1));
	if(p != desc_counts.end())
		p->first++;
	else
		desc_counts.push_back(make_pair(1, desc));
}
