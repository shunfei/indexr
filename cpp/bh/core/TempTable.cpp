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

#include "edition/local.h"
#include "ParametrizedFilter.h"
#include "TempTable.h"
#include "RSI_CMap.h"
#include "MysqlExpression.h"
#include "system/fet.h"
#include "ValueSet.h"
#include "vc/SingleColumn.h"
#include "vc/ExpressionColumn.h"
#include "vc/ConstExpressionColumn.h"
#include "vc/ConstColumn.h"
#include "vc/InSetColumn.h"
#include "vc/SubSelectColumn.h"
#include "vc/TypeCastColumn.h"
//#include "vc/IBConstExpressionColumn.h"
#include "Query.h"
#include "GroupDistinctTable.h"
#include "ColumnBinEncoder.h"
#include "ConditionEncoder.h"
#include "RCEngine.h"
#include "AggregationAlgorithm.h"

using namespace std;
using namespace boost;

class IBConstExpressionColumn;

template<class T>
TempTable::AttrBuffer<T>::AttrBuffer(uint page_size, uint elem_size, ConnectionInfo *conn) :
	CachedBuffer<T> (page_size, elem_size, conn)
{
	constant_value = false;
}

template<class T>
TempTable::AttrBuffer<T>::~AttrBuffer()
{
}

template<class T>
void TempTable::AttrBuffer<T>::Set(_int64 idx, T value)
{
	if(constant_value && value != CachedBuffer<T>::Get(0)) {
		// it is not the same value any longer, needs to be propagated
		constant_value = false;
		T val = CachedBuffer<T>::Get(0);
		for(_int64 i = 1; i < idx; i++)
			CachedBuffer<T>::Set(i, val);
	}
	if(constant_value)
		CachedBuffer<T>::Set(0, value);
	else
		CachedBuffer<T>::Set(idx, value);
}

template class TempTable::AttrBuffer<char>;
template class TempTable::AttrBuffer<short>;
template class TempTable::AttrBuffer<int>;
template class TempTable::AttrBuffer<_int64>;
template class TempTable::AttrBuffer<double>;
template class TempTable::AttrBuffer<RCBString>;

TempTable::Attr::Attr(const Attr &a) :
	PhysicalColumn(a)
{
	mode = a.mode;
	distinct = a.distinct;
	no_materialized = a.no_materialized;
	term = a.term;
	if(term.vc)
		term.vc->ResetLocalStatistics();
	dim = a.dim;
	//assert(a.buffer == NULL); // otherwise we cannot copy Attr !
	buffer = NULL;
	no_obj = a.no_obj;
	if(a.alias) {
		alias = new char[strlen(a.alias) + 1];
		strcpy(alias, a.alias);
	} else
		alias = NULL;

	page_size = a.page_size;
	orig_precision = a.orig_precision;
	not_complete = a.not_complete;
}

TempTable::Attr::Attr(CQTerm t, ColOperation m, bool dis, char* a, int dim, AttributeType type, uint scale, uint no_digits, NullMode n, DTCollation collation, bool is_unsigned)
	: mode(m), distinct(dis), term(t), dim(dim), not_complete(true)
{
	ct.Initialize(type, n, false, no_digits, scale, collation, is_unsigned);
	orig_precision = no_digits;
	buffer = NULL;
	no_obj = 0;
	no_materialized = 0;
	if(a) {
		alias = new char[strlen(a) + 1];
		strcpy(alias, a);
	} else
		alias = NULL;
	page_size = 1;
}

TempTable::Attr::~Attr()
{
	DeleteBuffer();
	delete[] alias;
}

TempTable::Attr& TempTable::Attr::operator=(const TempTable::Attr& a)
{
	mode = a.mode;
	distinct = a.distinct;
	no_materialized = a.no_materialized;
	term = a.term;
	if(term.vc)
		term.vc->ResetLocalStatistics();
	dim = a.dim;
	//assert(a.buffer == NULL); // otherwise we cannot copy Attr !
	buffer = NULL;
	no_obj = a.no_obj;
	delete [] alias;
	if(a.alias) {
		alias = new char[strlen(a.alias) + 1];
		strcpy(alias, a.alias);
	} else
		alias = NULL;

	page_size = a.page_size;
	orig_precision = a.orig_precision;
	not_complete = a.not_complete;
	return *this;
}

int TempTable::Attr::operator==(const TempTable::Attr &sec)
{
	return mode == sec.mode && distinct == sec.distinct && term == sec.term && Type() == sec.Type() && dim == sec.dim;
}

void TempTable::Attr::CreateBuffer(_uint64 size, ConnectionInfo *conn, bool not_c)
{
	// do not create larger buffer than size
	not_complete = not_c;
	if(size < page_size)
		page_size = (uint) size;
	no_obj = size;
	DeleteBuffer();
	//assert(buffer == NULL);		// otherwise memory leaks may occur
	//switch(TypeName()) {
	//	case RC_INT:
	//	case RC_MEDIUMINT:
	//		buffer = new AttrBuffer<int> (size, NULL_VALUE_32, page_size, 0, false, conn);
	//		break;
	//	case RC_BYTEINT:
	//		buffer = new AttrBuffer<char> (size, (char) NULL_VALUE_C, page_size, 0, false, conn);
	//		break;
	//	case RC_SMALLINT:
	//		buffer = new AttrBuffer<short> (size, (short) NULL_VALUE_SH, page_size, 0, false, conn);
	//		break;
	//	case RC_STRING:
	//	case RC_VARCHAR:
	//	case RC_BIN:
	//	case RC_BYTE:
	//	case RC_VARBYTE:
	//		buffer = new AttrBuffer<RCBString> (size, RCBString(), page_size, this->Type().GetInternalSize() + 4,
	//				false, conn);
	//		break;
	//	case RC_BIGINT:
	//	case RC_NUM:
	//	case RC_YEAR:
	//	case RC_TIME:
	//	case RC_DATE:
	//	case RC_DATETIME:
	//	case RC_TIMESTAMP:
	//		buffer = new AttrBuffer<_int64> (size, NULL_VALUE_64, page_size, 0, false, conn);
	//		break;
	//	case RC_REAL:
	//	case RC_FLOAT:
	//		buffer = new AttrBuffer<double> (size, NULL_VALUE_D, page_size, 0, false, conn);
	//		break;
	//}
	switch(TypeName()) {
		case RC_INT:
		case RC_MEDIUMINT:
			buffer = new AttrBuffer<int> (page_size, 0, conn);
			break;
		case RC_BYTEINT:
			buffer = new AttrBuffer<char> (page_size, 0, conn);
			break;
		case RC_SMALLINT:
			buffer = new AttrBuffer<short> (page_size, 0, conn);
			break;
		case RC_STRING:
		case RC_VARCHAR:
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
			buffer = new AttrBuffer<RCBString> (page_size, this->Type().GetInternalSize(), conn);
			break;
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			buffer = new AttrBuffer<_int64> (page_size, 0, conn);
			break;
		case RC_REAL:
		case RC_FLOAT:
			buffer = new AttrBuffer<double> (page_size, 0, conn);
			break;
	}
}

void TempTable::Attr::DeleteBuffer()
{
	switch(TypeName()) {
		case RC_INT:
		case RC_MEDIUMINT:
			delete (AttrBuffer<int>*) (buffer);
			break;
		case RC_BYTEINT:
			delete (AttrBuffer<char>*) (buffer);
			break;
		case RC_SMALLINT:
			delete (AttrBuffer<short>*) (buffer);
			break;
		case RC_STRING:
		case RC_VARCHAR:
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
			delete (AttrBuffer<RCBString>*) (buffer);
			break;
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			delete (AttrBuffer<_int64>*) (buffer);
			break;
		case RC_REAL:
		case RC_FLOAT:
			delete (AttrBuffer<double>*) (buffer);
			break;
	}
	buffer = NULL;
	no_obj = 0;
}

void TempTable::Attr::SetValueInt64(_int64 obj, _int64 val)
{
	no_materialized = obj + 1;
	no_obj = obj >= no_obj ? obj + 1 : no_obj;
	switch(TypeName()) {
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP: // 64-bit
			((AttrBuffer<_int64> *) buffer)->Set(obj, val);
			break;
		case RC_INT: // 32-bit
		case RC_MEDIUMINT: // 24-bit
			if(val == NULL_VALUE_64)
				((AttrBuffer<int> *) buffer)->Set(obj, NULL_VALUE_32);
			else if(val == PLUS_INF_64)
				((AttrBuffer<int> *) buffer)->Set(obj, BH_INT_MAX);
			else if(val == MINUS_INF_64)
				((AttrBuffer<int> *) buffer)->Set(obj, BH_INT_MIN);
			else
				((AttrBuffer<int> *) buffer)->Set(obj, (int) val);
			break;
		case RC_SMALLINT: // 16-bit
			if(val == NULL_VALUE_64)
				((AttrBuffer<short> *) buffer)->Set(obj, NULL_VALUE_SH);
			else if(val == PLUS_INF_64)
				((AttrBuffer<short> *) buffer)->Set(obj, BH_SMALLINT_MAX);
			else if(val == MINUS_INF_64)
				((AttrBuffer<short> *) buffer)->Set(obj, BH_SMALLINT_MIN);
			else
				((AttrBuffer<short> *) buffer)->Set(obj, (short) val);
			break;
		case RC_BYTEINT: // 8-bit
			if(val == NULL_VALUE_64)
				((AttrBuffer<char> *) buffer)->Set(obj, NULL_VALUE_C);
			else if(val == PLUS_INF_64)
				((AttrBuffer<char> *) buffer)->Set(obj, BH_TINYINT_MAX);
			else if(val == MINUS_INF_64)
				((AttrBuffer<char> *) buffer)->Set(obj, BH_TINYINT_MIN);
			else
				((AttrBuffer<char> *) buffer)->Set(obj, (char) val);
			break;
		case RC_REAL:
		case RC_FLOAT:
			if(val == NULL_VALUE_64)
				((AttrBuffer<double> *) buffer)->Set(obj, NULL_VALUE_D);
			else if(val == PLUS_INF_64)
				((AttrBuffer<double> *) buffer)->Set(obj, PLUS_INF_DBL);
			else if(val == MINUS_INF_64)
				((AttrBuffer<double> *) buffer)->Set(obj, MINUS_INF_DBL);
			else
				((AttrBuffer<double> *) buffer)->Set(obj, *(double *) &val);
			break;
		default:
			assert(0);
			break;
	}
}

void TempTable::Attr::InvalidateRow(_int64 obj)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(obj + 1 == no_materialized);
	no_obj--;
	no_materialized--;
}

void TempTable::Attr::SetNull(_int64 obj)
{
	no_materialized = obj + 1;
	no_obj = obj >= no_obj ? obj + 1 : no_obj;
	switch(TypeName()) {
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			((AttrBuffer<_int64> *) buffer)->Set(obj, NULL_VALUE_64);
			break;
		case RC_INT:
		case RC_MEDIUMINT:
			((AttrBuffer<int> *) buffer)->Set(obj, NULL_VALUE_32);
			break;
		case RC_SMALLINT:
			((AttrBuffer<short> *) buffer)->Set(obj, NULL_VALUE_SH);
			break;
		case RC_BYTEINT:
			((AttrBuffer<char> *) buffer)->Set(obj, NULL_VALUE_C);
			break;
		case RC_REAL:
		case RC_FLOAT:
			((AttrBuffer<double> *) buffer)->Set(obj, NULL_VALUE_D);
			break;
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
		case RC_STRING:
		case RC_VARCHAR:
			((AttrBuffer<RCBString> *) buffer)->Set(obj, RCBString());
			break;
		default:
			assert(0);
			break;
	}
}

void TempTable::Attr::SetMinusInf(_int64 obj)
{
	SetValueInt64(obj, MINUS_INF_64);
}

void TempTable::Attr::SetPlusInf(_int64 obj)
{
	SetValueInt64(obj, PLUS_INF_64);
}

void TempTable::Attr::SetValueString(_int64 obj, const RCBString& val)
{
	no_materialized = obj + 1;
	no_obj = obj >= no_obj ? obj + 1 : no_obj;
	_int64 val64 = 0;
	double valD = 0.0;
	switch(TypeName()) {
		case RC_INT:
		case RC_MEDIUMINT:
			((AttrBuffer<int> *) buffer)->Set(obj, atoi(val.Value()));
			break;
		case RC_BYTEINT:
			((AttrBuffer<char> *) buffer)->Set(obj, val.Value()[0]);
			break;
		case RC_SMALLINT:
			((AttrBuffer<short> *) buffer)->Set(obj, (short) atoi(val.Value()));
			break;
		case RC_BIGINT:
			((AttrBuffer<_int64> *) buffer)->Set(obj, strtoll(val.Value(),NULL, 10));
			break;
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
		case RC_STRING:
		case RC_VARCHAR:
			((AttrBuffer<RCBString> *) buffer)->Set(obj, val);
			break;
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			val64 = *(_int64 *) (val.Value());
			((AttrBuffer<_int64> *) buffer)->Set(obj, val64);
			break;
		case RC_REAL:
		case RC_FLOAT:
			if(!val.IsNullOrEmpty())
				valD = *(double *) (val.Value());
			else
				valD = NULL_VALUE_D;
			((AttrBuffer<double> *) buffer)->Set(obj, valD);
			break;
	}
}

RCValueObject TempTable::Attr::GetValue(_int64 obj, bool lookup_to_num )
{
	if(obj == NULL_VALUE_64)
		return RCValueObject();
	RCValueObject ret;
	if(ATI::IsStringType(TypeName())) {
		RCBString s;
		GetValueString(s,obj);
		ret = s;
	}
	else if(ATI::IsIntegerType(TypeName()))
		ret = RCNum(GetValueInt64(obj), 0, false, RC_NUM);
	else if(ATI::IsDateTimeType(TypeName()))
		ret = RCDateTime(this->GetValueInt64(obj), TypeName()/*, precision*/);
	else if(ATI::IsRealType(TypeName()))
		ret = RCNum(this->GetValueInt64(obj), 0, true);
	else if(TypeName() == RC_NUM)
		ret = RCNum((_int64) GetValueInt64(obj), Type().GetScale());
	return ret;
}

//RCBString& TempTable::Attr::GetValueString(_int64 obj)
//{
//	GetValueString(bufS, obj);
//	return bufS;
//}

void TempTable::Attr::GetValueString(RCBString& s, _int64 obj)
{
	if(obj == NULL_VALUE_64) {
		s = RCBString();
		return ;
	}
	double *d_p = NULL;
	switch(TypeName()) {
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
		case RC_STRING:
		case RC_VARCHAR:
			(*(AttrBuffer<RCBString> *) buffer).GetString(s,obj);
			break;
		case RC_BYTEINT: {
			RCNum rcn((_int64) (*(AttrBuffer<char> *) buffer)[obj], -1, false, TypeName());
			s = rcn.ToRCString();
			break;
		}
		case RC_SMALLINT: {
			RCNum rcn((_int64) (*(AttrBuffer<short>*) buffer)[obj], -1, false, TypeName());
			s = rcn.ToRCString();
			break;
		}
		case RC_INT:
		case RC_MEDIUMINT: {
			RCNum rcn((int) (*(AttrBuffer<int>*) buffer)[obj], -1, false, TypeName());
			s = rcn.ToRCString();
			break;
		}
		case RC_BIGINT: {
			RCNum rcn((_int64) (*(AttrBuffer<_int64>*) buffer)[obj], -1, false, TypeName());
			s = rcn.ToRCString();
			break;
		}
		case RC_NUM: {
			RCNum rcn((*(AttrBuffer<_int64>*) buffer)[obj], Type().GetScale());
			s = rcn.ToRCString();
			break;
		}
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP: {
			RCDateTime rcdt((*(AttrBuffer<_int64> *) buffer)[obj], TypeName());
			s = rcdt.ToRCString();
			break;
		}
		case RC_REAL:
		case RC_FLOAT: {
			d_p = &(*(AttrBuffer<double> *) buffer)[obj];
			RCNum rcn(*(_int64*) d_p, 0, true, TypeName());
			s = rcn.ToRCString();
			break;
		}
	}
}

_int64 TempTable::Attr::GetSum(int pack, bool &nonnegative)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ATI::IsNumericType(ct.GetTypeName()));
	_int64 start = pack * 65536;
	_int64 stop = (pack + 1) * 65536;
	stop = stop > no_obj ? no_obj : stop;
	if(not_complete || no_materialized < stop)
		return NULL_VALUE_64;
	_int64 sum = 0, val;
	double sum_d = 0.0;
	bool not_null_exists = false;
	nonnegative = true;
	for(_int64 i = start; i < stop; i++) {
		if(!IsNull(i)) {
			not_null_exists = true;
			val = GetNotNullValueInt64(i);
			nonnegative = nonnegative && (val >= 0);
			if(ATI::IsRealType(ct.GetTypeName())) {
				sum_d += *(double *)&val;
			} else
				sum += val;
		}
	}
	if(ATI::IsRealType(ct.GetTypeName()))
		sum = *(_int64*)&sum_d;
	return not_null_exists ? sum : NULL_VALUE_64;
}

_int64 TempTable::Attr::GetNoNulls(int pack)
{
	if(not_complete)
		return NULL_VALUE_64;
	_int64 start;
	_int64 stop;
	if(pack == -1) {
		start = 0;
		stop = no_obj;
	} else {
		start = pack * 65536;
		stop = (pack + 1) * 65536;
		stop = stop > no_obj ? no_obj : stop;
	}
	if(no_materialized < stop) 
		return NULL_VALUE_64;
	_int64 no_nulls = 0;
	for(_int64 i = start; i < stop; i++) {
		if(IsNull(i))
			no_nulls++;
	}
	return no_nulls;
}

RCBString TempTable::Attr::GetMinString(int pack)
{
	return RCBString(); // it should not touch data
}

RCBString TempTable::Attr::GetMaxString(int pack)
{
	return RCBString(); // it should not touch data
}

_int64 TempTable::Attr::GetMaxInt64(int pack)
{
	if(not_complete)
		return PLUS_INF_64;
	_int64 start = pack * 65536;
	_int64 stop = (pack + 1) * 65536;
	stop = stop > no_obj ? no_obj : stop;
	if(no_materialized < stop) 
		return NULL_VALUE_64;
	_int64 val, max = BH_BIGINT_MIN;
	for(_int64 i = start; i < stop; i++) {
		if(!IsNull(i)) {
			val = GetNotNullValueInt64(i);
			if( (ATI::IsRealType(ct.GetTypeName()) && (max == BH_BIGINT_MIN || *(double *)&val > *(double *)&max)) ||
				(!ATI::IsRealType(ct.GetTypeName()) && val > max) )
				max = val;
		}
	}
	return (max == BH_BIGINT_MIN ? PLUS_INF_64 : max);
}

_int64 TempTable::Attr::GetMinInt64(int pack)
{
	if(not_complete)
		return MINUS_INF_64;
	_int64 start = pack * 65536;
	_int64 stop = (pack + 1) * 65536;
	stop = stop > no_obj ? no_obj : stop;
	if(no_materialized < stop) 
		return NULL_VALUE_64;
	_int64 val, min = BH_BIGINT_MAX;
	for(_int64 i = start; i < stop; i++) {
		if(!IsNull(i)) {
			val = GetNotNullValueInt64(i);
			if( (ATI::IsRealType(ct.GetTypeName()) && (min == BH_BIGINT_MAX || *(double *)&val < *(double *)&min)) ||
				(!ATI::IsRealType(ct.GetTypeName()) && val < min) )
				min = val;
		}
	}
	return (min == BH_BIGINT_MAX ? MINUS_INF_64 : min);
}

_int64 TempTable::Attr::GetValueInt64(_int64 obj) const
{
	_int64 res = NULL_VALUE_64;
	if(obj == NULL_VALUE_64)
		return res;
	switch(TypeName()) {
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			if(!IsNull(obj))
				res = (*(AttrBuffer<_int64> *) buffer)[obj];
			break;
		case RC_INT:
		case RC_MEDIUMINT:
			if(!IsNull(obj))
				res = (*(AttrBuffer<int> *) buffer)[obj];
			break;
		case RC_SMALLINT:
			if(!IsNull(obj))
				res = (*(AttrBuffer<short> *) buffer)[obj];
			break;
		case RC_BYTEINT:
			if(!IsNull(obj))
				res = (*(AttrBuffer<char> *) buffer)[obj];
			break;
		case RC_STRING:
		case RC_VARCHAR:
			assert(0);
			break;
		case RC_REAL:
		case RC_FLOAT:
			if(!IsNull(obj))
				res = *(_int64 *) &(*(AttrBuffer<double> *) buffer)[obj];
			else
				res = *(_int64 *) &NULL_VALUE_D;
			break;
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
			assert(0);
			break;
	}
	return res;
}

_int64 TempTable::Attr::GetNotNullValueInt64(_int64 obj) const
{
	_int64 res = NULL_VALUE_64;
	switch(TypeName()) {
		case RC_INT:
		case RC_MEDIUMINT:
			res = (*(AttrBuffer<int> *) buffer)[obj];
			break;
		case RC_BYTEINT:
			res = (*(AttrBuffer<char> *) buffer)[obj];
			break;
		case RC_SMALLINT:
			res = (*(AttrBuffer<short> *) buffer)[obj];
			break;
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			res = (*(AttrBuffer<_int64> *) buffer)[obj];
			break;
		case RC_REAL:
		case RC_FLOAT:
			res = *(_int64 *) &(*(AttrBuffer<double> *) buffer)[obj];
			break;
		default:
			assert(0);
			break;
	}
	return res;
}

bool TempTable::Attr::IsNull(const _int64 obj) const
{
	if(obj == NULL_VALUE_64)
		return true;
	bool res = false;
	switch(TypeName()) {
		case RC_INT:
		case RC_MEDIUMINT:
			res = (*(AttrBuffer<int> *) buffer)[obj] == NULL_VALUE_32;
			break;
		case RC_BYTEINT:
			res = (*(AttrBuffer<char> *) buffer)[obj] == NULL_VALUE_C;
			break;
		case RC_SMALLINT:
			res = (*(AttrBuffer<short> *) buffer)[obj] == NULL_VALUE_SH;
			break;
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
		case RC_STRING:
		case RC_VARCHAR:
			res = (*(AttrBuffer<RCBString> *) buffer)[obj].IsNull();
			break;
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			res = (*(AttrBuffer<_int64> *) buffer)[obj] == NULL_VALUE_64;
			break;
		case RC_REAL:
		case RC_FLOAT:
			double_int_t v((*(AttrBuffer<double> *) buffer)[obj]);
			res = v.i == NULL_VALUE_64;
			break;
	}
	return res;
}

void TempTable::Attr::ApplyFilter(MultiIndex & mind, _int64 offset, _int64 last_index)
{
	assert(mind.NoDimensions() == 1);

	if(mind.NoDimensions() != 1)
		throw NotImplementedRCException("MultiIndex has too many dimensions.");
	if(mind.ZeroTuples() || no_obj == 0 || offset >= mind.NoTuples()) {
		DeleteBuffer();
		return;
	}

	if(last_index > mind.NoTuples())
		last_index = mind.NoTuples();

	void *old_buffer = buffer;
	buffer = NULL;
	CreateBuffer(last_index - offset, mind.m_conn);

	MIIterator mit(&mind);
	for(_int64 i = 0; i < offset; i++)
		++mit;
	_uint64 idx = 0;
	for(_int64 i = 0; i < last_index - offset; i++, ++mit) {
		idx = mit[0];
		assert(idx != NULL_VALUE_64); // null object should never appear in a materialized temp. table
		switch(TypeName()) {
			case RC_INT:
			case RC_MEDIUMINT:
				((AttrBuffer<int> *) buffer)->Set(i, (*(AttrBuffer<int> *) old_buffer)[idx]);
				break;
			case RC_BYTEINT:
				((AttrBuffer<char> *) buffer)->Set(i, (*(AttrBuffer<char> *) old_buffer)[idx]);
				break;
			case RC_SMALLINT:
				((AttrBuffer<short> *) buffer)->Set(i, (*(AttrBuffer<short> *) old_buffer)[idx]);
				break;
			case RC_STRING:
			case RC_VARCHAR:
			case RC_BIN:
			case RC_BYTE:
			case RC_VARBYTE:
				((AttrBuffer<RCBString> *) buffer)->Set(i, (*(AttrBuffer<RCBString> *) old_buffer)[idx]);
				break;
			case RC_BIGINT:
			case RC_NUM:
			case RC_YEAR:
			case RC_TIME:
			case RC_DATE:
			case RC_DATETIME:
			case RC_TIMESTAMP:
				((AttrBuffer<_int64> *) buffer)->Set(i, (*(AttrBuffer<_int64> *) old_buffer)[idx]);
				break;
			case RC_REAL:
			case RC_FLOAT:
				((AttrBuffer<double> *) buffer)->Set(i, (*(AttrBuffer<double> *) old_buffer)[idx]);
				break;
			default:
				assert(0); break;
		}
	}

	switch(TypeName()) {
		case RC_INT:
		case RC_MEDIUMINT:
			delete (AttrBuffer<int> *) old_buffer;
			break;
		case RC_BYTEINT:
			delete (AttrBuffer<char> *) old_buffer;
			break;
		case RC_SMALLINT:
			delete (AttrBuffer<short> *) old_buffer;
			break;
		case RC_STRING:
		case RC_VARCHAR:
		case RC_BIN:
		case RC_BYTE:
		case RC_VARBYTE:
			delete (AttrBuffer<RCBString> *) old_buffer;
			break;
		case RC_BIGINT:
		case RC_NUM:
		case RC_YEAR:
		case RC_TIME:
		case RC_DATE:
		case RC_DATETIME:
		case RC_TIMESTAMP:
			delete (AttrBuffer<_int64> *) old_buffer;
			break;
		case RC_REAL:
		case RC_FLOAT:
			delete (AttrBuffer<double> *) old_buffer;
			break;
		default:
			assert(0); break;
	}
}

//ValueOrNull TempTable::Attr::GetComplexValue(const _int64 obj)
//{
//	if(IsNull(obj))
//		return ValueOrNull();
//
//	if(ct.IsFixed() || ct.IsFloat() || ct.IsDateTime())
//		return ValueOrNull(GetNotNullValueInt64(obj));
//	if(ct.IsString()) {
//		ValueOrNull val;
//		RCBString s(GetValueString(obj));
//		val.SetString(s);
//		return val;
//	}
//	BHERROR("Unrecognized type in TempTable::Attr::GetComplexValue()");
//	return ValueOrNull();
//}
//void TempTable::Attr::SetComplexValue(_int64 obj, ValueOrNull val)
//{
//	if(ct.IsFixed() || ct.IsFloat() || ct.IsDateTime())
//		if(val.IsNull())
//			SetValueInt64(obj, NULL_VALUE_64 );
//		else {
//			_int64 v = val.Get64();
//			assert(v != NULL_VALUE_64);
//			SetValueInt64(obj, v);
//		}
//
//	else if(ct.IsString()) {
//		RCBString s;
//		if(!val.IsNull())
//			val.GetString(s);
//		SetValueString(obj, s);
//	} else
//		BHERROR("Unrecognized type in TempTable::Attr::SetComplexValue()");
//}

_uint64 TempTable::Attr::ApproxDistinctVals(bool incl_nulls, ::Filter *f, RSValue* rf, bool outer_nulls_possible) // provide the best upper approximation of number of diff. values (incl. null)
{
	// TODO: can it be done better?
	if(f)
		return f->NoOnes();
	return no_obj;
}

ushort TempTable::Attr::MaxStringSize(::Filter *f)			// maximal byte string length in column
{
	int res = ct.GetPrecision();
	if(ct.IsFixed())
		res = std::max(res, 21);					// max. numeric size ("-aaa.bbb")
	else if(!ct.IsString())
		res = std::max(res, 23);					// max. size of datetime/double etc.
	return (ushort)res;
}

void TempTable::Attr::SetNewPageSize(uint new_page_size)
{
	if(buffer) {
		switch(TypeName()) {
			case RC_INT:
			case RC_MEDIUMINT:
				static_cast<CachedBuffer<int> *>(buffer)->SetNewPageSize(new_page_size);
				break;
			case RC_BYTEINT:
				static_cast<CachedBuffer<char> *>(buffer)->SetNewPageSize(new_page_size);
				break;
			case RC_SMALLINT:
				static_cast<CachedBuffer<short> *>(buffer)->SetNewPageSize(new_page_size);
				break;
			case RC_STRING:
			case RC_VARCHAR:
			case RC_BIN:
			case RC_BYTE:
			case RC_VARBYTE:
				static_cast<CachedBuffer<RCBString> *>(buffer)->SetNewPageSize(new_page_size);
				break;
			case RC_BIGINT:
			case RC_NUM:
			case RC_YEAR:
			case RC_TIME:
			case RC_DATE:
			case RC_DATETIME:
			case RC_TIMESTAMP:
				static_cast<CachedBuffer<_int64> *>(buffer)->SetNewPageSize(new_page_size);
				break;
			case RC_REAL:
			case RC_FLOAT:
				static_cast<CachedBuffer<double> *>(buffer)->SetNewPageSize(new_page_size);
				break;
			default:
				BHASSERT(0, "Attr::SetNewPageSize: unrecognized type"); break;
		}
	}
	page_size = new_page_size;
}

TempTable::TempTable(const TempTable& t, bool is_vc_owner, bool _for_rough_query)
	: filter(t.filter, _for_rough_query), output_mind(t.output_mind), is_vc_owner(is_vc_owner), m_conn(t.m_conn)
{
	for_rough_query = _for_rough_query;
	no_obj = t.no_obj;
	materialized = t.materialized;
	aliases = t.aliases;
	group_by = t.group_by;
	no_cols = t.no_cols;
	mode = t.mode;
	virt_cols = t.virt_cols;
	virt_cols_for_having = t.virt_cols_for_having;
	having_conds = t.having_conds;
//	if(for_rough_query && having_conds.Size() > 0) {
//		having_conds.MakeSingleColsPrivate(virt_cols);
//	}
	tables = t.tables;
	join_types = t.join_types;
	buf_index = t.buf_index;
	order_by = t.order_by;
	manage_used_tables = t.manage_used_tables;
	has_temp_table = t.has_temp_table;
	displayable_attr = NULL;
	query = t.query;
	lazy = t.lazy;
	no_materialized = t.no_materialized;
	no_global_virt_cols = int(t.virt_cols.size());
	for(uint i = 0; i< t.attrs.size(); i++) {
		if(for_rough_query)
			attrs.push_back(new Attr(*t.attrs[i]));
		else
			attrs.push_back(new Attr(*t.attrs[i]));
	}
	is_sent = t.is_sent;
	mem_scale = t.mem_scale;
	rough_is_empty = t.rough_is_empty;
}

TempTable::~TempTable()
{
	MEASURE_FET("TempTable::~TempTable(...)");
	// remove all temporary tables used by *this
	// but only if *this should manage those tables
	try {
		if(is_vc_owner) {
			for(int i = 0; i < virt_cols.size(); i++)
				delete virt_cols[i];
		} else {
			TranslateBackVCs();
			for(uint i = no_global_virt_cols; i < virt_cols.size(); i++)
				delete virt_cols[i];
		}
		delete [] displayable_attr;
		if(manage_used_tables && has_temp_table) {
			for(uint i = 0; i < tables.size(); i++) {
				if(tables[i] && tables[i]->TableType() == TEMP_TABLE)
					delete tables[i];
			}
		}

		for(uint i = 0; i < attrs.size(); i++) {
			//if(/*!for_subq ||*/ for_rough_query)
			//	delete attrs[i];
			//else
				//attrs[i]->DeleteBuffer();
				delete attrs[i];
		}

	} catch ( ... ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! "exception from destructor" );
	}
}

void TempTable::TranslateBackVCs()
{
	for(int i = 0; i < no_global_virt_cols; i++)
		if(virt_cols[i] && virt_cols[i]->IsSingleColumn())
			static_cast<SingleColumn*>(virt_cols[i])->TranslateSourceColumns(attr_back_translation);
}

TempTablePtr TempTable::CreateMaterializedCopy(bool translate_order, bool in_subq)			// move all buffers to a newly created TempTable, make this VC-pointing to it; the new table must be deleted by DeleteMaterializedCopy()
{
	MEASURE_FET("TempTable::CreateMaterializedCopy(...)");
	// Copy buffers in a safe place for a moment
	vector<void*> copy_buf;
	map<PhysicalColumn *, PhysicalColumn *> attr_translation;	// new attr (this), old attr (working_copy)
	for(uint i = 0; i < attrs.size(); i++) {
		copy_buf.push_back(attrs[i]->buffer);
		attrs[i]->buffer = NULL;
	}
	if(no_global_virt_cols != -1) {
		// this is a TempTable copy
		for(int i = no_global_virt_cols; i < virt_cols.size(); i++)	{
			delete virt_cols[i];
			virt_cols[i] = NULL;
		}
		virt_cols.resize(no_global_virt_cols);
	}
	TempTablePtr working_copy = Create(*this, in_subq);		// Original VCs of this will be copied to working_copy, and then deleted in its destructor
	for(uint i = 0; i < attrs.size(); i++) {
		attrs[i]->mode = LISTING;
	}
	working_copy->mode = TableMode();			// reset all flags (distinct, limit) on copy
	//working_copy->for_subq = this->for_subq;
	//working_copy->no_global_virt_cols = this->no_global_virt_cols;
	for(uint i = 0; i < attrs.size(); i++) {
		working_copy->attrs[i]->buffer = copy_buf[i];		// copy the data buffers
		attrs[i]->no_obj = 0;
		attr_translation[attrs[i]] = working_copy->attrs[i];
	}
	// VirtualColumns are copied, and we should replace them by references to the temporary source
	delete filter.mind;
	filter.mind = new MultiIndex();
	filter.mind->AddDimension_cross(no_obj);
	if(virt_cols.size() < attrs.size())
		virt_cols.resize(attrs.size());
	fill(virt_cols.begin(), virt_cols.end(), (VirtualColumn*)NULL);
	for(uint i = 0; i < attrs.size(); i++) {
		VirtualColumn *new_vc = new SingleColumn(working_copy->attrs[i], filter.mind, 0, 0, working_copy.get(), 0);
		virt_cols[i] = new_vc;
		attrs[i]->term.vc = new_vc;
		attrs[i]->dim = 0;
	}
	if(translate_order) {
		order_by.clear();				// translation needed: virt_cols should point to working_copy as a data source
		for(uint i = 0; i < working_copy->virt_cols.size(); i++) {
			VirtualColumn* orig_vc = working_copy->virt_cols[i];
			//if(in_subq && orig_vc->IsSingleColumn())
			//	working_copy->virt_cols[i] = CreateVCCopy(working_copy->virt_cols[i]);
			for(uint j = 0; j < working_copy->order_by.size(); j++) {
				if(working_copy->order_by[j].vc == orig_vc) {
					working_copy->order_by[j].vc = working_copy->virt_cols[i];
				}
			}
			// TODO: redesign it for more universal solution
			VirtualColumn* vc = working_copy->virt_cols[i];
			if(vc) {
				if(vc->IsSingleColumn())
					static_cast<SingleColumn*>(vc)->TranslateSourceColumns(attr_translation);
				else {
					VirtualColumn::var_maps_t& var_maps = vc->GetVarMap();
					for(uint i = 0; i < var_maps.size(); i++) {
						if(var_maps[i].tab.lock().get() == this)
							var_maps[i].tab = working_copy;
					}
				}
			}
		}
		for(uint i = 0; i < working_copy->order_by.size(); i++) {		
			SortDescriptor sord;
			sord.vc = working_copy->order_by[i].vc;
			sord.dir = working_copy->order_by[i].dir;
			order_by.push_back(sord);
		}
	} else {	// Create() did move vc's used in order_by into working_copy, so we should bring them back
		for(uint i = 0; i < order_by.size(); i++) {
			// find which working_copy->vc is used in ordering
			for(uint j = 0; j < working_copy->virt_cols.size(); j++) {
				if(/*working_copy->*/order_by[i].vc == working_copy->virt_cols[j]) {
					//order_by[i].vc = working_copy->virt_cols[j];
					MoveVC(j, working_copy->virt_cols, virt_cols);				// moving vc - now it is back in this
					break;
				}
			}
		}
	}
	no_global_virt_cols = (int)virt_cols.size();
	return working_copy;	// must be deleted by DeleteMaterializedCopy()
}

void TempTable::DeleteMaterializedCopy(TempTablePtr& old_t)		// delete the external table and remove VC pointers, make this fully materialized
{
	MEASURE_FET("TempTable::DeleteMaterializedCopy(...)");
	for(uint i = 0; i < attrs.size(); i++) {		// Make sure VCs are deleted before the source table is deleted
		attrs[i]->term.vc = NULL;
		delete virt_cols[i];
		virt_cols[i] = NULL;
	}
	//if(for_subq) { // restore the original definitions of attr
	//	for(uint i = 0; i < attrs.size(); i++) {
	//		attrs[i]->mode = old_t->GetAttrP(i)->mode;
	//		attrs[i]->term = old_t->GetAttrP(i)->term;
	//	}
	//}
	old_t.reset();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

void TempTable::MoveVC(int colnum, std::vector<VirtualColumn*>& from, std::vector<VirtualColumn*>& to)
{
	VirtualColumn* vc = from[colnum];
	to.push_back(vc);
	from[colnum] = NULL;
	std::vector<VirtualColumn*> vv = vc->GetChildren();
	for(int i=0; i< vv.size(); i++)
		MoveVC(vv[i], from, to);
}

void TempTable::MoveVC(VirtualColumn* vc, std::vector<VirtualColumn*>& from, std::vector<VirtualColumn*>& to)
{
	for(int k =0; k< from.size(); k++)
		if(vc == from[k]) {
			MoveVC(k, from, to);
			break;
		}
}

void TempTable::ReserveVirtColumns(int no)
{
	assert(!no || no> virt_cols.size());
	no_global_virt_cols = -1;	// usable value only in TempTable copies and in subq
	virt_cols.resize(no);
	virt_cols_for_having.resize(no);
}


void TempTable::CreateDisplayableAttrP()
{
	if(attrs.size() == 0)
		return;
	delete [] displayable_attr;
	displayable_attr = new TempTable::Attr * [attrs.size()];
	int idx = 0;
	for(int i = 0; i < attrs.size(); i++)
		if(attrs[i]->alias) {
			displayable_attr[idx] = attrs[i];
			idx++;
		}
	for(int i = idx; i < attrs.size(); i++)
		displayable_attr[i] = NULL;
}

uint TempTable::GetDisplayableAttrIndex(uint attr)
{
	int idx = -1;
	uint i;
	for(i = 0; i < attrs.size(); i++)
		if(attrs[i]->alias) {
			idx++;
			if(idx == attr)
				break;
		}
	assert(i < attrs.size());
	return i;
}

void TempTable::AddConds(Condition* cond, CondType type)
{
	MEASURE_FET("TempTable::AddConds(...)");
	if(type == WHERE_COND) {
		// need to add one by one since where_conds can be non-empty
			filter.AddConditions(cond);
	} else if(type == HAVING_COND) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT((*cond)[0].tree);
		having_conds.AddDescriptor((*cond)[0].tree, this, (*cond)[0].left_dims.Size());
	} else
		assert(0);
 }

void TempTable::AddInnerConds(Condition* cond, std::vector<TabID>& dim_aliases)
{
	for(uint i = 0; i < cond->Size(); i++)
		for(uint j = 0; j < dim_aliases.size(); j++) 
			(*cond)[i].left_dims[GetDimension(dim_aliases[j])] = true;
	filter.AddConditions(cond);
}

void TempTable::AddLeftConds(Condition* cond, std::vector<TabID>& left_aliases, std::vector<TabID>& right_aliases)
{
	for(uint i = 0; i < cond->Size(); i++) {
		for(int d= 0; d < (*cond)[i].left_dims.Size(); d++)
			(*cond)[i].left_dims[d] = (*cond)[i].right_dims[d] = false;
		for(uint j = 0; j < left_aliases.size(); j++) 
			(*cond)[i].left_dims[GetDimension(left_aliases[j])] = true;
		for(uint j = 0; j < right_aliases.size(); j++) 
			(*cond)[i].right_dims[GetDimension(right_aliases[j])] = true;
	}
	filter.AddConditions(cond);
}

void TempTable::SetMode(TMParameter mode, _int64 mode_param1, _int64 mode_param2)
{
	switch(mode) {
		case TM_DISTINCT:
			this->mode.distinct = true;
			break;
		case TM_TOP:
			this->mode.top = true;
			this->mode.param1 = mode_param1; // offset
			this->mode.param2 = mode_param2; // limit
			break;
		case TM_EXISTS:
			this->mode.exists = true;
			break;
		default:
			assert(false); break;
	}
}

int TempTable::AddColumn(CQTerm e, ColOperation mode, char * alias, bool distinct)
{

	if(alias)
		no_cols++;
	AttributeType type; // type of column
	NullMode is_nullable = NO_NULLS; // true = NULLs are allowed
	uint scale = 0; // number of decimal places
	uint precision = 0; // number of all places
	bool is_unsigned = false;		// true only for some special cases

	// new code for VirtualColumn
	if (e.vc_id != NULL_VALUE_32) {
		assert(e.vc);
		// enum ColOperation {LISTING, COUNT, SUM, MIN, MAX, AVG, GROUP_BY};
		if(mode == COUNT) {
			type = RC_NUM; // 64 bit, Decimal(18,0)
			precision = 18;
			is_nullable = NO_NULLS;
		} else if(mode == BIT_AND || mode == BIT_OR || mode == BIT_XOR) {
			type = RC_BIGINT;
			precision = 19;
			is_unsigned = true;
			is_nullable = NO_NULLS;
		} else if(mode == AVG || mode == VAR_POP || mode == VAR_SAMP || mode == STD_POP || mode == STD_SAMP) {
			type = RC_REAL; // 64 bit
			precision = 18;
			is_nullable = AS_MISSED;
		} else {
			scale = e.vc->Type().GetScale();
			precision = e.vc->Type().GetPrecision();
			type = e.vc->TypeName();
			is_nullable = e.vc->Type().GetNullsMode();
			if(mode == SUM) {
				switch(e.vc->TypeName()) {
					case RC_INT:
					case RC_NUM:
					case RC_SMALLINT:
					case RC_BYTEINT:
					case RC_MEDIUMINT:
						type = RC_NUM;
						precision = 18;
						is_nullable = AS_MISSED;
						break;
					case RC_BIGINT:
						type = RC_BIGINT;
						precision = 19;
						is_nullable = AS_MISSED;
						break;
					case RC_REAL:
					case RC_FLOAT:
					default:			// all other types summed up as double (parsed from text)
						type = RC_REAL;
						is_nullable = AS_MISSED;
						break;
//					default:
//						throw NotImplementedRCException("SUM performed on non-numerical column.");
				}
			} else if(mode == MIN || mode == MAX) {
				is_nullable = AS_MISSED;
			} else if(mode == LISTING || mode == GROUP_BY) {
				if(mode == GROUP_BY)
					group_by = true;
				is_nullable =  e.vc->Type().GetNullsMode();
			}
		}
	} else if(mode == COUNT) {
		type = RC_NUM;
		precision = 18;
		is_nullable = NO_NULLS;
	} else {
		// illegal execution path: neither VC is set nor mode is COUNT
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"wrong execution path");
		//throw NotImplementedRCException("Invalid column on SELECT list.");
	}
	attrs.push_back(new Attr(e, mode, distinct, alias, -2, type, scale, precision, is_nullable, e.vc ? e.vc->GetCollation() : DTCollation(), is_unsigned));
	return -(int) attrs.size(); // attributes are counted from -1, -2, ...
}

void TempTable::AddOrder(VirtualColumn *vc, int direction)
{
	// don't sort by const expressions
	if(vc->IsConst())
		return;
	SortDescriptor d;
	d.vc = vc;
	d.dir = direction;
	bool already_added = false;
	for(uint j = 0; j < order_by.size(); j++) {
		if(order_by[j] == d) {
			already_added = true;
			break;
		}
	}
	if(!already_added)
		order_by.push_back(d);
}

void TempTable::Union(TempTable* t, int all)
{
	MEASURE_FET("TempTable::UnionOld(...)");
	if(!t) { // trivial union with single select and external order by
		this->Materialize();
		return;
	}
	assert( NoDisplaybleAttrs() == t->NoDisplaybleAttrs() );
	if(NoDisplaybleAttrs() != t->NoDisplaybleAttrs())
		throw NotImplementedRCException("UNION of tables with different number of columns.");
	if(this->IsParametrized() || t->IsParametrized())
		throw NotImplementedRCException("Materialize: not implemented union of parameterized queries.");
	rccontrol.lock(m_conn.GetThreadID()) << "UNION: materializing components." << unlock;
	this->Materialize();
	t->Materialize();
	if((!t->NoObj() && all) || (!this->NoObj() && !t->NoObj())) // no objects = no union
		return;

	::Filter first_f(NoObj()), first_mask(NoObj()); // mask of objects to be added to the final result set
	::Filter sec_f(t->NoObj()), sec_mask(t->NoObj());
	first_mask.Set();
	sec_mask.Set();
	if(!all) {
		rccontrol.lock(m_conn.GetThreadID()) << "UNION: excluding repetitions." << unlock;
		::Filter first_f(NoObj());
		first_f.Set();
		::Filter sec_f(t->NoObj());
		sec_f.Set();
		GroupDistinctTable dist_table;
		typedef shared_ptr<VirtualColumn> vc_ptr_t;
		typedef vector<vc_ptr_t> vcs_t;
		vcs_t first_vcs;
		vcs_t sec_vcs;
		uchar* input_buf = NULL;
		{	// block to ensure deleting encoder before deleting first_vcs, sec_vcs
			int size = 0;
			vector<ColumnBinEncoder> encoder;
			for(int i = 0; i < (int)NoDisplaybleAttrs(); i++) {
				first_vcs.push_back(vc_ptr_t( new SingleColumn(GetDisplayableAttrP(i), &output_mind, 0, -i - 1, this, 0) ));
				sec_vcs.push_back(vc_ptr_t( new SingleColumn(t->GetDisplayableAttrP(i), t->GetOutputMultiIndexP(), 1, -i - 1, t, 0) ));
				encoder.push_back(ColumnBinEncoder());
				bool encoder_created;
				if(NoObj() == 0)
					encoder_created = encoder[i].PrepareEncoder(sec_vcs[i].get());
				else if(t->NoObj() == 0)
					encoder_created = encoder[i].PrepareEncoder(first_vcs[i].get());
				else
					encoder_created = encoder[i].PrepareEncoder(first_vcs[i].get(), sec_vcs[i].get());
				if(!encoder_created) {
					stringstream ss;
					ss << "UNION of non-matching columns (column no " << i<< ") .";
					throw NotImplementedRCException(ss.str());
				}
				encoder[i].SetPrimaryOffset(size);
				size += encoder[i].GetPrimarySize();
			}
			input_buf = new uchar[size];
			dist_table.InitializeB(size, NoObj() + t->NoObj() / 2);		// optimization assumption: a half of values in the second table will be repetitions
			MIIterator first_mit(&output_mind);
			MIIterator sec_mit(t->GetOutputMultiIndexP());
			// check all objects from the first table
			do {
				first_mit.Rewind();
				dist_table.Clear();
				while(first_mit.IsValid()) {
					_int64 pos = **first_mit;
					if(first_f.Get(pos)) {
						for(int i = 0; i < encoder.size(); i++)
							encoder[i].Encode(input_buf, first_mit);
						GDTResult res = dist_table.Add(input_buf);
						if(res == GDT_EXISTS)
							first_mask.ResetDelayed(pos);
						if(res != GDT_FULL)				// note: if v is omitted here, it will also be omitted in sec!
							first_f.ResetDelayed(pos);
					}
					++first_mit;
					if(m_conn.killed())	
						throw KilledRCException();
				}
				sec_mit.Rewind();
				while(sec_mit.IsValid()) {
					_int64 pos = **sec_mit;
					if(sec_f.Get(pos)) {
						for(int i = 0; i < encoder.size(); i++)
							encoder[i].Encode(input_buf, sec_mit, sec_vcs[i].get());
						GDTResult res = dist_table.Add(input_buf);	
						if(res == GDT_EXISTS)
							sec_mask.ResetDelayed(pos);
						if(res != GDT_FULL)
							sec_f.ResetDelayed(pos);
					}
					++sec_mit;
					if(m_conn.killed())
						throw KilledRCException();
				}
				first_f.Commit();
				sec_f.Commit();
				first_mask.Commit();
				sec_mask.Commit();
			} while(!first_f.IsEmpty() || !sec_f.IsEmpty());
		}
		delete [] input_buf;
	}
	_int64 first_no_obj = first_mask.NoOnes();
	_int64 sec_no_obj = sec_mask.NoOnes();
	_int64 new_no_obj = first_no_obj + sec_no_obj;
	rccontrol.lock(m_conn.GetThreadID()) << "UNION: generating result (" << new_no_obj << " rows)." << unlock;
	int new_page_size = CalculatePageSize(new_no_obj);
	for(uint i = 0; i < NoDisplaybleAttrs(); i++) {
		Attr* first_attr = GetDisplayableAttrP(i);
		Attr* sec_attr = t->GetDisplayableAttrP(i);
		ColumnType new_type = GetUnionType(first_attr->Type(), sec_attr->Type());
		if(first_attr->Type() == sec_attr->Type() && first_mask.IsFull() && first_no_obj && sec_no_obj && 
			first_attr->Type().GetPrecision() >= sec_attr->Type().GetPrecision() ) {
			// fast path of execution: do not modify first buffer
			SetPageSize(new_page_size);
			FilterOnesIterator sec_fi(&sec_mask);
			for(_int64 j = 0; j < sec_no_obj; j++) {
				RCBString s;
				if(ATI::IsStringType(first_attr->TypeName())) {
					sec_attr->GetValueString(s, *sec_fi);
					first_attr->SetValueString(first_no_obj + j, s);
				} else {
					first_attr->SetValueInt64(first_no_obj + j, sec_attr->GetValueInt64(*sec_fi));
				}
				++sec_fi;
			}
			continue;
		}
		Attr* new_attr = new Attr(CQTerm(), LISTING, false, first_attr->alias, 0, new_type.GetTypeName(), new_type.GetScale(), new_type.GetPrecision(), new_type.GetNullsMode(), new_type.GetCollation());
		new_attr->page_size = new_page_size;
		new_attr->CreateBuffer(new_no_obj, &m_conn);
		if(first_attr->TypeName() == RC_NUM && sec_attr->TypeName() == RC_NUM && first_attr->Type().GetScale() != sec_attr->Type().GetScale()) {
			uint max_scale = new_attr->Type().GetScale(); 
			// copy attr from first table to new_attr
			double multiplier = PowOfTen(max_scale - first_attr->Type().GetScale());
			FilterOnesIterator first_fi(&first_mask);
			for(_int64 j = 0; j < first_no_obj; j++) {
				_int64 pos = *first_fi;
				++first_fi;
				if(!first_attr->IsNull(pos))
					new_attr->SetValueInt64(j, first_attr->GetNotNullValueInt64(pos) * (_int64) multiplier);
				else
					new_attr->SetValueInt64(j, NULL_VALUE_64);
			}
			// copy attr from second table to new_attr 
			multiplier = PowOfTen(max_scale - sec_attr->Type().GetScale());
			FilterOnesIterator sec_fi(&sec_mask);
			for(_int64 j = 0; j < sec_no_obj; j++) {
				_int64 pos = *sec_fi;
				++sec_fi;
				if(!sec_attr->IsNull(pos))
					new_attr->SetValueInt64(first_no_obj + j, sec_attr->GetNotNullValueInt64(pos) * (_int64) multiplier);
				else
					new_attr->SetValueInt64(first_no_obj + j, NULL_VALUE_64 );
			}
		} else if(ATI::IsStringType(new_attr->TypeName())) {

			RCBString s;
			FilterOnesIterator first_fi(&first_mask);
			for(_int64 j = 0; j < first_no_obj; j++) {
				first_attr->GetValueString(s,*first_fi);
				new_attr->SetValueString(j, s);
				++first_fi;
			}
			FilterOnesIterator sec_fi(&sec_mask);
			for(_int64 j = 0; j < sec_no_obj; j++) {
				sec_attr->GetValueString(s, *sec_fi);
				new_attr->SetValueString(first_no_obj + j,s);
				++sec_fi;
			}
		} else if(new_attr->Type().IsFloat()) {
			FilterOnesIterator first_fi(&first_mask);
			for(_int64 j = 0; j < first_no_obj; j++) {
				_int64 pos = *first_fi;
				++first_fi;
				if(first_attr->IsNull(pos))
					new_attr->SetValueInt64(j, NULL_VALUE_64);
				else if(first_attr->Type().IsFloat())
					new_attr->SetValueInt64(j, first_attr->GetNotNullValueInt64(pos));
				else {
					double v = (double)first_attr->GetNotNullValueInt64(pos) / PowOfTen(first_attr->Type().GetScale());
					new_attr->SetValueInt64(j, *(_int64*)&v);
				}
			}
			FilterOnesIterator sec_fi(&sec_mask);
			for(_int64 j = 0; j < sec_no_obj; j++) {
				_int64 pos = *sec_fi;
				++sec_fi;
				if(sec_attr->IsNull(pos))
					new_attr->SetValueInt64(first_no_obj + j, NULL_VALUE_64);
				else if(sec_attr->Type().IsFloat())
					new_attr->SetValueInt64(first_no_obj + j, sec_attr->GetNotNullValueInt64(pos));
				else {
					double v = (double)sec_attr->GetNotNullValueInt64(pos) / PowOfTen(sec_attr->Type().GetScale());
					new_attr->SetValueInt64(first_no_obj + j, *(_int64*)&v);
				}
			}
		} else {
			// copy attr from first table to new_attr
			double multiplier = PowOfTen(new_attr->Type().GetScale() - first_attr->Type().GetScale());
			FilterOnesIterator first_fi(&first_mask);
			for(_int64 j = 0; j < first_no_obj; j++) {
				_int64 pos = *first_fi;
				++first_fi;
				if(first_attr->IsNull(pos))
					new_attr->SetValueInt64(j, NULL_VALUE_64);
				else if(multiplier == 1.0)		// do not multiply by 1.0, as it causes precision problems on bigint
					new_attr->SetValueInt64(j, first_attr->GetNotNullValueInt64(pos));
				else
					new_attr->SetValueInt64(j, (_int64)(first_attr->GetNotNullValueInt64(pos) * multiplier));
			}
			multiplier = PowOfTen(new_attr->Type().GetScale() - sec_attr->Type().GetScale());
			FilterOnesIterator sec_fi(&sec_mask);
			for(_int64 j = 0; j < sec_no_obj; j++) {
				_int64 pos = *sec_fi;
				++sec_fi;
				if(sec_attr->IsNull(pos))
					new_attr->SetValueInt64(first_no_obj + j, NULL_VALUE_64);
				else if(multiplier == 1.0)		// do not multiply by 1.0, as it causes precision problems on bigint
					new_attr->SetValueInt64(first_no_obj + j, sec_attr->GetNotNullValueInt64(pos));
				else
					new_attr->SetValueInt64(first_no_obj + j, (_int64)(sec_attr->GetNotNullValueInt64(pos) * multiplier));
			}
		}
		attrs[GetDisplayableAttrIndex(i)] = new_attr;
		displayable_attr[i]	= new_attr;
		delete first_attr;
	}
	SetNoMaterialized(new_no_obj);
	//this->no_obj = new_no_obj;
	//this->Display();
	output_mind.Clear();
	output_mind.AddDimension_cross(no_obj);
}

void TempTable::Union(TempTable* t, int all, ResultSender* sender, _int64& g_offset, _int64& g_limit)
{
	MEASURE_FET("TempTable::UnionSender(...)");

	assert( NoDisplaybleAttrs() == t->NoDisplaybleAttrs() );
	if(NoDisplaybleAttrs() != t->NoDisplaybleAttrs())
		throw NotImplementedRCException("UNION of tables with different number of columns.");
	if(this->IsParametrized() || t->IsParametrized())
		throw NotImplementedRCException("Materialize: not implemented union of parameterized queries.");


	if(mode.top) {
		if(g_offset != 0 || g_limit != -1){
			mode.param2 = min(g_offset + g_limit, mode.param2);
		}
	} else if(g_offset != 0 || g_limit != -1) {
		mode.top = true;
		mode.param1 = 0;
		mode.param2 = g_limit + g_offset;
	}

	if(g_offset != 0 || g_limit != -1)
		sender->SetLimits(&g_offset, &g_limit);
	this->Materialize(false, sender);
	if(this->IsMaterialized() && !this->IsSent())
		sender->Send(this);

	if(t->mode.top) {
		if(g_offset != 0 || g_limit != -1){
			t->mode.param2 = min(g_offset + g_limit, t->mode.param2);
		}
	} else if(g_offset != 0 || g_limit != -1) {
		t->mode.top = true;
		t->mode.param1 = 0;
		t->mode.param2 = g_limit + g_offset;
	}

	// second query might want to switch to mysql but the first is already sent
	// we have to prevent switching to mysql otherwise Malformed Packet error may occur
	try {
		t->Materialize(false, sender);
	} catch(RCException& e) {
		std::string msg(e.what());
		msg.append(" Can't switch to MySQL execution path");
		throw InternalRCException(msg);
	}
	if(t->IsMaterialized() && !t->IsSent())
		sender->Send(t);
}

void TempTable::Display(ostream & out)
{
	out << "No obj.:" << no_obj << ", No attrs.:" << this->NoDisplaybleAttrs() << endl << "-----------" << endl;
	for(_int64 i = 0; i < no_obj; i++) {
		for(uint j = 0; j < this->NoDisplaybleAttrs(); j++) {
			if(!attrs[j]->alias)
				continue;
			if(!IsNull(i, j)){
				RCBString s;
				GetTable_S(s, i, j);
				out << s << " ";
			}
			else
				out << "NULL" << " ";
		}
		out << endl;
	}
	out << "-----------" << endl;
}

int TempTable::GetDimension(TabID alias)
{
	for(uint i = 0; i < aliases.size(); i++) {
		if(aliases[i] == alias.n)
			return i;
	}
	return NULL_VALUE_32;
}

_int64 TempTable::GetTable64(_int64 obj, int attr)
{
	if(no_obj == 0)
		return NULL_VALUE_64;
	assert(obj < no_obj && (uint)attr < attrs.size());
	return attrs[attr]->GetValueInt64(obj);
}

void TempTable::GetTable_S(RCBString& s, _int64 obj, int _attr)
{
	if(no_obj == 0) {
		s = RCBString();
		return;
	}
	uint attr = (uint) _attr;
	assert(obj < no_obj && attr < attrs.size());
	attrs[attr]->GetValueString(s, obj);
}

void TempTable::GetTable_B(_int64 obj, int _attr, int & size, char *val_buf)
{
	throw NotImplementedRCException("TempTable::GetTable_B.");
	assert(0);
	// TODO: get table B
	return;
}

bool TempTable::IsNull(_int64 obj, int attr)
{
	if (no_obj == 0)
		return true;
	assert(obj < no_obj && (uint)attr < attrs.size());
	return attrs[attr]->IsNull(obj);
}

_uint64 TempTable::ApproxAnswerSize(int attr, Descriptor &d) // provide the most probable approximation of number of objects matching the condition
{
	// TODO: can it be done better?
	return no_obj / 2;
}

void TempTable::GetTableString(RCBString& s, int64 obj, uint attr)
{
	if(no_obj == 0 )
		s = RCBString();
	assert(obj < no_obj && (uint)attr < attrs.size());
	attrs[attr]->GetValueString(s, obj);
}

RCValueObject TempTable::GetTable(_int64 obj, uint attr)
{
	if(no_obj == 0 )
		return RCValueObject();
	assert(obj < no_obj && (uint)attr < attrs.size());
	return attrs[attr]->GetValue(obj);
}

int TempTable::CalculatePageSize(_int64 _no_obj)
{
	_int64 new_no_obj = _no_obj == -1 ? no_obj : _no_obj;
	int size_of_one_record = 0;
	for(uint i = 0; i < attrs.size(); i++)
		if(attrs[i]->TypeName() == RC_BIN || attrs[i]->TypeName() == RC_BYTE || attrs[i]->TypeName() == RC_VARBYTE
				|| attrs[i]->TypeName() == RC_STRING || attrs[i]->TypeName() == RC_VARCHAR)
			size_of_one_record += attrs[i]->Type().GetInternalSize() + 4; // 4 bytes describing length
		else
			size_of_one_record += attrs[i]->Type().GetInternalSize();
	int raw_size = (int)new_no_obj;
	if(size_of_one_record < 1)
		size_of_one_record = 1;

	_uint64 cache_size;
	if(mem_scale == -1)
		mem_scale = TrackableObject::MemorySettingsScale();

	switch(mem_scale) {
		case 0: cache_size = 1l << 26; break; 	//64MB
		case 1: cache_size = 1l << 26; break;	//64MB
		case 2: cache_size = 1l << 27; break;	//128MB
		case 3: cache_size = 1l << 28; break;	//256MB
		case 4: cache_size = 1l << 28; break;	//256MB
		case 5: cache_size = 1l << 29; break;	//512MB
		case 6: cache_size = 1l << 29; break;	//512MB
		default: cache_size = 1l << 29; break;	//512MB
	}

//	std::cerr << "cs=" << cache_size << std::endl;

	if(cache_size / size_of_one_record <= (_uint64)new_no_obj) {
		raw_size = int(cache_size / size_of_one_record);
		raw_size = 1 << (CalculateBinSize((uint)raw_size)); // round up to 2^k
	}
	if(raw_size < 1)
		raw_size = 1;
	for(uint i = 0; i < attrs.size(); i++)
		attrs[i]->page_size = raw_size;
	return raw_size;
}

void TempTable::SetPageSize(_int64 new_page_size)
{
	for(uint i = 0; i < attrs.size(); i++)
		attrs[i]->SetNewPageSize((uint)new_page_size);
}

/*! Filter out rows from the given multiindex according to the tree of descriptors
*
*/

void TempTable::DisplayRSI()
{
	for(uint i = 0; i < tables.size(); i++) {
		if(tables[i]->TableType() == RC_TABLE)
			((RCTable*) tables[i])->DisplayRSI();
	}
}

////////////////////////////////////////////////////
// Refactoring: extracted methods

void TempTable::RemoveFromManagedList(const RCTable* tab)
{
	tables.erase(std::remove(tables.begin(), tables.end(), tab), tables.end());
}

void TempTable::ApplyOffset(_int64 limit, _int64 offset)
{
	// filter out all unwanted values from buffers
	no_obj = limit;
	for(uint i = 0; i < attrs.size(); i++) {
		if(attrs[i]->alias)
			attrs[i]->ApplyFilter(output_mind, offset, offset + limit);
		else
			attrs[i]->DeleteBuffer();
	}
}

void TempTable::ProcessParameters(const MIIterator& mit, const int alias)
{
	MEASURE_FET("TempTable::ProcessParameters(...)");
	for(uint i = 0; i< virt_cols.size(); i++)
		virt_cols[i]->RequestEval(mit,alias);
	filter.ProcessParameters();
	//filter.Prepare();
	if(mode.top && LimitMayBeAppliedToWhere())		// easy case - apply limits
		filter.UpdateMultiIndex( false, (mode.param2 >= 0 ? mode.param2 : 0));
	else
		filter.UpdateMultiIndex( false, -1);
}

void TempTable::RoughProcessParameters(const MIIterator& mit, const int alias)
{
	MEASURE_FET("TempTable::RoughProcessParameters(...)");
	for(uint i = 0; i< virt_cols.size(); i++)
		virt_cols[i]->RequestEval(mit,alias);
	filter.ProcessParameters();
	filter.RoughUpdateParamFilter();
}

bool TempTable::IsParametrized()
{
	for(int i = 0; i < virt_cols.size(); i++)
		if(virt_cols[i] && virt_cols[i]->IsParameterized())
			return true;
	return false;
}

int TempTable::DimInDistinctContext()
{
	// return a dimension number if it is used only in contexts where row repetitions may be omitted, e.g. distinct
	int d = -1;
	if(HasHavingConditions() || filter.mind->NoDimensions() == 1)	// having or no joins
		return -1;
	bool group_by_exists = false;
	bool aggregation_exists = false;
	for(uint i = 0; i < attrs.size(); i++) if(attrs[i]) {
		if(attrs[i]->mode == GROUP_BY)
			group_by_exists = true;
		if(attrs[i]->mode != GROUP_BY && attrs[i]->mode != LISTING && attrs[i]->mode != DELAYED)
			aggregation_exists = true;
		if(!attrs[i]->term.vc || attrs[i]->term.vc->IsConst())
			continue;
		int local_dim = attrs[i]->term.vc->GetDim();
		if(local_dim == -1 || (d != -1 && d != local_dim))
			return -1;
		d = local_dim;
	}
	// all columns are based on the same dimension
	if(group_by_exists && aggregation_exists)			// group by with aggregations - not distinct context
		return -1;
	if(group_by_exists || (mode.distinct && !aggregation_exists))	// select distinct a,b,c...; select a,b,c group by a,b,c
		return d;
	// aggregations exist - check their type
	for(uint i = 0; i < attrs.size(); i++)
		if(attrs[i] && 
			attrs[i]->mode != MIN && 
			attrs[i]->mode != MAX &&
			!attrs[i]->distinct)
			return -1;
	return d;
}

bool TempTable::LimitMayBeAppliedToWhere()
{
	if(order_by.size() > 0)			// ORDER BY => false
		return false;
	if(mode.distinct || HasHavingConditions())		// DISTINCT or HAVING  => false
		return false;
	for(uint i = 0; i < NoAttrs(); i++)	// GROUP BY or other aggregation => false
		if(attrs[i]->mode != LISTING)
			return false;
	return true;
}

int TempTable::AddVirtColumn(VirtualColumn* vc)
{
	virt_cols.push_back(vc);
	virt_cols_for_having.push_back(vc->GetMultiIndex() == &output_mind);
	return (int)virt_cols.size() - 1;
}

int	TempTable::AddVirtColumn(VirtualColumn* vc, int no)
{
	assert(no < virt_cols.size());
	virt_cols_for_having[no] = (vc->GetMultiIndex() == &output_mind);
	virt_cols[no] = vc;
	return no;
}

void TempTable::ResetVCStatistics()
{
	for(int i = 0; i < virt_cols.size(); i++)
		if(virt_cols[i])
			virt_cols[i]->ResetLocalStatistics();
}

void TempTable::SetVCDistinctVals(int dim, _int64 val)
{
	for(int i = 0; i < virt_cols.size(); i++)
		if(virt_cols[i] && virt_cols[i]->GetDim() == dim)
			virt_cols[i]->SetLocalDistVals(val);
}

TempTablePtr TempTable::Create(const TempTable& t, bool in_subq, bool for_rough_query)
{
	MEASURE_FET("TempTable::Create(...)");
	bool _is_vc_owner = !in_subq;
	TempTablePtr tnew = boost::shared_ptr<TempTable>(new TempTable(t, _is_vc_owner, for_rough_query));
	if(in_subq) {
		//map<PhysicalColumn *, PhysicalColumn *> attr_translation;	// SingleColumns should refer to columns of tnew
		for(uint i = 0; i < t.attrs.size(); i++) {
			//attr_translation[t.attrs[i]] = tnew->attrs[i];
			tnew->attr_back_translation[tnew->attrs[i]] = t.attrs[i];
		}
		//for(uint i = 0; i< tnew->virt_cols.size(); i++) {
		//	if(tnew->virt_cols[i]->IsSingleColumn())
		//		static_cast<SingleColumn*>(tnew->virt_cols[i])->TranslateSourceColumns(attr_translation);
		//	if(tnew->virt_cols_for_having[i]) {
		//		tnew->virt_cols[i]->SetMultiIndex(&tnew->output_mind, tnew);
		//	} else
		//		tnew->virt_cols[i]->SetMultiIndex(tnew->filter.mind);
		//}
	}
	return tnew;
}

TempTablePtr TempTable::Create(JustATable* const t, int alias, Query* q, bool for_subq)
{
	if(for_subq)
		return boost::shared_ptr<TempTable>(new TempTableForSubquery(t, alias, q));
	else
		return boost::shared_ptr<TempTable>(new TempTable(t, alias, q));
}

ColumnType TempTable::GetUnionType(ColumnType type1, ColumnType type2)
{
	if(type1.IsFloat() || type2.IsFloat())
		return type1.IsFloat() ? type1 : type2;
	if(type1.IsString() && type1.GetPrecision() != 0 && !type2.IsString()) {
		ColumnType t(type1);
		if(t.GetPrecision() < type2.GetPrecision())
			t.SetPrecision(type2.GetPrecision());
		return t;
	}
	if(type2.IsString() && type2.GetPrecision() != 0 && !type1.IsString()) {
		ColumnType t(type2);
		if(t.GetPrecision() < type1.GetPrecision())
			t.SetPrecision(type1.GetPrecision());
		return t;
	}
	if(type1.IsFixed() && type2.IsFixed() && type1.GetScale() != type2.GetScale()) {
		uint max_scale = type1.GetScale() > type2.GetScale() ? type1.GetScale() : type2.GetScale();
		uint max_ints = (type1.GetPrecision() - type1.GetScale()) > (type2.GetPrecision()
			- type2.GetScale()) ? (type1.GetPrecision() - type1.GetScale())
			: (type2.GetPrecision() - type2.GetScale());
		if(max_ints + max_scale > 18) {
			stringstream ss;
			ss << "UNION of non-matching columns (column no " << 0 << ") .";
			throw NotImplementedRCException(ss.str());
		}
		return type1.GetScale() > type2.GetScale() ? type1 : type2;
	}
	if(type1.GetInternalSize() < type2.GetInternalSize())
		return type2;
	else if(type1.GetInternalSize() == type2.GetInternalSize())
		return type1.GetPrecision() > type2.GetPrecision() ? type1 : type2;
	else
		return type1;
}

bool TempTable::SubqueryInFrom()
{
	for(int i = 0; i < tables.size(); i++)
		if(tables[i]->TableType() == TEMP_TABLE)
			return true;
	return false;
}

void TempTable::LockPackForUse(unsigned attr, unsigned pack_no, ConnectionInfo& conn)
{
	while(lazy && no_materialized < min(((_int64)pack_no << 16) + 0x10000, no_obj))
		Materialize(false, NULL, true);
}

bool TempTable::CanOrderSources()
{
	for(int i = 0; i < order_by.size(); i++) {
		if(order_by[i].vc->GetMultiIndex() != filter.mind)
			return false;
	}
	return true;
}

void TempTable::Materialize(bool in_subq, ResultSender* sender, bool lazy)
{
	MEASURE_FET("TempTable::Materialize()");
	CreateDisplayableAttrP();
	CalculatePageSize();
	_int64 offset = 0; // controls the first object to be materialized
	_int64 limit = -1;	// if(limit>=0)  ->  for(row = offset; row < offset + limit; row++) ....
	bool limits_present = mode.top;
	bool exists_only = mode.exists;

	if(limits_present) {
		offset = (mode.param1 >= 0 ? mode.param1 : 0);
		limit = (mode.param2 >= 0 ? mode.param2 : 0);
		// applied => reset to default values
		mode.top = false;
		mode.param2 = -1;
		mode.param1 = 0;
	}
	_int64 local_offset = 0; // controls the first object to be materialized in a given algorithm
	_int64 local_limit = -1;

	for(int i=0; i< virt_cols.size(); i++) {
		if(virt_cols[i] && virt_cols[i]->NeedsPreparing())
				virt_cols[i]->Prepare();
	}

	if(materialized && (order_by.size() > 0 || limits_present) && no_obj) {
		// if TempTable is already materialized but some additional constraints were specified, e.g., order by or limit
		// this is typical case for union, where constraints are specified for the result of union after materialization
		if(limits_present) {
			local_offset = offset;
			local_limit = min(limit, (_int64)no_obj - offset);
			local_limit = local_limit < 0 ? 0 : local_limit;
		} else
			local_limit = no_obj;
		if(exists_only) {
			if(local_limit == 0)		// else no change needed
				no_obj = 0;
			return;
		}

		if(order_by.size() != 0 && no_obj > 1) {
			TempTablePtr temporary_source_table = CreateMaterializedCopy(true, in_subq);		// true: translate definition of ordering
			OrderByAndMaterialize(order_by, local_limit, local_offset);
			DeleteMaterializedCopy(temporary_source_table);
		} else if(limits_present)
			ApplyOffset(local_limit, local_offset);
		order_by.clear();
		return;
	} 

	if((materialized && !this->lazy) || (this->lazy && no_obj == no_materialized)) {
		return;
	}

	bool table_distinct = this->mode.distinct;
	bool distinct_on_materialized = false;
	for(uint i = 0; i < NoAttrs(); i++)
		if(attrs[i]->mode != LISTING)
			group_by = true;
	if(table_distinct && group_by) {
		distinct_on_materialized = true;
		table_distinct = false;
	}

	if(order_by.size() > 0 || group_by || this->mode.distinct)
		lazy = false;
	this->lazy = lazy;

	bool no_rows_too_large = filter.mind->TooManyTuples();
	no_obj = -1; // no_obj not calculated yet - wait for better moment
	VerifyAttrsSizes(); // resize attr[i] buffers basing on the current multiindex state

	// the case when there is no grouping of attributes, check also DISTINCT modifier of TT
	if(!group_by && !table_distinct) {
		assert(!distinct_on_materialized);		// should by false here, otherwise must be added to conditions below

		if(limits_present) {
			if(no_rows_too_large && order_by.size() == 0)
				no_obj = offset + limit; // offset + limit in the worst case
			else
				no_obj = filter.mind->NoTuples();
			if(no_obj <= offset) {
				no_obj = 0;
				materialized = true;
				order_by.clear();
				return;
			}
			local_offset = offset;
			local_limit = min(limit, (_int64)no_obj - offset);
			local_limit = local_limit < 0 ? 0 : local_limit;
		} else {
			no_obj = filter.mind->NoTuples();
			local_limit = no_obj;
		}
		if(exists_only) {
			order_by.clear();
			return;
		}
		output_mind.Clear();
		output_mind.AddDimension_cross(local_limit);	// an artificial dimension for result

		CalculatePageSize();		// recalculate, as no_obj might changed
		// perform order by: in this case it can be done on source tables, not on the result
		bool materialized_by_ordering = false;
		if(CanOrderSources())
			// false if no sorting used		
			materialized_by_ordering = this->OrderByAndMaterialize(order_by, local_limit, local_offset, sender);	
		if(!materialized_by_ordering) 		// not materialized yet?
			// materialize without aggregations. If ordering then do not send result
			if(order_by.size() == 0)
				FillMaterializedBuffers(local_limit, local_offset, sender, lazy);		
			else // in case of order by we need to materialize all rows to be next ordered
				FillMaterializedBuffers(no_obj, 0, NULL, lazy);		
	} else {
		// GROUP BY or DISTINCT -  compute aggregations
		if(limits_present && !distinct_on_materialized && order_by.size() == 0) {
			local_offset = offset;
			local_limit = limit;
			if(exists_only)
				local_limit = 1;
		}
		if(HasHavingConditions() && in_subq)
			having_conds[0].tree->Simplify(true);

		ResultSender *local_sender = (distinct_on_materialized || order_by.size() > 0 ? NULL : sender);
		AggregationAlgorithm aggr(this);
		aggr.Aggregate(table_distinct, local_limit, local_offset, local_sender);		// this->tree (HAVING) used inside
		if(no_obj == 0) {
			order_by.clear();
			return;
		}

		output_mind.Clear();
		output_mind.AddDimension_cross(no_obj);	// an artificial dimension for result
	}

	local_offset = 0;
	local_limit = -1;

	// DISTINCT after grouping
	if(distinct_on_materialized && no_obj > 1) {
		if(limits_present && order_by.size() == 0) {
			local_offset = min((_int64)no_obj, offset);
			local_limit = min(limit, (_int64)no_obj - local_offset);
			local_limit = local_limit < 0 ? 0 : local_limit;
			if(no_obj <= offset) {
				no_obj = 0;
				materialized = true;
				order_by.clear();
				return;
			}
		} else
			local_limit = no_obj;
		if(exists_only)
			local_limit = 1;
		TempTablePtr temporary_source_table = CreateMaterializedCopy(false, in_subq);
		ResultSender *local_sender = (order_by.size() > 0 ? NULL : sender);

		AggregationAlgorithm aggr(this);
		aggr.Aggregate(true, local_limit, local_offset, local_sender);	// true => select-level distinct
		DeleteMaterializedCopy(temporary_source_table);
		output_mind.Clear();
		output_mind.AddDimension_cross(no_obj);	// an artificial dimension for result
	} // end of distinct part
	// ORDER BY, if not sorted until now
	if(order_by.size() != 0) {
		if(limits_present) {
			local_offset = min((_int64)no_obj, offset);
			local_limit = min(limit, (_int64)no_obj - local_offset);
			local_limit = local_limit < 0 ? 0 : local_limit;
			if(no_obj <= offset) {
				no_obj = 0;
				materialized = true;
				order_by.clear();
				return;
			}
		} else
			local_limit = no_obj;
		if(no_obj > 1 && !exists_only) {
			TempTablePtr temporary_source_table = CreateMaterializedCopy(true, in_subq);		// true: translate definition of ordering
			OrderByAndMaterialize(order_by, local_limit, local_offset, sender);
			DeleteMaterializedCopy(temporary_source_table);
		}
		order_by.clear();
		output_mind.Clear();
		output_mind.AddDimension_cross(no_obj);	// an artificial dimension for result
	}
	materialized = true;
}

bool TempTable::IsFullyMaterialized()
{
	return materialized && ((order_by.size() == 0 && !mode.top) || !no_obj);
}

RCDataType& TempTable::Record::operator[](uint i) const
{
	return *(_it.dataTypes[i]);
}

void TempTable::RecordIterator::PrepareValues()
{
	if (_currentRNo < _uint64(table->NoObj())) {
		uint no_disp_attr = table->NoDisplaybleAttrs();
		for(uint att = 0; att < no_disp_attr; ++att) {
			AttributeType attrt_tmp = table->GetDisplayableAttrP(att)->TypeName();
			if(attrt_tmp == RC_INT || attrt_tmp == RC_MEDIUMINT) {
				int& v = (*(TempTable::AttrBuffer<int>*) table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
				if(v == NULL_VALUE_32)
					dataTypes[att]->SetToNull();
				else
					((RCNum*)dataTypes[att])->Assign(v, 0, false, attrt_tmp);
			} else if(attrt_tmp == RC_SMALLINT) {
				short& v = (*(TempTable::AttrBuffer<short>*) table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
				if(v == NULL_VALUE_SH)
					dataTypes[att]->SetToNull();
				else
					((RCNum*)dataTypes[att])->Assign(v, 0, false, attrt_tmp);
			} else if(attrt_tmp == RC_BYTEINT) {
				char& v = (*(TempTable::AttrBuffer<char>*) table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
				if(v == NULL_VALUE_C)
					dataTypes[att]->SetToNull();
				else
					((RCNum*)dataTypes[att])->Assign(v, 0, false, attrt_tmp);
			} else if(ATI::IsRealType(attrt_tmp)) {
				double& v = (*(TempTable::AttrBuffer<double>*) table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
				if(v == NULL_VALUE_D)
					dataTypes[att]->SetToNull();
				else
					((RCNum*)dataTypes[att])->Assign(v);
			} else if(attrt_tmp == RC_NUM || attrt_tmp == RC_BIGINT ){
				_int64& v = (*(TempTable::AttrBuffer<_int64>*) table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
				if(v == NULL_VALUE_64)
					dataTypes[att]->SetToNull();
				else
					((RCNum*)dataTypes[att])->Assign(v, table->GetDisplayableAttrP(att)->Type().GetScale(), false, attrt_tmp);
			} else if (ATI::IsDateTimeType(attrt_tmp)) {
				_int64& v = (*(TempTable::AttrBuffer<_int64>*) table->GetDisplayableAttrP(att)->buffer)[_currentRNo];
				if(v == NULL_VALUE_64)
					dataTypes[att]->SetToNull();
				else
					((RCDateTime*)dataTypes[att])->Assign(v, attrt_tmp);
			} else {
				BHASSERT( ATI::IsStringType(attrt_tmp), "not all possible attr_types checked" );
				(*(TempTable::AttrBuffer<RCBString>*)table->GetDisplayableAttrP(att)->buffer).GetString(*(RCBString*)dataTypes[att], _currentRNo);
			}
		}
	}
}


TempTable::RecordIterator& TempTable::RecordIterator::operator ++ ( void )
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( _currentRNo < _uint64(table->NoObj()) );
	is_prepared = false;
	++ _currentRNo;
	return ( *this );
}

TempTable::RecordIterator TempTable::begin(ConnectionInfo *conn)
{
	return (RecordIterator(this, conn, 0));
}

TempTable::RecordIterator TempTable::end(ConnectionInfo *conn)
{
	return (RecordIterator(this, conn, NoObj()));
}

TempTable::RecordIterator::RecordIterator( void )
: table( NULL ), _currentRNo( 0 ), _conn( NULL ), is_prepared(false)
{
}

TempTable::RecordIterator::RecordIterator( TempTable* table_, ConnectionInfo* conn_, _uint64 rowNo_ )
: table( table_ ), _currentRNo( rowNo_ ), _conn( conn_ ), is_prepared(false)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( table != 0 );
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( _currentRNo <= _uint64(table->NoObj()) );
	for (uint att = 0; att < table->NoDisplaybleAttrs(); att++) {
		AttributeType att_type = table->GetDisplayableAttrP(att)->TypeName();
		if (att_type == RC_INT || att_type == RC_MEDIUMINT || att_type == RC_SMALLINT || att_type == RC_BYTEINT
				|| ATI::IsRealType(att_type) || att_type == RC_NUM || att_type == RC_BIGINT)
			dataTypes.push_back(new RCNum());
		else if (ATI::IsDateTimeType(att_type))
			dataTypes.push_back(new RCDateTime());
		else {
			BHASSERT( ATI::IsStringType(att_type), "not all possible attr_types checked" );
			dataTypes.push_back(new RCBString());
		}
	}
}

TempTable::RecordIterator::~RecordIterator()
{
	for (std::vector<RCDataType*>::const_iterator it = dataTypes.begin(); it != dataTypes.end(); it++)
		delete (*it);
	dataTypes.clear();
}

TempTable::RecordIterator::RecordIterator( RecordIterator const& it )
: table( it.table ), _currentRNo( it._currentRNo ), _conn( it._conn ), is_prepared(false)
{
	for (int att = 0; att < it.dataTypes.size(); att++) {
		if (it.dataTypes[att] != NULL) {
			std::auto_ptr<RCDataType> attr_datatype = it.dataTypes[att]->Clone();
			dataTypes.push_back(attr_datatype.release());
		}
		else
			dataTypes.push_back(NULL);
	}
}

TempTable::RecordIterator& TempTable::RecordIterator::operator = ( RecordIterator const& it )
{
	if ( &it != this ) {
		table = it.table;
		_currentRNo = it._currentRNo;
		_conn = it._conn;
		is_prepared = false;
		for (std::vector<RCDataType*>::const_iterator iter = dataTypes.begin(); iter != dataTypes.end(); iter++)
			delete (*iter);
		dataTypes.clear();
		for (int att = 0; att < it.dataTypes.size(); att++) {
			if (it.dataTypes[att] != NULL) {
				std::auto_ptr<RCDataType> attr_datatype = it.dataTypes[att]->Clone();
				dataTypes.push_back(attr_datatype.release());
			}
			else
				dataTypes.push_back(NULL);
		}
	}
	return ( *this );
}

bool TempTable::RecordIterator::operator == (RecordIterator const& it) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ( ! ( table || it.table ) ) || ( table == it.table ) );
	return ( _currentRNo == it._currentRNo );
}

bool TempTable::RecordIterator::operator != (RecordIterator const& it) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ( ! ( table || it.table ) ) || ( table == it.table ) );
	return ( _currentRNo != it._currentRNo );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
TempTableForSubquery::~TempTableForSubquery()
{
	if(no_global_virt_cols != -1) { //otherwise never evaluated
		for(uint i = no_global_virt_cols; i < virt_cols.size(); i++)
			delete virt_cols[i];

		virt_cols.resize(no_global_virt_cols);

		for(int i=0; i< template_virt_cols.size(); i++)
			if(copied_virt_cols[i]) {
				delete virt_cols[i];
			}
		virt_cols = template_virt_cols;
	}
	delete template_filter;
	SetAttrsForExact();
	for(int i = 0; i < attrs_for_rough.size(); i++)
		delete attrs_for_rough[i];
	for(int i = 0; i < template_attrs.size(); i++)
		delete template_attrs[i];
}

void TempTableForSubquery::ResetToTemplate(bool rough)
{
	if(!template_filter)
		return;

	for(uint i = no_global_virt_cols; i < virt_cols.size(); i++)
		delete virt_cols[i];

	virt_cols.resize(template_virt_cols.size());

	for(int i = 0; i < template_virt_cols.size(); i++)
		if(copied_virt_cols[i]) {
			virt_cols[i]->AssignFromACopy(template_virt_cols[i]);
		} else
			virt_cols[i] = template_virt_cols[i];

	for(uint i = 0; i < attrs.size(); i++) {
		void* orig_buf = (*attrs[i]).buffer;
		*attrs[i] = *template_attrs[i];
		(*attrs[i]).buffer = orig_buf;
	}

	filter = *template_filter;
	for(int i = 0; i < no_global_virt_cols; i++)
		if(!virt_cols_for_having[i])
			virt_cols[i]->SetMultiIndex(filter.mind);

	having_conds = template_having_conds;
	order_by = template_order_by;
	mode = template_mode;
	no_obj = 0;
	if(rough)
		rough_materialized = false;
	else
		materialized = false;
	no_materialized = 0;
	rough_is_empty = BHTribool();
}


void TempTableForSubquery::SetAttrsForRough()
{
	if(!is_attr_for_rough) {
		for(int i = (int)attrs_for_exact.size(); i < attrs.size(); i++)
			delete attrs[i];
		attrs = attrs_for_rough;
		is_attr_for_rough = true;
	}
}

void TempTableForSubquery::SetAttrsForExact()
{
	if(is_attr_for_rough)  {
		for(int i = (int)attrs_for_rough.size(); i < attrs.size(); i++)
			delete attrs[i];
		attrs = attrs_for_exact;
		is_attr_for_rough = false;
	}
}

void TempTableForSubquery::Materialize(bool in_subq, ResultSender* sender, bool lazy)
{
	CreateTemplateIfNotExists();
	SetAttrsForExact();
	TempTable::Materialize(in_subq);
}

void TempTableForSubquery::RoughMaterialize(bool in_subq, ResultSender* sender, bool lazy)
{
	CreateTemplateIfNotExists();
	SetAttrsForRough();
	if(rough_materialized)
		return;
	TempTable::RoughMaterialize(in_subq);
	materialized = false;
	rough_materialized = true;
}

void TempTableForSubquery::CreateTemplateIfNotExists()
{
	if(attrs_for_rough.size() == 0) {
		attrs_for_exact = attrs;
		is_attr_for_rough = false;
		for(int i = 0; i < attrs.size(); i++)
			attrs_for_rough.push_back(new Attr(*attrs[i]));
	}
	if(!template_filter && IsParametrized()) {
		template_filter = new ParameterizedFilter(filter);
		for(int i = 0; i < attrs.size(); i++)
			template_attrs.push_back(new Attr(*attrs[i]));
		no_global_virt_cols = int(virt_cols.size());
		template_having_conds = having_conds;
		template_order_by = order_by;
		template_mode = mode;
		template_virt_cols = virt_cols;
		for(int i=0; i< template_virt_cols.size(); i++)
			if(template_virt_cols[i]->MustCopyForSubq()) {
				copied_virt_cols.push_back(true);
				template_virt_cols[i]= virt_cols[i]->Copy();
			} else
				copied_virt_cols.push_back(false);
	}
}
