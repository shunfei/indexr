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

#include "TypeCastColumn.h"

#include "types/RCNum.h"

extern VirtualColumn* CreateVCCopy(VirtualColumn* vc);

using namespace std;

#ifndef PURE_LIBRARY
MYSQL_ERROR *IBPushWarning(THD *thd, MYSQL_ERROR::enum_warning_level level, uint code, const char *msg)
{
	static IBMutex ibm;
	IBGuard guard(ibm);
	return push_warning(thd, level, code, msg);
}
#endif

void GMTSec2GMTTime(MYSQL_TIME* tmp, my_time_t t)
{
#ifndef PURE_LIBRARY
	time_t tmp_t = (time_t)t;
	struct tm tmp_tm;
	gmtime_r(&tmp_t, &tmp_tm);
	localtime_to_TIME(tmp, &tmp_tm);
	tmp->time_type= MYSQL_TIMESTAMP_DATETIME;
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#endif
}

bool IsTimeStampZero(MYSQL_TIME& t)
{
#ifndef PURE_LIBRARY
	return t.day == 0 && t.hour == 0 && t.minute == 0 && t.month == 0 && t.second == 0 && t.second_part == 0 && t.year == 0;
#else
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return false;
#endif
}

TypeCastColumn::TypeCastColumn(VirtualColumn* from, ColumnType const& target_type)
	:	VirtualColumn(target_type.RemovedLookup(), from->GetMultiIndex()), full_const(false), vc(from)
{ 
	dim = from->GetDim(); 
}

TypeCastColumn::TypeCastColumn(const TypeCastColumn& c) : VirtualColumn(c)
{
	assert(CanCopy());
	vc = CreateVCCopy(c.vc);
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
String2NumCastColumn::String2NumCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if(full_const) {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if(rs.IsNull()) {
			rcv = RCNum();
			val = NULL_VALUE_64;
		} else {		
			BHReturnCode retc = ct.IsFixed() ? RCNum::ParseNum(rs, rcn, ct.GetScale()) : RCNum::Parse(rs, rcn, to.GetTypeName());
			if(retc != BHRC_SUCCESS) {
				std::string s = "Truncated incorrect numeric value: \'";
				s += (string)rs;
				s += "\'";
			}
		}
		rcv = rcn;
		val = rcn.GetValueInt64();
	}
#endif

}

_int64 String2NumCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(full_const)
		return val;
	RCBString rs;
	RCNum rcn;
	vc->GetValueString(rs, mit);
	if(ct.IsFixed()) {
		if(RCNum::ParseNum(rs, rcn, ct.GetScale()) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: \'";
			s += (string)rs;
			s += "\'";
			IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
	} else {
		if(RCNum::ParseReal(rs, rcn, ct.GetTypeName()) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: \'";
			s += (string)rs;
			s += "\'";
			IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
	}
	return rcn.GetValueInt64();
#endif

}

_int64 String2NumCastColumn::DoGetValueInt64(const MIIterator& mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(full_const)
		return val;
	else {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if(rs.IsNull()) {
			return NULL_VALUE_64;
		} else {	
			BHReturnCode rc;
			if(ct.IsInt()) { // distinction for convert function
				rc = RCNum::Parse(rs, rcn, ct.GetTypeName());
				if(rc != BHRC_SUCCESS) {
					std::string s = "Truncated incorrect numeric value: \'";
					s += (string)rs;
					s += "\'";
					IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				}
				if(rc == BHRC_OUT_OF_RANGE_VALUE && rcn.GetValueInt64() > 0)
					return -1;
			} else if(ct.IsFixed()) {
				rc = RCNum::ParseNum(rs, rcn, ct.GetScale());
				if(rc != BHRC_SUCCESS) {
					std::string s = "Truncated incorrect numeric value: \'";
					s += (string)rs;
					s += "\'";
					IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				}
			} else {
				if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
					std::string s = "Truncated incorrect numeric value: \'";
					s += (string)rs;
					s += "\'";
					IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				}
			}
			return rcn.GetValueInt64();
		}
	}
#endif
}

double String2NumCastColumn::DoGetValueDouble(const MIIterator& mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0.0;
#else
	if(full_const)
		return *(double*)&val;
	else {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if(rs.IsNull()) {
			return NULL_VALUE_D;
		} else {
			RCNum::Parse(rs, rcn, RC_FLOAT);
			//if(RCNum::Parse(rs, rcn, RC_FLOAT) != BHRC_SUCCESS) {
			//	std::string s = "Truncated incorrect numeric value: \'";
			//	s += (string)rs;
			//  s += "\'";
			//	IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
			//}
			return (double)rcn;
		}
	}
#endif
}

RCValueObject String2NumCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCValueObject();
#else
	if(full_const)
		return rcv;
	else {
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, mit);
		if(rs.IsNull()) {
			return RCNum();
		} else if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: \'";
			s += (string)rs;
			s += "\'";
			IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
		return rcn;
	}
#endif
}

_int64 String2NumCastColumn::DoGetMinInt64(const MIIterator& m)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return 0;
#else
	if(full_const)
		return val;
	else if(IsConst()) {
		// const with parameters
		RCBString rs;
		RCNum rcn;
		vc->GetValueString(rs, m);
		if(rs.IsNull()) {
			return NULL_VALUE_64;
		} else if(RCNum::Parse(rs, rcn) != BHRC_SUCCESS) {
			std::string s = "Truncated incorrect numeric value: \'";
			s += (string)rs;
			s += "\'";
			IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		}
		return rcn.GetValueInt64();
	} else
	return MINUS_INF_64;
#endif
}

_int64 String2NumCastColumn::DoGetMaxInt64(const MIIterator& m)
{
	_int64 v = DoGetMinInt64(m);
	return v != MINUS_INF_64 ? v : PLUS_INF_64;
}

String2DateTimeCastColumn::String2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	full_const = vc->IsFullConst();
	if(full_const) {
		RCBString rbs;
		MIIterator mit(NULL);
		vc->GetValueString(rbs, mit);
		if (rbs.IsNull()) {
			val = NULL_VALUE_64;
			rcv = RCDateTime();
		} else {
			RCDateTime rcdt;
			BHReturnCode rc = RCDateTime::Parse(rbs, rcdt, TypeName());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)rbs;
				s += "\'";
				// mysql is pushing this warning anyway, removed duplication
				//IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
			}
			if(to.GetTypeName() == RC_TIMESTAMP) {
				// needs to convert value to UTC
				MYSQL_TIME myt;
				memset(&myt, 0, sizeof(MYSQL_TIME));
				myt.year = rcdt.Year();
				myt.month = rcdt.Month();
				myt.day = rcdt.Day();
				myt.hour = rcdt.Hour();
				myt.minute = rcdt.Minute();
				myt.second = rcdt.Second();
				myt.time_type = MYSQL_TIMESTAMP_DATETIME;
				if(!IsTimeStampZero(myt)) {
					my_bool myb;
					// convert local time to UTC seconds from beg. of EPOCHE
					my_time_t secs_utc = ConnectionInfoOnTLS.Get().Thd().variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
					// UTC seconds converted to UTC TIME
					GMTSec2GMTTime(&myt, secs_utc);
				}
				rcdt = RCDateTime((short)myt.year, (short)myt.month, (short)myt.day, (short)myt.hour, (short)myt.minute, (short)myt.second, RC_TIMESTAMP);
			}
			val = rcdt.GetInt64();
			rcv = rcdt;
		}
	}
#endif
}

_int64 String2DateTimeCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return NULL_VALUE_64;
#else
	if(full_const)
		return val;

	RCDateTime rcdt;
	RCBString rbs;
	vc->GetValueString(rbs, mit);
	BHReturnCode rc = RCDateTime::Parse(rbs, rcdt, TypeName());
	if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
		std::string s = "Incorrect datetime value: \'";
		s += (string)rbs;
		s += "\'";
		IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
		return NULL_VALUE_64;
	}
	return rcdt.GetInt64();
#endif
}

_int64 String2DateTimeCastColumn::DoGetValueInt64(const MIIterator& mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return NULL_VALUE_64;
#else
	if(full_const)
		return val;
	else {
		RCDateTime rcdt;
		RCBString rbs;
		vc->GetValueString(rbs, mit);
		if (rbs.IsNull())
			return NULL_VALUE_64;
		else {
			BHReturnCode rc = RCDateTime::Parse(rbs, rcdt, TypeName());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)rbs;
				s += "\'";
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				return NULL_VALUE_64;
			}/* else if(BHReturn::IsWarning(res)) {
				std::string s = "Incorrect Date/Time value: ";
				s += (string)rbs;
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP, s.c_str());
			}*/
			return rcdt.GetInt64();
		}
	}
#endif
}

void String2DateTimeCastColumn::DoGetValueString(RCBString& rbs, const MIIterator& mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return;
#else
	if(full_const)
		rbs = rcv.ToRCString();
	else {
		RCDateTime rcdt;
		vc->GetValueString(rbs, mit);
		if(rbs.IsNull())
			return;
		else {
			BHReturnCode rc = RCDateTime::Parse(rbs, rcdt, TypeName());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)rbs;
				s += "\'";
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				rbs = RCBString();
				return;
			}/* else if(BHReturn::IsWarning(res)) {
				std::string s = "Incorrect Date/Time value: ";
				s += (string)rbs;
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_WARN_INVALID_TIMESTAMP, s.c_str());
			}*/
			rbs = rcdt.ToRCString();
		}
	}
#endif
}

RCValueObject String2DateTimeCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCValueObject();
#else
	if(full_const)
		return rcv;
	else {
		RCDateTime rcdt;
		RCBString rbs;
		vc->GetValueString(rbs, mit);
		if(rbs.IsNull())
			return RCDateTime();
		else {
			BHReturnCode rc = RCDateTime::Parse(rbs, rcdt, TypeName());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)rbs;
				s += "\'";
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				//return RCDateTime();
			}
			return rcdt;
		}
	}
#endif

}

_int64 String2DateTimeCastColumn::DoGetMinInt64(const MIIterator& m)
{
	if(full_const)
		return val;
	else if(IsConst()) {
		// const with parameters
		return ((RCDateTime*)DoGetValue(m).Get())->GetInt64();
	} else
		return MINUS_INF_64;
}

_int64 String2DateTimeCastColumn::DoGetMaxInt64(const MIIterator& m)
{
	_int64 v = DoGetMinInt64(m);
	return v != MINUS_INF_64 ? v : PLUS_INF_64;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Num2DateTimeCastColumn::Num2DateTimeCastColumn(VirtualColumn* from, ColumnType const& to) : String2DateTimeCastColumn(from, to)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if(full_const) {
		MIIterator mit(NULL);
		val = vc->GetValueInt64(mit);
		//rcv = from->GetValue(mit);
		RCDateTime rcdt;
		if(val != NULL_VALUE_64) {
			BHReturnCode rc = RCDateTime::Parse(val, rcdt, TypeName(), ct.GetPrecision());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)rcv.ToRCString();
				s += "\'";
				// mysql is pushing this warning anyway, removed duplication
				//IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
			}
			if(TypeName() == RC_TIMESTAMP) {
				// needs to convert value to UTC
				MYSQL_TIME myt;
				memset(&myt, 0, sizeof(MYSQL_TIME));
				myt.year = rcdt.Year();
				myt.month = rcdt.Month();
				myt.day = rcdt.Day();
				myt.hour = rcdt.Hour();
				myt.minute = rcdt.Minute();
				myt.second = rcdt.Second();
				myt.time_type = MYSQL_TIMESTAMP_DATETIME;
				if(!IsTimeStampZero(myt)) {
					my_bool myb;
					// convert local time to UTC seconds from beg. of EPOCHE
					my_time_t secs_utc = ConnectionInfoOnTLS.Get().Thd().variables.time_zone->TIME_to_gmt_sec(&myt, &myb);
					// UTC seconds converted to UTC TIME
					GMTSec2GMTTime(&myt, secs_utc);
				}
				rcdt = RCDateTime((short)myt.year, (short)myt.month, (short)myt.day, (short)myt.hour, (short)myt.minute, (short)myt.second, RC_TIMESTAMP);		
			}
			val = rcdt.GetInt64();
		}
		rcv = rcdt;
	}
#endif

}

RCValueObject Num2DateTimeCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return RCValueObject();
#else
	if(full_const)
		return rcv;
	else {
		RCDateTime rcdt;
		RCValueObject r(vc->GetValue(mit));
		if(!r.IsNull()) {
			BHReturnCode rc = RCDateTime::Parse(((RCNum)r).GetIntPart(), rcdt, TypeName(), ct.GetPrecision());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)r.ToRCString();
				s += "\'";
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				return RCDateTime();
			}
			return rcdt;
		} else
			return r;
	}
#endif
}

_int64 Num2DateTimeCastColumn::DoGetValueInt64(const MIIterator& mit)
{
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
	return NULL_VALUE_64;
#else
	if(full_const)
		return val;
	else {
		RCDateTime rcdt;
		_int64 v = vc->GetValueInt64(mit);
		RCNum r(v, vc->Type().GetScale(), vc->Type().IsFloat(), vc->Type().GetTypeName());
		if(v != NULL_VALUE_64) {
			BHReturnCode rc = RCDateTime::Parse(r.GetIntPart(), rcdt, TypeName(), ct.GetPrecision());
			if(BHReturn::IsWarning(rc) || BHReturn::IsError(rc)) {
				std::string s = "Incorrect datetime value: \'";
				s += (string)r.ToRCString();
				s += "\'";
				IBPushWarning(&(ConnInfo().Thd()), MYSQL_ERROR::WARN_LEVEL_WARN, ER_TRUNCATED_WRONG_VALUE, s.c_str());
				return NULL_VALUE_64;
			}
			return rcdt.GetInt64();;
		} else
			return v;
	}
#endif
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2VarcharCastColumn::DateTime2VarcharCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if(full_const){
		int64 i = vc->GetValueInt64(mit);
		if(i == NULL_VALUE_64) {
			rcv = RCBString();
		} else {
			RCDateTime rcdt(i, TypeName());
			rcv = rcdt.ToRCString();
		}
	}
}

RCValueObject DateTime2VarcharCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if(full_const)
		return rcv;
	else {
		RCBString rcb;
		vc->GetValueString(rcb, mit);
		if(rcb.IsNull()) {
			return RCBString();
		} else {
			return rcb;
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
Num2VarcharCastColumn::Num2VarcharCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if(full_const){
		rcv = vc->GetValue(mit);
		if(rcv.IsNull()) {
			rcv = RCBString();
		} else {
			rcv = rcv.ToRCString();
		}
	}
}

RCValueObject Num2VarcharCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if(full_const)
		return rcv;
	else {
		RCValueObject r(vc->GetValue(mit));
		if(r.IsNull()) {
			return RCBString();
		} else {
			return r.ToRCString();
		}
	}
}

void Num2VarcharCastColumn::DoGetValueString(RCBString& s, const MIIterator& m)
{
	if(full_const)
		s = rcv.ToRCString();
	else {
		_int64 v = vc->GetValueInt64(m);
		if(v == NULL_VALUE_64)
			s = RCBString();
		else {
			RCNum rcd(v, vc->Type().GetScale(), vc->Type().IsFloat(), vc->TypeName());
			s = rcd.ToRCString();
		}

	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
DateTime2NumCastColumn::DateTime2NumCastColumn(VirtualColumn* from, ColumnType const& to) : TypeCastColumn(from, to)
{
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if(full_const){
		rcv = vc->GetValue(mit);
		if(rcv.IsNull()) {
			rcv = RCNum();
			val = NULL_VALUE_64;
		} else {
			((RCDateTime*)rcv.Get())->ToInt64(val);
			if(vc->TypeName() == RC_YEAR && vc->Type().GetPrecision() == 2)
				val %= 100;
			if(to.IsFloat()) {
				double x = (double)val;
				val = *(_int64*)&x;
			}
			rcv = RCNum(val, ct.GetScale(), ct.IsFloat(), TypeName());
		}
	}
}

_int64 DateTime2NumCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
	if(full_const)
		return val;
	_int64 v = vc->GetNotNullValueInt64(mit);
	if(vc->TypeName() == RC_YEAR && vc->Type().GetPrecision() == 2)
		v %= 100;
	RCDateTime rdt(v, vc->TypeName());
	_int64 r;
	rdt.ToInt64(r);
	if(Type().IsFloat()) {
		double x = (double)r;
		r = *(_int64*)&x;
	}
	return r;
}

_int64 DateTime2NumCastColumn::DoGetValueInt64(const MIIterator& mit)
{
	if(full_const)
		return val;
	else {
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64)
			return v;
		RCDateTime rdt(v, vc->TypeName());
		_int64 r;
		rdt.ToInt64(r);
		if(vc->TypeName() == RC_YEAR && vc->Type().GetPrecision() == 2)
			r = r %100;
		if(Type().IsFloat()) {
			double x = (double)r;
			r = *(_int64*)&x;
		}
		return r;
	}
}

double DateTime2NumCastColumn::DoGetValueDouble(const MIIterator& mit)
{
	if(full_const) {
		_int64 v;
		RCDateTime rdt(val, vc->TypeName());
		rdt.ToInt64(v);
		return (double)v;
	} else {
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64)
			return NULL_VALUE_D;
		RCDateTime rdt(v, vc->TypeName());	
		rdt.ToInt64(v);
		return (double)v;
	}
}

RCValueObject DateTime2NumCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if(full_const)
		return rcv;
	else {
		RCValueObject v(vc->GetValue(mit));
		if(v.IsNull()) {
			return RCNum();
		} else {
			_int64 r;
			((RCDateTime*)v.Get())->ToInt64(r);
			if(vc->TypeName() == RC_YEAR && vc->Type().GetPrecision() == 2)
				r %= 100;
			if(Type().IsFloat()) {
				double x = (double)r;
				r = *(_int64*)&x;
			}
			return RCNum(r, ct.GetScale(), ct.IsFloat(), TypeName());
		}
	}
}

TimeZoneConversionCastColumn::TimeZoneConversionCastColumn(VirtualColumn* from) : TypeCastColumn(from, ColumnType(RC_DATETIME))
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(from->TypeName() == RC_TIMESTAMP);
	MIIterator mit(NULL);
	full_const = vc->IsFullConst();
	if(full_const){
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64) {
			rcv = RCDateTime();
			val = NULL_VALUE_64;
		} else {
			rcv = RCDateTime(v, vc->TypeName());
			RCDateTime::AdjustTimezone(rcv);
			val = ((RCDateTime*)rcv.Get())->GetInt64();
		}
	}
}

_int64 TimeZoneConversionCastColumn::GetNotNullValueInt64(const MIIterator &mit)
{
	if(full_const)
		return val;
	_int64 v = vc->GetNotNullValueInt64(mit);
	RCDateTime rdt(v, vc->TypeName());
	RCDateTime::AdjustTimezone(rdt);
	return rdt.GetInt64();
}

_int64 TimeZoneConversionCastColumn::DoGetValueInt64(const MIIterator& mit)
{
	if(full_const)
		return val;
	else {
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64)
			return v;
		RCDateTime rdt(v, vc->TypeName());
		RCDateTime::AdjustTimezone(rdt);
		return rdt.GetInt64();
	}
}

RCValueObject TimeZoneConversionCastColumn::DoGetValue(const MIIterator& mit, bool b)
{
	if(full_const) {
		if(Type().IsString())
			return rcv.ToRCString();
		return rcv;
	} else {
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64)
			return RCDateTime();
		RCDateTime rdt(v, vc->TypeName());
		RCDateTime::AdjustTimezone(rdt);
		if(Type().IsString())
			return rdt.ToRCString();
		return rdt;
	}
}

double TimeZoneConversionCastColumn::DoGetValueDouble(const MIIterator& mit)
{
	if(full_const) {
		_int64 v;
		RCDateTime rdt(val, vc->TypeName());
		rdt.ToInt64(v);
		return (double)v;
	} else {
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64)
			return NULL_VALUE_D;
		RCDateTime rdt(v, vc->TypeName());
		RCDateTime::AdjustTimezone(rdt);
		rdt.ToInt64(v);
		return (double)v;
	}
}

void TimeZoneConversionCastColumn::DoGetValueString(RCBString& s, const MIIterator& mit)
{
	if(full_const) {
		s = rcv.ToRCString();
	} else {
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64) {
			s = RCBString();
			return;
		}
		RCDateTime rdt(v, vc->TypeName());
		RCDateTime::AdjustTimezone(rdt);
		s = rdt.ToRCString();
	}
}

RCValueObject StringCastColumn::DoGetValue(const MIIterator& mit, bool lookup_to_num)
{
	RCBString s; 
	vc->GetValueString(s, mit);  
	return s;	
}
