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

#include <string.h>
#include <math.h>

#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/lexical_cast.hpp>

#include "ValueParserForText.h"

const uint PARS_BUF_SIZE = 128;

ParsingFunction ValueParserForText::ParsingFuntion(const AttributeTypeInfo& at)
{
	switch (at.Type()) {
		case RC_NUM :
#ifdef SOLARIS
            return boost::bind ( boost::type<BHReturnCode>(),
                                &ValueParserForText::ParseDecimal, 
                                _1, _2, at.Precision(), at.Scale());
#else
			return boost::bind<BHReturnCode>(&ValueParserForText::ParseDecimal, _1, _2, at.Precision(), at.Scale());
#endif

		case RC_REAL :
		case RC_FLOAT :
		case RC_BYTEINT :
		case RC_SMALLINT :
		case RC_MEDIUMINT :
		case RC_INT :
#ifdef SOLARIS
			return boost::bind ( boost::type<BHReturnCode>(),
                                 &ValueParserForText::ParseNumeric, 
                                 _1, _2, at.Type());
#else
			return boost::bind<BHReturnCode>(&ValueParserForText::ParseNumeric, _1, _2, at.Type());
#endif
		case RC_BIGINT :
			return &ValueParserForText::ParseBigIntAdapter;
		case RC_DATE :
		case RC_TIME :
		case RC_YEAR :
		case RC_DATETIME :
		case RC_TIMESTAMP:
#ifdef SOLARIS
			return boost::bind ( boost::type<BHReturnCode>(),
                                 &ValueParserForText::ParseDateTimeAdapter, 
                                 _1, _2, at.Type());
#else
			return boost::bind<BHReturnCode>(&ValueParserForText::ParseDateTimeAdapter, _1, _2, at.Type());
#endif
		default :
			BHERROR("type not supported");
			break;
	}
	return NULL;
}

BHReturnCode ValueParserForText::ParseNum(const RCBString& rcs, RCNum& rcn, short scale)
{
	//TODO: refactor
	char* val, *val_ptr;
	val = val_ptr = rcs.val;
	int len = rcs.len;
	EatWhiteSigns(val, len);
	if(rcs == RCBString("NULL")) {
		rcn.null = true;
		return BHRC_SUCCESS;
	}
	rcn.null = false;
	rcn.dbl = false;
	rcn.m_scale = 0;

	int ptr_len = len;
	val_ptr = val;
	ushort no_digs = 0;
	ushort no_digs_after_dot = 0;
	ptr_len = len;
	val_ptr = val;
	bool has_dot = false;
	bool has_unexpected_sign = false;
	_int64 v = 0;
	short sign = 1;
	bool last_set = false;
	short last = 0;
	BHReturnCode bhret = BHRC_SUCCESS;
	if(ptr_len > 0 && *val_ptr == '-') {
		val_ptr++;
		ptr_len--;
		sign = -1;
	}
	while(ptr_len > 0) {
		if(isdigit((uchar) *val_ptr)) {
			no_digs++;
			if(has_dot)
				no_digs_after_dot++;

			if((no_digs <= 18 && scale < 0) || (no_digs <= 18 && no_digs_after_dot <= scale && scale >= 0)) {
				v *= 10;
				v += *val_ptr - '0';
				last = *val_ptr - '0';
			} else {
				no_digs--;
				if(has_dot) {
					no_digs_after_dot--;
					if(*val_ptr != '0')
						bhret = BHRC_VALUE_TRUNCATED;
					if(!last_set) {
						last_set = true;
						if(*val_ptr > '4') {
							last += 1;
							v = (v / 10) * 10 + last;
						}
					}
				} else {
					rcn.value = (Uint64PowOfTen(18) - 1) * sign;
					rcn.m_scale = 0;
					return BHRC_OUT_OF_RANGE_VALUE;
				}
			}
		} else if(*val_ptr == '.' && !has_dot) {
			has_dot = true;
			if(v == 0)
				no_digs = 0;
		} else if(isspace((uchar) *val_ptr)) {
			EatWhiteSigns(val_ptr, ptr_len);
			if(ptr_len > 0)
				has_unexpected_sign = true;
			break;
		} else if(*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E') {
			bhret = RCNum::ParseReal(rcs, rcn, RC_REAL);
			rcn = rcn.ToDecimal(scale);
			return bhret;
		} else {
			has_unexpected_sign = true;
			break;
		}
		ptr_len--;
		val_ptr++;
	}
	if(scale != -1) {
		while(scale > no_digs_after_dot) {
			v *= 10;
			no_digs_after_dot++;
			no_digs++;
		}
		while(scale < no_digs_after_dot) {
			v /= 10;
			no_digs_after_dot--;
			no_digs--;
		}
	} else {
		scale = no_digs_after_dot;
	}

	if(no_digs > 18)
		rcn.value = (Uint64PowOfTen(18) - 1) * sign;
	else
		rcn.value = v * sign;
	rcn.m_scale = scale;
	if(has_unexpected_sign || no_digs > 18)
		return BHRC_VALUE_TRUNCATED;
	return bhret;
}

BHReturnCode ValueParserForText::Parse(const RCBString& rcs, RCNum& rcn, AttributeType at)
{
	if(at == RC_BIGINT)
		return ParseBigInt(rcs, rcn);
	//TODO: refactor
	char* val, *val_ptr;
	val = val_ptr = rcs.val;
	int len = rcs.len;
	EatWhiteSigns(val, len);
	int ptr_len = len;
	val_ptr = val;
	short scale = -1;
	if(at == RC_UNKNOWN) {
		bool has_dot = false;
		bool has_exp = false;
		bool has_unexpected_sign = false;
		bool can_be_minus = true;
		ushort no_digs = 0;
		ushort no_digs_after_dot = 0;
		int exponent = 0;
		_int64 v = 0;
		while(ptr_len > 0) {
			if(can_be_minus && *val_ptr == '-') {
				can_be_minus = false;
			} else if(*val_ptr == '.' && !has_dot && !has_exp) {
				has_dot = true;
				can_be_minus = false;
				if(v == 0)
					no_digs = 0;
			} else if(!has_exp && (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E')) {
				val_ptr++;
				ptr_len--;
				can_be_minus = true;
				has_exp = true;
				int tmp_len = ptr_len;
				BHReturnCode bhrc = EatInt(val_ptr, ptr_len, exponent);
				if(bhrc)
					return BHRC_FAILD;
				if(tmp_len != ptr_len) {
					can_be_minus = false;
					if(exponent == 0 && tmp_len - ptr_len == 1) {
						has_unexpected_sign = true;
						break;
					} else
						no_digs += abs(exponent);
				}
			} else if(isspace((uchar)*val_ptr)) {
				EatWhiteSigns(val_ptr, ptr_len);
				if(ptr_len > 0)
					has_unexpected_sign = true;
				break;
			} else if(!isdigit((uchar)*val_ptr)) {
				has_unexpected_sign = true;
				break;
			} else {
				no_digs++;
				if(has_dot)
					no_digs_after_dot++;
				v *= 10;
				v += *val_ptr - '0';
			}
			val_ptr++;
			ptr_len--;
		}

		if(has_exp)
			at = RC_REAL;
		else if(has_dot) {
			if(abs((no_digs - no_digs_after_dot) + exponent) + abs(no_digs_after_dot - exponent) <= 18) {
				scale = abs(no_digs_after_dot - exponent);
				at = RC_NUM;
			} else
				at = RC_REAL;
		} else if(no_digs <= 18 && no_digs > 9) {
			at = RC_NUM;
			scale = 0;
		} else if(no_digs > 18)
			at = RC_REAL;
		else
			at = RC_INT;
	} else if(!ATI::IsNumericType(at))
		return BHRC_FAILD;

	rcn.null = false;
	rcn.attrt = at;
	if(ATI::IsRealType(at)) {
		rcn.dbl = true;
		rcn.m_scale = 0;
	} else
		rcn.dbl = false;

	if(ATI::IsRealType(at))
		return ValueParserForText::ParseReal(rcs, rcn, at);
	else if(at == RC_NUM)
		return ValueParserForText::ParseNum(rcs, rcn, scale);

	rcn.m_scale = 0;
	rcn.dbl = false;

	if(rcs == RCBString("NULL")) {
		rcn.null = true;
		return BHRC_SUCCESS;
	}

	BHReturnCode ret = BHRC_SUCCESS;

	ptr_len = len;
	val_ptr = val;
	EatWhiteSigns(val_ptr, ptr_len);
	if(ATI::IsIntegerType(at)) {

		ret = ParseBigInt(rcs, rcn);
		_int64 v = (_int64) rcn;

		if(at == RC_BYTEINT) {
			if(v > BH_TINYINT_MAX) {
				v = BH_TINYINT_MAX;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			} else if(v < BH_TINYINT_MIN) {
				v = BH_TINYINT_MIN;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			}
		} else if(at == RC_SMALLINT) {
			if(v > BH_SMALLINT_MAX) {
				v = BH_SMALLINT_MAX;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			} else if(v < BH_SMALLINT_MIN) {
				v = BH_SMALLINT_MIN;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			}
		} else if(at == RC_MEDIUMINT) {
			if(v > BH_MEDIUMINT_MAX) {
				v = BH_MEDIUMINT_MAX;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			} else if(v < BH_MEDIUMINT_MIN) {
				v = BH_MEDIUMINT_MIN;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			}
		} else if(at == RC_INT) {
			if(v > BH_INT_MAX) {
				v = BH_INT_MAX;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			} else if(v < BH_INT_MIN) {
				v = BH_INT_MIN;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			}
		}
		rcn.dbl = false;
		rcn.m_scale = 0;
		rcn.value = v;
	}
	return ret;
}

BHReturnCode ValueParserForText::ParseReal(const RCBString& rcbs, RCNum& rcn, AttributeType at)
{
	//TODO: refactor
	if(at == RC_UNKNOWN)
		at = RC_REAL;
	if(!ATI::IsRealType(at))
		return BHRC_FAILD;

	if(rcbs == RCBString("NULL") || rcbs.IsNull()) {
		rcn.null = true;
		return BHRC_SUCCESS;
	}
	rcn.dbl = true;
	rcn.m_scale = 0;

	char* val = rcbs.val;
	int len = rcbs.len;
	EatWhiteSigns(val, len);

	char* val_ptr = val;
	int ptr_len = len;

	bool has_dot = false;
	bool has_exp = false;
	bool can_be_minus = true;

	BHReturnCode ret = BHRC_SUCCESS;

	while(ptr_len > 0) {
		if(can_be_minus && *val_ptr == '-') {
			can_be_minus = false;
		} else if(*val_ptr == '.' && !has_dot && !has_exp) {
			has_dot = true;
			can_be_minus = false;
		} else if(!has_exp && (*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E')) {
			val_ptr++;
			ptr_len--;
			can_be_minus = true;
			has_exp = true;
			int exponent = 0;
			if(EatInt(val_ptr, ptr_len, exponent) != BHRC_SUCCESS) {
				ret = BHRC_VALUE_TRUNCATED;
				break;
			}
		} else if(isspace((uchar)*val_ptr)) {
			EatWhiteSigns(val_ptr, ptr_len);
			if(ptr_len > 0)
				ret = BHRC_VALUE_TRUNCATED;
			break;
		} else if(!isdigit((uchar)*val_ptr)) {
			ret = BHRC_VALUE_TRUNCATED;
			break;
		}
		val_ptr++;
		ptr_len--;
	}
	char stempval[PARS_BUF_SIZE];
	if(rcbs.len >= PARS_BUF_SIZE)
		return BHRC_VALUE_TRUNCATED;
#ifndef NDEBUG
	// resetting stempval to avoid valgrind
	// false warnings
	memset(stempval, 0, PARS_BUF_SIZE);
#endif
	memcpy(stempval, rcbs.val, rcbs.len);
	stempval[rcbs.len] = 0;
	double d = 0.0;
	try {
		d = boost::lexical_cast<double>(stempval);
	}
#ifdef __WIN__
	catch (const boost::bad_lexical_cast&) {
		if (strnicmp(stempval, "NAN", 3) == 0) {
			d = std::numeric_limits<double>::quiet_NaN();
		} else if(strnicmp(stempval, "INF", 3) == 0) {
			d = std::numeric_limits<double>::infinity();
		} else if(strnicmp(stempval, "-INF", 4) == 0) {
			d = -std::numeric_limits<double>::infinity();
		} else
			d = atof(stempval);
	}
#else
#endif
	catch(...) {
		d = atof(stempval);
	}

	if(fabs(d) == 0.0)
		d = 0.0; //convert -0.0 to 0.0
	else if((boost::math::isnan)(d)) {
		d = 0.0;
		ret = BHRC_OUT_OF_RANGE_VALUE;
	}

  	rcn.attrt = at;
  	rcn.null = false;
	if(at == RC_REAL) {
		if(d > DBL_MAX) {
			d = DBL_MAX;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		} else if(d < -DBL_MAX) {
			d = -DBL_MAX;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		} else if(d > 0 && d < DBL_MIN) {
			d = /*DBL_MIN*/0;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		} else if(d < 0 && d > -DBL_MIN) {
			d = /*DBL_MIN*/0 * -1;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		}
		*(double*) &rcn.value = d;
	} else if(at == RC_FLOAT) {
		if(d > FLT_MAX) {
			d = FLT_MAX;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		} else if(d < -FLT_MAX) {
			d = -FLT_MAX;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		} else if(d > 0 && d < FLT_MIN) {
			d = 0;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		} else if(d < 0 && d > FLT_MIN * -1) {
			d = 0 * -1;
			ret = BHRC_OUT_OF_RANGE_VALUE;
		}
		*(double*) &rcn.value = d;
	}
	return ret;
}

BHReturnCode ValueParserForText::ParseBigInt(const RCBString& rcs, RCNum& rcn)
{
	char *val_ptr;
	val_ptr = rcs.val;
	int len = rcs.len;
	int ptr_len = len;
	BHReturnCode ret = BHRC_SUCCESS;

	rcn.null = false;
	rcn.attrt = RC_BIGINT;
	rcn.m_scale = 0;
	rcn.dbl = false;

	if(rcs == RCBString("NULL")) {
		rcn.null = true;
		return BHRC_SUCCESS;
	}
	_int64 v = 0;
	EatWhiteSigns(val_ptr, ptr_len);
	bool is_negative = false;
	if(ptr_len > 0 && *val_ptr == '-') {
		val_ptr++;
		ptr_len--;
		is_negative = true;
	}
	_int64 temp_v = 0;
	BHReturnCode rc = EatInt64(val_ptr, ptr_len, temp_v);
	if(rc != BHRC_SUCCESS) {
		if(rc == BHRC_OUT_OF_RANGE_VALUE) {
			ret = rc;
			v = BH_BIGINT_MAX;
		} else {
			ret = BHRC_VALUE_TRUNCATED;
			v = 0;
		}
	} else if(ptr_len) {
		if(*val_ptr == 'd' || *val_ptr == 'D' || *val_ptr == 'e' || *val_ptr == 'E') {
			ret = RCNum::ParseReal(rcs, rcn, RC_REAL);
			if(rcn.GetDecFractLen() != 0 || ret != BHRC_SUCCESS)
				ret = BHRC_VALUE_TRUNCATED;
			if(rcn.GetIntPartAsDouble() > BH_BIGINT_MAX) {
				v = BH_BIGINT_MAX;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			} else if(rcn.GetIntPartAsDouble() < BH_BIGINT_MIN) {
				v = BH_BIGINT_MIN;
				ret = BHRC_OUT_OF_RANGE_VALUE;
			} else {
				v = (_int64)rcn;
			}
			is_negative = false;
		} else if(*val_ptr == '.') {
			val_ptr++;
			ptr_len--;
			v = temp_v;
			if(ptr_len)	{
				int tmp_ptr_len = ptr_len;
				int fract = 0;
				if(EatInt(val_ptr, ptr_len, fract) != BHRC_SUCCESS)
					ret = BHRC_VALUE_TRUNCATED;
				else if(*val_ptr == 'e' || *val_ptr == 'E') {
					ret = RCNum::ParseReal(rcs, rcn, RC_REAL);
					if(rcn.GetDecFractLen() != 0 || ret != BHRC_SUCCESS)
						ret = BHRC_VALUE_TRUNCATED;
					if(rcn.GetIntPartAsDouble() > BH_BIGINT_MAX) {
						v = BH_BIGINT_MAX;
						ret = BHRC_OUT_OF_RANGE_VALUE;
					} else if(rcn.GetIntPartAsDouble() < BH_BIGINT_MIN) {
						v = BH_BIGINT_MIN;
						ret = BHRC_OUT_OF_RANGE_VALUE;
					} else {
						v = (_int64)rcn;
					}
					is_negative = false;
				} else {
					if((tmp_ptr_len - ptr_len >= 1) && Uint64PowOfTenMultiply5((tmp_ptr_len - ptr_len) - 1) <= fract)
						v++;
					ret = BHRC_VALUE_TRUNCATED;
				}
			}
		} else {
			EatWhiteSigns(val_ptr, ptr_len);
			v = temp_v;
			if(ptr_len) {
				ret = BHRC_VALUE_TRUNCATED;
			}
		}
	} else
		v = temp_v;

	if(is_negative)
		v *= -1;

	if(v > BH_BIGINT_MAX) {
		v = BH_BIGINT_MAX;
		ret = BHRC_OUT_OF_RANGE_VALUE;
	}
	else if(v < BH_BIGINT_MIN)	{
		v = BH_BIGINT_MIN;
		ret = BHRC_OUT_OF_RANGE_VALUE;
	}
	rcn.dbl = false;
	rcn.m_scale = 0;
	rcn.value = v;
	return ret;
}

BHReturnCode ValueParserForText::ParseNumeric(RCBString const& rcs, _int64& out, AttributeType at)
{
	RCNum number;
	BHReturnCode return_code = Parse(rcs, number, at);
	out = number.Value();
	return return_code;
}

BHReturnCode ValueParserForText::ParseBigIntAdapter(const RCBString& rcs, _int64& out)
{
	RCNum number;
	BHReturnCode return_code = ParseBigInt(rcs, number);
	out = number.Value();
	return return_code;
}

BHReturnCode ValueParserForText::ParseDecimal(RCBString const& rcs, _int64& out, short precision,  short scale)
{
	RCNum number;
	BHReturnCode return_code = ParseNum(rcs, number, scale);
	out = number.Value();
	if(out > 0 && out >= (_int64)Uint64PowOfTen(precision))	{
		out = Uint64PowOfTen(precision) - 1;
		return_code = BHRC_OUT_OF_RANGE_VALUE;
	} else if(out < 0 && out <= (_int64)Uint64PowOfTen(precision) * -1) {
		out = Uint64PowOfTen(precision) * -1 + 1;
		return_code = BHRC_OUT_OF_RANGE_VALUE;
	}
	return return_code;
}

BHReturnCode ValueParserForText::ParseDateTimeOrTimestamp(const RCBString& rcs, RCDateTime& rcv, AttributeType at)
{
	if(rcs.IsNull() || rcs == RCBString("NULL")) {
		rcv.at = at;
		rcv.null = true;
		return BHRC_SUCCESS;
	}
	char* buf = rcs.val;
	int buflen = rcs.len;
	EatWhiteSigns(buf, buflen);

	if(buflen == 0) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	_uint64 year = 0;
	uint month = 0, day = 0, hour = 0, minute = 0, second = 0;
	BHReturnCode bhrc = EatUInt64(buf, buflen, year);
	EatWhiteSigns(buf, buflen);
	if(bhrc == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	} else if(bhrc != BHRC_OUT_OF_RANGE_VALUE && buflen != 0)
		year = RCDateTime::ToCorrectYear((uint) year, at);

	if(buflen == 0)
		return RCDateTime::Parse(year, rcv, at);

	if(!EatDTSeparators(buf, buflen)) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}
	if(EatUInt(buf, buflen, month) == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}
	if(!EatDTSeparators(buf, buflen) == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	if(EatUInt(buf, buflen, day) == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	if(!RCDateTime::CanBeDate(year, month, day)) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	if(!EatWhiteSigns(buf, buflen) && !EatDTSeparators(buf, buflen)) {
		if((at == RC_DATETIME && RCDateTime::IsCorrectBHDatetime((short) year, month, day, RCDateTime::GetSpecialValue(
				at).Hour(), RCDateTime::GetSpecialValue(at).Minute(), RCDateTime::GetSpecialValue(at).Second())) || (at
				== RC_TIMESTAMP && RCDateTime::IsCorrectBHTimestamp((short) year, month, day,
				RCDateTime::GetSpecialValue(at).Hour(), RCDateTime::GetSpecialValue(at).Minute(),
				RCDateTime::GetSpecialValue(at).Second())))
			rcv = RCDateTime((short) year, month, day, RCDateTime::GetSpecialValue(at).Hour(),
					RCDateTime::GetSpecialValue(at).Minute(), RCDateTime::GetSpecialValue(at).Second(), at);
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	bool eat1 = true, eat2 = true;
	while(eat1 || eat2) {
		eat1 = EatDTSeparators(buf, buflen);
		eat2 = EatWhiteSigns(buf, buflen);
	}
	bhrc = BHRC_SUCCESS;
	bhrc = EatUInt(buf, buflen, hour);
	if(!BHReturn::IsError(bhrc)) {
		if(!RCDateTime::CanBeHour(hour)) {
			rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
			return BHRC_OUT_OF_RANGE_VALUE;
		} else if(!EatDTSeparators(buf, buflen)) {
			bhrc = BHRC_FAILD;
		}

	} else
		hour = RCDateTime::GetSpecialValue(at).Hour();
	if(!BHReturn::IsError(bhrc))
		bhrc = EatUInt(buf, buflen, minute);
	if(!BHReturn::IsError(bhrc)) {
		if(!RCDateTime::CanBeMinute(minute)) {
			rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
			return BHRC_OUT_OF_RANGE_VALUE;
		} else if(!EatDTSeparators(buf, buflen)) {
			rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
			return BHRC_OUT_OF_RANGE_VALUE;
		}
	} else
		minute = RCDateTime::GetSpecialValue(at).Minute();
	if(!BHReturn::IsError(bhrc))
		bhrc = EatUInt(buf, buflen, second);
	if(!BHReturn::IsError(bhrc)) {
		if(!RCDateTime::CanBeSecond(second)) {
			rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
			return BHRC_OUT_OF_RANGE_VALUE;
		}
	} else
		second = RCDateTime::GetSpecialValue(at).Second();
	if(bhrc == BHRC_FAILD)
		bhrc = BHRC_OUT_OF_RANGE_VALUE;

	EatWhiteSigns(buf, buflen);

	short add_hours = 0;
	if(buflen >= 2) {
		if(strncasecmp(buf, "PM", 2) == 0) {
			add_hours = 12;
			buf += 2;
			buflen -= 2;
		} else if(strncasecmp(buf, "AM", 2) == 0) {
			buf += 2;
			buflen -= 2;
		}
	}
	hour += add_hours;
	EatWhiteSigns(buf, buflen);

	try {
		if(at == RC_DATETIME) {
			if(RCDateTime::IsCorrectBHDatetime((short) year, (short) month, (short) day, (short) hour, (short) minute,
					(short) second)) {
				rcv = RCDateTime((short) year, (short) month, (short) day, (short) hour, (short) minute,
						(short) second, at);
				return bhrc;
			} else {
				rcv = RCDateTime((short) year, (short) month, (short) day,
						(short) RCDateTime::GetSpecialValue(at).Hour(),
						(short) RCDateTime::GetSpecialValue(at).Minute(),
						(short) RCDateTime::GetSpecialValue(at).Second(), at);
				bhrc = BHRC_OUT_OF_RANGE_VALUE;
			}
		} else if(at == RC_TIMESTAMP) {
			if(RCDateTime::IsCorrectBHTimestamp((short) year, (short) month, (short) day, (short) hour, (short) minute,
					(short) second)) {
				rcv = RCDateTime((short) year, (short) month, (short) day, (short) hour, (short) minute,
						(short) second, at);
				return bhrc;
			} else {

				rcv = RC_TIMESTAMP_SPEC;
				bhrc = BHRC_OUT_OF_RANGE_VALUE;
			}
		}

		if(buflen != 0)
			return BHRC_OUT_OF_RANGE_VALUE;
		return bhrc;
	} catch (DataTypeConversionRCException&) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(at));
		return BHRC_OUT_OF_RANGE_VALUE;
	}
	return BHRC_FAILD;
}

BHReturnCode ValueParserForText::ParseTime(const RCBString& rcs, RCDateTime& rcv)
{
	if(rcs.IsNull() || rcs == RCBString("NULL")) {
		rcv.at = RC_TIME;
		rcv.null = true;
		return BHRC_SUCCESS;
	}
	char* buf = rcs.val;
	int buflen = rcs.len;
	EatWhiteSigns(buf, buflen);
	if(buflen == 0) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	_uint64 tmp = 0;
	int hour = 0;
	int minute = 0;
	_uint64 second = 0;
	int sign = 1;
	bool colon = false;
	if(buflen > 0 && *buf == '-') {
		sign = -1;
		buf++;
		buflen--;
	}
	EatWhiteSigns(buf, buflen);
	if(buflen > 0 && *buf == '-') {
		sign = -1;
		buf++;
		buflen--;
	}
	EatWhiteSigns(buf, buflen);
	BHReturnCode bhrc = EatUInt64(buf, buflen, tmp);
	if(bhrc == BHRC_FAILD && buflen > 0 && *buf == ':') {//e.g. :12:34
		colon = true;
	} else if(bhrc != BHRC_SUCCESS) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
		return BHRC_VALUE_TRUNCATED;
	} else if(bhrc == BHRC_SUCCESS) {
		second = tmp;
		EatWhiteSigns(buf, buflen);
		if(buflen == 0) {//e.g. 235959
			return RCDateTime::Parse(second * sign, rcv, RC_TIME);
		}
	}

	if(buflen > 0 && *buf == ':') {
		buf++;
		buflen--;
		EatWhiteSigns(buf, buflen);
		uint tmp2 = 0;
		bhrc = EatUInt(buf, buflen, tmp2);
		if(!BHReturn::IsError(bhrc)) {//e.g. 01:02
			EatWhiteSigns(buf, buflen);
			if(colon) {// starts with ':'
				if(!RCDateTime::CanBeMinute(tmp2)) {
					rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
					return BHRC_VALUE_TRUNCATED;
				}
				minute = tmp2;
				hour = 0;
				EatWhiteSigns(buf, buflen);
				if(buflen > 0 && *buf == ':') {
					buf++;
					buflen--;
					EatWhiteSigns(buf, buflen);
					bhrc = EatUInt(buf, buflen, tmp2);
					if(bhrc == BHRC_SUCCESS && RCDateTime::CanBeSecond(tmp2))
						second = tmp2;
					else {
						second = RCDateTime::GetSpecialValue(RC_TIME).Second();
						bhrc = BHRC_VALUE_TRUNCATED;
					}
				} else {//e.g. :01:

				}
			} else {//e.g. 01:02:03
				hour = (int) second;
				if(!RCDateTime::CanBeMinute(tmp2)) {
					rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
					return BHRC_VALUE_TRUNCATED;
				}
				minute = tmp2;
				second = 0;
				if(buflen > 0 && *buf == ':') {
					buf++;
					buflen--;
					bhrc = EatUInt(buf, buflen, tmp2);
					if(bhrc == BHRC_SUCCESS && RCDateTime::CanBeSecond(tmp2))
						second = tmp2;
					else {
						second = RCDateTime::GetSpecialValue(RC_TIME).Second();
						bhrc = BHRC_VALUE_TRUNCATED;
					}
				}
			}
			EatWhiteSigns(buf, buflen);
			short add_hours = 0;
			if(buflen >= 2) {
				if(strncasecmp(buf, "PM", 2) == 0) {
					add_hours = 12;
					buf += 2;
					buflen -= 2;
				} else if(strncasecmp(buf, "AM", 2) == 0) {
					buf += 2;
					buflen -= 2;
				}
			}

			hour += add_hours;
			EatWhiteSigns(buf, buflen);

			if(bhrc != BHRC_SUCCESS || buflen != 0)
				bhrc = BHRC_VALUE_TRUNCATED;

			if(RCDateTime::IsCorrectBHTime(short(hour * sign), short(minute * sign), short(second * sign))) {
				rcv = RCDateTime(short(hour * sign), short(minute * sign), short(second * sign), RC_TIME);
				return bhrc;
			} else {
				if(hour * sign < -RC_TIME_MIN.Hour())
					rcv = RCDateTime(RC_TIME_MIN);
				else if(hour * sign > RC_TIME_MAX.Hour())
					rcv = RCDateTime(RC_TIME_MAX);
				else
					assert(0); //hmmm... ????

				return BHRC_OUT_OF_RANGE_VALUE;
			}
		} else {//e.g. 01:a... or 01::... only second is set
			if(buflen > 0 && *buf == ':') {//01::
				EatWhiteSigns(buf, buflen);
				bhrc = EatUInt(buf, buflen, tmp2);
				if(bhrc == BHRC_SUCCESS) {
					hour = (int) second;
					if(!RCDateTime::CanBeSecond(tmp2)) {
						rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
						return BHRC_VALUE_TRUNCATED;
					}
					second = tmp2;
				}
			} else
				return RCDateTime::Parse(second * sign, rcv, RC_TIME);
			if(RCDateTime::IsCorrectBHTime(short(hour * sign), short(minute * sign), short(second * sign))) {
				rcv = RCDateTime(short(hour * sign), short(minute * sign), short(second * sign), RC_TIME);
				return BHRC_VALUE_TRUNCATED;
			} else {
				rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
				return BHRC_VALUE_TRUNCATED;
			}
		}
	} else {
		if(RCDateTime::IsCorrectBHTime(short(hour * sign), short(minute * sign), short(second * sign))) {
			rcv = RCDateTime(short(hour * sign), short(minute * sign), short(second * sign), RC_TIME);
			return BHRC_VALUE_TRUNCATED;
		} else {
			rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_TIME));
			return BHRC_VALUE_TRUNCATED;
		}
	}
	return BHRC_SUCCESS;
}

BHReturnCode ValueParserForText::ParseDate(const RCBString& rcs, RCDateTime& rcv)
{
	if(rcs.IsNull() || rcs == RCBString("NULL")) {
		rcv.at = RC_DATE;
		rcv.null = true;
		return BHRC_SUCCESS;
	}
	char* buf = rcs.val;
	int buflen = rcs.len;
	EatWhiteSigns(buf, buflen);
	if(buflen == 0) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	if(buflen > 0 && *buf == '-') {
		rcv = RCDateTime::GetSpecialValue(RC_DATE);
		return BHRC_VALUE_TRUNCATED;
	}

	uint year = 0, month = 0, day = 0;
	BHReturnCode bhrc = EatUInt(buf, buflen, year);
	if(bhrc == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_VALUE_TRUNCATED;
	} else if(bhrc != BHRC_OUT_OF_RANGE_VALUE && buflen > 0) {
		year = RCDateTime::ToCorrectYear(year, RC_DATE);
	}

	if(buflen == 0)
		return RCDateTime::Parse((_int64) year, rcv, RC_DATE);

	EatWhiteSigns(buf, buflen);
	if(!EatDTSeparators(buf, buflen)) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_VALUE_TRUNCATED;
	}
	EatWhiteSigns(buf, buflen);
	if(EatUInt(buf, buflen, month) == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_VALUE_TRUNCATED;
	}
	EatWhiteSigns(buf, buflen);
	if(!EatDTSeparators(buf, buflen) == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_VALUE_TRUNCATED;
	}
	EatWhiteSigns(buf, buflen);
	if(EatUInt(buf, buflen, day) == BHRC_FAILD) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_VALUE_TRUNCATED;
	}
	EatWhiteSigns(buf, buflen);
	if(!RCDateTime::CanBeDate(year, month, day)) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_DATE));
		return BHRC_VALUE_TRUNCATED;
	} else
		rcv = RCDateTime(year, month, day, RC_DATE);
	if(buflen != 0)
		return BHRC_VALUE_TRUNCATED;
	return BHRC_SUCCESS;
}

BHReturnCode ValueParserForText::ParseYear(const RCBString& rcs, RCDateTime& rcv)
{
	if(rcs.IsNull() || rcs == RCBString("NULL")) {
		rcv.at = RC_YEAR;
		rcv.null = true;
		return BHRC_SUCCESS;
	}
	char* buf = rcs.val;
	int buflen = rcs.len;
	EatWhiteSigns(buf, buflen);
	if(buflen == 0) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_YEAR));
		return BHRC_OUT_OF_RANGE_VALUE;
	}

	if(buflen > 0 && *buf == '-') {
		rcv = RCDateTime::GetSpecialValue(RC_YEAR);
		return BHRC_VALUE_TRUNCATED;
	}
	uint year = 0;
	int tmp_buf_len = buflen;
	BHReturnCode bhrc = EatUInt(buf, buflen, year);
	if(bhrc != BHRC_SUCCESS) {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_YEAR));
		return BHRC_VALUE_TRUNCATED;
	} else if(year == 0 && (tmp_buf_len - buflen) < 4) {
		year = 2000;
	} else {
		year = RCDateTime::ToCorrectYear(year, RC_YEAR);
	}

	if(RCDateTime::IsCorrectBHYear(year)) {
		EatWhiteSigns(buf, buflen);
		rcv = RCDateTime(year);
		if(buflen)
			return BHRC_OUT_OF_RANGE_VALUE;
		return BHRC_SUCCESS;
	} else {
		rcv = RCDateTime(RCDateTime::GetSpecialValue(RC_YEAR));
		return BHRC_OUT_OF_RANGE_VALUE;
	}
	return BHRC_SUCCESS;
}

BHReturnCode ValueParserForText::ParseDateTime(const RCBString& rcs, RCDateTime& rcv, AttributeType at)
{
	assert(ATI::IsDateTimeType(at));
	switch(at) {
		case RC_TIMESTAMP :
		case RC_DATETIME :
			return ValueParserForText::ParseDateTimeOrTimestamp(rcs, rcv, at);
		case RC_TIME :
			return ValueParserForText::ParseTime(rcs, rcv);
		case RC_DATE :
			return ValueParserForText::ParseDate(rcs, rcv);
		default : //case RC_YEAR :
			return ValueParserForText::ParseYear(rcs, rcv);
	}
}

BHReturnCode ValueParserForText::ParseDateTimeAdapter(RCBString const& rcs, _int64& out, AttributeType at)
{
	RCDateTime dt;
	BHReturnCode return_code = ValueParserForText::ParseDateTime(rcs, dt, at);
	out = dt.GetInt64();
	return return_code;
}
