/* Copyright (C)  2005-2009 Infobright Inc.

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

#include "ColumnBinEncoder.h"
#include "edition/vc/VirtualColumn.h"
#include "bintools.h"
#include "MIIterator.h"

using namespace std;

ColumnBinEncoder::ColumnBinEncoder(int flags)
{
	ignore_nulls		= ((flags & ENCODER_IGNORE_NULLS)	!= 0);
	monotonic_encoding	= ((flags & ENCODER_MONOTONIC)		!= 0);
	noncomparable		= ((flags & ENCODER_NONCOMPARABLE)	!= 0);
	descending			= ((flags & ENCODER_DESCENDING)		!= 0);
	decodable			= ((flags & ENCODER_DECODABLE)		!= 0);

	implicit = false;
	disabled = false;

	vc = NULL;
	val_offset = 0;
	val_sec_offset = 0;
	val_size = 0;
	val_sec_size = 0;

	my_encoder.reset();
}

ColumnBinEncoder::ColumnBinEncoder(ColumnBinEncoder const &sec)
{
	implicit = sec.implicit;
	ignore_nulls = sec.ignore_nulls;
	monotonic_encoding = sec.monotonic_encoding;
	noncomparable = sec.noncomparable;
	descending = sec.descending;
	decodable = sec.decodable;
	disabled = sec.disabled;
	vc = sec.vc;
	val_offset = sec.val_offset;
	val_sec_offset = sec.val_sec_offset;
	val_size = sec.val_size;
	val_sec_size = sec.val_sec_size;
	if(sec.my_encoder.get())
		my_encoder = ColumnValueEncoder_ptr_t(sec.my_encoder->Copy());
	else
		my_encoder.reset();
}

ColumnBinEncoder& ColumnBinEncoder::operator = (ColumnBinEncoder const &sec)
{
	if(&sec != this) {
		ignore_nulls = sec.ignore_nulls;
		monotonic_encoding = sec.monotonic_encoding;
		noncomparable = sec.noncomparable;
		descending = sec.descending;
		decodable = sec.decodable;
		disabled = sec.disabled;
		implicit = sec.implicit;
		vc = sec.vc;
		val_offset = sec.val_offset;
		val_sec_offset = sec.val_sec_offset;
		val_size = sec.val_size;
		val_sec_size = sec.val_sec_size;
		my_encoder = sec.my_encoder;
	}
	return *this;
}

ColumnBinEncoder::~ColumnBinEncoder()
{
	if(vc && !implicit) 
		vc->UnlockSourcePacks();
}

bool ColumnBinEncoder::PrepareEncoder(VirtualColumn* _vc, VirtualColumn* _vc2)
{
	if(_vc == NULL) 
		return false;
	bool nulls_possible = false;
	if(!ignore_nulls)
		nulls_possible = _vc->NullsPossible() || (_vc2 != NULL && _vc2->NullsPossible());
	vc = _vc;
	ColumnType vct = vc->Type();
	ColumnType vct2 = _vc2 ? _vc2->Type() : ColumnType();
	bool lookup_encoder = false;
	bool text_stat_encoder = false;
	if(vct.IsFixed() && (!_vc2 || (vct2.IsFixed() && vct.GetScale() == vct2.GetScale()))) {		// int/dec of the same scale
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderInt(vc, decodable, nulls_possible, descending));
	} else if(vct.IsFloat() || (_vc2 && vct2.IsFloat())) {
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderDouble(vc, decodable, nulls_possible, descending));
	} else if(vct.IsFixed() && !vct2.IsString()) {	// Decimals for different scale (the same scale is done by EncoderInt)
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderDecimal(vc, decodable, nulls_possible, descending));
	} else if(vct.GetTypeName() == RC_DATE) {
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderDate(vc, decodable, nulls_possible, descending));
	} else if(vct.GetTypeName() == RC_YEAR) {
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderYear(vc, decodable, nulls_possible, descending));
	} else if(!monotonic_encoding && vct.IsLookup() && _vc2 == NULL && !RequiresUTFConversions(vc->GetCollation())) {		// Lookup encoding: only non-UTF
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderLookup(vc, decodable, nulls_possible, descending));
		lookup_encoder = true;
	} else if(!monotonic_encoding && vct.IsLookup() && _vc2 != NULL && vct2.IsLookup()) {		// Lookup in joining - may be UTF
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderLookup(vc, decodable, nulls_possible, descending));
		lookup_encoder = true;
	} else if(vct.IsString() || vct2.IsString()) {
		DTCollation col_v1 = vc->GetCollation();
		DTCollation coll = col_v1;
		if(_vc2) {
			DTCollation col_v2 = _vc2->GetCollation();
			coll = ResolveCollation(col_v1, col_v2);
		}
		if(!noncomparable && RequiresUTFConversions(coll))		// noncomparable => non-sorted cols in sorter, don't UTF-encode.
			my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderText_UTF(vc, decodable, nulls_possible, descending));
		else {
			my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderTextStat(vc, decodable, nulls_possible, descending));
			if(!my_encoder->Valid()) {
				if(!monotonic_encoding && !decodable && vc->MaxStringSize() > HASH_FUNCTION_BYTE_SIZE)
					my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderTextMD5(vc, decodable, nulls_possible, descending));
				else
					my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
			} else
				text_stat_encoder = true;
		}
	} else if(vct.IsDateTime()) {		// Date/time types except special cases (above)
			my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderInt(vc, decodable, nulls_possible, descending));
	} else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"wrong combination of encoded columns");	// Other types not implemented yet
		my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
	}
	if(_vc2 != NULL) {		// multiple column encoding?
		bool encoding_possible = my_encoder->SecondColumn(_vc2);
		if(!encoding_possible) {
			bool second_try = false;
			if(lookup_encoder) {			// try to use text (UTF) encoder instead of lookup
				my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderText_UTF(vc, decodable, nulls_possible, descending));
				second_try = my_encoder->SecondColumn(_vc2);
			}
			if(text_stat_encoder) {
				if(!monotonic_encoding && !decodable && vc->MaxStringSize() > HASH_FUNCTION_BYTE_SIZE)
					my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderTextMD5(vc, decodable, nulls_possible, descending));
				else
					my_encoder = ColumnValueEncoder_ptr_t(new ColumnBinEncoder::EncoderText(vc, decodable, nulls_possible, descending));
				second_try = my_encoder->SecondColumn(_vc2);
			}
			if(!second_try)
				return false;
		}
	}
	val_size = my_encoder->ValueSize();
	val_sec_size = my_encoder->ValueSizeSec();
	return true;
}

void ColumnBinEncoder::Encode(uchar* buf, MIIterator& mit, VirtualColumn* alternative_vc, bool update_stats)
{
	if(implicit)
		return;
	my_encoder->Encode(buf + val_offset, buf + val_sec_offset, (alternative_vc ? alternative_vc : vc), mit, update_stats);
}

bool ColumnBinEncoder::PutValue64(uchar* buf, _int64 v, bool sec_column, bool update_stats)
{
	if(implicit)
		return false;
	return my_encoder->Encode(buf + val_offset, buf + val_sec_offset, v, sec_column, update_stats);
}

bool ColumnBinEncoder::PutValueString(uchar* buf, RCBString& v, bool sec_column, bool update_stats)
{
	if(implicit)
		return false;
	return my_encoder->Encode(buf + val_offset, buf + val_sec_offset, v, sec_column, update_stats);
}

_int64 ColumnBinEncoder::ValEncode(MIIterator& mit, bool update_stats)
{
	if(implicit)
		return NULL_VALUE_64;
	return my_encoder->ValEncode(vc, mit, update_stats);
}

_int64 ColumnBinEncoder::ValPutValue64(_int64 v, bool update_stats)
{
	if(implicit)
		return NULL_VALUE_64;
	return my_encoder->ValEncode(v, update_stats);
}

_int64 ColumnBinEncoder::ValPutValueString(RCBString& v, bool update_stats)
{
	if(implicit)
		return NULL_VALUE_64;
	return my_encoder->ValEncode(v, update_stats);
}

bool ColumnBinEncoder::IsString()		
{ 
	return my_encoder->IsString(); 
}

_int64 ColumnBinEncoder::GetValue64(uchar* buf, const MIDummyIterator &mit, bool &is_null)
{
	is_null = false;
	if(implicit) {
		vc->LockSourcePacks(mit);
		_int64 v = vc->GetValueInt64(mit);
		if(v == NULL_VALUE_64)
			is_null = true;
		return v;
	}
	if(my_encoder->IsNull(buf + val_offset, buf + val_sec_offset)) {
		is_null = true;
		return NULL_VALUE_64;
	}
	return my_encoder->GetValue64(buf + val_offset, buf + val_sec_offset);
}

RCBString ColumnBinEncoder::GetValueT(uchar* buf, const MIDummyIterator &mit)
{
	if(implicit) {
		vc->LockSourcePacks(mit);
		RCBString s;
		if(vc->IsNull(mit))
			return s;
		vc->GetNotNullValueString(s, mit);
		return s;
	}
	if(my_encoder->IsNull(buf + val_offset, buf + val_sec_offset))
		return RCBString();
	return my_encoder->GetValueT(buf + val_offset, buf + val_sec_offset);
}

void ColumnBinEncoder::UpdateStatistics(unsigned char *buf)						// get value from the buffer and update internal statistics
{
	assert(!implicit);
	my_encoder->UpdateStatistics(buf + val_offset);
}

bool ColumnBinEncoder::ImpossibleValues(_int64 pack_min, _int64 pack_max)		// return true if the current contents of the encoder is out of scope
{
	if(implicit)
		return false;
	return my_encoder->ImpossibleValues(pack_min, pack_max);
}

bool ColumnBinEncoder::ImpossibleValues(RCBString& pack_min, RCBString& pack_max)
{
	return my_encoder->ImpossibleValues(pack_min, pack_max);
}

void ColumnBinEncoder::ClearStatistics()
{
	my_encoder->ClearStatistics();
}

_int64 ColumnBinEncoder::MaxCode()							// Maximal integer code, if it makes any sense (or NULL_VALUE_64)
{ 
	return my_encoder->MaxCode();	
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderInt::EncoderInt(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending, bool calc_int_statistics)
					: ColumnValueEncoder(vc, decodable, nulls_possible, _descending)
{
	min_val = 0;
	if(calc_int_statistics) {		// false if the column is actually not an integer (e.g. dec, date, textstat) and stats are calculated elsewhere
		min_val = vc->RoughMin();
		max_code = _uint64(vc->RoughMax() - min_val);
		if(nulls_possible) {
			if(max_code < UINT64_MAX) {
				null_status = 1;	// 0 is null
				max_code++;
			} else
				null_status = 2;	// separate byte
		} else
			null_status = 0;
		size = CalculateByteSize(max_code) + (null_status == 2 ? 1 : 0);
		size_sec = 0;
	}
	min_found = PLUS_INF_64;
	max_found = MINUS_INF_64;
}

bool ColumnBinEncoder::EncoderInt::SecondColumn(VirtualColumn* vc)
{
	if(!vc->Type().IsFixed() && !(this->vc_type.IsDateTime() && vc->Type().IsDateTime())) {
		rccontrol.lock(vc->ConnInfo().GetThreadID()) << "Nontrivial comparison: date/time with non-date/time" << unlock;
		return false;
	}
	bool is_timestamp1 = (this->vc_type.GetTypeName() == RC_TIMESTAMP);
	bool is_timestamp2 = (vc->Type().GetTypeName() == RC_TIMESTAMP);
	if(is_timestamp1 || is_timestamp2 && !(is_timestamp1 && is_timestamp2))
		return false;		// cannot compare timestamp with anything different than timestamp
	// Easy case: integers/decimals with the same precision
	_int64 new_min_val = vc->RoughMin();
	_int64 max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
	_int64 new_max_val = vc->RoughMax();
	if(min_val > new_min_val)
		min_val = new_min_val;
	if(max_val < new_max_val)
		max_val = new_max_val;
	max_code = _uint64(max_val - min_val);
	if(null_status == 1 && max_code == (UINT64_MAX))
			null_status = 2;	// separate byte
	size = CalculateByteSize(max_code) + (null_status == 2 ? 1 : 0);
	return true;
}

void ColumnBinEncoder::EncoderInt::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		SetNull(buf, buf_sec);
	else
		Encode(buf, buf_sec, vc->GetNotNullValueInt64(mit), false, update_stats);
}

bool ColumnBinEncoder::EncoderInt::Encode(uchar* buf, uchar* buf_sec, _int64 v, bool sec_column, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64) {
		SetNull(buf, buf_sec);
		return true;
	}
	_uint64 coded_val;
	int loc_size = size;
	if(update_stats) {
		if(v > max_found)
			max_found = v;
		if(v < min_found)
			min_found = v;
	}
	coded_val = _uint64(v - min_val) + (null_status == 1 ? 1 : 0);
	if(descending)
		coded_val = max_code - coded_val;
	if(null_status == 2) {
		*buf = (descending ? '\0' : '\1');			// not null
		buf++;
		loc_size--;
	}
	uchar* val_ptr = (uchar *)(&coded_val) + loc_size; 	// the end of meaningful bytes
	for(int i = 0; i < loc_size; i++)
		*(buf++) = *(--val_ptr);			// change endianess - to make numbers comparable by memcmp()
	return true;
}

_int64 ColumnBinEncoder::EncoderInt::ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		return ValEncode(NULL_VALUE_64, update_stats);
	return ValEncode(vc->GetNotNullValueInt64(mit), update_stats);
}

_int64 ColumnBinEncoder::EncoderInt::ValEncode(_int64 v, bool update_stats)
{
	assert(null_status < 2);		// should be used only for small values, when an additional byte is not needed
	if(v == NULL_VALUE_64) {
		if(descending) 
			return max_code;
		return 0;
	}
	if(update_stats) {
		if(v > max_found)
			max_found = v;
		if(v < min_found)
			min_found = v;
	}
	_int64 coded_val = (v - min_val) + (null_status == 1 ? 1 : 0);
	if(descending)
		return max_code - coded_val;
	return coded_val;
}


void ColumnBinEncoder::EncoderInt::SetNull(uchar* buf, uchar* buf_sec)
{
	// Assume three bytes of int
	// mode              |   null    |   not null  |
	// ---------------------------------------------
	// status=1, ascend. |    000    |    xxx      |
	// status=2, ascend. |   0000    |   1xxx      |
	// status=1, descend.|    max    |    xxx      |
	// status=2, descend.|   1000    |   0xxx      |
	// ---------------------------------------------

	assert(null_status > 0);
	memset(buf, 0, size);		// zero on the first and all other bytes (to be changed below)
	if(descending) {
		if(null_status == 1) {
			uchar* val_ptr = (uchar *)(&max_code) + size; 	// the end of meaningful bytes
			for(int i = 0; i < size; i++)
				*(buf++) = *(--val_ptr);			// change endianess - to make numbers comparable by memcmp()
		} else {
			*buf = '\1';
		}
	}
}

bool ColumnBinEncoder::EncoderInt::IsNull(uchar* buf, uchar* buf_sec)
{
	if(null_status == 0)
		return false;
	_uint64 zero = 0;
	if(descending) {
		if(null_status == 1) {
			_uint64 coded_val = 0;
			uchar* val_ptr = (uchar *)(&coded_val) + size; 	// the end of meaningful bytes
			for(int i = 0; i < size; i++)
				*(--val_ptr) = *(buf++);			// change endianess
			return (coded_val == max_code);
		} else
			return (*buf == '\1');
	} else {
		if(null_status == 1)
			return (memcmp(buf, &zero, size) == 0);
		else
			return (*buf == '\0');
	}
}

_int64 ColumnBinEncoder::EncoderInt::GetValue64(uchar* buf, uchar* buf_sec)
{
	int loc_size = size;
	if(null_status == 2) {
		if((!descending && *buf == '\0') ||
			(descending && *buf == '\1'))
			return NULL_VALUE_64;
		buf++;
		loc_size--;
	}
	_uint64 coded_val = 0;
	uchar* val_ptr = (uchar *)(&coded_val) + loc_size; 	// the end of meaningful bytes
	for(int i = 0; i < loc_size; i++)
		*(--val_ptr) = *(buf++);			// change endianess

	if(descending)
		coded_val = max_code - coded_val;
	return coded_val - (null_status == 1 ? 1 : 0) + min_val;
}

void ColumnBinEncoder::EncoderInt::UpdateStatistics(unsigned char *buf)
{
	_int64 v = GetValue64(buf, NULL);
	if(null_status > 0 && v == NULL_VALUE_64)
		return;
	if(v > max_found)
		max_found = v;
	if(v < min_found)
		min_found = v;
}

bool ColumnBinEncoder::EncoderInt::ImpossibleValues(_int64 pack_min, _int64 pack_max)
{
	if(pack_min == NULL_VALUE_64 || pack_max == NULL_VALUE_64 || min_found == PLUS_INF_64)
		return false;
	if(pack_min > max_found || pack_max < min_found)
		return true;
	return false;
}

void ColumnBinEncoder::EncoderInt::ClearStatistics()
{
	min_found = PLUS_INF_64;
	max_found = MINUS_INF_64;
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderDecimal::EncoderDecimal(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
: EncoderInt(vc, decodable, nulls_possible, _descending, false)
{
	multiplier = 1.0;
	sec_multiplier = 1.0;
	scale = vc->Type().GetScale();
	_int64 pmin = vc->RoughMin();
	_int64 pmax = vc->RoughMax();
	min_val = pmin;
	max_code = _uint64(pmax - pmin);
	if(nulls_possible) {
		if(max_code < UINT64_MAX) {
			null_status = 1;	// 0 is null
			max_code++;
		} else
			null_status = 2;	// separate byte
	} else
		null_status = 0;
	size = CalculateByteSize(max_code);
	size_sec = 0;
}

bool ColumnBinEncoder::EncoderDecimal::SecondColumn(VirtualColumn* vc)
{
	_int64 max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
	_int64 new_min_val = vc->RoughMin();
	_int64 new_max_val = vc->RoughMax();
	int new_scale = vc->Type().GetScale();
	if(new_scale > scale) {
		multiplier = PowOfTen(new_scale - scale);
		min_val *= _int64(multiplier);
		max_val *= _int64(multiplier);
	} else if(new_scale < scale) {
		sec_multiplier = PowOfTen(scale - new_scale);
		new_min_val *= _int64(sec_multiplier);
		new_max_val *= _int64(sec_multiplier);
	}
	scale = max(new_scale, scale);
	if(min_val > new_min_val)
		min_val = new_min_val;
	if(max_val < new_max_val)
		max_val = new_max_val;
	max_code = _uint64(max_val - min_val);
	if(null_status == 1 && max_code == (UINT64_MAX))
		null_status = 2;	// separate byte
	size = CalculateByteSize(max_code) + (null_status == 2 ? 1 : 0);
	return true;
}

void ColumnBinEncoder::EncoderDecimal::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator &mit, bool update_stats /*= false*/)
{
	if(null_status > 0 && vc->IsNull(mit))
		SetNull(buf, buf_sec);
	else {
		_int64 v = vc->GetNotNullValueInt64(mit);
		if(vc->Type().GetScale() < scale) 
			v *= _int64(PowOfTen(scale - vc->Type().GetScale()));
		EncoderInt::Encode(buf, buf_sec, v, false, update_stats);
	}
}

bool ColumnBinEncoder::EncoderDecimal::Encode(uchar* buf, uchar* buf_sec, _int64 v, bool sec_column, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64)
		SetNull(buf, buf_sec);
	else {
		if(v != PLUS_INF_64 && v != MINUS_INF_64) 
			v *= _int64(sec_column ? sec_multiplier : multiplier);
		return EncoderInt::Encode(buf, buf_sec, v, false, update_stats);
	}
	return true;
}

bool ColumnBinEncoder::EncoderDecimal::ImpossibleValues(_int64 pack_min, _int64 pack_max)
{
	if(pack_min == NULL_VALUE_64 || pack_max == NULL_VALUE_64 || min_found == PLUS_INF_64)
		return false;
	if(pack_min != MINUS_INF_64)
		pack_min *= _int64(sec_multiplier);			// assuming that pack_min, pack_max always belong to the second column!
	if(pack_max != PLUS_INF_64)
		pack_max *= _int64(sec_multiplier);
	if(pack_min > max_found || pack_max < min_found)
		return true;
	return false;
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderDate::EncoderDate(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
						: EncoderInt(vc, decodable, nulls_possible, _descending, false)
{
	_int64 pmin = DateSortEncoding(vc->RoughMin());
	_int64 pmax = DateSortEncoding(vc->RoughMax());
	min_val = pmin;
	max_code = _uint64(pmax - pmin);
	if(nulls_possible) {
		null_status = 1;	// 0 is null - because dates never reach max. int.
		if(max_code != _uint64(UINT64_MAX))
			max_code++;
	} else
		null_status = 0;
	size = CalculateByteSize(max_code);
	size_sec = 0;
}

bool ColumnBinEncoder::EncoderDate::SecondColumn(VirtualColumn* vc)
{
	// Possible conversions: only dates.
	if(vc->Type().GetTypeName() != RC_DATE) {
		rccontrol.lock(vc->ConnInfo().GetThreadID()) << "Nontrivial comparison: date with non-date" << unlock;
		return false;
	}
	_int64 new_min_val = DateSortEncoding(vc->RoughMin());
	_int64 max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
	_int64 new_max_val = DateSortEncoding(vc->RoughMax());
	if(min_val > new_min_val)
		min_val = new_min_val;
	if(max_val < new_max_val)
		max_val = new_max_val;
	max_code = _uint64(max_val - min_val) + (null_status == 1 && max_code != UINT64_MAX ? 1 : 0);;
	size = CalculateByteSize(max_code);
	return true;
}

void ColumnBinEncoder::EncoderDate::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator &mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		SetNull(buf, buf_sec);
	else
		EncoderInt::Encode(buf, buf_sec, DateSortEncoding(vc->GetNotNullValueInt64(mit)), false, update_stats);
}

bool ColumnBinEncoder::EncoderDate::Encode(uchar* buf, uchar* buf_sec, _int64 v, bool sec_column, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64)
		SetNull(buf, buf_sec);
	else
		return EncoderInt::Encode(buf, buf_sec, DateSortEncoding(v), false, update_stats);
	return true;
}

_int64 ColumnBinEncoder::EncoderDate::ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		return EncoderInt::ValEncode(NULL_VALUE_64, update_stats);
	return EncoderInt::ValEncode(DateSortEncoding(vc->GetNotNullValueInt64(mit)), update_stats);
}

_int64 ColumnBinEncoder::EncoderDate::ValEncode(_int64 v, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64)
		return EncoderInt::ValEncode(NULL_VALUE_64, update_stats);
	return EncoderInt::ValEncode(DateSortEncoding(v), update_stats);
}

_int64 ColumnBinEncoder::EncoderDate::GetValue64(uchar* buf, uchar* buf_sec)
{
	if(IsNull(buf, buf_sec))
		return NULL_VALUE_64;
	return DateSortDecoding(EncoderInt::GetValue64(buf, buf_sec));
}

void ColumnBinEncoder::EncoderDate::UpdateStatistics(unsigned char *buf)
{
	_int64 v = EncoderInt::GetValue64(buf, NULL);
	if(null_status > 0 && v == NULL_VALUE_64)
		return;
	if(v > max_found)					// min/max_found as DateSortEncoding values
		max_found = v;
	if(v < min_found)
		min_found = v;
}

bool ColumnBinEncoder::EncoderDate::ImpossibleValues(_int64 pack_min, _int64 pack_max)
{
	if(pack_min == NULL_VALUE_64 || pack_max == NULL_VALUE_64 || min_found == PLUS_INF_64)
		return false;
	if(DateSortEncoding(pack_min) > max_found || DateSortEncoding(pack_max) < min_found)
		return true;
	return false;
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderYear::EncoderYear(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
						: EncoderInt(vc, decodable, nulls_possible, _descending, false)
{
	_int64 pmin = YearSortEncoding(vc->RoughMin());
	_int64 pmax = YearSortEncoding(vc->RoughMax());
	min_val = pmin;
	max_code = _uint64(pmax - pmin);
	if(nulls_possible) {
		null_status = 1;	// 0 is null - because years never reach max. int.
		if(max_code != UINT64_MAX)
			max_code++;
	} else
		null_status = 0;
	size = CalculateByteSize(max_code);
	size_sec = 0;
}

bool ColumnBinEncoder::EncoderYear::SecondColumn(VirtualColumn* vc)
{
	// Possible conversions: only years.
	if(vc->Type().GetTypeName() != RC_YEAR) {
		rccontrol.lock(vc->ConnInfo().GetThreadID()) << "Nontrivial comparison: year with non-year" << unlock;
		return false;
	}

	_int64 new_min_val = YearSortEncoding(vc->RoughMin());
	_int64 max_val = max_code + min_val - (null_status == 1 ? 1 : 0);
	_int64 new_max_val = YearSortEncoding(vc->RoughMax());
	if(min_val > new_min_val)
		min_val = new_min_val;
	if(max_val < new_max_val)
		max_val = new_max_val;
	max_code = _uint64(max_val - min_val);
	if(max_code != UINT64_MAX && null_status == 1 )
		max_code++;
	size = CalculateByteSize(max_code);
	return true;
}

void ColumnBinEncoder::EncoderYear::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator &mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		SetNull(buf, buf_sec);
	else
		EncoderInt::Encode(buf, buf_sec, YearSortEncoding(vc->GetNotNullValueInt64(mit)), false, update_stats);
}

bool ColumnBinEncoder::EncoderYear::Encode(uchar* buf, uchar* buf_sec, _int64 v, bool sec_column, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64)
		SetNull(buf, buf_sec);
	else
		return EncoderInt::Encode(buf, buf_sec, YearSortEncoding(v), false, update_stats);
	return true;
}

_int64 ColumnBinEncoder::EncoderYear::ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		return EncoderInt::ValEncode(NULL_VALUE_64, update_stats);
	return EncoderInt::ValEncode(YearSortEncoding(vc->GetNotNullValueInt64(mit)), update_stats);
}

_int64 ColumnBinEncoder::EncoderYear::ValEncode(_int64 v, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64)
		return EncoderInt::ValEncode(NULL_VALUE_64, update_stats);
	return EncoderInt::ValEncode(YearSortEncoding(v), update_stats);
}

_int64 ColumnBinEncoder::EncoderYear::GetValue64(uchar* buf, uchar* buf_sec)
{
	if(IsNull(buf, buf_sec))
		return NULL_VALUE_64;
	return YearSortDecoding(EncoderInt::GetValue64(buf, buf_sec));
}

void ColumnBinEncoder::EncoderYear::UpdateStatistics(unsigned char *buf)
{
	_int64 v = EncoderInt::GetValue64(buf, NULL);
	if(null_status > 0 && v == NULL_VALUE_64)
		return;
	if(v > max_found)					// min/max_found as YearSortEncoding values
		max_found = v;
	if(v < min_found)
		min_found = v;
}

bool ColumnBinEncoder::EncoderYear::ImpossibleValues(_int64 pack_min, _int64 pack_max)
{
	if(pack_min == NULL_VALUE_64 || pack_max == NULL_VALUE_64 || min_found == PLUS_INF_64)
		return false;
	if(YearSortEncoding(pack_min) > max_found || YearSortEncoding(pack_max) < min_found)
		return true;
	return false;
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderDouble::EncoderDouble(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
							: ColumnValueEncoder(vc, decodable, nulls_possible, _descending)
{
	multiplier_vc1 = 0;	// 0 if the first vc is double, otherwise it is regarded as int (decimal) which must be multiplied
	multiplier_vc2 = 0;	// 0 if the second vc is double, otherwise it is regarded as int (decimal) which must be multiplied
	if(vc->Type().IsFixed())
		multiplier_vc1 = _int64(PowOfTen(vc->Type().GetScale()));		// encode int/dec as double
	if(nulls_possible) {
		null_status = 2;
		size = 9;
	} else {
		null_status = 0;
		size = 8;
	}
}

bool ColumnBinEncoder::EncoderDouble::SecondColumn(VirtualColumn* vc)
{
	// Possible conversions: all numericals.
	if(!vc->Type().IsFixed() && !vc->Type().IsFloat()) {
		rccontrol.lock(vc->ConnInfo().GetThreadID()) << "Nontrivial comparison: floating-point with non-numeric" << unlock;
		return false;
	}
	if(vc->Type().IsFixed())
		multiplier_vc2 = _int64(PowOfTen(vc->Type().GetScale()));		// encode int/dec as double
	return true;
}

void ColumnBinEncoder::EncoderDouble::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats)
{
	if(null_status == 2) {
		if(vc->IsNull(mit)) {
			SetNull(buf, buf_sec);
			return;
		}
		*buf = (descending ? '\0' : '\1');			// not null
		buf++;
	}
	double val = vc->GetValueDouble(mit);			// note that non-float columns will be properly converted here
	_int64 coded_val = MonotonicDouble2Int64(*(_int64*)&val);
	if(descending)
		Negate((uchar *)&coded_val, 8);
	uchar* val_ptr = (uchar *)(&coded_val) + 8; 	// the end of meaningful bytes
	for(int i = 0; i < 8; i++)
		*(buf++) = *(--val_ptr);			// change endianess - to make numbers comparable by memcmp()
}

bool ColumnBinEncoder::EncoderDouble::Encode(uchar* buf, uchar* buf_sec, _int64 v, bool sec_column, bool update_stats)
{
	if(null_status == 2) {
		if(v == NULL_VALUE_64) {
			SetNull(buf, buf_sec);
			return true;
		}
		*buf = (descending ? '\0' : '\1');			// not null
		buf++;
	}
	_int64 local_mult = (sec_column ? multiplier_vc2 : multiplier_vc1);
	_int64 coded_val;
	if(local_mult == 0)
		coded_val = MonotonicDouble2Int64(v);
	else {
		double d = double(v) / local_mult;			// decimal encoded as double
		coded_val = MonotonicDouble2Int64(*((_int64*)(&d)));
	}
	if(descending)
		Negate((uchar *)&coded_val, 8);
	uchar* val_ptr = (uchar *)(&coded_val) + 8; 	// the end of meaningful bytes
	for(int i = 0; i < 8; i++)
		*(buf++) = *(--val_ptr);			// change endianess - to make numbers comparable by memcmp()
	return true;
}

void ColumnBinEncoder::EncoderDouble::SetNull(uchar* buf, uchar* buf_sec)
{
	// mode              |   null    |   not null  |
	// ---------------------------------------------
	// status=2, ascend. |   00..00  |   1<64bit>  |
	// status=2, descend.|   10..00  |   0<64bit>  |
	// ---------------------------------------------
	assert(null_status == 2);
	memset(buf, 0, 9);
	if(descending)
		*buf = '\1';
}

bool ColumnBinEncoder::EncoderDouble::IsNull(uchar* buf, uchar* buf_sec)
{
	if(null_status != 2)
		return false;
	if(descending)
		return (*buf == '\1');
	return (*buf == '\0');
}

_int64 ColumnBinEncoder::EncoderDouble::GetValue64(uchar* buf, uchar* buf_sec)
{
	if(null_status == 2) {
		if((!descending && *buf == '\0') ||
			(descending && *buf == '\1'))
			return NULL_VALUE_64;
		buf++;
	}
	_uint64 coded_val = 0;
	uchar* val_ptr = (uchar *)(&coded_val) + 8; 	// the end of meaningful bytes
	for(int i = 0; i < 8; i++)
		*(--val_ptr) = *(buf++);			// change endianess
	if(descending)
		Negate((uchar *)&coded_val, 8);
	return MonotonicInt642Double(coded_val);
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderText::EncoderText(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
: ColumnValueEncoder(vc, decodable, nulls_possible, _descending), min_max_set(false)

{
	size = vc->MaxStringSize() + 2;						// 2 bytes for len
	size_sec = 0;
	null_status = (nulls_possible ? 1 : 0);
}

ColumnBinEncoder::EncoderText::~EncoderText()
{}

bool ColumnBinEncoder::EncoderText::SecondColumn(VirtualColumn* vc)
{
	size = max(size, vc->MaxStringSize() + 2);						// 2 bytes for len
	return true;
}

void ColumnBinEncoder::EncoderText::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit)) {
		SetNull(buf, buf_sec);
		return;
	}
	memset(buf, 0, size);
	RCBString s;
	vc->GetNotNullValueString(s, mit);
	if(update_stats) {
		if(!min_max_set) {
			maxs.PersistentCopy(s);
			mins.PersistentCopy(s);
			min_max_set = true;
		} else {
			if(s > maxs)
				maxs.PersistentCopy(s);
			if(s < mins)
				mins.PersistentCopy(s);
		}
	}
	BHASSERT(s.len <= (uint)size, "Size of buffer too small"); 
	if(s.len > 0)
		memcpy(buf, s.GetDataBytesPointer(), s.len);
	buf[size - 2] = (s.len + 1) / 256;
	buf[size - 1] = (s.len + 1) % 256;
	if(descending)
		Negate(buf, size);
}

bool ColumnBinEncoder::EncoderText::Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats)
{
	if(null_status > 0 && s.IsNull()) {
		SetNull(buf, buf_sec);
		return true;
	}
	memset(buf, 0, size);
	if(update_stats) {
		if(!min_max_set) {
			maxs.PersistentCopy(s);
			mins.PersistentCopy(s);
			min_max_set = true;
		} else {
			if(s > maxs)
				maxs.PersistentCopy(s);
			if(s < mins)
				mins.PersistentCopy(s);
		}
	}
	BHASSERT(s.len <= (uint)size, "Size of buffer too small"); 
	if(s.len > 0)
		memcpy(buf, s.GetDataBytesPointer(), s.len);
	buf[size - 2] = (s.len + 1) / 256;
	buf[size - 1] = (s.len + 1) % 256;
	if(descending)
		Negate(buf, size);
	return true;
}

void ColumnBinEncoder::EncoderText::SetNull(uchar* buf, uchar* buf_sec)
{
	if(descending)
		memset(buf, 255, size);
	else
		memset(buf, 0, size);
}

bool ColumnBinEncoder::EncoderText::IsNull(uchar* buf, uchar* buf_sec)
{
	if(descending)
		return (buf[size - 2] == 255 && buf[size - 1] == 255);
	return (buf[size - 2] == 0 && buf[size - 1] == 0);
}

RCBString ColumnBinEncoder::EncoderText::GetValueT(uchar* buf, uchar* buf_sec)
{
	if(IsNull(buf, buf_sec))
		return RCBString();
	if(descending)
		Negate(buf, size);
	int len = int(buf[size - 2]) * 256 + int(buf[size - 1]) - 1;
	if(len == 0)
		return RCBString("");
	return RCBString((char*)buf, len);			// the RCBString is generated as temporary
}

bool ColumnBinEncoder::EncoderText::ImpossibleValues(RCBString& pack_min, RCBString& pack_max)
{
	int lenmin = min(pack_min.len, maxs.len);
	int lenmax = min(pack_max.len, mins.len);

	if(strncmp(pack_min.val, maxs.val, lenmin) > 0 || strncmp(pack_max.val, mins.val, lenmax) < 0 )
		return true;
	return false;
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderText_UTF::EncoderText_UTF(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
: ColumnValueEncoder(vc, decodable, nulls_possible, _descending), min_max_set(false)
{
	collation = vc->GetCollation();
	int coded_len = CollationBufLen(collation,  vc->MaxStringSize());
	size = coded_len + 2;						// 2 bytes for len
	size_sec = 0;
	if(decodable)
		size_sec = vc->MaxStringSize() + 2;		// just a raw data plus len
	null_status = (nulls_possible ? 1 : 0);
}

ColumnBinEncoder::EncoderText_UTF::~EncoderText_UTF()
{}

bool ColumnBinEncoder::EncoderText_UTF::SecondColumn(VirtualColumn* vc2)
{
	if(vc_type.IsString() && vc2->Type().IsString() && collation.collation != vc2->GetCollation().collation) {
		rccontrol.lock(vc2->ConnInfo().GetThreadID()) << "Nontrivial comparison: " << collation.collation->name << " with " << vc2->GetCollation().collation->name << unlock;
		return false;
	}
	int coded_len = CollationBufLen(collation, vc2->MaxStringSize());
	size = max(size, coded_len + 2);						// 2 bytes for len
	if(size_sec > 0)
		size_sec = max(size_sec, vc2->MaxStringSize() + 2);						// 2 bytes for len
	return true;
}

void ColumnBinEncoder::EncoderText_UTF::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit)) {
		SetNull(buf, buf_sec);
		return;
	}
	memset(buf, 0, size);
	RCBString s;
	vc->GetNotNullValueString(s, mit);
	if(update_stats) {
		if(!min_max_set) {
			maxs.PersistentCopy(s);
			mins.PersistentCopy(s);
			min_max_set = true;
		} else {
			if(CollationStrCmp(collation, s, maxs) > 0)
				maxs.PersistentCopy(s);
			if(CollationStrCmp(collation, s, mins) < 0)
				mins.PersistentCopy(s);
		}
	}
	strnxfrm(collation, buf, size - 2, (uchar*)s.Value(), s.len);
	//int coded_len = CollationBufLen(collation, s.len);
	buf[size - 2] = (size - 2 + 1) / 256;
	buf[size - 1] = (size - 2 + 1) % 256;
	if(descending)
		Negate(buf, size);
	if(size_sec > 0) {
		memset(buf_sec, 0, size_sec);
		buf_sec[size_sec - 2] = (s.len + 1) / 256;
		buf_sec[size_sec - 1] = (s.len + 1) % 256;
		if(s.len > 0)
			memcpy(buf_sec, s.GetDataBytesPointer(), s.len);
	}
}

bool ColumnBinEncoder::EncoderText_UTF::Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats)
{
	if(null_status > 0 && s.IsNull()) {
		SetNull(buf, buf_sec);
		return true;
	}
	memset(buf, 0, size);
	if(update_stats) {
		if(!min_max_set) {
			maxs = s;
			mins = s;
			min_max_set = true;
		} else {
			if(CollationStrCmp(collation, s, maxs) > 0)
				maxs = s;
			if(CollationStrCmp(collation, s, mins) < 0)
				mins = s;
		}
	}
	strnxfrm(collation, buf, size - 2, (uchar*)s.Value(), s.len);
	//int coded_len = CollationBufLen(collation, s.len);
	buf[size - 2] = (size - 2 + 1) / 256;
	buf[size - 1] = (size - 2 + 1) % 256;
	if(descending)
		Negate(buf, size);
	if(size_sec > 0) {
		memset(buf_sec, 0, size_sec);
		buf_sec[size_sec - 2] = (s.len + 1) / 256;
		buf_sec[size_sec - 1] = (s.len + 1) % 256;
		if(s.len > 0)
			memcpy(buf_sec, s.GetDataBytesPointer(), s.len);
	}
	return true;
}

void ColumnBinEncoder::EncoderText_UTF::SetNull(uchar* buf, uchar* buf_sec)
{
	if(descending)
		memset(buf, 255, size);
	else
		memset(buf, 0, size);
	if(size_sec > 0)
		memset(buf_sec, 0, size_sec);
}

bool ColumnBinEncoder::EncoderText_UTF::IsNull(uchar* buf, uchar* buf_sec)
{
	assert(size_sec > 0);
	return (buf_sec[size_sec - 2] == 0 && buf_sec[size_sec - 1] == 0);
}

RCBString ColumnBinEncoder::EncoderText_UTF::GetValueT(uchar* buf, uchar* buf_sec)
{
	assert(size_sec > 0);
	if(null_status > 0 && IsNull(buf, buf_sec))
		return RCBString();
	int len = int(buf_sec[size_sec - 2]) * 256 + int(buf_sec[size_sec - 1]) - 1;
	if(len ==0)
		return RCBString("");
	else
		return RCBString((char *)buf_sec, len, false);			// the RCBString is generated as temporary
}

bool ColumnBinEncoder::EncoderText_UTF::ImpossibleValues(RCBString& pack_min, RCBString& pack_max)
{
	_uint64 min = 0;
	_uint64 max = 0;
	memcpy((char*)&min, pack_min.val, pack_min.len);
	memcpy((char*)&max, pack_max.val, pack_max.len);
	if(!maxs.GreaterEqThanMinUTF(min, collation) || !mins.LessEqThanMaxUTF(max, collation))
		return true;
	return false;
}
/////////////////////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderLookup::EncoderLookup(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
: EncoderInt(vc, decodable, nulls_possible, _descending, false)
{
	_int64 pmin = vc->RoughMin();
	_int64 pmax = vc->RoughMax();
	min_val = pmin;
	max_code = _uint64(pmax - pmin);
	// 0 is always NULL for lookup (because we must encode non-existing strings)
	null_status = 1;	// 0 is null
	max_code++;
	size = CalculateByteSize(max_code);
	size_sec = 0;
	first_vc = vc;
	sec_vc = NULL;
	translate2 = NULL;
	no_sec_values = -1;
}

ColumnBinEncoder::EncoderLookup::EncoderLookup(EncoderLookup &sec) : EncoderInt(sec)
{
	no_sec_values = sec.no_sec_values;
	first_vc = sec.first_vc;
	sec_vc = sec.sec_vc;
	if(sec.translate2) {
		translate2  = new int [no_sec_values];
		memcpy(translate2, sec.translate2, no_sec_values * sizeof(int));
	} else
		translate2 = NULL;
}

ColumnBinEncoder::EncoderLookup::~EncoderLookup()
{
	delete [] translate2;
}

bool ColumnBinEncoder::EncoderLookup::SecondColumn(VirtualColumn* vc)
{
	sec_vc = vc;
	_int64 max_val = max_code + min_val - 1;
	_int64 new_min_val = vc->RoughMin();
	_int64 new_max_val = vc->RoughMax();
	no_sec_values = (int)new_max_val + 1;
	delete [] translate2;
	translate2  = new int [no_sec_values];
	DTCollation collation = first_vc->GetCollation();
	if(collation.collation != sec_vc->GetCollation().collation)
		return false;
	if(RequiresUTFConversions(collation)) {
		for(int i = 0; i < no_sec_values; i++) {
			int code = -1;
			RCBString val2 = sec_vc->DecodeValue_S(i);
			for(int j = (int)min_val; j < max_val + 1; j++) {
				RCBString val1 = first_vc->DecodeValue_S(j);
				if(CollationStrCmp(collation, val1, val2) == 0) {
					if(code != -1)
						return false;		// ambiguous translation - cannot encode properly
					code = j;
				}
			}
			translate2[i] = code;
		}
	} else {								// binary encoding: internal dictionary search may be used
		for(int i = 0; i < no_sec_values; i++) {
			RCBString val2 = sec_vc->DecodeValue_S(i);
			int code = first_vc->EncodeValue_S(val2);		// NULL_VALUE_32 if not found
			translate2[i] = (code < 0 ? -1 : code);
		}
	}
	if(min_val > new_min_val)
		min_val = new_min_val;
	if(max_val < new_max_val)
		max_val = new_max_val;
	max_code = _uint64(max_val - min_val);
	size = CalculateByteSize(max_code);
	return true;
}

RCBString ColumnBinEncoder::EncoderLookup::GetValueT(unsigned char* buf, unsigned char* buf_sec)
{
	if(IsNull(buf, buf_sec))
		return RCBString();
	_int64 v = EncoderInt::GetValue64(buf, buf_sec);
	return first_vc->DecodeValue_S(v);
}

void ColumnBinEncoder::EncoderLookup::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator &mit, bool update_stats /*= false*/)
{
	if(null_status > 0 && vc->IsNull(mit))
		SetNull(buf, buf_sec);
	else {
		_int64 v = vc->GetNotNullValueInt64(mit);
		if(vc == sec_vc) {
			v = translate2[v];
			if(v == -1) {
				SetNull(buf, buf_sec);
				return;
			}
		}
		EncoderInt::Encode(buf, buf_sec, v, false, update_stats);
	}
}

bool ColumnBinEncoder::EncoderLookup::Encode(uchar* buf, uchar* buf_sec, _int64 v, bool sec_column, bool update_stats)
{
	if(null_status > 0 && v == NULL_VALUE_64)
		SetNull(buf, buf_sec);
	else {
		if(sec_column) {
			v = translate2[v];
			if(v == -1)
				v = NULL_VALUE_64;		// value not found
		}
		return EncoderInt::Encode(buf, buf_sec, v, false, update_stats);
	}
	return true;
}

bool ColumnBinEncoder::EncoderLookup::ImpossibleValues(_int64 pack_min, _int64 pack_max)
{
	// assuming that pack_min, pack_max always belong to the second column!
	// Calculate min. and max. codes in the v1 encoding
	_int64 pack_v1_min = PLUS_INF_64;
	_int64 pack_v1_max = MINUS_INF_64;
	if(pack_min < 0)
		pack_min = 0;
	if(pack_max > no_sec_values - 1)
		pack_max = no_sec_values - 1;
	for(int i = (int)pack_min; i <= pack_max; i++) {
		if(pack_v1_min > translate2[i])
			pack_v1_min = translate2[i];
		if(pack_v1_max < translate2[i])
			pack_v1_max = translate2[i];
	}
	if(pack_v1_min != PLUS_INF_64 && (pack_v1_min > max_found || pack_v1_max < min_found))
		return true;
	return false;
}

/////////////////////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderTextStat::EncoderTextStat(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
: EncoderInt(vc, decodable, nulls_possible, _descending, false)
{
	vc->GetTextStat(coder);
	coder.CreateEncoding();
	valid = coder.IsValid();
	min_val = 0;
	max_code = coder.MaxCode() + 2;		// +1 for a PLUS_INF value, +1 for NULL or MINUS_INF value
	null_status = 1;	// 0 is null
	size = CalculateByteSize(max_code);
	size_sec = 0;
}

bool ColumnBinEncoder::EncoderTextStat::SecondColumn(VirtualColumn* vc)
{
	vc->GetTextStat(coder);
	coder.CreateEncoding();		// may be created again only if there was no decoding in the meantime
	valid = coder.IsValid();
	min_val = 0;
	max_code = coder.MaxCode() + 2;		// +1 for a PLUS_INF value, +1 for NULL or MINUS_INF value
	size = CalculateByteSize(max_code);
	return valid;
}

RCBString ColumnBinEncoder::EncoderTextStat::GetValueT(unsigned char* buf, unsigned char* buf_sec)
{
	if(IsNull(buf, buf_sec))
		return RCBString();
	_int64 v = EncoderInt::GetValue64(buf, buf_sec);
	return coder.Decode(v);
}

void ColumnBinEncoder::EncoderTextStat::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator &mit, bool update_stats /*= false*/)
{
	if(null_status > 0 && vc->IsNull(mit))
		SetNull(buf, buf_sec);
	else {
		RCBString vs;
		vc->GetValueString(vs, mit);
		_int64 v = coder.Encode(vs);
		EncoderInt::Encode(buf, buf_sec, v, false, update_stats);
	}
}

bool ColumnBinEncoder::EncoderTextStat::Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats)
{
	if(null_status > 0 && s.IsNull())
		SetNull(buf, buf_sec);
	else {
		_int64 v = coder.Encode(s);
		return EncoderInt::Encode(buf, buf_sec, v, false, update_stats);
	}
	return true;
}

_int64 ColumnBinEncoder::EncoderTextStat::ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats)
{
	if(null_status > 0 && vc->IsNull(mit))
		return EncoderInt::ValEncode(NULL_VALUE_64, update_stats);
	RCBString vs;
	vc->GetValueString(vs, mit);
	_int64 v = coder.Encode(vs);
	return EncoderInt::ValEncode(v, update_stats);
}

bool ColumnBinEncoder::EncoderTextStat::ImpossibleValues(RCBString& pack_min, RCBString& pack_max)
{
	_int64 v_min = coder.Encode(pack_min);
	_int64 v_max = coder.Encode(pack_max, true);
	return EncoderInt::ImpossibleValues(v_min, v_max);
}

////////////////////////////////////////////////////////////////////////

ColumnBinEncoder::EncoderTextMD5::EncoderTextMD5(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending)
: ColumnValueEncoder(vc, decodable, nulls_possible, _descending), min_max_set(false)

{
	size = HASH_FUNCTION_BYTE_SIZE;
	size_sec = 0;
	null_status = 1;		// ignored
	assert(!decodable);
	memset(null_buf, 0, HASH_FUNCTION_BYTE_SIZE);
	memset(empty_buf, 0, HASH_FUNCTION_BYTE_SIZE);
	memcpy(null_buf, "-- null --", 10);					// constant values for special purposes
	memcpy(empty_buf, "-- empty --", 11);
}

ColumnBinEncoder::EncoderTextMD5::~EncoderTextMD5()
{}

ColumnBinEncoder::EncoderTextMD5::EncoderTextMD5(EncoderTextMD5 &sec) : ColumnValueEncoder(sec), mins(sec.mins), maxs(sec.maxs), min_max_set(sec.min_max_set)
{
	size = HASH_FUNCTION_BYTE_SIZE;
	memset(null_buf, 0, HASH_FUNCTION_BYTE_SIZE);
	memset(empty_buf, 0, HASH_FUNCTION_BYTE_SIZE);
	memcpy(null_buf, "-- null --", 10);					// constant values for special purposes
	memcpy(empty_buf, "-- empty --", 11);
}

bool ColumnBinEncoder::EncoderTextMD5::SecondColumn(VirtualColumn* vc)
{
	return true;
}

void ColumnBinEncoder::EncoderTextMD5::Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats)
{
	if(vc->IsNull(mit)) {
		SetNull(buf, buf_sec);
		return;
	}
	RCBString s;
	vc->GetNotNullValueString(s, mit);
	if(update_stats) {
		if(!min_max_set) {
			maxs.PersistentCopy(s);
			mins.PersistentCopy(s);
			min_max_set = true;
		} else {
			if(s > maxs)
				maxs.PersistentCopy(s);
			if(s < mins)
				mins.PersistentCopy(s);
		}
	}
	if(s.len > 0) {
		HashMD5((unsigned char*)s.Value(), s.len, buf);
		*((uint*)buf) ^= s.len;
	} else
		memcpy(buf, empty_buf, size);
}

bool ColumnBinEncoder::EncoderTextMD5::Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats)
{
	if(s.IsNull()) {
		SetNull(buf, buf_sec);
		return true;
	}
	if(update_stats) {
		if(!min_max_set) {
			maxs.PersistentCopy(s);
			mins.PersistentCopy(s);
			min_max_set = true;
		} else {
			if(s > maxs)
				maxs.PersistentCopy(s);
			if(s < mins)
				mins.PersistentCopy(s);
		}
	}
	if(s.len > 0) {
		HashMD5((unsigned char*)s.Value(), s.len, buf);
		*((uint*)buf) ^= s.len;
	} else
		memcpy(buf, empty_buf, size);
	return true;
}

void ColumnBinEncoder::EncoderTextMD5::SetNull(uchar* buf, uchar* buf_sec)
{
	memcpy(buf, null_buf, size);
}

bool ColumnBinEncoder::EncoderTextMD5::IsNull(uchar* buf, uchar* buf_sec)
{
	return (memcmp(buf, null_buf, size) == 0);
}

bool ColumnBinEncoder::EncoderTextMD5::ImpossibleValues(RCBString& pack_min, RCBString& pack_max)
{
	int lenmin = min(pack_min.len, maxs.len);
	int lenmax = min(pack_max.len, mins.len);

	if(strncmp(pack_min.val, maxs.val, lenmin) > 0 || strncmp(pack_max.val, mins.val, lenmax) < 0 )
		return true;
	return false;
}

////////////////////////////////////////////////////////////////////////

MultiindexPositionEncoder::MultiindexPositionEncoder(MultiIndex *mind, DimensionVector &dims)
{
	val_size = 0;
	val_offset = 0;
	for(int i = 0; i < mind->NoDimensions(); i++) if(dims[i]) {
		dim_no.push_back(i);
		int loc_size = DimByteSize(mind, i);
		dim_size.push_back(loc_size);
		dim_offset.push_back(0);					// must be set by SetPrimaryOffset()
		val_size += loc_size;
	}
}

void MultiindexPositionEncoder::SetPrimaryOffset(int _offset)			// offset of the primary storage in buffer
{ 
	val_offset = _offset;
	if(dim_no.size() > 0) {
		dim_offset[0] = val_offset;
		for(int i = 1; i < dim_offset.size(); i++)
			dim_offset[i] = dim_offset[i - 1] + dim_size[i - 1];
	}
}

void MultiindexPositionEncoder::Encode(unsigned char *buf, MIIterator& mit)			// set the buffer basing on iterator position
{
	for(int i = 0; i < dim_no.size(); i++) {
		_int64 val = mit[dim_no[i]];
		if(val == NULL_VALUE_64)
			val = 0;
		else
			val++;
		memcpy(buf + dim_offset[i], &val, dim_size[i]);
	}
}

void MultiindexPositionEncoder::GetValue(unsigned char *buf, MIDummyIterator &mit)	// set the iterator position basing on buffer
{
	for(int i = 0; i < dim_no.size(); i++) {
		_int64 val = 0;
		memcpy(&val, buf + dim_offset[i], dim_size[i]);
		if(val == 0)
			val = NULL_VALUE_64;
		else
			val--;
		mit.Set(dim_no[i], val);
	}
}

int MultiindexPositionEncoder::DimByteSize(MultiIndex *mind, int dim)			// how many bytes is needed to encode dimensions
{
	_uint64 no_rows = mind->OrigSize(dim) + 1;
	return CalculateByteSize(no_rows);
}
