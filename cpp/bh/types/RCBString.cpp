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

#include "RCDataTypes.h"
#include "common/bhassert.h"
#include "core/tools.h"
#include "edition/vc/VirtualColumn.h"

using namespace std;
RCBString::RCBString()				// null string
{
	null = true;
	len = 0;
	val = 0;
	pos = 0;
	persistent = false;
	zero_term = 0;
}

RCBString::RCBString(const char *val, int len, bool persistent, bool _zero_term)
:	persistent(persistent), zero_term(_zero_term ? 1 : 0)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!_zero_term ||(_zero_term && persistent));
	// NOTE: we allow val to be NULL. In this case, no value will be copied (just reserve a place for future use). Only persistent!
	pos = 0;
	this->null = false;
	if(len == -1) {
		this->len = *(ushort*)val;
		if(persistent == false)
			this->val = const_cast<char*>(val) + sizeof(ushort);
		else {
			this->val = new char [this->len + this->zero_term];
			if(val)
				memcpy(this->val, val + sizeof(ushort), this->len);
			if(zero_term)
				this->val[len] = 0;
		}
	} else if(len == -2) {
		this->len = *(int*)val;
		if(persistent == false)
			this->val = const_cast<char*>(val) + sizeof(int);
		else {
			this->val = new char [this->len + this->zero_term];
			if(val)
				memcpy(this->val, val + sizeof(int), this->len);
			if(zero_term)
				this->val[len] = 0;
		}
	} else {
		if(len == 0) {
			if(val)
				this->len = (int)strlen(val);
			else
				this->len = 0;
		} else
			this->len = (ushort)len;
		if(persistent == false)
			this->val = const_cast<char*>(val);
		else {
			this->val = new char [this->len + this->zero_term];
			if(val)
				memcpy(this->val, val, this->len);
			if(zero_term)
				this->val[this->len] = 0;
		}
	}
}

//RCBString::RCBString(char *buffer, const char *val, int len, bool _zero_term)
//:	persistent(false), zero_term(_zero_term ? 1 : 0)
//{
//	// NOTE: we allow val to be NULL. In this case, no value will be copied (just reserve a place for future use). Only persistent!
//	pos = 0;
//	this->null = false;
//	if(len == -1) {
//		this->len = *(ushort*)val;
//		this->val = buffer;
//		if(val)
//			memcpy(this->val, val + sizeof(ushort), this->len);
//		if(zero_term)
//			this->val[len] = 0;
//	} else if(len == -2) {
//		this->len = *(int*)val;
//		this->val = buffer;
//		if(val)
//			memcpy(this->val, val + sizeof(int), this->len);
//		if(zero_term)
//			this->val[len] = 0;
//	} else {
//		if(len == 0) {
//			if(val)
//				this->len = (int)strlen(val);
//			else
//				this->len = 0;
//		} else
//			this->len = (ushort)len;
//
//		this->val = buffer;
//		if(val)
//			memcpy(this->val, val, this->len);
//		if(zero_term)
//			this->val[this->len] = 0;
//	}
//}

RCBString::RCBString(const RCBString& rcbs)
	: pos(rcbs.pos), persistent(rcbs.persistent), zero_term(rcbs.zero_term)
{
	this->null = rcbs.null;
	if(!null) {
		len = rcbs.len;
		if(persistent) {
			val = new char [len + pos + zero_term];
			memcpy(val, rcbs.val, len + pos);
			if(zero_term)
				val[len + pos] = 0;
		} else
			val = rcbs.val;
	} else {
		len = 0;
		val = 0;
		pos = 0;
		persistent = false;
		zero_term = 0;
	}
}

RCBString::~RCBString()
{
	if(persistent)
		delete [] val;
}

RCBString& RCBString::operator=(const RCDataType& rcdt)
{
	if (this == &rcdt)
		return *this;

	if(rcdt.GetValueType() == STRING_TYPE)
		*this = (RCBString&)rcdt;
	else
		BHERROR("bad cast");

	return *this;
}

bool RCBString::Parse(RCBString& in, RCBString& out)
{
	out = in;
	return true;
}

AttributeType RCBString::Type() const
{
	return RC_STRING;
}

void RCBString::PutString(char* &dest, ushort len, bool move_ptr) const
{
	BHASSERT(this->len <= len, "should be 'this->len <= len'");
	if(this->len == 0)
		memset(dest, ' ', len);
	else {
		memcpy(dest, val, this->len);
		memset(dest+this->len, ' ', len - this->len);
	}
	if(move_ptr)
		dest += len;
}

void RCBString::PutVarchar(char* &dest, uchar prefixlen, bool move_ptr) const
{
	if(prefixlen == 0)
		PutString(dest, len);
	if(len == 0) {
		memset(dest, 0, prefixlen);
		if(move_ptr)
			dest += prefixlen;
	} else {
		switch (prefixlen) {
			case 1 :
				*(uchar*)dest = (uchar)len;
				break;
			case 2 :
				*(ushort*)dest = (ushort)len;
				break;
			case 4 :
				*(uint*)dest = (uint)len;
				break;
			default:
				BHERROR("not implemented");
		}
		memcpy(dest + prefixlen, val, len);
		if(move_ptr)
			dest += prefixlen + len;
	}
}

void RCBString::Put(char* &dest, ushort len) const
{
	if(len)
		PutString(dest, len);
	else
		PutString(dest, this->len);
}

RCBString& RCBString::operator=(const RCBString& rcbs)
{
	if(this == &rcbs)
		return *this;

	null = rcbs.null;
	if(null) {
		if(persistent)
			delete [] val;
		val = 0;
		len = 0;
		pos = 0;
		zero_term = 0;
	} else {
		if(rcbs.persistent) {
			uint tmp_len = rcbs.len + rcbs.pos + rcbs.zero_term;
			if(!persistent || tmp_len > len + pos + zero_term) {
				if(persistent)
					delete [] val;
				val = new char [tmp_len];
			}
			len = rcbs.len;
			pos = rcbs.pos;
			zero_term = rcbs.zero_term;
			memcpy(val, rcbs.val, len + pos);
			if(zero_term)
				val[len + pos] = 0;
		} else {
			if(persistent)
				delete [] val;
			len = rcbs.len;
			pos = rcbs.pos;
			zero_term = rcbs.zero_term;
			val = rcbs.val;
		}
	}
	persistent = rcbs.persistent;
	return *this;
}

void RCBString::PersistentCopy(const RCBString& rcbs)
{
	if(this == &rcbs) {
		MakePersistent();
		return;
	}

	null = rcbs.null;
	if(null) {
		delete [] val;
		val = 0;
		len = 0;
		pos = 0;
		zero_term = 0;
	} else {
		uint tmp_len = rcbs.len + rcbs.pos + rcbs.zero_term;
		if(!persistent || tmp_len > len + pos + zero_term) {
			if(persistent)
				delete [] val;
			val = new char [tmp_len];
		}
		len = rcbs.len;
		pos = rcbs.pos;
		zero_term = rcbs.zero_term;
		memcpy(val, rcbs.val, len + pos);
		if(zero_term)
			val[len + pos] = 0;
	}
	persistent = true;
}

RCBString::operator std::string() const
{
	if(len)
		return std::string(val + pos, len);
	return std::string();
}

char& RCBString::operator*()
{
	return val[pos];
}

RCBString& RCBString::operator++()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(len > 0);
	pos++;
	len--;
	return *this;
}

RCBString RCBString::operator++(int)
{
	RCBString ret(*this);
	++(*this);
	return ret;
}

RCBString& RCBString::operator--()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pos > 0);
	pos--;
	len++;
	return *this;
}

RCBString RCBString::operator--(int)
{
	RCBString ret(*this);
	--(*this);
	return ret;
}

char& RCBString::operator[](size_t pos) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pos < len);		// Out of RCBString. Note: val is not ended by '\0'.
	return val[this->pos + pos];
}

RCBString& RCBString::operator+=(ushort pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((int)len - pos >= 0);
	this->pos = this->pos + (ushort)pos;
	this->len -= pos;
	return *this;
}

RCBString RCBString::operator+(ushort pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((int)len - pos >= 0);
	return RCBString(this->val + pos, len - pos);
}

bool RCBString::Like(const RCBString& pattern, char escape_character)
{
	if(pattern.IsEmpty())
		return this->IsEmpty();
	RCBString processed_pattern;		// to be used as an alternative source in case of processed pattern (escape chars)
	RCBString processed_wildcards;
	char *p = pattern.val;				// a short for pattern (or processed pattern)
	char *w = pattern.val;				// a short for wildcard map (or na original pattern, if no escape chars)
	char *v = val + pos;				// a short for the data itself
	uint pattern_len = pattern.len;

	// Escape characters processing
	bool escaped = false;
	for(uint i = 0; i < pattern_len - 1; i++) {
		if(p[i] == escape_character && (p[i + 1] == escape_character || p[i + 1] == '_' || p[i + 1] == '%')) {
			escaped = true;
			break;
		}
	}
	if(escaped) {						// redefine the pattern by processing escape characters
		processed_pattern = RCBString(NULL, pattern_len, true);
		processed_wildcards = RCBString(NULL, pattern_len, true);
		uint i = 0;						// position of the processed pattern
		uint j = 0;						// position of the original pattern
		while(j < pattern_len) {
			if(j < pattern_len - 1 && p[j] == escape_character && (p[j + 1] == escape_character || p[j + 1] == '_' || p[j + 1] == '%')) {
				j++;
				processed_pattern[i] = p[j];
				processed_wildcards[i] = ' ';
				j++;
				i++;
			} else {
				processed_pattern[i] = p[j];
				if(p[j] == '_' || p[j] == '%')
					processed_wildcards[i] = p[j];		// copy only wildcards
				else
					processed_wildcards[i] = ' ';
				j++;
				i++;
			}
		}
		pattern_len = i;				// the rest of pattern buffers are just ignored
		p = processed_pattern.val;
		w = processed_wildcards.val;
	}

	// Pattern processing
	bool was_wild = false;				// are we in "after %" mode?
	uint cur_p = 0,  cur_p_beg = 0;		// pattern positions
	uint cur_s = 0,  cur_s_beg = 0;		// source positions

	do {
		while(cur_p < pattern_len && w[cur_p] == '%') {		// first omit all %
			was_wild = true;
			cur_p++;
		}
		cur_s_beg = cur_s;
		cur_p_beg = cur_p;
		do {								// internal loop: try to match a part between %...%
			while(cur_p < pattern_len && cur_s < len &&				// find the first match...
				  (v[cur_s] == p[cur_p] || w[cur_p] == '_') &&
				  w[cur_p] != '%' ) {
				cur_s++;
				cur_p++;
			}
			if(cur_s < len && ((cur_p < pattern_len && w[cur_p] != '%') || cur_p >= pattern_len)) {		// not matching (loop finished prematurely) - try the next source position
				if(!was_wild) 
					return false;	// no % up to now => the first non-matching is critical
				cur_p = cur_p_beg;
				cur_s = ++cur_s_beg;			// step forward in the source, rewind the matching pointers
			}
			if(cur_s == len) {	 	// end of the source
				while(cur_p < pattern_len) {
					if(w[cur_p] != '%')			// Pattern nontrivial yet? No more chances for matching.
						return false;
					cur_p++;
				}
				return true;
			}
		} while(cur_p < pattern_len && w[cur_p] != '%');		// try the next match position
	} while(cur_p < pattern_len && cur_s < len);
	return true;
}

RCBString& RCBString::MakePersistent(bool _zero_term)
{
	if(!persistent || this->zero_term != (_zero_term ? 1 : 0)) {
		this->zero_term = (_zero_term ? 1 : 0);
		int tmp_len = len + pos + zero_term;
		char* n_val = new char [tmp_len];
		memcpy(n_val, val, len + pos);
		if(persistent) {
			delete [] val;
			val = 0;
		}
		if(zero_term)
			n_val[len + pos] = 0;
		val = n_val;
		persistent = true;
	}
	return *this;
}

bool RCBString::GreaterEqThanMin(_int64 txt_min)
{
	if(null == true)
		return false;
	unsigned char *min = (unsigned char *)(&txt_min);
	uint min_len = 8;
	while(min_len > 0 && min[min_len - 1] == '\0') 
		min_len--;
	for(uint i = 0; i < min_len && i < len; i++)
		if(((unsigned char*)val)[i + pos] < min[i])
			return false;
		else if(((unsigned char*)val)[i + pos] > min[i])
			return true;
	if(len < min_len)
		return false;
	return true;
}

bool RCBString::GreaterEqThanMinUTF(_int64 txt_min, DTCollation col, bool use_full_len)
{
#ifndef PURE_LIBRARY
	if(null == true)
		return false;
	if(RequiresUTFConversions(col)) {
		uint useful_len = 0;
		uint min_byte_len = memchr((char*)(&txt_min), 0, 8) ? (uint)strlen((char*)&txt_min) : 8;
		if(!use_full_len) {
			uint min_charlen = uint(col.collation->cset->numchars(col.collation,(char*)(&txt_min), (char*)(&txt_min)+min_byte_len));

			int next_char_len, chars_included = 0;
			while(true) {
				if(useful_len >= len || chars_included == min_charlen)
				  break;
				next_char_len = col.collation->cset->mbcharlen(col.collation, (uchar)val[useful_len+pos]);
				assert("wide character unrecognized" && next_char_len > 0);
				useful_len += next_char_len;
				chars_included++;
			}
		} else
			useful_len = len;
		return col.collation->coll->strnncoll(col.collation, (uchar*)val, useful_len, (uchar*) &txt_min, min_byte_len, 0) >= 0;
	} else
		return GreaterEqThanMin(txt_min);
#else
	return GreaterEqThanMin(txt_min);
#endif
}

bool RCBString::LessEqThanMax(_int64 txt_max)
{
	if(null == true)
		return false;
	unsigned char *max = (unsigned char *)(&txt_max);
	for(uint i=0;i<8 && i<len;i++)
		if(((unsigned char*)val)[i+pos]>max[i])
			return false;
		else if(((unsigned char*)val)[i+pos]<max[i])
			return true;
	return true;
}

bool RCBString::LessEqThanMaxUTF(_int64 txt_max, DTCollation col, bool use_full_len)
{
#ifndef PURE_LIBRARY
	if(null == true)
		return false;
	if(RequiresUTFConversions(col)) {
		uint useful_len = 0;
		uint max_byte_len = memchr((char*)(&txt_max), 0, 8) ? (uint)strlen((char*)&txt_max) : (uint)8;
		if(!use_full_len) {

			uint max_charlen = uint(col.collation->cset->numchars(col.collation,(char*)(&txt_max), (char*)(&txt_max)+max_byte_len));

			int next_char_len, chars_included = 0;
			while(true) {
				if(useful_len >= len || chars_included == max_charlen)
					break;
				next_char_len = col.collation->cset->mbcharlen(col.collation, (uchar)val[useful_len+pos]);
				assert("wide character unrecognized" && next_char_len > 0);
				useful_len += next_char_len;
				chars_included++;
			}
		} else
			useful_len = len;
		return col.collation->coll->strnncoll(col.collation, (uchar*)val, useful_len, (uchar*) &txt_max, max_byte_len, 0) <= 0;
	} else
		return LessEqThanMax(txt_max);
#else
	return LessEqThanMax(txt_max);
#endif
}

bool RCBString::IsEmpty() const
{
	if(null == true)
		return false;
	return len == 0 ? true : false;
}

bool RCBString::IsNullOrEmpty() const
{
	return ((len == 0 || null) ? true : false);
}

size_t RCBString::size() const
{
	return (size_t)len;
}

bool RCBString::operator==(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == STRING_TYPE)
		return strcmp(*this, (RCBString&)rcdt) == 0;
	return strcmp(*this, rcdt.ToRCString()) == 0;
}

bool RCBString::operator==(const RCBString& rcs) const
{
	if(null || rcs.IsNull())
		return false;
	return strcmp(*this, rcs) == 0;
}

bool RCBString::operator<(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == STRING_TYPE)
		return strcmp(*this, (RCBString&)rcdt) < 0;
	return strcmp(*this, rcdt.ToRCString()) < 0;
}

bool RCBString::operator>(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == STRING_TYPE)
		return strcmp(*this, (RCBString&)rcdt) > 0;
	return strcmp(*this, rcdt.ToRCString()) > 0;
}

//bool RCBString::Greater(const RCDataType& rcdt, DTCollation collation) const
//{
//	if(null || rcdt.IsNull())
//		return false;
//	if(rcdt.GetValueType() == STRING_TYPE)
//		return strcmp(*this, (RCBString&)rcdt, collation.collation) > 0;
//	BHERROR("bad cast");
//	return false;
//}

bool RCBString::operator>=(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == STRING_TYPE)
		return strcmp(*this, (RCBString&)rcdt) >= 0;
	return strcmp(*this, rcdt.ToRCString()) >= 0;
}

bool RCBString::operator<=(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return false;
	if(rcdt.GetValueType() == STRING_TYPE)
		return strcmp(*this, (RCBString&)rcdt) <= 0;
	return strcmp(*this, rcdt.ToRCString()) <= 0;
}

bool RCBString::operator!=(const RCDataType& rcdt) const
{
	if(null || rcdt.IsNull())
		return true;
	if(rcdt.GetValueType() == STRING_TYPE)
		return strcmp(*this, (RCBString&)rcdt) != 0;
	return strcmp(*this, rcdt.ToRCString()) != 0;
}

uint RCBString::GetHashCode() const
{
	if(null)
		return 0;
	uint hc = 0;
	int a = 1040021;
	for(uint i = 0; i < len; i++)
		hc = (hc*a + val[i])&1048575;
	return hc;
}

size_t RCBString::GetStorageByteSize() const
{
	return sizeof(null) + (null ? 0 : sizeof(zero_term) + sizeof(len) + sizeof(pos) + (len + pos));
}

void RCBString::ToByteStream(char*& buf) const
{
	store_bytes(buf, null);
	if(!null) {
		store_bytes(buf, zero_term);
		store_bytes(buf, pos);
		this->PutVarchar(buf, 4, true);
	}
}

void RCBString::AssignFromByteStream(char*& buf)
{
	if(persistent)
		delete [] val;
	len = 0;
	pos = 0;
	val = 0;
	zero_term = 0;
	persistent = true;
	unstore_bytes(null, buf);
	if(null) {
		zero_term = 0;
	} else 	{
		unstore_bytes(zero_term, buf);
		unstore_bytes(pos, buf);
		unstore_bytes(len, buf);
		val = new char [len + pos + zero_term];
		memcpy(val, buf, len + pos);
		buf += len;
		if(zero_term)
			val[len + pos] = 0;
	}
}

RCBString& RCBString::RightTrim(RCBString& str)
{
	int last = str.len - 1;
	while(last >= 0 && str.val[last--] == ' ')
		--str.len;
	if(str.zero_term)
		str.val[str.len] = 0;
	return str;
}

std::ostream& operator<<(std::ostream &out, const RCBString& rcbs)
{
	out.write(rcbs.val + rcbs.pos, rcbs.len);
	return out;
}

std::size_t strlen(const RCBString& rcbs)
{
	return rcbs.len;
}

void strcpy(char* dest, const RCBString& rcbs)
{
	memcpy(dest, rcbs.val+rcbs.pos, rcbs.len);
	dest[rcbs.len] = 0;
}

void strcpy(uchar* dest, const RCBString& rcbs)
{
	memcpy(dest, (uchar*)rcbs.val+rcbs.pos, rcbs.len);
	dest[rcbs.len] = 0;
}

// TODO: finish this function, peristent, zero_term, etc.
void strcpy(RCBString& dest, const RCBString& rcbs)
{
	dest = rcbs;
}

void strncpy(char* dest, const RCBString& rcbs, unsigned count)
{
	strncpy((uchar*)dest, rcbs, count);
}

void strncpy(uchar* dest, const RCBString& rcbs, unsigned count)
{
	uint len =  (rcbs.len - rcbs.pos) < count ? (rcbs.len - rcbs.pos) : count;
	memcpy(dest, (uchar*)(rcbs.val + rcbs.pos), len);
	if(len <= count)
		memset(dest + len, 0, count - len);
}

int strcmp(const RCBString& rcbs1, const RCBString& rcbs2)
{
	int ret = 0;
	int len = min(rcbs1.len, rcbs2.len);
	if(len == 0) {
		if(rcbs1.len == 0 && rcbs2.len == 0)
			ret = 0;
		else if(rcbs1.len == 0)
			ret = -1;
		else
			ret = 1;
	} else if(rcbs1.len != rcbs2.len) {
		ret = memcmp(rcbs1.val + rcbs1.pos, rcbs2.val + rcbs2.pos, len);
		if(ret == 0)
			if(rcbs1.len < rcbs2.len)
				return -1;
			else
				return 1;
	} else
		ret = memcmp(rcbs1.val + rcbs1.pos, rcbs2.val + rcbs2.pos, len);

	return ret;
}

//int strcmp(const RCBString& rcbs1, const RCBString& rcbs2, CHARSET_INFO* charset)
//{
//	int ret = 0;
//	int len = max(rcbs1.len, rcbs2.len);
//	if(len == 0) {
//		if(rcbs1.len == 0 && rcbs2.len == 0)
//			ret = 0;
//		else if(rcbs1.len == 0)
//			ret = -1;
//		else
//			ret = 1;
//		return ret;
//	} 
//	
//	int buf_len = len * charset->strxfrm_multiply;
//	uchar* buf1 = new uchar[buf_len];
//	uchar* buf2 = new uchar[buf_len];
//	charset->coll->strnxfrm(charset, buf1, buf_len, (uchar*)(rcbs1.val + rcbs1.pos), len);
//	charset->coll->strnxfrm(charset, buf2, buf_len, (uchar*)(rcbs2.val + rcbs2.pos), len);
//	ret = memcmp(buf1, buf2, buf_len);
//	delete [] buf1;
//	delete [] buf2;
//	if(rcbs1.len != rcbs2.len) {
//		if(ret == 0)
//			if(rcbs1.len < rcbs2.len)
//				return -1;
//			else
//				return 1;
//	} 
//
//	return ret;
//}

int stricmp(const RCBString& rcbs1, const RCBString& rcbs2)
{
	int ret = 0;
	int len = min(rcbs1.len, rcbs2.len);
	if(len == 0) {
		if(rcbs1.len == 0 && rcbs2.len == 0)
			ret = 0;
		else if(rcbs1.len == 0)
			ret = -1;
		else
			ret = 1;
	} else if(rcbs1.len != rcbs2.len) {
		ret = strncasecmp(rcbs1.val+rcbs1.pos, rcbs2.val+rcbs2.pos, len);
		if(ret == 0)
			if(rcbs1.len < rcbs2.len)
				return -1;
			else
				return 1;
	} else
		ret = strncasecmp(rcbs1.val+rcbs1.pos, rcbs2.val+rcbs2.pos, len);

	return ret;
}

bool operator!=(const RCBString& rcbs1, const RCBString& rcbs2)
{
	if(rcbs1.IsNull() || rcbs2.IsNull())
		return true;
	return strcmp(rcbs1, rcbs2) != 0;
}

////////////////////////////////////////////////////////////////////////////////////

TextStat::TextStat()
{
	chars_found = NULL;
	encode_table = NULL;
	Reset();
}

TextStat::TextStat(TextStat &sec)
{
	encode_table = new uchar [256 * 48 * sizeof(uchar)];
	memcpy(encode_table, sec.encode_table, 256 * 48 * sizeof(uchar));
	chars_found = new uchar [256 * 48 * sizeof(uchar)];
	memcpy(chars_found, sec.chars_found, 256 * 48 * sizeof(uchar));
	for(int i = 0; i < 48; i++)
		len_table[i] = sec.len_table[i];
	max_string_size = sec.max_string_size;
	chars_found_for_decoding = sec.chars_found_for_decoding;
	max_code = sec.max_code;
	valid = sec.valid;
}

TextStat::~TextStat(void)
{
	delete [] encode_table;
	delete [] chars_found;
}

void TextStat::Reset()
{
	delete [] encode_table;
	encode_table = new uchar [256 * 48 * sizeof(uchar)];
	memset(encode_table, 255, 256 * 48 * sizeof(uchar));
	memset(len_table, 0, 48 * sizeof(int));
	max_string_size = 0;
	delete [] chars_found;
	chars_found = new uchar [256 * 48 * sizeof(uchar)];
	memset(chars_found, 0, 256 * 48 * sizeof(uchar));
	chars_found_for_decoding = false;
	max_code = 0;
	valid = true;
}

bool TextStat::AddString(const RCBString& rcbs)
{
	int len = int(rcbs.size());
	if(len > 48) {
		valid = false;
		return false;
	}
	for(int i = 0; i < len; i++) {
		if(rcbs[i] == 0) {
			valid = false;
			return false;
		}
		chars_found[256 * i + uchar(rcbs[i])] = 1;
	}
	if(len < 48)
		chars_found[256 * len] = 1;			// value of len n puts 0 on position n (starting with 0)
	if(len > max_string_size)
		max_string_size = len;
	return true;
}

bool TextStat::AddChar(uchar v, int pos)	// return false if out of range
{
	if(pos >= 48 || v == 0) {
		valid = false;
		return false;
	}
	chars_found[256 * pos + v] = 1;
	if(pos + 1 > max_string_size)
		max_string_size = pos + 1;
	return true;
}

void TextStat::AddLen(int pos)				// value of len n puts 0 on position n (starting with 0)
{
	if(pos == 48)
		return;
	assert(pos < 48);
	chars_found[256 * pos] = 1;
	if(pos > max_string_size)
		max_string_size = pos;
}

bool TextStat::CheckIfCreatePossible()		// return false if cannot create encoding (too wide), do not actually create anything
{
	if(chars_found_for_decoding) {
		valid = false;
		return false;
	}
	int total_len = 0;
	for(int pos = 0; pos < max_string_size; pos++) {
		int loc_len_table = 0;
		for(int i = 0; i < 256; i++) {
			if(chars_found[256 * pos + i] == 1) {
				if(loc_len_table == 255) {
					valid = false;
					return false;			// too many characters
				}
				loc_len_table++;
			}
		}
		if(loc_len_table > 1)
			total_len += GetBitLen(uint(loc_len_table) - 1);
		if(total_len > 63) {
			valid = false;
			return false;
		}
	}
	return true;
}

bool TextStat::CreateEncoding()
{
	if(chars_found_for_decoding) {			// chars_found is already used as a decoding structure - cannot create decoding twice with a decoding in between
		valid = false;
		return false;
	}
	int total_len = 0;
	max_code = 0;
	for(int pos = 0; pos < max_string_size; pos++) {
		len_table[pos] = 0;
		for(int i = 0; i < 256; i++) {
			if(chars_found[256 * pos + i] == 1) {
				encode_table[i + 256 * pos] = len_table[pos];			// Encoding a string: Code(character c) = encode_table[c][pos]
				// Note: initial value of 255 means "illegal character", so we must not allow to use such code value
				if(len_table[pos] == 255) {
					valid = false;
					return false;			// too many characters
				}
				len_table[pos]++;
			}
		}
		// translate lengths into binary sizes
		if(len_table[pos] == 1)
			len_table[pos] = 0;
		else if(len_table[pos] > 1) {
			uint max_charcode = len_table[pos] - 1;
			len_table[pos] = GetBitLen(max_charcode);
			max_code <<= len_table[pos];
			max_code += max_charcode;
		}
		total_len += len_table[pos];
		if(total_len > 63) {
			valid = false;
			return false;
		}
	}
	return true;
}

_int64 TextStat::Encode(const RCBString& rcbs, bool round_up)	// round_up = true => fill the unused characters by max codes
{
	_int64 res = 0;
	int len = int(rcbs.size());
	if(len > 48)
		return NULL_VALUE_64;
	int charcode;
	for(int i = 0; i < len; i++) {					
		if(rcbs[i] == 0)							// special cases, for encoding DPNs
			charcode = 0;
		else if(uchar(rcbs[i]) == 255 && encode_table[256 * i + 255] == 255)
			charcode = (1 << len_table[i]) - 1;
		else {
			charcode = encode_table[256 * i + uchar(rcbs[i])];
			if(charcode == 255)
				return NULL_VALUE_64;
		}
		res <<= len_table[i];
		res += charcode;
	}
	if(!round_up) {
		if(len < max_string_size) {
			charcode = encode_table[256 * len];			// add 0 on the end
			if(charcode == 255)
				return NULL_VALUE_64;
			if(len_table[len] > 0) {
				res <<= len_table[len];
				res += charcode;
			}
		}
		for(int i = len + 1; i < max_string_size; i++)	// ignore the rest of string
			if(len_table[i] > 0)
				res <<= len_table[i];

	} else {
		for(int i = len; i < max_string_size; i++)		// fill the rest with maximal values
			if(len_table[i] > 0) {
				res <<= len_table[i];
				res += int((1 << len_table[i]) - 1);
			}
	}
	return res;
}

RCBString TextStat::Decode(_int64 code)
{
	if(!chars_found_for_decoding) {
		// redefine chars_found as decoding dictionary
		chars_found_for_decoding = true;
		for(int i = 0; i < max_string_size; i++) {
			chars_found[256 * i] = 0;
			for(int j = 0; j < 256; j++)
				if(encode_table[j + 256 * i] != 255)
					chars_found[encode_table[j + 256 * i] + 256 * i] = j;
		}
	}
	int charcode;
	int len = max_string_size;
	static int const TMP_BUFFER_SIZE = 49;
	char buf_val[TMP_BUFFER_SIZE];
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(max_string_size < TMP_BUFFER_SIZE);
	buf_val[max_string_size] = 0;
	for(int i = max_string_size - 1; i >= 0; i--) {
		charcode = 0;
		if(len_table[i] > 0) {
			charcode = int(code & int((1 << len_table[i]) - 1));		// take last len_table[i] bits
			code >>= len_table[i];
		}
		buf_val[i] = chars_found[charcode + 256 * i];
		if(buf_val[i] == 0)										// finding the first 0
			len = i;
	}
	return RCBString(buf_val, len, true);			// materialized copy of the value
}

//////////////////////////////////////////////////////////////////////////////////////////////////////

#ifndef PURE_LIBRARY
void ConvertToBinaryForm(VirtualColumn* vc, const MIIterator& mit, RCBString& buf, DTCollation coll)
{
	if(!vc->IsNull(mit)) {
		RCBString s;
		vc->GetNotNullValueString(s, mit);
		coll.collation->coll->strnxfrm(coll.collation, (uchar*)buf.val, buf.len, (uchar*)(s.val + s.pos), s.len);
	} else
		buf.null = true;
}
#endif
