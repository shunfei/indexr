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

#ifndef _TYPES_RCDATATYPES_H_
#define _TYPES_RCDATATYPES_H_

#include <math.h>
#include <boost/utility.hpp>
#include <boost/shared_ptr.hpp>

#ifdef __GNUC__
#include <ext/hash_set>
#else
#include <hash_set>
#define llabs(x)  _abs64(x)
#pragma warning (disable:4290) // disable VC++ warning that it ignores "throw" in function declaration
#endif

#include "common/CommonDefinitions.h"
#include "compress/tools.h"
#include "core/bintools.h"
#include "system/RCException.h"
#include "core/RCAttrTypeInfo.h"
#include "system/TextUtils.h"
#include "common/bhassert.h"

class RCBString;
class RCNum;
class ValueOrNull;
class VirtualColumn;
class MIIterator;

bool AreComparable(AttributeType att1, AttributeType att2);

inline _int64 DateSortEncoding(_int64 v)
{
	// WARNING: it is based on RCDateTime::DT structure!
	if(v != MINUS_INF_64 && v != PLUS_INF_64)
		return (v >> 28);		// omit sec, min, hour (6+6+16 bits)
	return v;
}

inline _int64 DateSortDecoding(_int64 v)
{
	// WARNING: it is based on RCDateTime::DT structure!
	if(v != MINUS_INF_64 && v != PLUS_INF_64)
		return (v << 28);		// omit sec, min, hour (6+6+16 bits)
	return v;
}

inline _int64 YearSortEncoding(_int64 v)
{
	// WARNING: it is based on RCDateTime::DT structure!
	if(v != MINUS_INF_64 && v != PLUS_INF_64)
		return (v >> 37);		// omit sec, min, hour, day, month (6+6+16+5+4 bits)
	return v;
}

inline _int64 YearSortDecoding(_int64 v)
{
	// WARNING: it is based on RCDateTime::DT structure!
	if(v != MINUS_INF_64 && v != PLUS_INF_64)
		return (v << 37);		// omit sec, min, hour, day, month (6+6+16+5+4 bits)
	return v;
}

enum ValueTypeEnum { NULL_TYPE, DATE_TIME_TYPE, NUMERIC_TYPE, STRING_TYPE };

class RCDataType
{
public:
	RCDataType() : null(true)	{}
	virtual ~RCDataType();

public:
	virtual RCDataType& operator=(const RCDataType& rcn) = 0;
	virtual std::auto_ptr<RCDataType> Clone() const = 0;
//	virtual std::auto_ptr<ValueOrNull> ToValueOrNullAPtr() const = 0;
	virtual RCBString ToRCString() const = 0;

	virtual AttributeType	Type() const = 0;
	virtual ValueTypeEnum 	GetValueType()	const = 0;

	bool AreComperable(const RCDataType&) const;
	bool compare(const RCDataType& rcdt, Operator op, char like_esc) const;

	virtual bool operator==(const RCDataType& rcdt) const = 0;
	virtual bool operator<(const RCDataType& rcdt) const = 0;
	virtual bool operator>(const RCDataType& rcdt) const = 0;
	virtual bool operator>=(const RCDataType& rcdt) const = 0;
	virtual bool operator<=(const RCDataType& rcdt) const = 0;
	virtual bool operator!=(const RCDataType& rcdt) const = 0;

	bool 			IsNull() const	{ return null; }
	virtual uint	GetHashCode() const = 0;

	virtual char*	GetDataBytesPointer() const = 0;
	virtual size_t	GetStorageByteSize() const = 0;
	virtual void	ToByteStream(char* &buf) const = 0;
	virtual void	AssignFromByteStream(char* &buf) = 0;

	void			SetToNull() { null = true; }

	typedef size_t size_type;
//protected:
	bool null;

public:
	static ValueTypeEnum GetValueType(AttributeType attr_type);
	static bool	ToDecimal(const RCDataType& in, int scale, RCNum& out);
	static bool	ToInt(const RCDataType& in, RCNum& out);
	static bool	ToReal(const RCDataType& in, RCNum& out);

	static bool	AreComperable(const RCDataType& rcdt1, const RCDataType& rcdt2);
	static bool	compare(const RCDataType& rcdt1, const RCDataType& rcdt2, Operator op, char like_esc);
};

template<typename T>
class ValueBasic : public RCDataType
{
public:
	virtual ValueTypeEnum GetValueType() const 		{ return  T::value_type; }
	virtual std::auto_ptr<RCDataType> Clone() const { return std::auto_ptr<RCDataType>(new T((T&)*this)); };
//	virtual std::auto_ptr<ValueOrNull> ToValueOrNullAPtr() const;

	static T null_value;
	static T& NullValue() { return T::null_value; }
};

template<typename T>
T ValueBasic<T>::null_value;

typedef boost::shared_ptr<RCDataType> RCDataTypePtr;

class RCBString : public ValueBasic<RCBString>
{

	friend std::ostream& operator<<(std::ostream &out, const RCBString& rcbs);
	friend void strcpy(char* dest, const RCBString& rcbs);
	friend void strcpy(RCBString& dest, const RCBString& rcbs);
	friend void strcpy(uchar* dest, const RCBString& rcbs);
	friend void strncpy(char* dest, const RCBString& rcbs, unsigned count);
	friend void strncpy(uchar* dest, const RCBString& rcbs, unsigned count);
	friend size_t strlen(const RCBString& rcbs);
	friend bool operator!=(const RCBString& rcbs1, const RCBString& rcbs2);
public:

	RCBString();
	RCBString(const char *val, int len = 0, bool materialize = false, bool _zero_term = false);
	// len == -1 or -2  => the length is stored on the first 2 or 4 (respectively, ushort / int) bytes of val.
	// len == 0  => the length is a result of strlen(val), i.e. val is 0-terminated
	// zero-term = true  => this is a non-null empty string, or a longer zero-terminated string
	//RCBString(char *buffer, const char *val, int len = 0, bool zero_term=false);
	RCBString(const RCBString& rcbs);
	~RCBString();

	RCBString& operator=(const RCBString& rcbs);
	RCBString& operator=(const RCDataType& rcn);
	void PersistentCopy(const RCBString& rcbs);				// like "=", but makes this persistent

	static bool Parse(RCBString& in, RCBString& out);
	AttributeType Type() const;

	void PutString(char* &dest, ushort len, bool move_ptr = true) const;
	void PutVarchar(char* &dest, uchar prefixlen = 2, bool move_ptr = true) const;
	void Put(char* &dest, ushort len = 0) const;
	void Get(char* src);
	RCBString& MakePersistent(bool _zero_term = false);
	bool IsPersistent() const  {return persistent;}

	bool IsEmpty() const;				//return true if this is 0 len string, if this is null this function will return false
	bool IsNullOrEmpty() const;	//return true if this is null or this is 0 len string
	bool IsZeroTerm() const { return ( zero_term ); }

	operator std::string() const;
	RCBString ToRCString() const	{return *this;}

	char& operator*();
	char* Value() const	{return val + pos;};
	char* GetDataBytesPointer() const { return Value(); }
	char* begin(void) const { return Value(); }
	char* end(void) const { return begin() + len; }

	RCBString& operator++();
	RCBString operator++(int);
	RCBString& operator--();
	RCBString operator--(int);
	RCBString& operator+=(ushort pos);
	RCBString operator+(ushort pos);

	bool operator==(const RCDataType& rcn)const;
	bool operator<(const RCDataType& rcn) const;
	bool operator>(const RCDataType& rcn) const;
	bool Greater(const RCDataType& rcdt, DTCollation collation) const;
	bool operator>=(const RCDataType& rcn) const;
	bool operator<=(const RCDataType& rcn) const;
	bool operator!=(const RCDataType& rcn) const;

	bool operator==(const RCBString& rcs) const;

	bool Like(const RCBString& pattern, char escape_character);		// Wildcards: "_" is any character, "%" is 0 or more characters

	bool GreaterEqThanMin(_int64 txt_min);			// These functions work on _int64 treated as special 8-byte string (min/max on pack)
	bool LessEqThanMax(_int64 txt_max);			// txt_min stops at the first nonzero character (from the end), txt_max is treated as filled by 0xFF to infinity.

	bool GreaterEqThanMinUTF(_int64 txt_min, DTCollation col, bool use_full_len = false);
	bool LessEqThanMaxUTF(_int64 txt_max, DTCollation col, bool use_full_len = false);

	uint GetHashCode() const;
	size_t size() const;
	char& operator[](size_t pos) const;
	typedef size_t size_type;

	virtual size_t GetStorageByteSize() const;
	virtual void ToByteStream(char*& buf) const;
	virtual void AssignFromByteStream(char*& buf);


	char *val;
	uint len;
	uint pos;
private:
	bool persistent;
	uint zero_term;			// 1 - the string is zero-terminated, 0 otherwise

public:
	static RCBString& RightTrim(RCBString& str);
public:
	const static ValueTypeEnum value_type = STRING_TYPE;

};

typedef boost::shared_ptr<RCBString> RCBStringPtr;

int strcmp(const RCBString& rcbs1, const RCBString& rcbs2);
//int strcmp(const RCBString& rcbs1, const RCBString& rcbs2, CHARSET_INFO* charset);
int stricmp(const RCBString& rcbs1, const RCBString& rcbs2);
int strcmp(const RCBString& rcbs1, wchar_t const rcbs2[], std::locale const &loc = std::locale());
void strncpy(char* dest, const RCBString& rcbs, unsigned count);
void strncpy(uchar* dest, const RCBString& rcbs, unsigned count);

class RCDateTime : public ValueBasic<RCDateTime>
{
	friend class ValueParserForText;
public:

	RCDateTime(_int64 dt, AttributeType at);
	RCDateTime(short year = NULL_VALUE_SH);
	RCDateTime(short year, short month, short day, short hour, short minute, short second, AttributeType at); // DataTime , Timestamp
	RCDateTime(short yh, short mm, short ds, AttributeType at); //Date or Time
	RCDateTime(const RCDateTime& rcdt);

	RCDateTime(RCNum& rcn, AttributeType at);// throw(DataTypeConversionRCException);
	~RCDateTime();

public:
	RCDateTime& operator=(const RCDateTime& rcdt);
	RCDateTime& operator=(const RCDataType& rcdt);
	RCDateTime& Assign(_int64 v, AttributeType at);

	bool 			IsNegative() const { return Sign() == -1; }
	bool			IsZero() const;
	_int64			GetInt64() const;
	bool			GetInt64(_int64& value) const	{ value = GetInt64(); return true; };

	/** Convert RCDateTime to 64 bit integer in the following format:
	 * YEAR: 				YYYY
	 * TIME:				(+/-)HHH:MM:SS
	 * Date: 				YYYYMMDD
	 * DATETIME/TIMESTAM:	YYYYMMDDHHMMSS
	 * \param value result of the conversion
	 * \return false if it is NULL, true otherwise
	 */
	bool			ToInt64(_int64& value) const;
	char*			GetDataBytesPointer() const 	{ return (char*)&dt; }
	RCBString		ToRCString() const;
	AttributeType	Type() const;
	uint			GetHashCode() const;

	bool operator==(const RCDataType& rcdt) const;
	bool operator<(const RCDataType& rcdt) const;
	bool operator>(const RCDataType& rcdt) const;
	bool operator>=(const RCDataType& rcdt) const;
	bool operator<=(const RCDataType& rcdt) const;
	bool operator!=(const RCDataType& rcdt) const;
	_int64 operator-(const RCDateTime& sec) const;			// difference in days, only for RC_DATE

	short Year() const		{ _int64 v = llabs(*(_int64*)(&dt)); return (short)(*(DT*)&v).year * Sign(); }
	short Month() const		{ _int64 v = llabs(*(_int64*)(&dt)); return (short)(*(DT*)&v).month * Sign(); }
	short Day()	const		{ _int64 v = llabs(*(_int64*)(&dt)); return (short)(*(DT*)&v).day * Sign(); }
	short Hour() const		{ _int64 v = llabs(*(_int64*)(&dt)); return (short)(*(DT*)&v).hour; }
	short Minute() const	{ _int64 v = llabs(*(_int64*)(&dt)); return (short)(*(DT*)&v).minute; }
	short Second() const	{ _int64 v = llabs(*(_int64*)(&dt)); return (short)(*(DT*)&v).second; }

	_int64 ToDays();										// the number of days since year 0
	_int64 DayOfYear();										// the number of days since beg. of year
	void	Negate() { *(_int64*)&dt = *(_int64*)&dt * -1; }

	virtual size_t GetStorageByteSize() const;
	virtual void ToByteStream(char*& buf) const;
	virtual void AssignFromByteStream(char*& buf);

	void ShiftOfPeriod(short sign_, short minutes_);

private:
	struct DT{
		_uint64 second	: 6;
		_uint64 minute	: 6;
		_uint64 hour	: 16;
		_uint64 day		: 5;
		_uint64 month	: 4;
		_uint64 year	: 14;
		_uint64 unused	: 13;
	} dt;
	AttributeType at;

private:
	char	Sign() const { return *(_int64*)(&dt) < 0 ? -1 : 1; }
	int		compare(const RCDateTime& rcdt) const;
	int		compare(const RCNum& rcdt) const;

public:
	static void AdjustTimezone(RCDateTime& dt);
	static BHReturnCode Parse(const RCBString&, RCDateTime&, AttributeType);
	static BHReturnCode Parse(const _int64&, RCDateTime&, AttributeType, int precision = -1);

	static bool CanBeYear(_int64 year);
	static bool CanBeMonth(_int64 month);
	static bool CanBeDay(_int64 day);
	static bool CanBeHour(_int64 hour);
	static bool CanBeMinute(_int64 minute);
	static bool CanBeSecond(_int64 second);

	static bool CanBeDate(_int64 year, _int64 month, _int64 day);
	static bool CanBeTime(_int64 hour, _int64 minute, _int64 second);
	static bool CanBeTimestamp(_int64 year, _int64 month, _int64 day, _int64 hour, _int64 minute, _int64 second);
	static bool CanBeDatetime(_int64 year, _int64 month, _int64 day, _int64 hour, _int64 minute, _int64 second);

	static bool IsLeapYear(short year);// throw(DataTypeConversionRCException);
	static ushort NoDaysInMonth(short year, ushort month);

	static bool IsCorrectBHYear(short year);
	static bool IsCorrectBHDate(short year, short month, short day);
	static bool IsCorrectBHTime(short hour, short minute, short second);
	static bool IsCorrectBHTimestamp(short year, short month, short day, short hour, short minute, short second);
	static bool IsCorrectBHDatetime(short year, short month, short day, short hour, short minute, short second);

	static short ToCorrectYear(uint v, AttributeType at, bool is_year_2 = false);
	static RCDateTime GetSpecialValue(AttributeType at);
	static RCDateTime GetCurrent();	

	static _int64 TruncateInt64ToDate(const _int64& v) { return !((1 << 28) - 1) & v; }
	static _int64 TruncateInt64ToTime(const _int64& v) { return ((1 << 28) - 1) & v; }

public:
	const static ValueTypeEnum value_type = DATE_TIME_TYPE;
};

class RCValueObject //: public RCDataType
{
	//friend class RCDataType;
public:
	RCValueObject();
	RCValueObject(const RCValueObject& rcvo);
	RCValueObject(const RCDataType& rcvo);


	~RCValueObject();
	RCValueObject& operator=(const RCValueObject& rcvo);
//	RCDataType& operator=(const RCDataType& rcn);
//	std::auto_ptr<RCDataType> Clone() const;
//	std::auto_ptr<ValueOrNull> ToValueOrNullAPtr() const;

	bool compare(const RCValueObject& rcvo, Operator op, char like_esc) const;

	bool operator==(const RCValueObject& rcvo)const;
	bool operator<(const RCValueObject& rcvo) const;
	bool operator>(const RCValueObject& rcvo) const;
	bool operator>=(const RCValueObject& rcvo) const;
	bool operator<=(const RCValueObject& rcvo) const;
	bool operator!=(const RCValueObject& rcvo) const;

	bool operator==(const RCDataType& rcdt) const;
	bool operator<(const RCDataType& rcdt) const;
	bool operator>(const RCDataType& rcdt) const;
	bool operator>=(const RCDataType& rcdt) const;
	bool operator<=(const RCDataType& rcdt) const;
	bool operator!=(const RCDataType& rcdt) const;

	bool IsNull() const;

	AttributeType	Type() const 			{ return value.get() ? value->Type() : RC_UNKNOWN; }
	ValueTypeEnum 	GetValueType() const	{ return value.get() ? value->GetValueType() : NULL_TYPE; }

	RCBString ToRCString() const;
	//operator RCDataType*()		{ return value.get(); }
	RCDataType* Get() const 		{ return value.get(); }

	RCDataType&	operator*() const;

	//RCDataType& operator*() const	{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(value.get()); return *value; }

	operator RCNum&() const;
	//operator RCBString&() const;
	operator RCDateTime&() const;
	void MakePersistent() ;
	uint GetHashCode() const;
	char* GetDataBytesPointer() const {return value->GetDataBytesPointer();}

private:
	inline void construct(const RCDataType& rcdt);

protected:
	std::auto_ptr<RCDataType> value;

public:
	static bool compare(const RCValueObject& rcvo1, const RCValueObject& rcvo2, Operator op, char like_esc);
};


template<class T> class rc_hash_compare
{
private:
     typedef T Key;
public:
     static const size_t bucket_size = 4;
     static const size_t min_buckets = 1024;//1048576;
     size_t operator()(const Key k) const
	 {
         return k->GetHashCode()&1048575;
     };

#ifdef __GNUC__
     bool operator()(const Key& k1, const Key& k2) const
	 {
		 if(dynamic_cast<RCNum*>(k1))
		 {
			 if(dynamic_cast<RCNum*>(k2))
				return *k1 == *k2;
		 }
		 else if(AreComparable(k1->Type(), k2->Type()))
			 return *k1 == *k2;
		 return false;
     };
#else
     bool operator()(const Key& k1, const Key& k2) const
	 {
		 if(RCNum* rcn = dynamic_cast<RCNum*>(k1))
		 {
			 if(RCNum* rcn1 = dynamic_cast<RCNum*>(k2))
				return *k1 < *k2;
		 }
		 else if(AreComparable(k1->Type(), k2->Type()))
			 return *k1 < *k2;
		 return true;
     };
#endif
};

template<class T> class rc_hash_compare_for_obj
{
private:
	typedef T Key;
public:
	static const size_t bucket_size = 4;
	static const size_t min_buckets = 1024;//1048576;
	size_t operator()(const Key k) const
	{
		return k.GetHashCode()&1048575;
	};
#ifdef __GNUC__
	bool operator()(const Key& k1, const Key& k2) const
	{
		if(AreComparable(k1.Type(), k2.Type()))
			return k1 == k2;
		BHERROR("types not comparable");
		return true;
	};
#else
	bool operator()(const Key& k1, const Key& k2) const
	{
		if(AreComparable(k1.Type(), k2.Type()))
			return k1 < k2;
		BHERROR("types not comparable");
		return false;
	};
#endif
};


class TextStat
{
//////////////////////////////////////////////
// 
// Encoding of strings into 63-bit positive integer (monotonically), if possible.
// Limitations: the strings must not contain 0 (reserved value), the max. width must not exceed 48, not all 255 values are present on a position.
public:
	TextStat();
	TextStat(TextStat &sec);
	~TextStat();

	void Reset();
	bool AddString(const RCBString& rcbs); // return false if cannot create encoding
	bool AddChar(uchar v, int pos);
	void AddLen(int pos);				// value of len n puts 0 on position n (starting with 0)
	bool CheckIfCreatePossible();		// return false if cannot create encoding (too wide), do not actually create anything
	bool CreateEncoding();				// return false if cannot create encoding (too wide), reset temporary statistics

	_int64    Encode(const RCBString& rcbs, bool round_up = false);	// return NULL_VALUE_64 if not encodable, round_up = true => fill the unused characters by max codes
	_int64    MaxCode()					{ return max_code; }	// return the maximal code which may occur
	RCBString Decode(_int64 code);

	bool IsValid()						{ return valid; }
	void Invalidate()					{ valid = false; }

private:
	int len_table[48];			// binary length of position n, value 0 means that this is a constant character
	uchar* encode_table;		// encode_table[c + 256 * n] is a character for code c on position n

	uchar* chars_found;			// a buffer for two purposes: collecting 0/1 info about chars found, or collecting decoding into
	bool chars_found_for_decoding;	// true, if chars_found is initialized for value decoding
	bool valid;
	int max_string_size;
	_int64 max_code;
};

/*! \brief Converts RCBString according to a given collation to the binary form that can be used by memcmp 
 * \param src - source RCBString
 * \param dst - destination RCBString
 * \param charset - character set to be used
*/

#ifndef PURE_LIBRARY
static inline void ConvertToBinaryForm(const RCBString& src, RCBString& dst, DTCollation coll) 
{
	if(!src.IsNull())
		coll.collation->coll->strnxfrm(coll.collation, (uchar*)dst.val, dst.len, (uchar*)(src.val), src.len);
	else
		dst.null = true;
}

void ConvertToBinaryForm(VirtualColumn* vc, const MIIterator& mit, RCBString& buf, DTCollation coll);

static bool inline CollationIsSubstring(DTCollation coll, const RCBString& s1, const RCBString& s2) 
{
	my_match_t mm;
	return coll.collation->coll->instr(coll.collation, s1.val, s1.len, s2.val, s2.len, &mm, 1) > 0;
}

static int inline CollationStrCmp(DTCollation coll, const RCBString& s1, const RCBString& s2) 
{
	return coll.collation->coll->strnncoll(coll.collation, (const uchar*)s1.val, s1.len, (const uchar*)s2.val, s2.len, 0);
}

static bool inline CollationStrCmp(DTCollation coll, const RCBString& s1, const RCBString& s2, Operator op) 
{
	int res = coll.collation->coll->strnncoll(coll.collation, (const uchar*)s1.val, s1.len, (const uchar*)s2.val, s2.len, 0);
	switch(op) {
		case O_EQ:
			return (res == 0);
		case O_NOT_EQ:
			return (res != 0);
		case O_MORE:
			return (res > 0);
		case O_MORE_EQ:
			return (res >= 0);
		case O_LESS:
			return (res < 0);
		case O_LESS_EQ:
			return (res <= 0);
		default:
			BHASSERT(false, "OPERATOR NOT IMPLEMENTED");
			return false;
	}
}

static int inline CollationBufLen(DTCollation coll, int max_str_size) 
{
	return (int)coll.collation->coll->strnxfrmlen(coll.collation, max_str_size * coll.collation->mbmaxlen);
}
#else
static inline void ConvertToBinaryForm(const RCBString& src, RCBString& dst, DTCollation coll)
{
	BHERROR("NOT IMPLEMENTED!");
}


static bool inline CollationIsSubstring(DTCollation coll, const RCBString& s1, const RCBString& s2)
{
	BHERROR("NOT IMPLEMENTED!");
	return false;
}

static int inline CollationStrCmp(DTCollation coll, const RCBString& s1, const RCBString& s2)
{
	BHERROR("NOT IMPLEMENTED!");
	return 0;
}

static bool inline CollationStrCmp(DTCollation coll, const RCBString& s1, const RCBString& s2, Operator op)
{
	BHERROR("NOT IMPLEMENTED!");
	return 0;
}

static int inline CollationBufLen(DTCollation coll, int max_str_size)
{
	BHERROR("NOT IMPLEMENTED!");
	return 0;
}
#endif

static bool conv_required_table[] = {
		0,1,1,1,1,1,1,1,1,1,
		1,1,1,1,1,1,1,1,1,1, //10-
		1,1,1,1,1,1,1,1,1,1, //20-
		1,1,1,1,1,1,1,1,1,1, //30-
		1,1,1,0,1,1,1,0,1,1, //40-
		0,1,1,0,1,1,1,1,0,1, //50
		1,1,1,0,0,0,0,0,0,0, //60
		0,0,0,0,0,0,1,0,0,0, //70
		0,0,0,1,0,0,0,0,1,0, //80
		1,1,1,0,1,1,0,1,1,1, //90
		1,1,1,1,1,1,1,1,1,1, //100
		1,1,1,1,1,1,1,1,1,1, //110
		1,1,1,1,1,1,1,1,1,1, //120
		1,1,1,1,1,1,1,1,1,1, //130
		1,1,1,1,1,1,1,1,1,1, //140
		1,1,1,1,1,1,1,1,1,1, //150
		1,1,1,1,1,1,1,1,1,1, //160
		1,1,1,1,1,1,1,1,1,1, //170
		1,1,1,1,1,1,1,1,1,1, //180
		1,1,1,1,1,1,1,1,1,1, //190
		1,1,1,1,1,1,1,1,1,1, //200
		1,1,1,1,1,1,1,1,1,1, //210
		1,1,1,1,1,1,1,1,1,1, //220
		1,1,1,1,1,1,1,1,1,1, //230
		1,1,1,1,1,1,1,1,1,1, //240
		1,1,1,1,1,1			 //250
};

#ifndef PURE_LIBRARY

static inline bool RequiresUTFConversions(const DTCollation &coll)
{
	assert(coll.collation->number < 256);
	return (conv_required_table[coll.collation->number]);
}

static inline bool IsUnicode(DTCollation coll)
{
	return (strcmp(coll.collation->csname, "utf8") == 0 || strcmp(coll.collation->csname, "ucs2") == 0);
}

static inline bool IsBinary(DTCollation coll)
{
	return (strcmp(coll.collation->csname, "binary") == 0);
}

static inline bool IsBin(DTCollation coll)
{
	return (strstr(coll.collation->csname, "_bin") == 0);
}

inline DTCollation ResolveCollation(DTCollation first, DTCollation sec)
{
	if(sec.collation != first.collation && sec.derivation <= first.derivation) {
		if((IsUnicode(first) && !IsUnicode(sec)) || (IsBinary(first) && !IsBinary(sec)))
			return first;
		if(sec.derivation < first.derivation ||
			(IsUnicode(sec) && !IsUnicode(first)) || (IsBinary(sec) && !IsBinary(first)))
			return sec;
		if(strcmp(sec.collation->csname, first.collation->csname) == 0) {
			if(IsBin(first))
				return first;
			if(IsBin(sec))
				return sec;
		}
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!"Error: Incompatible collations!");
	}
	return first;
}

#else
static inline bool RequiresUTFConversions(DTCollation coll)
{
	return false;
}

static inline bool IsUnicode(DTCollation coll)
{
	return false;
}

static inline bool IsBinary(DTCollation coll)
{
	BHERROR("NOT IMPLEMENTED!");
	return true;
}

static inline bool IsBin(DTCollation coll)
{
	BHERROR("NOT IMPLEMENTED!");
	return true;
}

inline DTCollation ResolveCollation(DTCollation first, DTCollation sec)
{
	BHERROR("NOT IMPLEMENTED!");
	return DTCollation();
}

#endif

static inline double PowOfTen(short exponent)
{
	static double pvalues[] = {
		1ULL,
		10ULL,
		100ULL,
		1000ULL,
		10000ULL,
		100000ULL,
		1000000ULL,
		10000000ULL,
		100000000ULL,
		1000000000ULL,
		10000000000ULL,
		100000000000ULL,
		1000000000000ULL,
		10000000000000ULL,
		100000000000000ULL,
		1000000000000000ULL,
		10000000000000000ULL,
		100000000000000000ULL,
		1000000000000000000ULL,
		10000000000000000000ULL};

	static double nvalues[] = {
		1,
		0.1,
		0.01,
		0.001,
		0.0001,
		0.00001,
		0.000001,
		0.0000001,
		0.00000001,
		0.000000001,
		0.0000000001,
		0.00000000001,
		0.000000000001,
		0.0000000000001,
		0.00000000000001,
		0.000000000000001,
		0.0000000000000001,
		0.00000000000000001,
		0.000000000000000001,
		0.0000000000000000001};

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(exponent >= -19 && exponent <= 19);
	if(exponent < 0 && -exponent < (sizeof(pvalues)/sizeof(double)))
		return nvalues[-exponent];
	if(exponent < (sizeof(pvalues)/sizeof(double)))
		return pvalues[exponent];
	return pow((double)10, exponent);
}

static inline _uint64 Uint64PowOfTen(short exponent)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(exponent >= 0 && exponent < 20);

	static _uint64 v[] =
	{
		1ULL,
		10ULL,
		100ULL,
		1000ULL,
		10000ULL,
		100000ULL,
		1000000ULL,
		10000000ULL,
		100000000ULL,
		1000000000ULL,
		10000000000ULL,
		100000000000ULL,
		1000000000000ULL,
		10000000000000ULL,
		100000000000000ULL,
		1000000000000000ULL,
		10000000000000000ULL,
		100000000000000000ULL,
		1000000000000000000ULL,
		10000000000000000000ULL
	};

	if(exponent >= 0 && exponent < 20)
		return v[exponent];
	else
		return (_uint64)PowOfTen(exponent);
}

static inline _int64 Int64PowOfTen(short exponent)
{
	return _int64(Uint64PowOfTen(exponent));
}

static inline _uint64 Uint64PowOfTenMultiply5(short exponent)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(exponent >= 0 && exponent < 19);

	static _uint64 v[] =
	{
		5ULL,
		50ULL,
		500ULL,
		5000ULL,
		50000ULL,
		500000ULL,
		5000000ULL,
		50000000ULL,
		500000000ULL,
		5000000000ULL,
		50000000000ULL,
		500000000000ULL,
		5000000000000ULL,
		50000000000000ULL,
		500000000000000ULL,
		5000000000000000ULL,
		50000000000000000ULL,
		500000000000000000ULL,
		5000000000000000000ULL
	};
	if(exponent >= 0 && exponent < 19)
		return v[exponent];
	return (_uint64)PowOfTen(exponent)*5;
}

char* Text(_int64 value, char buf[], int scale);

const static RCDateTime RC_YEAR_MIN(1901);
const static RCDateTime RC_YEAR_MAX(2155);
const static RCDateTime RC_YEAR_SPEC(0);

const static RCDateTime RC_TIME_MIN(-838, 59, 59, RC_TIME);
const static RCDateTime RC_TIME_MAX(838, 59, 59, RC_TIME);
const static RCDateTime RC_TIME_SPEC(0, RC_TIME);

const static RCDateTime RC_DATE_MIN(100, 1, 1, RC_DATE);
const static RCDateTime RC_DATE_MAX(9999, 12, 31, RC_DATE);
const static RCDateTime RC_DATE_SPEC(0, RC_DATE);

const static RCDateTime RC_DATETIME_MIN(100, 1, 1, 0, 0, 0, RC_DATETIME);
const static RCDateTime RC_DATETIME_MAX(9999, 12, 31, 23, 59, 59, RC_DATETIME);
const static RCDateTime RC_DATETIME_SPEC(0, RC_DATETIME);

const static RCDateTime RC_TIMESTAMP_MIN(1970, 01, 01, 00, 00, 00, RC_TIMESTAMP);
const static RCDateTime RC_TIMESTAMP_MAX(2038, 01, 01, 00, 59, 59, RC_TIMESTAMP);
const static RCDateTime RC_TIMESTAMP_SPEC(0, RC_TIMESTAMP);

#include "RCNum.h"
#endif //_TYPES_RCDATATYPES_H_

