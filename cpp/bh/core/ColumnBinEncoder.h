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


#ifndef COLUMNBINENCODER_H_
#define COLUMNBINENCODER_H_


#include "edition/vc/VirtualColumn.h"
#include "RSI_CMap.h"

class MIIterator;
class MIDummyIterator;

class ColumnBinEncoder
{
public:
	static const int ENCODER_IGNORE_NULLS	= 0x01;		// don't encode nulls - not present, or just ignore them
	static const int ENCODER_MONOTONIC		= 0x02;		// code order follow value order; if false, only equality comparisons work
	static const int ENCODER_NONCOMPARABLE	= 0x04;		// store values, do not allow comparisons (e.g. UTF8 transformations not applied)
	static const int ENCODER_DESCENDING		= 0x08;		// ordering direction
	static const int ENCODER_DECODABLE		= 0x10;		// GetValue works

	class ColumnValueEncoder;
	// Special cases:
	class EncoderInt;
	class EncoderDecimal;
	class EncoderDate;
	class EncoderYear;
	class EncoderDouble;
	class EncoderText;
	class EncoderText_UTF;
	class EncoderLookup;
	class EncoderTextStat;
	class EncoderTextMD5;

	/////////////////////////////////////////////////
	ColumnBinEncoder(int flags = 0);
	ColumnBinEncoder(ColumnBinEncoder const &sec);
	ColumnBinEncoder& operator=(ColumnBinEncoder const&);
	~ColumnBinEncoder();

//	void Clear();								// clear all encoding data, keep flags
	bool PrepareEncoder(VirtualColumn *vc1, VirtualColumn *vc2 = NULL);		// encoder for one column, or a common encoder for two of them
	// return false if encoding of a second column is not possible (incompatible)

	void Disable()								{ disabled = true; }
	bool IsEnabled()							{ return !disabled; }	// disabled if set manually or or implicit or trivial value for sorting
	bool IsNontrivial()							{ return (val_size > 0); }

	void SetImplicit()							{ implicit = true; disabled = true; }	// implicit: the column will be read as vc->GetValue(iterator) instead of orig. values

	// Buffer descriptions
	int GetPrimarySize()						// no. of bytes in the primary buffer
	{ return val_size; }
	int GetSecondarySize()						// no. of bytes in the secondary buffer (or 0 if not needed)
	{ return val_sec_size; }

	void SetPrimaryOffset(int _offset)			// offset of the primary storage in buffer
	{ val_offset = _offset; }
	void SetSecondaryOffset(int _offset)		// offset of the secondary storage in buffer
	{ val_sec_offset = _offset; }

	// Set / retrieve values
	void LockSourcePacks(MIIterator &mit)
	{ if(vc && !implicit) vc->LockSourcePacks(mit); }

	void Encode(unsigned char *buf, MIIterator& mit, VirtualColumn *alternative_vc = NULL, bool update_stats = false);
	bool PutValue64(unsigned char *buf, _int64 v, bool sec_column, bool update_stats = false);				// used in special cases only (e.g. rough), return false if cannot encode
	bool PutValueString(unsigned char *buf, RCBString& v, bool sec_column, bool update_stats = false);		// as above

	// Versions of encoders which returns integer code (monotonic for normal inequalities)
	_int64 ValEncode(MIIterator& mit, bool update_stats = false);
	_int64 ValPutValue64(_int64 v, bool update_stats = false);
	_int64 ValPutValueString(RCBString& v, bool update_stats = false);

	_int64 GetValue64(unsigned char *buf, const MIDummyIterator &mit, bool &is_null);
	RCBString GetValueT(unsigned char *buf, const MIDummyIterator &mit);
	void UpdateStatistics(unsigned char *buf);						// get value from the buffer and update internal statistics

	bool ImpossibleValues(_int64 pack_min, _int64 pack_max);		// return true if the current contents of the encoder is out of scope
	bool ImpossibleValues(RCBString& pack_min, RCBString& pack_max);	// return true if the current contents of the encoder is out of scope
	// Note: pack_min/pack_max may be wider than the original string size! They usually has 8 bytes.
	bool IsString();
	_int64 MaxCode();							// Maximal integer code, if it makes any sense (or NULL_VALUE_64)
	void ClearStatistics();

private:
	VirtualColumn *vc;

	bool ignore_nulls;
	bool monotonic_encoding;
	bool descending;
	bool decodable;
	bool noncomparable;

	bool implicit;				// if true, then the column will be read as vc->GetValue(iterator) instead of orig. values
	bool disabled;				// if true, then the column will not be encoded

	// Address part:
	int	val_offset;		// buffer offset of the value - externally set
	int	val_sec_offset;	// secondary buffer, if needed - externally set
	int	val_size;		// number of bytes in the main part
	int	val_sec_size;	// number of bytes in the secondary part, if needed

	typedef std::auto_ptr<ColumnValueEncoder> ColumnValueEncoder_ptr_t;
	mutable ColumnValueEncoder_ptr_t my_encoder;
};

/////////////////////////////////////////////////////////////////////////

class ColumnBinEncoder::ColumnValueEncoder			// base class for detailed encoders
{
public:
	ColumnValueEncoder(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending)
						: descending(_descending), null_status(0), vc_type(vc->Type()), size(0), size_sec(0) {}
	ColumnValueEncoder(ColumnValueEncoder &sec)
		: descending(sec.descending), null_status(sec.null_status), vc_type(sec.vc_type), size(sec.size), size_sec(sec.size_sec) {}
	virtual ColumnValueEncoder *Copy() = 0;

	virtual ~ColumnValueEncoder()	{}

	// Encode the second column to make common encoding, false if cannot
	virtual bool SecondColumn(VirtualColumn *vc) 			{ return false; }
	// Valid() is false if we failed to construct a proper encoder (rare case)
	virtual bool Valid()						 			{ return true; }

	virtual void SetNull(unsigned char *buf, unsigned char *buf_sec) = 0;
	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator& mit, bool update_stats = false) = 0;
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats)		{ return false; }	// used in special cases only, return false if cannot encode
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, RCBString& v, bool sec_column, bool update_stats)	{ return false; }

	virtual _int64 ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats = false) { assert(0); return 0; }
	virtual _int64 ValEncode(_int64 v, bool update_stats)			{ assert(0); return 0; }	// used in special cases only
	virtual _int64 ValEncode(RCBString& v, bool update_stats)		{ assert(0); return 0; }	// used in special cases only

	virtual bool IsNull(unsigned char *buf, unsigned char *buf_sec) = 0;
	virtual _int64 GetValue64(unsigned char *buf, unsigned char *buf_sec)			{ return NULL_VALUE_64; }	// null when executed e.g. for string columns
	virtual RCBString GetValueT(unsigned char *buf, unsigned char *buf_sec)			{ return RCBString(); }		// null when executed e.g. for numerical columns
	virtual void UpdateStatistics(unsigned char *buf)								{ return; }					// get value from the buffer and update internal statistics
	virtual bool ImpossibleValues(_int64 pack_min, _int64 pack_max)					{ return false; }			// default: all values are possible
	virtual bool ImpossibleValues(RCBString& pack_min, RCBString& pack_max)			{ return false; }			// default: all values are possible
	virtual void ClearStatistics()													{}
	virtual bool IsString()															{ return false; }
	virtual _int64 MaxCode()														{ return NULL_VALUE_64;	}

	int ValueSize()											{ return size; }
	int ValueSizeSec()										{ return size_sec; }

protected:
	bool descending;		// true if descending
	int	null_status;		// 0 - no nulls, 1 - integer shifted (encoded as 0), 2 - separate byte
	ColumnType vc_type;		// a type of the first encoded virtual column
	int size;				// total size in bytes
	int size_sec;			// total size in bytes for a secondary location, or 0

	void Negate(unsigned char *buf, int loc_size)
	{ for(int i = 0; i < loc_size; i++) buf[i] = ~(buf[i]); }
};
////////////////////////////////////////////////////////////////////////

class ColumnBinEncoder::EncoderInt : public ColumnBinEncoder::ColumnValueEncoder
{
public:
	EncoderInt(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending, bool calc_int_stats = true);
	EncoderInt(EncoderInt &sec) : ColumnValueEncoder(sec), min_val(sec.min_val), max_code(sec.max_code), min_found(sec.min_found), max_found(sec.max_found) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderInt(*this); }

	virtual bool SecondColumn(VirtualColumn *vc);

	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator& mit, bool update_stats = false);
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats);
	virtual void SetNull(unsigned char *buf, unsigned char *buf_sec);
	virtual bool IsNull(unsigned char *buf, unsigned char *buf_sec);
	virtual _int64 GetValue64(unsigned char *buf, unsigned char *buf_sec);
	virtual _int64 MaxCode()														{ return max_code; }
	virtual void UpdateStatistics(unsigned char *buf);

	virtual _int64 ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats = false);
	virtual _int64 ValEncode(_int64 v, bool update_stats);
	
	virtual bool ImpossibleValues(_int64 pack_min, _int64 pack_max);
	virtual void ClearStatistics();

protected:
	_int64 min_val;
	_uint64 max_code;

	_int64 min_found;				// local statistics, for rough evaluations
	_int64 max_found;
};

class ColumnBinEncoder::EncoderDecimal : public ColumnBinEncoder::EncoderInt
{
public:
	EncoderDecimal(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
	EncoderDecimal(EncoderDecimal &sec) : EncoderInt(sec), scale(sec.scale), multiplier(sec.multiplier), sec_multiplier(sec.sec_multiplier) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderDecimal(*this); }

	virtual bool SecondColumn(VirtualColumn *vc);
	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator &mit, bool update_stats = false);
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats);

	virtual bool ImpossibleValues(_int64 pack_min, _int64 pack_max);
protected:
	int scale;
	double multiplier;
	double sec_multiplier;
};

class ColumnBinEncoder::EncoderDate : public ColumnBinEncoder::EncoderInt
{
public:
	EncoderDate(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
	EncoderDate(EncoderDate &sec) : EncoderInt(sec) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderDate(*this); }

	virtual bool SecondColumn(VirtualColumn *vc);

	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator &mit, bool update_stats = false);
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats);
	virtual _int64 ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats = false);
	virtual _int64 ValEncode(_int64 v, bool update_stats);
	virtual _int64 GetValue64(unsigned char *buf, unsigned char *buf_sec);
	virtual void UpdateStatistics(unsigned char *buf);
	virtual bool ImpossibleValues(_int64 pack_min, _int64 pack_max);
};

class ColumnBinEncoder::EncoderYear : public ColumnBinEncoder::EncoderInt
{
public:
	EncoderYear(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
	EncoderYear(EncoderYear &sec) : EncoderInt(sec) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderYear(*this); }

	virtual bool SecondColumn(VirtualColumn *vc);

	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator &mit, bool update_stats = false);
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats);
	virtual _int64 ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats = false);
	virtual _int64 ValEncode(_int64 v, bool update_stats);
	virtual _int64 GetValue64(unsigned char *buf, unsigned char *buf_sec);
	virtual void UpdateStatistics(unsigned char *buf);
	virtual bool ImpossibleValues(_int64 pack_min, _int64 pack_max);
};

class ColumnBinEncoder::EncoderDouble : public ColumnBinEncoder::ColumnValueEncoder
{
public:
	EncoderDouble(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
	EncoderDouble(EncoderDouble &sec) : ColumnValueEncoder(sec), multiplier_vc1(sec.multiplier_vc1), multiplier_vc2(sec.multiplier_vc2) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderDouble(*this); }

	virtual bool SecondColumn(VirtualColumn *vc);

	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats);
	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator& mit, bool update_stats = false);
	virtual void SetNull(unsigned char *buf, unsigned char *buf_sec);
	virtual bool IsNull(unsigned char *buf, unsigned char *buf_sec);
	virtual _int64 GetValue64(unsigned char *buf, unsigned char *buf_sec);

protected:
	_int64 multiplier_vc1;	// 0 if the first vc is double, otherwise it is regarded as int (decimal) which must be multiplied
	_int64 multiplier_vc2;	// 0 if the second vc is double, otherwise it is regarded as int (decimal) which must be multiplied
};

class ColumnBinEncoder::EncoderText : public ColumnBinEncoder::ColumnValueEncoder
{
public:
	EncoderText(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending);
	~EncoderText();
	EncoderText(EncoderText &sec) : ColumnValueEncoder(sec), mins(sec.mins), maxs(sec.maxs), min_max_set(sec.min_max_set) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderText(*this); }

	virtual bool SecondColumn(VirtualColumn* vc);

	// Encoding:
	// <comparable text><len+1>,   text is padded with zeros
	// null value: all 0, len = 0;    empty string: all 0, len = 1
	virtual void Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats = false);
	virtual bool Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats);
	virtual void SetNull(unsigned char* buf, unsigned char* buf_sec);
	virtual bool IsNull(unsigned char* buf, unsigned char* buf_sec);
	virtual RCBString GetValueT(unsigned char* buf, unsigned char* buf_sec);
	virtual bool ImpossibleValues(RCBString& pack_min, RCBString& pack_max);
	virtual bool IsString()															{ return true; }
	RCBString mins, maxs;
	bool min_max_set;
};

class ColumnBinEncoder::EncoderText_UTF: public ColumnBinEncoder::ColumnValueEncoder
{
public:
	EncoderText_UTF(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending);
	~EncoderText_UTF();
	EncoderText_UTF(EncoderText_UTF &sec) : ColumnValueEncoder(sec), collation(sec.collation), mins(sec.mins), maxs(sec.maxs), min_max_set(sec.min_max_set) {}
	virtual ColumnValueEncoder *Copy()				{ return new EncoderText_UTF(*this); }

	virtual bool SecondColumn(VirtualColumn* vc);

	// Encoding:
	// <comparable text><len+1>,   text is padded with zeros
	// null value: all 0, len = 0;    empty string: all 0, len = 1
	virtual void Encode(unsigned char* buf, unsigned char* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats = false);
	virtual bool Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats);
	virtual void SetNull(unsigned char* buf, unsigned char* buf_sec);
	virtual bool IsNull(unsigned char* buf, unsigned char* buf_sec);
	virtual RCBString GetValueT(unsigned char* buf, unsigned char* buf_sec);
	virtual bool ImpossibleValues(RCBString& pack_min, RCBString& pack_max);
	virtual bool IsString()															{ return true; }

private:
	DTCollation collation;
	RCBString mins, maxs;
	bool min_max_set;
};

class ColumnBinEncoder::EncoderLookup : public ColumnBinEncoder::EncoderInt
{
public:
	EncoderLookup(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
	virtual ~EncoderLookup();
	EncoderLookup(EncoderLookup &sec);
	virtual ColumnValueEncoder *Copy()				{ return new EncoderLookup(*this); }

	virtual bool SecondColumn(VirtualColumn *vc);
	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator &mit, bool update_stats = false);
	virtual bool Encode(unsigned char *buf, unsigned char *buf_sec, _int64 v, bool sec_column, bool update_stats);
	virtual RCBString GetValueT(unsigned char* buf, unsigned char* buf_sec);
	virtual bool ImpossibleValues(_int64 pack_min, _int64 pack_max);

protected:
	int *translate2;				// code_v1 = translate2[code_v2]. Value -1 means not matching string.
	int no_sec_values;				// number of possible values of the second column
	VirtualColumn *first_vc;		// we need both vc to construct translation table
	VirtualColumn *sec_vc;
};

class ColumnBinEncoder::EncoderTextStat : public ColumnBinEncoder::EncoderInt
{
public:
	EncoderTextStat(VirtualColumn *vc, bool decodable, bool nulls_possible, bool _descending);
	EncoderTextStat(EncoderTextStat &sec) : EncoderInt(sec), mins(sec.mins), maxs(sec.maxs), min_max_set(sec.min_max_set), valid(sec.valid), coder(sec.coder) {}	
	virtual ColumnValueEncoder *Copy()				{ return new EncoderTextStat(*this); }

	virtual bool Valid()							{ return valid; }
	virtual bool SecondColumn(VirtualColumn *vc);
	virtual void Encode(unsigned char *buf, unsigned char *buf_sec, VirtualColumn *vc, MIIterator &mit, bool update_stats = false);
	virtual bool Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats);
	virtual _int64 ValEncode(VirtualColumn *vc, MIIterator& mit, bool update_stats = false);
	virtual bool ImpossibleValues(RCBString& pack_min, RCBString& pack_max);
	virtual bool IsString()															{ return true; }
	virtual RCBString GetValueT(unsigned char* buf, unsigned char* buf_sec);

protected:
	RCBString mins, maxs;
	bool min_max_set;
	bool valid;
	TextStat coder;
};

class ColumnBinEncoder::EncoderTextMD5 : public ColumnBinEncoder::ColumnValueEncoder
{
public:
	EncoderTextMD5(VirtualColumn* vc, bool decodable, bool nulls_possible, bool _descending);
	~EncoderTextMD5();
	EncoderTextMD5(EncoderTextMD5 &sec);	
	virtual ColumnValueEncoder *Copy()				{ return new EncoderTextMD5(*this); }

	virtual bool SecondColumn(VirtualColumn* vc);

	virtual void Encode(uchar* buf, uchar* buf_sec, VirtualColumn* vc, MIIterator& mit, bool update_stats = false);
	virtual bool Encode(uchar* buf, uchar* buf_sec, RCBString& s, bool sec_column, bool update_stats);
	virtual void SetNull(unsigned char* buf, unsigned char* buf_sec);
	virtual bool IsNull(unsigned char* buf, unsigned char* buf_sec);
	virtual bool ImpossibleValues(RCBString& pack_min, RCBString& pack_max);
	virtual bool IsString()															{ return true; }

	RCBString mins, maxs;
	bool min_max_set;

private:
	char null_buf[HASH_FUNCTION_BYTE_SIZE];
	char empty_buf[HASH_FUNCTION_BYTE_SIZE];
};

/////////////////////////////////////////////////////////////////////////////////////////

class MultiindexPositionEncoder
{
public:
	MultiindexPositionEncoder(MultiIndex *mind, DimensionVector &dims);

	int GetPrimarySize()						// no. of bytes in the primary buffer
	{ return val_size; }
	void SetPrimaryOffset(int _offset);			// offset of the primary storage in buffer

	void Encode(unsigned char *buf, MIIterator& mit);			// set the buffer basing on iterator position
	void GetValue(unsigned char *buf, MIDummyIterator &mit);	// set the iterator position basing on buffer

	// tools:
	static int DimByteSize(MultiIndex *mind, int dim);			// how many bytes is needed to encode the dimension

private:
	// Encoding: 0 = NULL, or k+1. Stored on a minimal number of bytes.
	int	val_offset;		// buffer offset of the value - externally set
	int	val_size;		// number of bytes for all the stored dimensions

	std::vector<int>	dim_no;		// actual dimension number stored on i-th position
	std::vector<int>	dim_offset;	// offset of the i-th stored dimension (dim_offset[0] = val_offset)
	std::vector<int>	dim_size;	// byte size of the i-th stored dimension
};

#endif /* COLUMNBINENCODER_H_ */
