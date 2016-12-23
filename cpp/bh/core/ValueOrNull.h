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

#ifndef VaLUEORNULL_H_INCLUDED
#define VaLUEORNULL_H_INCLUDED

#include <set>
#include <map>
#include <cassert>

#include "common/CommonDefinitions.h"
#include "types/RCDataTypes.h"

struct ColumnType;
struct CQTerm;

// Type of intermediate results of expression evaluation.
// VT_FLOAT - floating-point real number (double)
// VT_FIXED - fixed-point real (decimal) or integer, encoded as _int64 with base-10 scale
// VT_STRING - pointer to character string, accompanied by string length
// VT_DATETIME - date/time value encoded as int64, taken from RCDateTime representation
//               DataType::attrtype must be set to precisely denote which date/time type is considered.

enum ValueType { VT_FLOAT, VT_FIXED, VT_STRING, VT_DATETIME, VT_NOTKNOWN = 255 };

struct DataType
{
	ValueType		valtype;
	AttributeType	attrtype;			// storage type of BH (only for source columns; otherwise RC_UNKNOWN)
	int				fixscale;			// base-10 scale of VT_FIXED (no. of decimal digits after comma)
	_int64			fixmax;				// maximum _absolute_ value possible (upper bound) of VT_FIXED;
										// fixmax = -1  when upper bound is unknown or doesn't fit in _int64;
										// precision of a decimal = QuickMath::precision10(fixmax)
	DTCollation     collation;			// character set of VT_STRING + coercibility
	int 			precision;

	DataType()		{ valtype = VT_NOTKNOWN; attrtype = RC_UNKNOWN; fixscale = 0; fixmax = -1; collation = DTCollation(); precision = -1; }
	DataType(AttributeType atype, int prec = 0, int scale = 0, DTCollation collation = DTCollation());
	DataType& operator= (const ColumnType& ct);

	bool	IsKnown()		const { return valtype != VT_NOTKNOWN; }
	bool	IsFixed()		const { return valtype == VT_FIXED; }
	bool	IsFloat()		const { return valtype == VT_FLOAT; }
	bool	IsInt()			const { return IsFixed() && (fixscale == 0); }
	bool	IsString()		const { return valtype == VT_STRING; }
	bool	IsDateTime()		const { return valtype == VT_DATETIME; }
};

struct VarID
{
	//JustATable*	tab_p;
	int			tab;

	int			col;
	VarID()					:/*tab_p(NULL),*/tab(0),col(0) {}
	VarID(int t, int c)		:/*tab_p(NULL),*/tab(t),col(c) {}

	// this is necessary if VarID is used as a key in std::map
	bool operator<(const VarID& cd) const	{ return tab<cd.tab || (tab==cd.tab && col<cd.col); }
	bool operator==(const VarID& cd) const	{ return tab==cd.tab && col==cd.col; }
};

class ValueOrNull
{
public:
	typedef _int64	Value;		// 8-byte value of an expression; interpreted as _int64 or double
	Value		x;				// 8-byte value of an expression; interpreted as _int64 or double
	uint		len;			// string length; used only for VT_STRING
	char*		sp;				// != 0 if string value assigned
	bool		string_owner;	// if true, destructor must deallocate sp
	bool		null;

	ValueOrNull()						: x(NULL_VALUE_64), len(0), sp(0), string_owner(false), null(true) {}
	ValueOrNull(ValueOrNull const& von);
	ValueOrNull(Value x, bool n=false)	: x(x), len(0), sp(0), string_owner(false), null(n) {assert(x != NULL_VALUE_64 || n);}
	ValueOrNull(RCNum const& rcn);
	ValueOrNull(RCDateTime const& rcdt);
	ValueOrNull(RCBString const& rcs);

	~ValueOrNull()							{ Clear(); }
	ValueOrNull& operator=(ValueOrNull const& von);
	void swap(ValueOrNull& von);
	bool operator!=(ValueOrNull const& v) const {
		if(null && v.null)
			return false;
		else if((null && (!v.null)) || (!null && v.null))
			return true;
		else {
			bool diff = false;
			if(sp) {
				diff = (v.sp ? ( (len != v.len) || (memcmp(sp, v.sp, len) != 0) ) : true);
			} else if(v.sp || ( x != v.x ))
				diff = true;
			return diff;
		}
	}

	bool IsNull()	const					{ return null; }
	void SetNull()							{ Clear(); }
	bool IsString() const					{ return (sp != NULL); }

	_int64 Get64() const					{ return x; }

	void SetFixed(_int64 v)					{ Clear(); x = v; null = false; }
	void SetDouble(double v)					{ Clear(); x = *(_int64*)&v; null = false; }
	_int64 GetFixed() const					{ return x; }
	double GetDouble() const					{ return *(double*)&x; }

	//! assign an externally allocated string value
	void SetString(char* s, uint n)			{ Clear(); sp = s; len = n; null = false; }

	//! assign a string from RCBString
	//! \param rcs may be null, if is persistent, then create a string copy and become string owner
	void SetString(const RCBString& rcs);

	//! create a local copy of the string pointed by sp
	void MakeStringOwner();

	//! Returns a null-terminated copy of string (char*)x.
	//! This copy must be deallocated by the client.
	char* GetStringCopy();

	 /*! Get a string in the form of RSBString
		* \param rcs is given the string value from this
		* Warning 1: the string pointed to by 'rcs' should be copied as soon
		* as possible and not used later on
		* (it may point to an internal buffer of MySQL String object ).
		* Warning 2: 'rcs' must NOT be persistent, otherwise memory leaks may occur
		*/
	void GetString(RCBString& rcs) const;

	_int64 GetDateTime64()	const	{ return x; }

private:
	void Clear() {
		if(string_owner) 
			delete[] sp;
		sp = NULL; 
		string_owner = false; 
		null = true; 
		x = NULL_VALUE_64; 
		len = 0;
	}
};

#endif

