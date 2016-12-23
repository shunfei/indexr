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

#ifndef COLUMNTYPE_H_
#define COLUMNTYPE_H_
/*! \brief Definition of the type of a column.
 *
 * Groups all the information about column type
 */

#include "common/CommonDefinitions.h"
#include "RCAttrTypeInfo.h"

struct DataType;

struct ColumnType {
public:

	ColumnType() :	type(RC_INT), precision(0), scale(0), is_lookup(false), nulls_mode(AS_MISSED),
					internal_size(4), display_size(11), collation(DTCollation()), is_unsigned(false)
	  {}

  //! create ColumnType object and calculate proper internal_ and display_size
	ColumnType(AttributeType t, NullMode nulls = AS_MISSED, bool lookup = false, int prec = 0, int sc = 0, DTCollation collation = DTCollation(), bool is_unsign = false):
		type(t), precision(prec), scale(sc), is_lookup(lookup), nulls_mode(nulls),
		internal_size(InternalSize()), display_size(ATI::TextSize(t, prec, sc, collation)),
		collation(collation), is_unsigned(is_unsign)
		{}

	void Initialize(AttributeType t, NullMode nulls, bool lookup, int prec, int sc,
			DTCollation collation = DTCollation(), bool is_unsign = false) {
		type = t;
		nulls_mode = nulls;
		is_lookup = lookup;
		precision = prec;
		scale = sc;
		this->collation = collation; 
		is_unsigned = is_unsign;
		Recalculate();
	}

	explicit ColumnType(const DataType& dt);

	bool operator== (const ColumnType&) const;

	inline AttributeType GetTypeName() const { return type; };

	/*! column width, as X in CHAR(X) or DEC(X,*)
	 *
	 */
	inline int GetPrecision() const { return precision; }

	/*! \brief Set column width, as X in DEC(X,*)
	 *
	 * Used in type conversions in Attr. Cannot be used as Column::Type().setPrecision(p)
	 * because Type() returns const, so can be used only by Column subclasses and friends
	 */
	inline void SetPrecision(int prec) { precision = prec; Recalculate();}

	/*! number of decimal places after decimal point, as Y in DEC(*,Y)
	 *
	 */
	inline int GetScale() const { return scale; }

	/*! \brief number of decimal places after decimal point, as Y in DEC(*,Y)
	 *
	 * Used in type conversions in Attr. Cannot be used as Column::Type().SetScale(p)
	 * because Type() returns const, so can be used only by Column subclasses and friends
	 */
	inline void SetScale(int sc) { scale = sc; Recalculate();}

	/*! maximal number of bytes occupied in memory by a value of this type
	 *
	 */
	inline int GetInternalSize() const { return internal_size; }

	//! Use in cases where actual string length is less then declared, before materialization of Attr
	void OverrideInternalSize(int size) {internal_size = size;};

	inline int GetDisplaySize() const { return display_size; }

	inline NullMode GetNullsMode() const { return nulls_mode; }

	inline bool IsLookup() const { return is_lookup; }

	ColumnType RemovedLookup() const;

	inline bool IsUnsigned() const { return is_unsigned; }

	bool IsNumeric() const 	{
		switch(type) {
			case RC_BIN:
			case RC_BYTE:
			case RC_VARBYTE:
			case RC_STRING:
			case RC_VARCHAR:
				return false;
			default:
				return true;
		}
	}

	bool IsKnown() const 	{ return type != RC_UNKNOWN;};
	bool IsFixed() const 	{ return ATI::IsFixedNumericType(type);};
	bool IsFloat() const	{ return ATI::IsRealType(type); };
	bool IsInt()   const	{ return IsFixed() && scale == 0;}
	bool IsString() const   { return ATI::IsStringType(type); }
	bool IsDateTime()  const	{ return ATI::IsDateTimeType(type); }
	const DTCollation& GetCollation() const { return collation; }
	void SetCollation(DTCollation _collation) { collation = _collation; }
	bool IsNumComparable(const ColumnType &sec) const
	{
		if(is_lookup || sec.is_lookup || IsString() || sec.IsString())
			return false;
		if(scale != sec.scale)
			return false;
		if(IsFixed() && sec.IsFixed())
			return true;
		if(IsFloat() && sec.IsFloat())
			return true;
		if(IsDateTime() && sec.IsDateTime())
			return true;
		return false;
	}

private:
	AttributeType type;
	int precision;
	int scale;
	bool is_lookup;
	NullMode nulls_mode;
	int internal_size;
	int display_size;
	DTCollation collation;
	bool is_unsigned;
	/*! \brief calculate maximal number of bytes a value of a given type can take in memory
	 *
	 */
	int InternalSize();

	void Recalculate() 	{ 		internal_size = InternalSize();
								display_size = ATI::TextSize(type, precision, scale, collation); }
};



#endif /* COLUMNTYPE_H_ */
