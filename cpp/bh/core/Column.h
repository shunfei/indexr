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

#ifndef COLUMN_H_
#define COLUMN_H_

#include "RCAttrTypeInfo.h"
#include "types/RCDataTypes.h"
#include "common/bhassert.h"
#include "common/CommonDefinitions.h"
#include "ColumnType.h"
#include "Descriptor.h"

class ConnectionInfo;

enum PackOntologicalStatus	{ NULLS_ONLY, UNIFORM, UNIFORM_AND_NULLS, SEQUENTIAL, NORMAL };

/*! \brief Base class for columns.
 *
 * Defines the common interface for RCAttr, Attr and VirtualColumn
 */
class Column {
public:

	Column(ColumnType ct = ColumnType()) :
		ct(ct) {}

	Column(const Column& c) { if(this != &c) *this = c; }


	/*! \brief Get the full type of the column
	 *
	 */
	inline const ColumnType& Type() const { return ct; }

	inline void CoerceType(const ColumnType& new_type) { ct = new_type; }

	/*! \brief Get simple type of the column
	 *
	 */
	inline const AttributeType TypeName() const { return ct.GetTypeName(); }

	void SetCollation(DTCollation collation) { ct.SetCollation(collation); }
	DTCollation GetCollation() { return ct.GetCollation(); }
protected:
	ColumnType ct;
};
#endif
