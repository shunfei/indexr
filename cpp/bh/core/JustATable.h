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

#ifndef _JUST_A_TABLE_H_
#define _JUST_A_TABLE_H_

#include <memory>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "fwd.h"
#include "Filter.h"
#include "MysqlExpression.h"

enum TType { RC_TABLE, TEMP_TABLE };

class ConnectionInfo;
class RCBString;
class AttributeTypeInfo;
class PhysicalColumn;

class TempTable;
class Descriptor;

class JustATable : public boost::enable_shared_from_this<JustATable>
{
public:

	int NoPacks() { return bh::common::NoObj2NoPacks(NoObj()); }
	static unsigned PackIndex(_int64 obj)
	{ return (obj==NULL_VALUE_64 ? 0xFFFFFFFF : (unsigned)(obj >> 16) ); }	// null pack number (interpreted properly)
	virtual void LockPackForUse(unsigned attr, unsigned pack_no, ConnectionInfo& conn) = 0;
	virtual void UnlockPackFromUse(unsigned attr, unsigned pack_no) = 0;
	virtual void PrefetchPack(unsigned attr, int pack_no, ConnectionInfo& conn) {}
	virtual void PrefetchCancel(int attr, ConnectionInfo& conn) {};
	virtual _int64 NoObj() = 0;
	virtual uint NoAttrs() const = 0;
	virtual uint NoDisplaybleAttrs() const = 0;
	virtual TType TableType() const = 0;

	virtual _int64 GetTable64(_int64 obj, int attr ) = 0;
	virtual void GetTable_S(RCBString& s, _int64 obj, int attr) = 0;

	virtual bool IsNull(_int64 obj, int attr) = 0;

	virtual ushort MaxStringSize(int n_a, Filter *f = NULL) = 0;

	//virtual AttributeType AttrType(int n_a) = 0;
	virtual std::vector<ATI> GetATIs(bool orig = false) = 0;
	//virtual int GetAttrScale(int n_a) = 0;
	//virtual int GetAttrSize(int n_a) = 0;
	//virtual int GetFieldSize(int n_a) = 0;
	//virtual NullMode GetAttrNullMode(int n_a) = 0;

	virtual PhysicalColumn* GetColumn(int col_no) = 0;

	virtual const ColumnType& GetColumnType(int n_a)	= 0;

	//! Returns column value in the form required by complex expressions
	ValueOrNull GetComplexValue(const _int64 obj, const int attr);

	virtual ~JustATable(){};

 };



#endif

