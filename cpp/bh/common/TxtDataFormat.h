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

#ifndef TXTDATAFORMAT_H_
#define TXTDATAFORMAT_H_

#include "DataFormat.h"

class TxtDataFormat : public DataFormat
{
public:
	TxtDataFormat();
	TxtDataFormat(std::string name, EDF edf);
	virtual bool BinaryAsHex() const { return true; }

	virtual DataParserAutoPtr   CreateDataParser(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr = MAX_PACK_ROW_SIZE) const;
	virtual DataExporterAutoPtr CreateDataExporter(const IOParameters& iop) const;
	virtual ValueParserAutoPtr 	CreateValueParser() const;
	virtual ParsingStrategyAutoPtr 	CreateParsingStrategy(const IOParameters& iop) const;

	//! Number of bytes taken by a value when written out,
	//! or number of chars if collation not binary and RC_STRING or RC_VARCHAR
	virtual uint ExtrnalSize(AttributeType attrt, int precision, int scale, const DTCollation* col = NULL) const
	{ return StaticExtrnalSize(attrt, precision, scale, col); }


	virtual ushort ObjPrefixSizeByteSize(AttributeType attr_type) const;
	virtual bool IsVariableSize(AttributeType attr_type) const;

//STATICS
public:
	static uint StaticExtrnalSize(AttributeType attrt, int precision, int scale, const DTCollation* col);

};

#endif /* TXTDATAFORMAT_H_ */
