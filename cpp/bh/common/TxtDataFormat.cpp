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

#include "TxtDataFormat.h"
#include "exporter/DataExporterTxt.h"
#include "types/ValueParserForText.h"
#include "loader/DataParserForText.h"
#include "loader/ParsingStrategyText.h"

using namespace std;

TxtDataFormat::TxtDataFormat()
	:	DataFormat("TXT_VARIABLE", TXT_VARIABLE)
{
}

TxtDataFormat::TxtDataFormat(std::string name, EDF edf)
	:	DataFormat(name, edf)
{
}

DataParserAutoPtr TxtDataFormat::CreateDataParser(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr) const
{
	return std::auto_ptr<DataParser>(new DataParserForText(attrs, buffer, iop, mpr));
}

DataExporterAutoPtr TxtDataFormat::CreateDataExporter(const IOParameters& iop) const
{
	return DataExporterAutoPtr(new RCDEforTxtVariable(iop));
}

ValueParserAutoPtr TxtDataFormat::CreateValueParser() const
{
	return ValueParserAutoPtr(new ValueParserForText());
}

ParsingStrategyAutoPtr TxtDataFormat::CreateParsingStrategy(const IOParameters& iop) const
{
	return ParsingStrategyAutoPtr(new ParsingStrategyText(iop));
}

//! Number of bytes taken by a value when written out,
//! or number of chars if collation not binary and RC_STRING or RC_VARCHAR
uint TxtDataFormat::StaticExtrnalSize(AttributeType attrt, int precision, int scale, const DTCollation* col)
{
	if(attrt == RC_NUM)
		return 1 + precision + (scale ? 1 : 0) + (precision == scale ? 1 : 0);
	// because "-0.1" may be declared as DEC(1,1), so 18 decimal places may take 21 characters
	if(attrt == RC_BYTE || attrt == RC_VARBYTE || attrt == RC_BIN) return precision * 2;
	if(attrt == RC_TIME_N || attrt == RC_DATETIME_N) return precision + scale + 1;
	if(attrt == RC_TIME)
		return 10;
	if(attrt == RC_DATETIME || attrt == RC_TIMESTAMP)
		return 19;
	if(attrt == RC_DATE)
		return 10;
	else if(attrt == RC_INT)
		return 11;
	else if(attrt == RC_BIGINT)
		return 20;
	else if(attrt == RC_BYTEINT || attrt == RC_YEAR)
		return 4;
	else if(attrt == RC_SMALLINT)
		return 6;
	else if(attrt == RC_MEDIUMINT)
		return 8;
	else if(attrt == RC_REAL)
		return 23;
	else if(attrt == RC_FLOAT)
		return 15;
	if (col == NULL)
		return precision;
	else
		return precision / col->collation->mbmaxlen; //at creation, the charlen was multiplicated by mbmaxlen
}

ushort TxtDataFormat::ObjPrefixSizeByteSize(AttributeType attr_type) const
{
	return 0;
}

bool TxtDataFormat::IsVariableSize(AttributeType attr_type) const
{
	if(attr_type == RC_VARCHAR || attr_type == RC_VARBYTE || attr_type == RC_BIN)
		return true;
	return false;
}
