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


#ifndef DATAFORMAT_H_
#define DATAFORMAT_H_

#include <vector>
#include <map>
#include <string>

#include "common/CommonDefinitions.h"

class IOParameters;
class RCAttrLoad;
class Buffer;

class DataExporter;
typedef std::auto_ptr<DataExporter> DataExporterAutoPtr;

class DataFormat;
typedef boost::shared_ptr<DataFormat> DataFormatPtr;

class DataParser;
typedef std::auto_ptr<DataParser> DataParserAutoPtr;

class ValueParser;
typedef std::auto_ptr<ValueParser> ValueParserAutoPtr;

class BHError;

class DataFormat
{
public:
	virtual ~DataFormat();
protected:
	DataFormat(std::string name, EDF edf);

public:
	std::string GetName() const		{ return name; }
	int 		GetId() const 		{ return id; }
	EDF 		GetEDF() const 		{ return edf; }

	virtual bool BinaryAsHex() const = 0;

	virtual DataParserAutoPtr   CreateDataParser(std::vector<RCAttrLoad*> attrs, Buffer& buffer, const IOParameters& iop, uint mpr = MAX_PACK_ROW_SIZE) const = 0;
	virtual DataExporterAutoPtr CreateDataExporter(const IOParameters& iop) const = 0;
	virtual ValueParserAutoPtr 	CreateValueParser() const = 0;
	virtual ParsingStrategyAutoPtr 	CreateParsingStrategy(const IOParameters& iop) const = 0;

	//! Number of bytes taken by a value when written out,
	//! or number of chars if collation not binary and (RC_STRING or RC_VARCHAR) and txt_variable format
	virtual uint	ExtrnalSize(AttributeType attrt, int precision, int scale, const DTCollation* col = NULL) const = 0;
	virtual ushort	ObjPrefixSizeByteSize(AttributeType attr_type) const = 0;
	virtual short	RowPrefixSize() const { return 0; }
	virtual bool	IsVariableSize(AttributeType attr_type) const = 0;
	virtual bool	CanImport(const IOParameters& iop, BHError& bherror) const;
	virtual bool 	CanExport() const;


private:
	std::string name;
	int id;
	EDF edf;

public:
    typedef std::map<std::string, DataFormatPtr> map_type; // SOLARIS

	static DataFormatPtr	GetDataFormat(const std::string& name);
	static DataFormatPtr	GetDataFormat(int id);
	static DataFormatPtr	GetDataFormat(EDF edf);
	static DataFormatPtr	GetDataFormatbyEDF(EDF edf);
	static ushort			GetNoFormats() { return ushort(GetExternalDataFormats()->size()); }

    //static map_type PrepareMapOfExternalDataFormats(); // SOLARIS
    static map_type* GetExternalDataFormats(); // SOLARIS

private:
	//typedef std::map<std::string, DataFormatPtr> map_type;
	//static map_type external_data_formats;  SOLARIS
	static int no_formats;
};

#endif /* DATAFORMAT_H_ */
