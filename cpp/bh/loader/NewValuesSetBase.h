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

#ifndef NEW_VALUES_SET_BASE
#define NEW_VALUES_SET_BASE

#include "common/CommonDefinitions.h"

class RCBString;

class NewValuesSetBase
{
public:
	virtual ~NewValuesSetBase() {}
	virtual bool 	IsNull(int ono) = 0;
	virtual _uint64 SumarizedSize() = 0;
	virtual uint	Size(int ono) const = 0;
	virtual void 	GetIntStats(_int64& min, _int64& max, _int64& sum) = 0;
	virtual void 	GetRealStats(double& min, double& max, double& sum) = 0;
	virtual void 	GetStrStats(RCBString& min, RCBString& max, ushort& maxlen) = 0;
	virtual char*	GetDataBytesPointer(int ono) = 0;
	virtual int		NoValues() = 0;
	virtual bool IsBinType() { return false; }
	virtual bool BinaryAsHex() { return false; }
	virtual uint GetObjSize(int ono) { return 0; }
	virtual	bool UTFConversions() { return false; }
	virtual int UTFStrCmp(RCBString &a, RCBString &b) { return 0; }

};

#endif
