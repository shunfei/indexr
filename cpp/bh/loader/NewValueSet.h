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

#ifndef _NEW_VALUE_SET_H_
#define _NEW_VALUE_SET_H_

#include "core/bintools.h"
#include "compress/tools.h"
#include "system/ChannelOut.h"
#include "system/LargeBuffer.h"
#include "DataParser.h"
#include "common/DataFormat.h"

#include "loader/NewValuesSetBase.h"

class NewValuesSet : public NewValuesSetBase
{
public:
	NewValuesSet(DataParser* rcdl, int attr_no, DTCollation col)
		: rcdl(rcdl), col(col)
	{
			this->attr_no = attr_no;
	}

	bool IsNull(int ono)	{return rcdl->IsNull(ono);}

	_uint64 SumarizedSize()
	{
		int nov = rcdl->NoPrepared();
		_uint64 size = 0;
		for(int i = 0; i < nov; i++)
			size += rcdl->GetObjSize(i);
		if(ATI::IsBinType(rcdl->CurrentAttributeType()) && DataFormat::GetDataFormat(rcdl->GetEDF())->BinaryAsHex())
			size /= 2;
		return size;
	}

	uint Size(int ono)
	{
		return rcdl->GetValueSize(ono);
	}

	void GetIntStats(_int64& min, _int64& max, _int64& sum)
	{
		min = PLUS_INF_64;
		max = MINUS_INF_64;
		sum = 0;
		_int64 v = 0;
		for(int i = 0; i < rcdl->NoPrepared(); i++)
		{
			if(!rcdl->IsNull(i))
			{
				v = *(_int64*)rcdl->GetValue(i);
				sum += v;
				if(min > v)
					min = v;
				if(max < v)
					max = v;
			}
		}
	}

	void GetRealStats(double& min, double& max, double& sum)
	{
		min = PLUS_INF_DBL;
		max = MINUS_INF_DBL;
		sum = 0;
		double v = 0;
		int noo = rcdl->NoPrepared();
		for(int i = 0; i < noo; i++)
		{
			if(!rcdl->IsNull(i))
			{
				v = *(double*)rcdl->GetValue(i);
				sum += v;
				if(min > v)
					min = v;
				if(max < v)
					max = v;
			}
		}
	}

	void GetStrStats(RCBString& min, RCBString& max, ushort& maxlen)
	{
		min = RCBString();
		max = RCBString();
		maxlen = 0;

		RCBString v = 0;
		int noo = rcdl->NoPrepared();
		for(int i = 0; i < noo; i++) {
			if(!rcdl->IsNull(i)) {
				v.val = GetDataBytesPointer(i);
				v.len = Size(i);
				if(v.len > maxlen)
					maxlen = v.len;

				if(min.IsNull())
					min = v;
				else if(RequiresUTFConversions(col)) {
					if(CollationStrCmp(col, min, v) > 0)
						min = v;
				} else if(min > v)
					min = v;

				if(max.IsNull())
					max = v;
				else if(RequiresUTFConversions(col)) {
					if(CollationStrCmp(col, max, v) < 0)
						max = v;
				} else if(max < v)
					max = v;
			}
		}
	}

	char*	GetDataBytesPointer(int ono)			{ return rcdl->GetValue(ono); }
	int		NoValues()				{ return rcdl->NoPrepared(); }

private:
	DataParser* rcdl;
	int attr_no;
	DTCollation col;
};

#endif
