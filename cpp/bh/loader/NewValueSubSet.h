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

#ifndef _NEW_VALUE_SUBSET_H_
#define _NEW_VALUE_SUBSET_H_

#include "core/bintools.h"
#include "compress/tools.h"
#include "system/ChannelOut.h"
#include "system/LargeBuffer.h"
#include "loader/DataParser.h"
#include "common/DataFormat.h"

#include "loader/NewValuesSetBase.h"


class NewValuesSubSet : public NewValuesSetBase
{
public:
	NewValuesSubSet(NewValuesSetBase *full, std::vector<uint> &_map, int _start_row, int _num_rows) // add sequence that represents clustered order
		: superset(full), map(_map), start_row(_start_row), num_rows(_num_rows)
	{

	}

	~NewValuesSubSet()
	{

	}

	bool IsNull(int ono)	{ return superset->IsNull(map[start_row+ono]); }

	_uint64 SumarizedSize()
	{
		int nov = num_rows;
		_uint64 size = 0;
		for(int i = 0; i < nov; i++)
			size += superset->GetObjSize(map[i+start_row]);
		if(superset->IsBinType() && superset->BinaryAsHex() ) 
			size /= 2;
		return size;
	}

	virtual uint Size(int ono) const
	{
		return superset->Size(map[start_row+ono]);
	}

	void GetIntStats(_int64& min, _int64& max, _int64& sum)
	{
		min = PLUS_INF_64;
		max = MINUS_INF_64;
		sum = 0;

		_int64 v = 0;
		for(int i = 0; i < num_rows; i++)
		{
			int index=map[start_row+i];
			if(!superset->IsNull(index))
			{
				v = *(_int64 *)superset->GetDataBytesPointer(index);
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
		int noo = num_rows;
		for(int i = 0; i < noo; i++)
		{
			int index=map[start_row+i];
			if(!superset->IsNull(index))
			{
				v = *(double*)superset->GetDataBytesPointer(index);
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
		int noo = num_rows;
		bool UTFConversions = superset->UTFConversions();
		for(int i = 0; i < noo; i++) {
			int index=map[start_row+i];
			if(!superset->IsNull(index)) {
				v.val = superset->GetDataBytesPointer(index);
				v.len = superset->Size(index);
				if(v.len > maxlen)
					maxlen = v.len;

				if(min.IsNull())
					min = v;
				else if(UTFConversions) {
					if(superset->UTFStrCmp(min, v) > 0)
						min = v;
				} else if(min > v)
					min = v;

				if(max.IsNull())
					max = v;
				else if(UTFConversions) {
					if(superset->UTFStrCmp(max, v) < 0)
						max = v;
				} else if(max < v)
					max = v;
			}
		}
	}

	bool UTFConversions() { return superset->UTFConversions(); }
	int UTFStrCmp(RCBString &a, RCBString &b) { return superset->UTFStrCmp(a, b); }

	char*	GetDataBytesPointer(int ono)			
	{ 
		return superset->GetDataBytesPointer( map[start_row + ono] );
	}
	
	int		NoValues()				{ return num_rows; }
private:
	NewValuesSetBase *superset;
	std::vector<uint> &map;
	int start_row,num_rows;
};

#endif
