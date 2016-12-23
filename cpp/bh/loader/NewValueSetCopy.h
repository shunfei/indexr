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

#ifndef _NEW_VALUE_SET_COPY_H_
#define _NEW_VALUE_SET_COPY_H_

#include "core/bintools.h"
#include "compress/tools.h"
#include "system/ChannelOut.h"
#include "system/LargeBuffer.h"
#include "loader/DataParser.h"
#include "common/DataFormat.h"

#include "loader/NewValuesSetBase.h"

class NewValuesSetCopy : public NewValuesSetBase
{
public:
	NewValuesSetCopy(DataParser *parser, int attr_no, int start_row, AttrPackType pt, DTCollation col)
		: col(col)
	{
		if(attr_no == -1)
			this->attr_no = parser->CurrentAttribute();
		else
			this->attr_no = attr_no;

		num_rows = parser->NoPrepared();
		nulls = parser->copy_nulls( start_row, start_row + num_rows );
		obj_sizes = parser->copy_obj_sizes( start_row, start_row + num_rows );
		//sizes = parser->copy_sizes( start_row, start_row + row_count );
		val_sizes = parser->copy_value_sizes( attr_no, start_row, start_row + num_rows );
		if( pt == PackN ) {
			num_values64 = parser->copy_num_values( start_row, start_row + num_rows );
			value_ptr = NULL;
		} else {
			num_values64 = NULL;
			value_ptr = parser->copy_values_ptr( start_row, start_row + num_rows );
		}
		attr_type = parser->CurrentAttributeType();
		attr_edf = parser->GetEDF();
		pack_type = parser->GetPackType(attr_no);
		needs_UTFCollation = RequiresUTFConversions(col);
	}

	~NewValuesSetCopy()
	{
		if( value_ptr != NULL ) delete[] value_ptr;
		delete[] nulls;
		delete[] obj_sizes;
		delete[] val_sizes;		
		if( num_values64 != NULL ) delete[] num_values64;
	}

	bool IsNull(int ono)	{ return nulls[ono] == 1; }

	_uint64 SumarizedSize()
	{
		int nov = num_rows;
		_uint64 size = 0;
		for(int i = 0; i < nov; i++)
			size += obj_sizes[i];
		if(ATI::IsBinType(attr_type) && DataFormat::GetDataFormat(attr_edf)->BinaryAsHex())
			size /= 2;
		return size;
	}

	virtual uint Size(int ono) const
	{
		return val_sizes[ono];
	}

	void GetIntStats(_int64& min, _int64& max, _int64& sum)
	{
		min = PLUS_INF_64;
		max = MINUS_INF_64;
		sum = 0;
		_int64 v = 0;
		for(uint i = 0; i < num_rows; i++)
		{
			if(nulls[i] != 1)
			{
				v = num_values64[i];
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
			if(nulls[i] != 1)
			{
				v = *(double*)&num_values64[i];
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
		bool UTFConversions = RequiresUTFConversions(col);
		for(int i = 0; i < noo; i++) {
			if(nulls[i] != 1) {
				v.val = GetDataBytesPointer(i);
				v.len = Size(i);
				if(v.len > maxlen)
					maxlen = v.len;

				if(min.IsNull())
					min = v;
				else if(UTFConversions) {
					if(CollationStrCmp(col, min, v) > 0)
						min = v;
				} else if(min > v)
					min = v;

				if(max.IsNull())
					max = v;
				else if(UTFConversions) {
					if(CollationStrCmp(col, max, v) < 0)
						max = v;
				} else if(max < v)
					max = v;
			}
		}
	}

	char*	GetDataBytesPointer(int ono)			
	{ 
		if( pack_type == PackN )
			return (char *)&num_values64[ono];
		else
			return value_ptr[ono]; 
	}
	
	int compare(int ono_a, int ono_b)
	{
		if( nulls[ono_a] == 1) {
			if( nulls[ono_b] == 1 ) 
				return 0;
			else
				return -1;
		}
		if( nulls[ono_b] == 1 )
			return 1;

		if( pack_type == PackN ) {
			_uint64 vx = num_values64[ono_a];
			_uint64 vy = num_values64[ono_b];

			if( vx < vy ) return -1;
			if( vx > vy ) return 1;
			return 0;
		}
			
		if( needs_UTFCollation ) {
			RCBString a(value_ptr[ono_a],val_sizes[ono_a]),
					  b(value_ptr[ono_b],val_sizes[ono_b]);
			return CollationStrCmp(col, a, b);
		} else {
			int len_a = val_sizes[ono_a];
			int len_b = val_sizes[ono_b];
			int ret = strncmp(value_ptr[ono_a], value_ptr[ono_b], 
							std::min(len_a,len_b));

			if( ret == 0 ) {
				if( len_a == len_b ) return 0;
				return (len_a < len_b ? -1 : 1);
			}			
			return ret;
		}
	}
	
	int		NoValues()				{ return num_rows; }

	bool IsBinType() { return ATI::IsBinType(attr_type);  }
	bool BinaryAsHex() { return DataFormat::GetDataFormat(attr_edf)->BinaryAsHex(); }
	uint GetObjSize(int ono) { return obj_sizes[ono]; }
	bool UTFConversions() { return RequiresUTFConversions(col); }
	int UTFStrCmp(RCBString &a, RCBString &b) { return CollationStrCmp(col, a, b); }

	void Nullify()
	{
		for(uint i = 0; i < num_rows; i++) 
			nulls[i] = 1;
	}
	
private:
	char **value_ptr;
	_int64* num_values64;

	char *nulls;
	int *obj_sizes;
	uint *val_sizes;
	uint num_rows;
	int attr_no;
	AttributeType attr_type;
	EDF attr_edf;
	AttrPackType pack_type;

	bool needs_UTFCollation;
	DTCollation col;
};

#endif
