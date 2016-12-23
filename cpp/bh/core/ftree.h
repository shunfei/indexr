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

//////////////// Ftree - dictionary for strings ////////////////////////////

#ifndef __FTREE__
#define __FTREE__

#include "bintools.h"
#include "../types/RCDataTypes.h"

#include "../system/MemoryManagement/TrackableObject.h"

class FTree
	: public TrackableObject
{
public:

	FTree();
	FTree(const FTree &ft);		
	~FTree();
	std::auto_ptr<FTree> Clone() const;

	void Init(int width);

	RCBString GetRealValue(int v);
	char *GetBuffer(int v);			// the pointer to the memory place of the real value

	int GetEncodedValue(const RCBString&);	// return -1 when value not found

	int Add(const RCBString&);				// return -1 when len=0

	int CountOfUniqueValues();

	int ByteSize();							// number of bytes required to compress the dictionary
	void SaveData(unsigned char *&buf);
	void Init(unsigned char *&buf);

	int ValueSize()			{ return value_size; }
	int ValueSize(int i)	{ return len[i]; }
	int MaxValueSize()		{ return MaxValueSize(0, dic_size - 1); }
	int MaxValueSize(int start, int end);				// max. value size for an interval of codes

	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_FTREE;}
	void Release();
	int CheckConsistency();			// 0 = dictionary consistent, otherwise corrupted
private:
	char *mem;			// dictionary: dic_size*value_size bytes. Values are NOT terminated by anything and may contain any character (incl. 0)
	unsigned short *len;// table of lengths of values

	void InitHash();
	int HashFind(char* v, int v_len, int position_if_not_found = -1);	// return a position or -1 if not found;

	int *value_offset;	// mem + value_offset[v] is an address of value v

	int dic_size;		// number of different values
	int total_dic_size;	// may be more than dic_size, because we reserve some place for new values
	int total_buf_size;	// in bytes, the current size of value buffer
	int last_code;		// last code used
	int value_size;		// max. length of one string (limit)
	int max_value_size;	// max. length of a string found in dictionary

	// Compression section - not used yet
	int changed,compressed_size,comp_mode;

	// Hash section

	int*	hash_table;
	int		hash_size;

	void Destroy();
};

#endif

