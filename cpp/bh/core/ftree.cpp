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

#include "system/RCSystem.h"
#include "ftree.h"

using namespace std;
/////////////////////////////////////////////////////////////////////

FTree::FTree()
{
	mem = NULL;
	len = NULL;
	value_size = 0;
	last_code = -1;
	dic_size = 0;
	total_dic_size = 0;
	changed = 0;
	compressed_size = 0;
	comp_mode = -1;
	total_buf_size = 0;
	value_offset = NULL;
	hash_table = NULL;
	hash_size = 0;
	max_value_size = 0;
}

FTree::FTree(const FTree &ft) :
	mem(0), len(0), value_offset(0), dic_size(ft.dic_size),
	total_dic_size(ft.total_dic_size), 	total_buf_size(ft.total_buf_size), last_code(ft.last_code),		
	value_size(ft.value_size), max_value_size(ft.max_value_size), changed(ft.changed),
	compressed_size(ft.compressed_size), comp_mode(ft.comp_mode), hash_table(0), hash_size(ft.hash_size)
{
	
	//cerr << "FTree::FTree(const FTree &ft) ft.GetSizeInBytes() = " << ft.GetSizeInBytes() << endl;

	mem = (char*) alloc(total_buf_size, BLOCK_TEMPORARY, true);
	len = (unsigned short *) alloc(total_dic_size * sizeof(unsigned short), BLOCK_TEMPORARY, true);
	value_offset = (int *) alloc(total_dic_size * sizeof(int), BLOCK_TEMPORARY, true);
	
	if(total_buf_size > 0 && (!mem || !len || !value_offset) ) {
		Destroy();
		rclog << lock << "Error: FTree, out of memory." << unlock;
		throw OutOfMemoryRCException();
	}

	memcpy(mem, ft.mem, total_buf_size); 	
	memcpy(len, ft.len, dic_size * sizeof(unsigned short));	
	memcpy(value_offset, ft.value_offset, dic_size * sizeof(int));	

	if(ft.hash_table)
		hash_table = (int *) alloc(hash_size * sizeof(int), BLOCK_TEMPORARY, true);

	if (ft.hash_table && !hash_table) {
		Destroy();
		rclog << lock << "Error: FTree, out of memory." << unlock;
		throw OutOfMemoryRCException();
	}

	if(ft.hash_table)
		memcpy(hash_table, ft.hash_table, hash_size * sizeof(int));
}

void FTree::Destroy()
{
	//cerr << "FTree::Destroy() GetSizeInBytes() = " << GetSizeInBytes() << endl;

	if (mem) {
		dealloc(mem);
		mem = NULL;
	}

	if (len) {
		dealloc(len);
		len = NULL;
	}

	if (value_offset) {
		dealloc(value_offset);
		value_offset = NULL;
	}

	if (hash_table) {
		dealloc(hash_table);
		hash_table = NULL;
	}
}

void 
FTree::Release()
{
	//if(owner) owner->DropObjectByMM(_coord);
}

FTree::~FTree()
{
	Destroy();
}

std::auto_ptr<FTree> FTree::Clone() const
{
	//cerr << "FTree::Clone() GetSizeInBytes() = ??? " << endl;
	return std::auto_ptr<FTree>(new FTree(*this) );
}

RCBString FTree::GetRealValue(int v)
{
	if(v >= 0 && v < dic_size) {
		if(len[v] == 0)
			return RCBString("", 0);
		return RCBString((mem + value_offset[v]), len[v]);
	}
	return RCBString();
}

char *FTree::GetBuffer(int v)
{
	if(v >= 0 && v < dic_size)
		return (mem + value_offset[v]);
	return NULL;
}

int FTree::GetEncodedValue(const RCBString &str)
{
	if(mem == NULL) 
		return -1;
	assert(value_size > 0);
	int local_last_code = last_code;	// for multithread safety

	if(local_last_code > -1 &&
		str.len == len[local_last_code] &&
		memcmp(str.val, (mem + value_offset[local_last_code]),str.len)==0 ) return local_last_code;
	return HashFind(str.val, str.len);
}

void FTree::Init(int width)
{
	value_size = width;
	dic_size = 0;
	total_dic_size = 10;	// minimal dictionary size
	total_buf_size = value_size * 10 + 10;
	if(mem) dealloc(mem);
	mem = (char*)alloc(total_buf_size*sizeof(char), BLOCK_TEMPORARY, true);
	if( len == NULL )
		len = (unsigned short *) alloc(total_dic_size * sizeof(unsigned short), BLOCK_TEMPORARY, true);
	if( value_offset == NULL )
		value_offset = (int *) alloc(total_dic_size * sizeof(int), BLOCK_TEMPORARY, true);

	if(!mem) { 
		rclog << lock << "Error: FTree:Init out of memory (" << total_buf_size << " bytes failed). (11)" << unlock;
		throw OutOfMemoryRCException();
	}
	memset(mem, 0, total_buf_size);
	last_code = -1;
	changed = 0;
}

int FTree::Add(const RCBString &str)
{
	assert(value_size > 0);
	if(dic_size > 214748300)		// limit of hash size (and other tables)
		throw(OutOfMemoryRCException("Too many lookup values"));

	int code = 	HashFind(str.val, str.len, dic_size);
	if(code < dic_size) 
		return code;				// Found? Do not add. Not found? The first unused number returned.
	changed = 1;
	if(total_dic_size < dic_size + 1) {			// Enlarge tables, if required
		int new_dic_size = int((total_dic_size + 10) * 1.2);
		if(new_dic_size > 536870910)
			new_dic_size = total_dic_size + 10;
		len = (unsigned short*)rc_realloc(len, new_dic_size * sizeof(ushort), BLOCK_TEMPORARY);
		value_offset = (int*)rc_realloc(value_offset, new_dic_size * sizeof(int), BLOCK_TEMPORARY);
		if(len == NULL || value_offset == NULL)
			throw(OutOfMemoryRCException("Too many lookup values"));
		for(int i = total_dic_size; i < new_dic_size; i++) {
			len[i] = 0;
			value_offset[i] = NULL_VALUE_32;
		}
		total_dic_size = new_dic_size;
	}
	int new_value_offset = (dic_size == 0 ? 0 : value_offset[dic_size - 1] + len[dic_size - 1]);
	if(total_buf_size < new_value_offset + int(str.len)) {			// Enlarge tables, if required
		_int64 new_buf_size = _int64((_int64(total_buf_size) + str.len + 10) * 1.2);
		if(new_buf_size > 2147483647)		// 2 GB: a limit for integer offsets and a reasonable dictionary size
			new_buf_size = _int64(total_buf_size) + str.len;
		if(new_buf_size > 2147483647)
			throw(OutOfMemoryRCException("Too many lookup values"));
		mem = (char*)rc_realloc(mem, new_buf_size, BLOCK_TEMPORARY);
		memset(mem + total_buf_size,0,new_buf_size - total_buf_size);
		total_buf_size = int(new_buf_size);
	}
	if(str.len > 0) 
		memcpy(mem + new_value_offset, str.val, str.len);
	value_offset[dic_size] = new_value_offset;
	len[dic_size] = str.len;
	if(str.len > max_value_size)
		max_value_size = str.len;
	dic_size++;
	return dic_size - 1;
}

int FTree::MaxValueSize(int start, int end)				// max. value size for an interval of codes
{
	if(end - start > dic_size / 2 || end - start > 100000)
		return max_value_size;
	unsigned short max_size = 0;
	for(int i = start; i <= end; i++)
		if(max_size < len[i])
			max_size = len[i];
	return max_size;
}

int FTree::CountOfUniqueValues()
{
	return dic_size;
}

/////////////////

int FTree::ByteSize()
{
	if(changed == 0 && compressed_size > 0) 
		return compressed_size;
	// header format: <format><dic_size><value_size><compressed_size><comp_mode><...len...>
	int header_size = 12 + 2 * dic_size;
	comp_mode = -1;
	if(dic_size == 0) 
		return header_size;

	int len_sum = 0;
	for(int i = 0; i < dic_size; i++) 
		len_sum += len[i];

	// format 0:
	compressed_size = header_size + len_sum;
	return compressed_size;
}

//////////////////
//  Save formats:
//
//  <format><dic_size><value_size><compressed_size><comp_mode> <len>...<len> <data>
//
//  where:
//		format=0:
//			<data>= <val1><val2>...<val_n>		- flat file; <val_i> takes exactly len[i] bytes
//

void FTree::SaveData(unsigned char *&buf)
{
	*buf=0;				// current format
	buf++;
	*((int*)buf)=dic_size;
	buf+=4;
	*((unsigned short int*)buf)=value_size;
	buf+=2;
	*((int*)buf)=compressed_size;
	buf+=4;
	*((char*)buf)=char(comp_mode);
	buf++;
	if(dic_size==0) return;
	for(int i=0;i<dic_size;i++)
	{
		if (len[i] > value_size)
			throw InternalRCException("Invalid length of a lookup value");
		*((unsigned short int*)buf)=len[i];
		buf+=2;
	}
	int len_sum = (dic_size > 0 ? value_offset[dic_size - 1] + len[dic_size - 1] : 0);
	memcpy(buf, mem, len_sum);
	//cerr << "FTree::SaveData() GetSizeInBytes() = " << GetSizeInBytes() << endl;
}

void FTree::Init(unsigned char *&buf)
{
	int format = (*buf);
	buf++;
	if(format != 0) {
		rclog << lock << "Error: save mode " << format << " not implemented (FTree)" << unlock;
		return;
	}
	dic_size = *((int*) buf);
	total_dic_size = dic_size;
	buf += 4;
	value_size = *((unsigned short int*) buf);
	buf += 2;
	compressed_size = *((int*) buf);
	buf += 4;
	comp_mode = *((char*) buf);
	buf++;

	int i;

	if(len) {
		dealloc(len);
		len = 0;
	}

	if(value_offset) {
		dealloc(value_offset);
		value_offset = 0;
	}

	len = (ushort*) alloc(total_dic_size * sizeof(ushort), BLOCK_TEMPORARY, true);
	value_offset = (int*) alloc(total_dic_size * sizeof(int), BLOCK_TEMPORARY, true);

	if(!len || !value_offset) {
		rclog << lock << "Error: FTree::Init, out of memory (" << total_dic_size * 6 << " bytes failed). (13a)" << unlock;
		throw OutOfMemoryRCException();
	}
	
	max_value_size = 0;
	unsigned short int loc_len;
	for(i = 0; i < dic_size; i++) {
		loc_len = *((unsigned short int*) buf);
		len[i] = loc_len;
		if(loc_len > max_value_size)
			max_value_size = loc_len;
		value_offset[i] = (i == 0 ? 0 : value_offset[i - 1] + len[i - 1]);
		buf += 2;
	}

	int len_sum = (dic_size > 0 ? value_offset[dic_size - 1] + len[dic_size - 1] : 0);
	dealloc(mem);
	mem = 0;

	if(dic_size < 3)
		total_buf_size = value_size * 10 + 10;
	else
		total_buf_size = len_sum;

	mem = (char*)alloc(total_buf_size * sizeof(char), BLOCK_TEMPORARY, true);
	if(!mem) {
		rclog << lock << "Error: FTree::Init, out of memory (" << total_buf_size * sizeof(char) << " bytes failed). (13)" << unlock;
		throw OutOfMemoryRCException();
	}
	memset(mem, 0, total_buf_size);
	memcpy(mem, buf, len_sum);
	last_code = -1;
	changed = 0;
	//cerr << "FTree::Init() GetSizeInBytes() = " << GetSizeInBytes() << endl;
}

////////////////// Hash part //////////////////////////////

void FTree::InitHash()
{
	// Note: from the limitation below, we cannot use more than dic_size = 214748300
	hash_size = int(dic_size < 30 ? 97 : (dic_size + 10) * 2.5);
	while(hash_size % 2 == 0 || hash_size % 3 == 0 || hash_size % 5 == 0 || hash_size % 7 == 0)
		hash_size++;
	dealloc(hash_table);
	hash_table = (int *)alloc(hash_size * sizeof(int), BLOCK_TEMPORARY);	// 2 GB max.
	if(hash_table == NULL)
		throw(OutOfMemoryRCException("Too many lookup values"));

	memset(hash_table, 0xFF, hash_size * sizeof(int));			// set -1 for all positions
	for(int i = 0; i < dic_size; i++)
		HashFind(mem + value_offset[i], len[i], i);
}

int FTree::HashFind(char* v, int v_len, int position_if_not_found) 
{
	if(hash_table == NULL)
		InitHash();
	unsigned int crc_code = (v_len == 0 ? 0 : HashValue((const unsigned char*)v, v_len));
	int row = crc_code % hash_size;
	int step = 3 + crc_code % 8;			// step: 3, 4, 5, 6, 7, 8, 9, 10  => hash_size should not be dividable by 2, 3, 5, 7
	while(step != 0) {						// infinite loop
		int hash_pos = hash_table[row];
		if(hash_pos == -1) {
			if(position_if_not_found > -1) {
				if(dic_size > hash_size * 0.9) {			// too many values, hash table should be rebuilt
					InitHash();
					HashFind(v, v_len, position_if_not_found);	// find again, as hash_size changed
				} else
					hash_table[row] = position_if_not_found;
				return position_if_not_found;
			}
			return -1;
		}
		if(len[hash_pos] == v_len && (v_len == 0 || memcmp(mem + value_offset[hash_pos], v, v_len) == 0))
			return hash_pos;
		row = (row + step) % hash_size;
	}
	return -1;		// should never reach this line
}

int FTree::CheckConsistency()
{
	if (dic_size==0)
		return 0;
	for (int i=0; i<dic_size; i++)
		if (len[i] > value_size)
			return 1;
	map<uint, set<int> > crc_to_pos;
	for (int i=0; i<dic_size; i++)
		crc_to_pos[HashValue((const unsigned char*)(mem + value_offset[i]), len[i])].insert(i);
	map<uint, set<int> >::const_iterator map_it;
	set<int>::const_iterator it1, it2;
	for (map_it=crc_to_pos.begin(); map_it!=crc_to_pos.end(); map_it++) {
		const set<int>& pos_set = map_it->second;
		for (it1=pos_set.begin(); it1!=pos_set.end(); it1++)
			for (it2=pos_set.begin(); it2!=pos_set.end(); it2++)
				if (*it1!=*it2 && len[*it1]==len[*it2])
					if (memcmp(mem + value_offset[*it1], mem + value_offset[*it2], len[*it1]) == 0)
						return 2;
	}
	return 0;
}

