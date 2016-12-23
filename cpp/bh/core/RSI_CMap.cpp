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

#include "RSI_CMap.h"

using namespace std;

RSIndex_CMap::RSIndex_CMap(int no_pos)
	: no_positions(no_pos), total_size(0), cmap_buffers()				// create an empty index
{
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	start_pack = 0;
	end_pack = 0;
}

RSIndex_CMap::~RSIndex_CMap()
{
	Deallocate();
}

void RSIndex_CMap::Deallocate()
{
	for ( cmap_buffers_t::iterator it(cmap_buffers.begin()), end(cmap_buffers.end()); it != end; ++it)
		dealloc(*it);
	cmap_buffers.clear();
}

void RSIndex_CMap::Clear()
{
	Deallocate();
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	no_positions = 64;
	start_pack = 0;
	end_pack = 0;
}

void RSIndex_CMap::Create(_int64 _no_obj, int no_pos)
{
	Deallocate();
	no_obj	= _no_obj;
	no_positions = no_pos;
	no_pack = bh::common::NoObj2NoPacks(no_obj);
	no_pack_declared = (no_pack / 32 + 1) * 32;
	Alloc(no_pack_declared);

	start_pack = 0;
	end_pack = no_pack - 1;

	//// below loop is for testing purpose only, should be deleted
	//for(int i = 0; i < no_pack; i++) {
	//	UnSet(i, 0, 0);
	//	UnSet(i, 255, 63);
	//}
}

inline int block_count( _int64 size, _int64 block_size )
{
	return (int)( ( size / block_size ) + ( ( size % block_size ) ? 1 : 0 ) );
}

void RSIndex_CMap::Alloc(_int64 new_no_pack_declared)
{
	int64 size = (int64)(new_no_pack_declared - start_pack) * no_positions * 32; // * 256 / 8 = 32
	if (size > total_size) {
		int old_bc(block_count(total_size, MAX_CMAP_BLOCK_SIZE));
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(old_bc == cmap_buffers.size());
		int bc(block_count(size, MAX_CMAP_BLOCK_SIZE));
		if (bc > old_bc) {
			cmap_buffers.resize(bc);
			if (old_bc > 0 && (total_size % MAX_CMAP_BLOCK_SIZE) != 0) {
				cmap_buffers[old_bc - 1] = static_cast<uint*>(rc_realloc(cmap_buffers[old_bc - 1], MAX_CMAP_BLOCK_SIZE, BLOCK_TEMPORARY));
				memset(cmap_buffers[old_bc - 1] + (total_size % MAX_CMAP_BLOCK_SIZE) / sizeof ( int ),
						255,
						MAX_CMAP_BLOCK_SIZE - (total_size % MAX_CMAP_BLOCK_SIZE) );
			}
			for (int i(old_bc); i < (bc - 1); ++i) {
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!cmap_buffers[i]);
				cmap_buffers[i] = static_cast<uint*>(alloc(MAX_CMAP_BLOCK_SIZE, BLOCK_TEMPORARY));
				memset(cmap_buffers[i], 255, MAX_CMAP_BLOCK_SIZE);
			}
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!cmap_buffers[bc-1]);
			size_t no_bytes_in_new_last_block = size % MAX_CMAP_BLOCK_SIZE;
			if(no_bytes_in_new_last_block == 0)
				no_bytes_in_new_last_block = MAX_CMAP_BLOCK_SIZE;
			cmap_buffers[bc-1] = static_cast<uint*>(alloc(no_bytes_in_new_last_block, BLOCK_TEMPORARY));
			memset(cmap_buffers[bc-1], 255, no_bytes_in_new_last_block);
		} else {
			cmap_buffers[bc-1] = static_cast<uint*>(rc_realloc(cmap_buffers[bc-1], size % MAX_CMAP_BLOCK_SIZE, BLOCK_TEMPORARY));
			memset(cmap_buffers[bc-1] + (total_size % MAX_CMAP_BLOCK_SIZE) / sizeof ( int ), 255, size - total_size);
		}
		total_size = size;
	}
}

uint* RSIndex_CMap::PackBuf(_int64 no_pack)
{
	int64 offset((no_pack - start_pack) * no_positions * 32);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(offset < total_size);
	int bno = (int)(offset / MAX_CMAP_BLOCK_SIZE);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(bno < cmap_buffers.size());
	int boff = int(offset % MAX_CMAP_BLOCK_SIZE);
	return (cmap_buffers[bno] + boff / sizeof ( int ));
}

void RSIndex_CMap::AppendKNs(int new_no_pack)
{
	BHASSERT(new_no_pack >= no_pack, "Shrinking CMAP is not allowed.");
	int new_no_pack_declared = (new_no_pack / 32 + 1) * 32; // now the minimal size of hist is below 4 KB (including header)
	Alloc( new_no_pack_declared );
	no_pack = new_no_pack;
	no_pack_declared = new_no_pack_declared;
	// start_pack remains unchanged
	end_pack = no_pack - 1;

}

void RSIndex_CMap::CopyRepresentation(void *buf, int pack)
{
	if( cmap_buffers.empty() || pack > no_pack)
		AppendKNs(pack+1);
	memcpy(PackBuf(pack), buf, no_positions * 32);	// used before actual update
}

void RSIndex_CMap::Update(_int64 _new_no_obj)		// enlarge buffers for the new number of objects
{
	uint new_no_pack = bh::common::NoObj2NoPacks(_new_no_obj);
	AppendKNs(new_no_pack);
	no_obj  = _new_no_obj;
}

bool RSIndex_CMap::UpToDate(_int64 cur_no_obj, int pack) // true, if this pack is up to date (i.e. either cur_obj==no_obj, or pack is not the last pack in the index)
{
	return (cur_no_obj == no_obj || ((pack < no_pack - 1) && (pack < (cur_no_obj / MAX_PACK_ROW_SIZE))));
}

void RSIndex_CMap::ClearPack(int pack)
{
	assert(pack >= start_pack && pack <= end_pack);
	if(!cmap_buffers.empty() && pack >= start_pack && pack <= end_pack)
		memset(PackBuf(pack), 0, no_positions * 32);
	// below code is for testing purpose only, should be commented
	//Set(pack, 0, 0);
	//Set(pack, 255, 63);
}

//void RSIndex_CMap::SetPack(int pack)
//{
//	assert(pack >= start_pack && pack <= end_pack);
//	if(!cmap_buffers.empty() && pack >= start_pack && pack <= end_pack)
//		memset(PackBuf(pack), 255, no_positions * 32);
//}

void RSIndex_CMap::Load(IBFile* frs_index, int current_read_loc)
{
	// current_read_loc: 0 or 1, determines which version of header and last pack should be loaded
	// File format:
	// Header0:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// Header1:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// LastPack0:	no_positions * 256 / 8 bytes
	// LastPack1:	no_positions * 256 / 8 bytes
	// <table_t>									- (no_pack - 1) * no_positions * 256 / 8 bytes

	Deallocate();
	bool zero = current_read_loc == 0;
	unsigned char header[14];
	_int64 bytes(frs_index->Seek(zero ? 0 : 14, SEEK_SET));
	bytes = frs_index->ReadExact(header, 14);
	no_obj	= *(_int64 *)(header + 1);
	no_pack	= *(uint *)(header + 9);
	no_pack_declared = (no_pack / 32 + 1) * 32;
	no_positions = uint(header[13]);
	start_pack = 0;
	end_pack = no_pack - 1;
	if(zero) // skip Header1
		bytes = frs_index->Seek(14, SEEK_CUR);
	else // skip LastPack0
		bytes = frs_index->Seek(no_positions * 32, SEEK_CUR);
	if(no_pack * no_positions > 0) {
		Alloc(no_pack_declared);
		// read last pack
		bytes = frs_index->ReadExact(PackBuf(no_pack - 1), no_positions * 32);
		if(zero) // skip LastPack1
			bytes = frs_index->Seek(no_positions * 32, SEEK_CUR);
		// read all packs but last
		_int64 to_read(((_int64)no_pack - 1) * no_positions * 32);
		int block(0);
		while(to_read > 0) {
			if(to_read > MAX_CMAP_BLOCK_SIZE) {
				frs_index->ReadExact(cmap_buffers[block], MAX_CMAP_BLOCK_SIZE);
				to_read -= MAX_CMAP_BLOCK_SIZE;
			} else {
				frs_index->ReadExact(cmap_buffers[block], (uint)to_read);
				to_read = 0;
			}
			++block;
		}
		for(int i(0); i < (no_pack_declared - no_pack); ++i)
			memset(PackBuf(no_pack + i), 255, no_positions * 32);
	}
}

void RSIndex_CMap::LoadLastOnly(IBFile* frs_index, int current_read_loc)
{
	// function reads the header and allocates memory for future packs.
	// Last pack is not read from disc since it will be generated from scratch
	// all values of last pack are set to 255
	//
	// current_read_loc: 0 or 1, determines which version of header and last pack should be loaded
	// File format:
	// Header0:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// Header1:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// LastPack0:	no_positions * 256 / 8 bytes
	// LastPack1:	no_positions * 256 / 8 bytes
	// <table_t>									- (no_pack - 1) * no_positions * 256 / 8 bytes

	Deallocate();
	unsigned char header[14];

	bool zero = current_read_loc == 0;

	_int64 bytes(frs_index->Seek(zero ? 0 : 14, SEEK_SET));
	bytes = frs_index->ReadExact(header, 14);
	no_obj	= *(_int64 *)(header + 1);
	no_pack	= *(int *)(header + 9);
	no_pack_declared = (no_pack / 32 + 1) * 32;
	no_positions = uint(header[13]);
	if(no_pack * no_positions > 0) {
		end_pack = start_pack = no_pack - 1;
		if(zero) // skip Header1
			bytes = frs_index->Seek(14, SEEK_CUR);
		else // skip LastPack0
			bytes = frs_index->Seek(no_positions * 32, SEEK_CUR);
		Alloc(no_pack_declared);
		// read last pack
		bytes = frs_index->ReadExact(cmap_buffers[0], no_positions * 32);
		BHASSERT(bytes == no_positions * 32,"corrupt cmap file");
	}
}

void RSIndex_CMap::Save(IBFile* frs_index, int current_save_loc)
{
	// File format:
	// Header0:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// Header1:	<version><no_obj><no_pack><no_positions>	- 1+8+4+1 = 14 bytes
	// LastPack0:	no_positions * 256 / 8 bytes
	// LastPack1:	no_positions * 256 / 8 bytes
	// <table_t>									- (no_pack - 1) * no_positions * 256 / 8 bytes
	unsigned char header[14];
	header[0]				= 1u; // version number
	*(_int64*)(header + 1)	= no_obj;
	*(int*) (header + 9)	= no_pack;
	header[13]				= (unsigned char)no_positions;

	bool zero = current_save_loc == 0;

	frs_index->Seek(zero ? 0 : 14, SEEK_SET);
	frs_index->WriteExact(header, 14);

	if(zero) // skip Header1
		frs_index->Seek(14, SEEK_CUR);
	else // skip LastPack0
		frs_index->Seek(no_positions * 32, SEEK_CUR);

	if(!cmap_buffers.empty()) {
		// write LastPack
		frs_index->WriteExact(PackBuf(no_pack - 1), no_positions * 32);
		if(zero) // skip LastPack1
			frs_index->Seek(no_positions * 32, SEEK_CUR);

		// write all the other packs
		_int64 offset = static_cast<_int64>( start_pack ) * no_positions * 32; // position (32bit) of last pack
		frs_index->Seek(offset, SEEK_CUR);
		_int64 to_write((no_pack - static_cast<_int64>(start_pack) - 1) * no_positions * 32);
		int block(0);
		while ( to_write > 0 ) {
			if (to_write > MAX_CMAP_BLOCK_SIZE) {
				frs_index->WriteExact(cmap_buffers[block], MAX_CMAP_BLOCK_SIZE);
				to_write -= MAX_CMAP_BLOCK_SIZE;
			} else {
				frs_index->WriteExact(cmap_buffers[block], (uint)to_write);
				to_write = 0;
			}
			++block;
		}
		to_write = ((no_pack - static_cast<_int64>(start_pack) - 1) * no_positions * 32);
	}
}

int RSIndex_CMap::NoOnes(int pack, uint pos)
{
	assert(pack >= start_pack && pack <= end_pack);
	int d = 0, beg = 0;
	uint* packBeg(PackBuf(pack) + pos * 32 / sizeof(int));
	for(int i = 0; i < 32 / sizeof(int); i++) {
		d += CalculateBinSum(packBeg[i]);
	}
	return d;
}

//////////////////////////////////////////////////////////////////

RSValue RSIndex_CMap::IsValue(RCBString min_v, RCBString max_v, int pack)
{
	// Results:		RS_NONE - there is no objects having values between min_v and max_v (including)
	//				RS_SOME - some objects from this pack may have values between min_v and max_v
	//				RS_ALL	- all objects from this pack do have values between min_v and max_v

	assert(pack >= start_pack && pack <= end_pack);

	if(min_v == max_v) {
		uint size = min_v.size() < no_positions ? (uint)min_v.size() : no_positions;
		for(uint pos = 0; pos < size; pos++) {
			if(!IsSet(pack, (unsigned char)min_v[pos], pos))
				return RS_NONE;
		}
		return RS_SOME;
	} else {
		// TODO: may be further optimized
		unsigned char f = 0, l = 255;
		if(min_v.len > 0) 
			f = (unsigned char)min_v[0];		// min_v.len == 0 usually means -inf
		if(max_v.len > 0) 
			l = (unsigned char)max_v[0];
		if(f > l || !IsAnySet(pack, f, l, 0))
			return RS_NONE;
		return RS_SOME;
	}
}

RSValue	RSIndex_CMap::IsLike(RCBString pattern, int pack, char escape_character)
{
	// we can exclude cases: "ala%..." and "a_l_a%..."
	char *p = pattern.val;		// just for short...
	uint pos = 0;
	while(pos < pattern.len && pos < (uint)no_positions) {
		if(p[pos] == '%' || p[pos] == escape_character)
			break;
		if(p[pos] != '_' && !IsSet(pack, p[pos], pos))
			return RS_NONE;
		pos++;
	}
	return RS_SOME;
}

void RSIndex_CMap::PutValue(RCBString& v, int pack)		// set information that value v does exist in this pack
{
	if(v.IsNullOrEmpty())
		return;
	uint size = v.size() < no_positions ? (uint)v.size() : no_positions;

	assert(pack >= start_pack && pack <= end_pack);
	for(uint i = 0; i < size; i++) {
		Set(pack, (unsigned char)v[i], i);
		//PackBuf(pack)[ i * 32 / sizeof(int) + c / 32] |= ( 0x00000001u << (c % 32) );
	}
}

//bool RSIndex_CMap::Intersection(int pack, RSIndex_CMap *sec, int pack2)
//{
//	// we may assume that min-max of packs was already checked
//	bool set1, set2;
//	bool any_set = false;
//	for(int v = 0; v <= 255; v++) {
//		// if there is at least one common char on first position return true
//		set1 = IsSet(pack, v, 0);
//		set2 = sec->IsSet(pack2, v, 0);
//		if( set1 && set2 )
//			return true;
//		if( set1 || set2 )
//			any_set = true;
//	}
//	if( !any_set )
//		return true;	// special case: no characters at all in both packs (e.g. CMAP empty because of common prefix)
//	return false;		// no intersection possible - all values do not match the second histogram
//}
//
//void RSIndex_CMap::Display(int pack, unsigned char c)
//{
//	if(pack < start_pack || pack > end_pack) {
//		cout << "Invalid pack: " << pack << endl;
//		return;
//	}
//	uint int_pack_size = no_positions * 32 / sizeof(int);
//	uint int_col_size = 32 / sizeof(int);
//	cout << c << ": ";
//	for(uint i = 0; i < no_positions; i++)
//		cout << ( (int)IsSet(pack, c, i) ) << ( (i + 1) % 8 == 0 ? " " : "");
//	cout << endl;
//}

bool RSIndex_CMap::IsSet(int pack, unsigned char c, uint pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!cmap_buffers.empty());
	//uint int_pack_size = no_positions * 32 / sizeof(int);
	//uint int_col_size = 32 / sizeof(int);
	//uint n = t[pack * int_pack_size + pos * int_col_size + c / 32]; // old code, probably unusable
	assert(pack >= start_pack && pack <= end_pack);
	return ( (PackBuf(pack)[pos * 32 / sizeof(int) + c / 32] >> (c % 32)) % 2 == 1 ) ;
}

// true, if there is at least one 1 in [first, last]
bool RSIndex_CMap::IsAnySet(int pack, unsigned char first, unsigned char last, uint pos)
{	// TODO: could be done more efficiently
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(first <= last);
	for(int c = first; c <= last; c++)
		if(IsSet(pack, c, pos))
			return true;
	return false;
}

void RSIndex_CMap::Set(int pack, unsigned char c, uint pos)
{
	//uint int_pack_size = no_positions * 32 / sizeof(int);
	//uint int_col_size = 32 / sizeof(int);
	assert(pack >= start_pack && pack <= end_pack);
	PackBuf(pack)[pos * 32 / sizeof(int) + c / 32] |= ( 0x00000001u << (c % 32) );
}

//void RSIndex_CMap::UnSet(int pack, unsigned char c, uint pos)
//{
//	//uint int_pack_size = no_positions * 32 / sizeof(int);
//	//uint int_col_size = 32 / sizeof(int);
//	assert(pack >= start_pack && pack <= end_pack);
//	PackBuf(pack)[pos * 32 / sizeof(int) + c / 32] |= ( 0x00000000u << (c % 32) );
//}

void RSIndex_CMap::AppendKN(int pack, RSIndex_CMap* cmap, uint no_values_to_add)
{
	uint no_objs_to_add_to_last_DP = uint(no_obj % MAX_PACK_ROW_SIZE ? MAX_PACK_ROW_SIZE - no_obj % MAX_PACK_ROW_SIZE : 0);
	if(pack < cmap->no_pack) {
		AppendKNs(no_pack + 1);
		memcpy(PackBuf(no_pack -1), cmap->PackBuf(pack), no_positions * 32);
		no_obj  += no_objs_to_add_to_last_DP + no_values_to_add;
	}
}



