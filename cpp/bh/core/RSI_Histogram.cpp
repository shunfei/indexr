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

#include <limits>
#include "RSI_Histogram.h"

using namespace std;



RSIndex_Hist::RSIndex_Hist()				// create an empty index
{
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	start_pack = 0;
	end_pack = 0;
	fixed = false;
}

RSIndex_Hist::~RSIndex_Hist()
{
	Deallocate();
}

void RSIndex_Hist::Clear()
{
	Deallocate();
	no_obj = 0;
	no_pack = 0;
	no_pack_declared = 0;
	fixed = false;
	start_pack = 0;
	end_pack = 0;
}

void RSIndex_Hist::Create(_int64 _no_obj, bool _fixed)
{
	Clear();
	fixed	= _fixed;
	no_obj	= _no_obj;
	Alloc(bh::common::NoObj2NoPacks(no_obj));
	start_pack = 0;
	end_pack = no_pack - 1;
}

void RSIndex_Hist::Alloc(uint64 new_no_pack)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_no_pack >= no_pack && new_no_pack > 0);
	if(new_no_pack > no_pack_declared) {

		uint64 new_no_pack_declared = (new_no_pack / 32 + 1) * 32;

		uint64 old_bc = no_pack_declared > 0 ? BlockCount(no_pack_declared - start_pack) : 0;
		uint64 new_bc = BlockCount(new_no_pack_declared - start_pack);

		hist_buffers.resize(new_bc);

		if(old_bc > 0) {
			// extend last block
			uint64 new_no_pack_in_last_block = new_bc > old_bc ? NO_HISTS_IN_BLOCK : NoPacksInLastBlock(new_no_pack_declared - start_pack);
			uint64 old_no_pack_in_last_block = NoPacksInLastBlock(no_pack_declared - start_pack);

			hist_buffers[old_bc - 1] = (uint*)(rc_realloc(hist_buffers[old_bc - 1], new_no_pack_in_last_block * RSI_HIST_SIZE, BLOCK_TEMPORARY));
			memset(hist_buffers[old_bc - 1] + old_no_pack_in_last_block * RSI_HIST_INT_RES, 255, (new_no_pack_in_last_block - old_no_pack_in_last_block) * RSI_HIST_SIZE);
		}

		// Create new full blocks
		for(uint64 i = old_bc; i < new_bc - 1; ++i) {
			hist_buffers[i] = (uint*)alloc(MAX_HIST_BLOCK_SIZE, BLOCK_TEMPORARY);
			memset(hist_buffers[i], 255, MAX_HIST_BLOCK_SIZE);
		}

		// Create last block
		if(new_bc > old_bc) {
			hist_buffers[new_bc - 1] = (uint*)alloc((size_t)NoPacksInLastBlock(new_no_pack_declared - start_pack) * RSI_HIST_SIZE, BLOCK_TEMPORARY);
			memset(hist_buffers[new_bc - 1], 255, (size_t)NoPacksInLastBlock(new_no_pack_declared - start_pack) * RSI_HIST_SIZE);
		}

		no_pack_declared = (int)new_no_pack_declared;
	}
	no_pack = (int)new_no_pack;
}

uint* RSIndex_Hist::PackBuf(uint64 pack_no) const
{
	return hist_buffers[(pack_no - start_pack) / NO_HISTS_IN_BLOCK] + ((pack_no - start_pack) % NO_HISTS_IN_BLOCK) * RSI_HIST_INT_RES;
}

void RSIndex_Hist::Deallocate()
{
	for(hist_buffers_t::iterator it(hist_buffers.begin()), end(hist_buffers.end()); it != end; ++it)
		dealloc(*it);
	hist_buffers.clear();
}

void RSIndex_Hist::AppendKNs(int new_no_pack)
{
	BHASSERT(new_no_pack >= no_pack, "Shrinking histogram is not allowed.");
	Alloc(new_no_pack);
	// start_pack remains unchanged
	end_pack = no_pack - 1;

}

void RSIndex_Hist::Update(_int64 _new_no_obj)		// enlarge buffers for the new number of objects
{
	AppendKNs(bh::common::NoObj2NoPacks(_new_no_obj));
	no_obj  = _new_no_obj;
}

bool RSIndex_Hist::UpToDate(_int64 cur_no_obj, int pack)	// true, if this pack is up to date (i.e. either cur_obj==no_obj, or pack is not the last pack in the index)
{
	return (cur_no_obj == no_obj || ((pack < no_pack - 1) && (pack < (cur_no_obj / MAX_PACK_ROW_SIZE))));
}

void RSIndex_Hist::ClearPack(int pack)
{
//	assert(pack >= start_pack && pack <= end_pack);			// too restrictive?
	if(!hist_buffers.empty() && pack >= start_pack && pack <= end_pack)
		memset(PackBuf(pack), 0, RSI_HIST_SIZE);	// used before actual update
}

void RSIndex_Hist::CopyRepresentation(void* buf, int pack)
{
	if(hist_buffers.empty() || pack > end_pack)
		AppendKNs(pack+1);

	memcpy(PackBuf(pack), buf, RSI_HIST_SIZE); // used before actual update
}

void RSIndex_Hist::Load(IBFile *frs_index, int current_read_loc)
{
	// Old file format:
	// Header:	<no_obj><no_pack><int_res><fixed>		- 8+4+1+1 = 14 bytes
	//			<table_t>								- no_pack*int_res*sizeof(int) bytes
	// File format:
	// Header0:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// Header1:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// LastPack0: RSI_HIST_SIZE bytes
	// LastPack1: RSI_HIST_SIZE bytes
	// <table_t>: (no_pack - 1) * RSI_HIST_SIZE bytes

	Clear();

	// Check which version of file is being loaded

	frs_index->Seek(8, SEEK_SET);
	frs_index->ReadExact(&no_pack, 4);
	frs_index->Seek(0, SEEK_END);

	bool zero = current_read_loc == 0;
	unsigned char header[15];
	frs_index->Seek(zero ? 0 : 15, SEEK_SET);
	frs_index->ReadExact(header, 15);
	no_obj	= *(_int64*)(header + 1);
	no_pack	= *(int*)(header + 9);
	fixed	= (header[14] > 0);
	end_pack = no_pack - 1;
	if(zero) // skip Header1
		frs_index->Seek(15, SEEK_CUR);
	else // skip LastPack0
		frs_index->Seek(RSI_HIST_SIZE, SEEK_CUR);
	if((uint64)no_pack > 0) {
		// allocate memory for no_pack_declared packs
		Alloc(no_pack);
		// read last pack
		frs_index->ReadExact(PackBuf(no_pack - 1), RSI_HIST_SIZE);
		if(zero) // skip LastPack1
			(int)frs_index->Seek(RSI_HIST_SIZE, SEEK_CUR);
		// read all packs but last

		_int64 to_read = ((uint64)no_pack - 1) * RSI_HIST_SIZE;
		int block(0);
		while ( to_read > 0 ) {
			if (to_read > MAX_HIST_BLOCK_SIZE) {
				frs_index->ReadExact(hist_buffers[block], MAX_HIST_BLOCK_SIZE);
				to_read -= MAX_HIST_BLOCK_SIZE;
			} else {
				frs_index->ReadExact(hist_buffers[block], (uint)to_read);
				to_read = 0;
			}
			++block;
		}
	}
}

void RSIndex_Hist::LoadLastOnly(IBFile *frs_index, int current_read_loc)
{
	// If file is in old format all packs must be loaded!

	// Old file format:
	// Header:	<no_obj><no_pack><int_res><fixed>		- 8+4+1+1 = 14 bytes
	//			<table_t>								- no_pack*int_res*sizeof(int) bytes
	// File format:
	// Header0:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// Header1:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// LastPack0: RSI_HIST_SIZE bytes
	// LastPack1: RSI_HIST_SIZE bytes
	// <table_t>: (no_pack - 1) * RSI_HIST_SIZE bytes

	// Check which version of file is being loaded

	frs_index->Seek(8, SEEK_SET);
	frs_index->ReadExact(&no_pack, 4);
	frs_index->Seek(0, SEEK_END);

	// new format

	Clear();

	uchar header[15];
	bool zero = current_read_loc == 0;
	frs_index->Seek(zero ? 0 : 15, SEEK_SET);
	frs_index->ReadExact(header, 15);
	no_obj	= *(_int64*)(header + 1);
	no_pack	= *(int*)(header + 9);
	fixed = (header[14] > 0);
	if(no_pack > 0) {
		end_pack = start_pack = no_pack - 1;
		if(zero) // skip Header1
			frs_index->Seek(15, SEEK_CUR);
		else // skip LastPack0
			frs_index->Seek(RSI_HIST_SIZE, SEEK_CUR);
		Alloc(no_pack);
		frs_index->ReadExact(PackBuf(start_pack), RSI_HIST_SIZE);
	}
}

void RSIndex_Hist::Save(IBFile *frs_index, int current_save_loc)
{
	// File format:
	// Header0:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// Header1:	<version><no_obj><no_pack><int_res><fixed>	- 1+8+4+1+1 = 15 bytes
	// LastPack0: RSI_HIST_SIZE bytes
	// LastPack1: RSI_HIST_SIZE bytes
	// <table_t>: (no_pack - 1) * RSI_HIST_SIZE bytes
	unsigned char header[15];
	header[0]				= 1u; // version number
	*(_int64*)(header + 1)	= no_obj;
	*(int*)(header + 9)		= no_pack;
	header[13]				= (uchar)RSI_HIST_INT_RES;
	header[14]				= (fixed ? 1 : 0);

	bool zero = current_save_loc == 0;

	frs_index->Seek(zero ? 0 : 15, SEEK_SET);
	frs_index->WriteExact(header, 15);

	if(zero) // skip Header1
		frs_index->Seek(15, SEEK_CUR);
	else // skip LastPack0
		frs_index->Seek(RSI_HIST_SIZE, SEEK_CUR);

	if(!hist_buffers.empty()) {
		// write LastPack
		frs_index->WriteExact(PackBuf((uint64)no_pack - 1), RSI_HIST_SIZE );
		if(zero) // skip LastPack1
			frs_index->Seek(RSI_HIST_SIZE, SEEK_CUR);
		frs_index->Seek((uint64)start_pack * RSI_HIST_SIZE, SEEK_CUR);
		// write all the other packs


		_int64 to_write = ((uint64)no_pack - start_pack - 1) * RSI_HIST_SIZE;
		int block(0);
		while ( to_write > 0 ) {
			if (to_write > MAX_HIST_BLOCK_SIZE) {
				frs_index->WriteExact(hist_buffers[block], MAX_HIST_BLOCK_SIZE);
				to_write -= MAX_HIST_BLOCK_SIZE;
			} else {
				frs_index->WriteExact(hist_buffers[block], (uint)to_write);
				to_write = 0;
			}
			++block;
		}
	}
}

int RSIndex_Hist::NoOnes(int pack, int width)
{
	if(pack < start_pack || pack > end_pack)
		return 0;
	int d = 0;

	uint* hist = PackBuf(pack);
	int start_int = 0;
	int stop_int = 0;
	if(width >= RSI_HIST_INT_RES * 32)
		stop_int += RSI_HIST_INT_RES; // normal mode
	else
		stop_int += width / 32; // exact mode

	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(stop_int - start_int <= RSI_HIST_INT_RES);

	for(int i = start_int; i < stop_int; i++)
		d += CalculateBinSum(hist[i]);

	int bits_left = width%32;
	if(stop_int - start_int < RSI_HIST_INT_RES && bits_left > 0) {	// there are bits left in exact mode
		uint last_t = hist[ stop_int ];
		uint mask = ~((uint)0xFFFFFFFF << bits_left);	// 0x0000FFFF, i.e. bits_left ones
		last_t &= mask;
		d += CalculateBinSum( last_t );
	}
	return d;
}

//////////////////////////////////////////////////////////////////

RSValue RSIndex_Hist::IsValue(_int64 min_v, _int64 max_v, int pack, _int64 pack_min, _int64 pack_max)
{
	// Results:		RS_NONE - there is no objects having values between min_v and max_v (including)
	//				RS_SOME - some objects from this pack may have values between min_v and max_v
	//				RS_ALL	- all objects from this pack do have values between min_v and max_v
	if(pack < start_pack || pack > end_pack)
		return RS_SOME;
	if(IntervalTooLarge(pack_min, pack_max) || IntervalTooLarge(min_v, max_v))
		return RS_SOME;
	int min_bit = 0, max_bit = 0;
	if(!fixed) {	// floating point
		double dmin_v = *(double*)(&min_v);
		double dmax_v = *(double*)(&max_v);
		double dpack_min = *(double*)(&pack_min);
		double dpack_max = *(double*)(&pack_max);
		assert(dmin_v <= dmax_v);
		if(dmax_v < dpack_min || dmin_v > dpack_max)
			return RS_NONE;
		if(dmax_v >= dpack_max && dmin_v <= dpack_min)
			return RS_ALL;
		if(dmax_v >= dpack_max || dmin_v <= dpack_min)
			return RS_SOME;			// pack_min xor pack_max are present
		// now we know that (max_v<pack_max) and (min_v>pack_min) and there is only RS_SOME or RS_NONE answer possible
		double interval_len = (dpack_max - dpack_min) / double(32 * RSI_HIST_INT_RES);
		min_bit = int((dmin_v - dpack_min) / interval_len);
		max_bit = int((dmax_v - dpack_min) / interval_len);
	} else {
		assert(min_v <= max_v);
		if(max_v < pack_min || min_v > pack_max)
			return RS_NONE;
		if(max_v >= pack_max && min_v <= pack_min)
			return RS_ALL;
		if(max_v >= pack_max || min_v <= pack_min)
			return RS_SOME;			// pack_min xor pack_max are present
		// now we know that (max_v<pack_max) and (min_v>pack_min) and there is only RS_SOME or RS_NONE answer possible
		if(ExactMode(pack_min, pack_max)) {		// exact mode
			min_bit = int(min_v - pack_min - 1);			// translate into [0,...]
			max_bit = int(max_v - pack_min - 1);
		} else {			// interval mode
			double interval_len = (pack_max - pack_min - 1) / double(32 * RSI_HIST_INT_RES);
			min_bit = int((min_v - pack_min - 1) / interval_len);
			max_bit = int((max_v - pack_min - 1) / interval_len);
		}
	}
	assert(min_bit >= 0);
	if(max_bit >= 32 * RSI_HIST_INT_RES)
		return RS_SOME;		// it may happen for extremely large numbers ( >2^52 )
	for(int i = min_bit; i <= max_bit; i++)	{
		if(((*(PackBuf(pack) + i / 32) >> (i % 32)) & 0x00000001 ) != 0)
			return RS_SOME;
	}
	return RS_NONE;
}

void RSIndex_Hist::PutValue(_int64 v, int pack, _int64 pack_min, _int64 pack_max)		// set information that value v does exist in this pack
{
	if(pack < start_pack || pack > end_pack)
		return;
	if( v == NULL_VALUE_64 || IntervalTooLarge(pack_min, pack_max) )
		return;			// Note: use ClearPack() first! Default histogram has all ones.
	int bit = -1;
	if(fixed == false) {		// floating point
		double dv = *(double*)(&v);
		double dpack_min = *(double*)(&pack_min);
		double dpack_max = *(double*)(&pack_max);
		assert(dv >= dpack_min && dv <= dpack_max);
		if(dv == dpack_min || dv == dpack_max)
			return;
		double interval_len = (dpack_max - dpack_min) / double(32 * RSI_HIST_INT_RES);
		bit = int( (dv - dpack_min) / interval_len );
	} else {
		assert(v >= pack_min && v <= pack_max);
		if(v == pack_min || v == pack_max)
			return;
		if(ExactMode(pack_min, pack_max))	{	// exact mode
			bit = int(v - pack_min - 1);			// translate into [0,...]
		} else {			// interval mode
			double interval_len = _uint64(pack_max - pack_min - 1) / double(32 * RSI_HIST_INT_RES);
			bit = int( _uint64(v - pack_min - 1) / interval_len );
		}
	}
	if(bit >= 32 * RSI_HIST_INT_RES)
		return;		// it may happen for extremely large numbers ( >2^52 )
	assert(bit >= 0);
	*(PackBuf(pack) + bit / 32) |= ( 0x00000001u << (bit % 32) );
}

bool RSIndex_Hist::Intersection(				   int pack,  _int64 pack_min,  _int64 pack_max,
								RSIndex_Hist *sec, int pack2, _int64 pack_min2, _int64 pack_max2)
{
	// we may assume that min-max of packs was already checked
	if(!fixed || !sec->fixed /*|| int_res != sec->int_res*/)
		return true;	// not implemented - intersection possible
	if( IntervalTooLarge(pack_min, pack_max) || IntervalTooLarge(pack_min2, pack_max2))
		return true;

	if(	sec->IsValue(pack_min, pack_min, pack2,pack_min2,pack_max2) != RS_NONE ||
		sec->IsValue(pack_max, pack_max, pack2,pack_min2,pack_max2) != RS_NONE ||
		     IsValue(pack_min2,pack_min2,pack, pack_min, pack_max)  != RS_NONE ||
		     IsValue(pack_max2,pack_max2,pack, pack_min, pack_max)  != RS_NONE )
		return true;	// intersection found (extreme values)

	if(ExactMode(pack_min, pack_max)  &&  ExactMode(pack_min2, pack_max2)) {		// exact mode
		int bit1, bit2;
		_int64 min_v = ( pack_min < pack_min2 ? pack_min2 + 1 : pack_min + 1 );		// these values are possible
		_int64 max_v = ( pack_max > pack_max2 ? pack_max2 - 1 : pack_max - 1 );
		for(_int64 v = min_v; v <= max_v; v++) {
			bit1 = int(v - pack_min  - 1);
			bit2 = int(v - pack_min2 - 1);
			if( (((*(PackBuf(pack) + bit1 / 32) >> (bit1 % 32)) & 0x00000001)!=0 ) &&
				(((*(PackBuf(pack) + bit2 / 32) >> (bit2 % 32)) & 0x00000001)!=0 ) )
					return true;	// intersection found
		}
		return false;	// no intersection possible - all values do not math the second histogram
	}
	// TODO: all other possibilities, not only the exact cases
	return true;		// we cannot foreclose intersecting
}

//void RSIndex_Hist::Display(uint pack)
//{
//	if(pack < start_pack || pack > end_pack || no_pack == 0) {
//		cout << "Invalid pack: " << pack << endl;
//		return;
//	}
//	for(uint i = 0; i < 32; i++) {
//		cout << i << ": ";
//		uint v = t[(pack - start_pack) * int_res + i];
//		for (int j = 0; j < 32; j++) {
//			cout << (v >> j) % 2;
//		}
//		cout << endl;
//	}
//	cout << endl;
//}

/*
 * /pre Index is locked for update
 * Append a KN by the contents of the given KN
 * */

void RSIndex_Hist::AppendKN(int pack, RSIndex_Hist* hist, uint no_values_to_add)
{
	uint no_objs_to_add_to_last_DP = uint(no_obj % MAX_PACK_ROW_SIZE ? MAX_PACK_ROW_SIZE - no_obj % MAX_PACK_ROW_SIZE : 0);
	if(pack < hist->no_pack) {
		AppendKNs(no_pack + 1);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pack != no_pack - 1);
		if(pack != no_pack - 1)
			memcpy(PackBuf(no_pack - 1), hist->PackBuf(pack), RSI_HIST_SIZE);
		no_obj  += no_objs_to_add_to_last_DP + no_values_to_add;
	}
}
