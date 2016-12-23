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

#include <iostream>
#include <assert.h>
#include <fcntl.h>
#ifdef __GNUC__
#include <sys/stat.h>
#endif

#include "Filter.h"
#include "loader/Loader.h"
#include "system/IBFile.h"
#include "system/ib_system.h"
#include "common/bhassert.h"
#include "tools.h"

using namespace std;
using namespace bh;

const int Filter::bitBlockSize = BIT_BLOCK_SIZE;
const int Filter::compressedBitBlockSize = COMPRESSED_BIT_BLOCK_SIZE;
#ifndef _MSC_VER
const uchar Filter::FB_FULL;			// block status: full
const uchar Filter::FB_EMPTY;		// block status: empty
const uchar Filter::FB_MIXED;		// block status: neither full nor empty
#endif

IBMutex IBHeapAllocator::mutex;
TheFilterBlockOwner* the_filter_block_owner = 0;

Filter::Filter(_int64 no_obj, bool all_ones, bool shallow) :
no_blocks(0), block_status(0), blocks(0), block_allocator(0), no_of_bits_in_last_block(0),
track_changes(false), shallow(shallow), block_last_one(NULL),
bit_block_pool(0), delayed_stats(-1), delayed_block(-1), delayed_stats_set(-1), delayed_block_set(-1), bit_mut(0)
{
	MEASURE_FET("Filter::Filter(int, bool, bool)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(no_obj >= 0);
	if(no_obj > MAX_ROW_NUMBER)
		throw OutOfMemoryRCException("Too many tuples.    (48)");

	no_blocks = bh::common::NoObj2NoPacks(no_obj);
	no_of_bits_in_last_block = (no_obj & 0xFFFF) == 0 ? 0x10000 : int(no_obj & 0xFFFF);
	if(no_obj == 0)
		no_of_bits_in_last_block = 0;

	blocks = 0;
	Construct(all_ones);
}

Filter::Filter(const Filter& filter) :
no_blocks(0), block_status(0), blocks(0), no_of_bits_in_last_block(0),
track_changes(filter.track_changes), shallow(false), block_last_one(NULL),
bit_block_pool(0),  delayed_stats(-1), delayed_block(-1), delayed_stats_set(-1), delayed_block_set(-1), bit_mut(0)
{
	MEASURE_FET("Filter::Filter(const Filter&)");
	Filter& tmp_filter = const_cast<Filter&> (filter);
	no_blocks = filter.NoBlocks();
	no_of_bits_in_last_block = tmp_filter.NoAddBits();
	blocks = new Block *[no_blocks];
	ConstructPool();

	for(int i = 0; i < no_blocks; i++) {
		if(tmp_filter.GetBlock(i)) {
			blocks[i] = block_allocator->Alloc(false);
			new(blocks[i]) Block(*(tmp_filter.GetBlock(i)), this); 
		} else
			blocks[i] = NULL;
	}
	block_status = new uchar[no_blocks];
	memcpy(block_status, filter.block_status, no_blocks);
	block_last_one = new ushort [no_blocks];
	memcpy(block_last_one, filter.block_last_one, no_blocks * sizeof(ushort));
	if(track_changes)
		was_block_changed = filter.was_block_changed;
}

Filter::Filter() :
no_blocks(0), block_status(0), blocks(0), block_allocator(0), no_of_bits_in_last_block(0),
track_changes(false), shallow(false), block_last_one(NULL),
bit_block_pool(0),  delayed_stats(-1), delayed_block(-1), delayed_stats_set(-1), delayed_block_set(-1), bit_mut(0)
{}


void Filter::Construct(bool all_ones)
{
	try {
		if(no_blocks > 0) {
			blocks = new Block *[no_blocks];
			block_status = new uchar[no_blocks];
			memset(block_status, (all_ones ? (int) FB_FULL : (int) FB_EMPTY), no_blocks); // set the filter to all empty
			block_last_one = new ushort [no_blocks];
			for(int i = 0; i < no_blocks; i++) {
				block_last_one[i] = 65535;
				blocks[i] = NULL;
			}
			block_last_one[no_blocks - 1] = no_of_bits_in_last_block - 1;
			//No idea how to create an allocator for the pool below to use BH heap, due to static methods in allocator
		}
		if(!shallow)
			ConstructPool();
		//FIXME if we put throw statement here (which is handled by the catch statement below) then we have crash
		//			on many sql statements e.g. show tables, select * from t - investigate this
	} catch (...) {
		this->~Filter();
		throw OutOfMemoryRCException();
	}
}

void Filter::ConstructPool()
{
	bit_block_pool = new boost::pool<IBHeapAllocator>(bitBlockSize);
	bit_mut = new IBMutex();
	block_allocator = new BlockAllocator();
}

Filter* Filter::ShallowCopy(Filter& f)
{
	Filter* sc = new Filter(f.NoObj(), true, true); //shallow construct - no pool
	sc->shallow = true;
	sc->block_allocator = f.block_allocator;
	sc->track_changes = f.track_changes;
	sc->bit_block_pool = f.bit_block_pool;
	sc->bit_mut = f.bit_mut;
	if(sc->track_changes)
		sc->was_block_changed.resize(sc->no_blocks, false);
	sc->delayed_stats = -1;
	sc->delayed_block = -1;
	sc->delayed_stats_set = -1;
	sc->delayed_block_set = -1; 
	for(int i = 0; i < sc->no_blocks; i++)
		sc->blocks[i] = f.blocks[i];
	memcpy(sc->block_status, f.block_status, sc->no_blocks);
	memcpy(sc->block_last_one, f.block_last_one, sc->no_blocks * sizeof(short));
	return sc;
}


Filter::~Filter()
{
	MEASURE_FET("Filter::~Filter()");
	if(blocks) {
		// not required, as the Block and bit_block objects are removed along with the memory pools
//		if(!shallow)
//			for(int i=0; i< no_blocks; i++)
//				if(blocks[i])
//					DeleteBlock(i);
		delete[] blocks;
	}
	delete [] block_status;
	delete [] block_last_one;

	if(!shallow) {
		delete bit_block_pool;
		delete bit_mut;
		delete block_allocator;
	}
}

std::auto_ptr<Filter> Filter::Clone() const
{
	return std::auto_ptr<Filter>(new Filter(*this) );
}

void Filter::ResetDelayed(int b, int pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	assert(delayed_block_set == -1);				// no mixing!
	if(block_status[b] == FB_MIXED) {
		Reset(b, pos);
		return;
	}
	if(block_status[b] == FB_FULL) {
		if(delayed_block != b) {
			Commit();
			delayed_block = b;
			delayed_stats = -1;
		}
		if(pos == delayed_stats + 1) {
			delayed_stats++;
		} else if(pos > delayed_stats + 1) {// then we can't delay
			if(delayed_stats >= 0)
				ResetBetween(b, 0, b, delayed_stats);
			Reset(b, pos);
			delayed_stats = -2; // not to use any longer
		}
		// else do nothing
	}
}

void Filter::SetDelayed(int b, int pos)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	assert(delayed_block == -1);				// no mixing!
	if(block_status[b] == FB_MIXED) {
		Set(b, pos);
		return;
	}
	if(block_status[b] == FB_EMPTY) {
		if(delayed_block_set != b) {
			Commit();
			delayed_block_set = b;
			delayed_stats_set = -1;
		}
		if(pos == delayed_stats_set + 1) {
			delayed_stats_set++;
		} else if(pos > delayed_stats_set + 1) {// then we can't delay
			if(delayed_stats_set >= 0)
				SetBetween(b, 0, b, delayed_stats_set);
			Set(b, pos);
			delayed_stats_set = -2; // not to use any longer
		}
		// else do nothing
	}
}

void Filter::Commit()
{
	if(delayed_block > -1) {
		if(block_status[delayed_block] == FB_FULL && delayed_stats >= 0)
			ResetBetween(delayed_block, 0, delayed_block, delayed_stats);
		delayed_block = -1;
		delayed_stats = -1;
	}
	if(delayed_block_set > -1) {
		if(block_status[delayed_block_set] == FB_EMPTY && delayed_stats_set >= 0)
			SetBetween(delayed_block_set, 0, delayed_block_set, delayed_stats_set);
		delayed_block_set = -1;
		delayed_stats_set = -1;
	}
}

void Filter::Set()
{
	MEASURE_FET("voFilter::Set()");
	if(no_blocks == 0)
		return;
	memset(block_status, FB_FULL, no_blocks); // set the filter to all full

	for(int b = 0; b < no_blocks; b++) {
		block_last_one[b] = 65535;
		if(blocks[b])
			DeleteBlock(b);
	}
	block_last_one[no_blocks - 1] = no_of_bits_in_last_block - 1;
	delayed_stats = -1;
	if(track_changes)
		was_block_changed.set();
}

void Filter::SetBlock(int b)
{
	MEASURE_FET("Filter::SetBlock(int)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	block_status[b] = FB_FULL; // set the filter to all full
	block_last_one[b] = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535);
	if(blocks[b])
		DeleteBlock(b);
	if(track_changes)
		was_block_changed.set(b);
}

void Filter::Set(int b, int n)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	bool make_mixed = false;
	if(block_status[b] == FB_FULL) {
		if(n == int(block_last_one[b]) + 1)
			block_last_one[b]++;
		else if(n > int(block_last_one[b]) + 1)			// else do nothing - already set
			make_mixed = true;
	}
	if(make_mixed || block_status[b] == FB_EMPTY) {
		if(n == 0) {
			block_status[b] = FB_FULL;
			block_last_one[b] = 0;
		} else {
			int new_block_size = (b == no_blocks - 1 ? no_of_bits_in_last_block : 65536);
			block_status[b] = FB_MIXED;
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, new_block_size);
			if(blocks[b] == NULL)
				throw OutOfMemoryRCException();
		}
		if(make_mixed) {
			blocks[b]->Set(0, block_last_one[b]);		// create a block with a contents before Set
		}
	}
	if(blocks[b]) {
		bool full = blocks[b]->Set(n);
		if(full)
			SetBlock(b);
	}
	if(track_changes)
		was_block_changed.set(b);
}

void Filter::SetBetween(_int64 n1, _int64 n2)
{
	if(n1 == n2)
		Set(int(n1 >> 16), int(n1 & 65535));
	else
		SetBetween(int(n1 >> 16), int(n1 & 65535), int(n2 >> 16), int(n2 & 65535));
}

void Filter::SetBetween(int b1, int n1, int b2, int n2)
{
	MEASURE_FET("Filter::SetBetween(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b2 < no_blocks);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL && n1 <= int(block_last_one[b1]) + 1) {
			block_last_one[b1] = max(block_last_one[b1], ushort(n2));
		} else if(block_status[b1] == FB_EMPTY || block_status[b1] == FB_FULL) {
			// if full block
			if(n1 == 0) {
				block_status[b1] = FB_FULL;
				block_last_one[b1] = n2;
			} else {
				if(b1 == no_blocks - 1) {
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this, no_of_bits_in_last_block);
				} else {
					blocks[b1] = block_allocator->Alloc();
					new(blocks[b1]) Block(this);
				}
				if(blocks[b1] == NULL)
					throw OutOfMemoryRCException();
				if(block_status[b1] == FB_FULL)
					blocks[b1]->Set(0, block_last_one[b1]);		// create a block with a contents before Set
				block_status[b1] = FB_MIXED;
			}
		}
		if(blocks[b1]) {
			bool full = blocks[b1]->Set(n1, n2);
			if(full)
				SetBlock(b1);
		}
		if(track_changes)
			was_block_changed.set(b1);
	} else {
		if(n1 == 0)
			SetBlock(b1);
		else
			SetBetween(b1, n1, b1, 65535); // note that b1 is never the last block
		for(int i = b1 + 1; i < b2; i++)
			SetBlock(i);
		SetBetween(b2, 0, b2, n2);
	}
}

void Filter::Reset()
{
	MEASURE_FET("Filter::Reset()");
	memset(block_status, FB_EMPTY, no_blocks); // set the filter to all empty
	for(int b = 0; b < no_blocks; b++) {
		if(blocks[b])
			DeleteBlock(b);
	}
	delayed_stats = -1;
	if(track_changes)
		was_block_changed.set();
}

void Filter::ResetBlock(int b)
{
	MEASURE_FET("Filter::ResetBlock(int)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	block_status[b] = FB_EMPTY;
	if(blocks[b])
		DeleteBlock(b);
	if(track_changes)
		was_block_changed.set(b);
}
/*
void Filter::Reset(int b, int n)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	if(block_status[b] == FB_FULL) {
		block_status[b] = FB_MIXED;
		if(b == no_blocks - 1) {
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, no_of_bits_in_last_block, true); // set as full, then reset a part of it
		} else {
			blocks[b] = block_allocator->Alloc();
			new(blocks[b]) Block(this, 65536, true);
		}
		if(blocks[b] == NULL)
			throw OutOfMemoryRCException();
	}
	if(blocks[b]) {
		if(blocks[b]->Reset(n))
			ResetBlock(b);
	}
	if(track_changes)
		was_block_changed.set(b);
}
*/
void Filter::ResetBetween(_int64 n1, _int64 n2)
{
	if(n1 == n2)
		Reset(int(n1 >> 16), int(n1 & 65535));
	else
		ResetBetween(int(n1 >> 16), int(n1 & 65535), int(n2 >> 16), int(n2 & 65535));
}

void Filter::ResetBetween(int b1, int n1, int b2, int n2)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b2 <= no_blocks);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL) {
			// if full block
			if(n1 > 0 && n2 >= block_last_one[b1]) {
				block_last_one[b1] = min(ushort(n1 - 1), block_last_one[b1]);
			} else if(n1 == 0 && n2 >= block_last_one[b1]) {
				block_status[b1] = FB_EMPTY;
			} else {
				int new_block_size = (b1 == no_blocks - 1 ? no_of_bits_in_last_block : 65536);
				blocks[b1] = block_allocator->Alloc();
				block_status[b1] = FB_MIXED;
				if(block_last_one[b1] == new_block_size - 1) {
					new(blocks[b1]) Block(this, new_block_size, true); // set as full, then reset a part of it
					if(blocks[b1] == NULL)
						throw OutOfMemoryRCException();
				} else {
					new(blocks[b1]) Block(this, new_block_size, false); // set as empty, then set the beginning
					if(blocks[b1] == NULL)
						throw OutOfMemoryRCException();
					blocks[b1]->Set(0, block_last_one[b1]);		// create a block with a contents before Reset
				}
			}
		}
		if(blocks[b1]) {
			bool empty = blocks[b1]->Reset(n1, n2);
			if(empty)
				ResetBlock(b1);
		}
		if(track_changes)
			was_block_changed.set(b1);
	} else {
		if(n1 == 0)
			ResetBlock(b1);
		else
			ResetBetween(b1, n1, b1, 65535); // note that b1 is never the last block
		for(int i = b1 + 1; i < b2; i++)
			ResetBlock(i);
		ResetBetween(b2, 0, b2, n2);
	}
}

void Filter::Reset(Filter &f2)
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	for(int b = 0; b < mb; b++) {
		if(f2.block_status[b] == FB_FULL)
			ResetBetween(b, 0, b, f2.block_last_one[b]);
		else if(f2.block_status[b] != FB_EMPTY) { // else no change
			if(block_status[b] == FB_FULL) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				blocks[b]->Not(); // always nontrivial
				int new_block_size = (b == no_blocks - 1 ? no_of_bits_in_last_block : 65536);
				if(block_last_one[b] < new_block_size - 1)
					blocks[b]->Reset(block_last_one[b] + 1, new_block_size - 1);
				block_status[b] = FB_MIXED;
			} else if(blocks[b]) {
				bool empty = blocks[b]->AndNot(*(f2.GetBlock(b)));
				if(empty)
					ResetBlock(b);
			}
		}
	}
}

bool Filter::Get(int b, int n)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	if(block_status[b] == FB_EMPTY)
		return false;
	if(block_status[b] == FB_FULL)
		return (n <= block_last_one[b]);
	return blocks[b]->Get(n);
}

bool Filter::IsEmpty()
{
	for(int i = 0; i < no_blocks; i++)
		if(block_status[i] != FB_EMPTY)
			return false;
	return true;
}

bool Filter::IsEmpty(int b) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(b < no_blocks);
	return (block_status[b] == FB_EMPTY);
}

bool Filter::IsEmptyBetween(_int64 n1, _int64 n2)	// true if there are only 0 between n1 and n2, inclusively
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((n1 >= 0) && (n1 <= n2));
	if(n1 == n2)
		return !Get(n1);
	int b1 = int(n1 >> 16);
	int b2 = int(n2 >> 16);
	int nn1 = int(n1 & 65535);
	int nn2 = int(n2 & 65535);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL)
			return (nn1 > block_last_one[b1]);
		if(block_status[b1] == FB_EMPTY)
			return true;
		return blocks[b1]->IsEmptyBetween(nn1, nn2);
	} else {
		int full_pack_start = (nn1 == 0? b1 : b1 + 1);
		int full_pack_stop  = b2 - 1;
		if(nn2 == 65535 || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
			full_pack_stop	= b2;
		for(int i = full_pack_start; i <= full_pack_stop; i++)
			if(block_status[i] != FB_EMPTY)
				return false;
		if(b1 != full_pack_start) {
			if(block_status[b1] == FB_FULL) {
				 if(nn1 <= block_last_one[b1])
					 return false;
			} else if( block_status[b1] != FB_EMPTY &&
				!blocks[b1]->IsEmptyBetween(nn1, 65535))  // note that b1 is never the last block
				return false;
		}
		if(b2 != full_pack_stop) {
			if(block_status[b2] == FB_FULL)
				return false;
			if( block_status[b2] != FB_EMPTY &&
				!blocks[b2]->IsEmptyBetween(0, nn2))
				return false;
		}
	}
	return true;
}

bool Filter::IsFullBetween(_int64 n1, _int64 n2)	// true if there are only 1 between n1 and n2, inclusively
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((n1 >= 0) && (n1 <= n2));
	if(n1 == n2)
		return Get(n1);
	int b1 = int(n1 >> 16);
	int b2 = int(n2 >> 16);
	int nn1 = int(n1 & 65535);
	int nn2 = int(n2 & 65535);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL)
			return (nn2 <= block_last_one[b1]);
		if(block_status[b1] == FB_EMPTY)
			return false;
		return blocks[b1]->IsFullBetween(nn1, nn2);
	} else {
		int full_pack_start = (nn1 == 0? b1 : b1 + 1);
		int full_pack_stop  = b2 - 1;
		if(nn2 == 65535 || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
			full_pack_stop	= b2;
		for(int i = full_pack_start; i <= full_pack_stop; i++)
			if(!IsFull(i))
				return false;
		if(b1 != full_pack_start) {
			if(block_status[b1] == FB_EMPTY)
				return false;
			if(!IsFull(b1) &&
				!blocks[b1]->IsFullBetween(nn1, 65535))  // note that b1 is never the last block
				return false;
		}
		if(b2 != full_pack_stop) {
			if(block_status[b2] == FB_EMPTY)
				return false;
			if(block_status[b2] == FB_FULL)
				return (nn2 <= block_last_one[b2]);
			if(!blocks[b2]->IsFullBetween(0, nn2))
				return false;
		}
	}
	return true;
}

bool Filter::IsFull() const
{
	for(int b = 0; b < no_blocks; b++)
		if(!IsFull(b))
			return false;
	return true;
}

void Filter::CopyBlock(Filter& f, int block)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(block < no_blocks);
	assert(!f.shallow || bit_block_pool == f.bit_block_pool);

	block_status[block] = f.block_status[block];
	block_last_one[block] = f.block_last_one[block];

	if(f.GetBlock(block)) {
		if(bit_block_pool == f.bit_block_pool) { //f is a shallow copy of this
			blocks[block] = f.blocks[block]->MoveFromShallowCopy(this);
			f.blocks[block] = NULL;
		} else {
			if(blocks[block])
				blocks[block]->CopyFrom(*f.blocks[block], this);
			else {
				blocks[block] = block_allocator->Alloc();
				new(blocks[block]) Block(*(f.GetBlock(block)), this);
			}
		}
	} else {
		// no block, just status to copy
		if(bit_block_pool == f.bit_block_pool)  //f is a shallow copy of this
			blocks[block] = NULL;
		else if(blocks[block])
			DeleteBlock(block);
	}

	if(track_changes)
		was_block_changed.set(block);
	assert(!blocks[block] || blocks[block]->Owner() == this);
}

void Filter::DeleteBlock(int pack)
{
	blocks[pack]->~Block();
	block_allocator->Dealloc(blocks[pack]);
	blocks[pack] = NULL;
}

void Filter::CopyChangedBlocks(Filter& f)
{
	assert(no_blocks == f.no_blocks && f.track_changes);

	for(int i = 0; i<no_blocks; ++i)
		if(f.WasBlockChanged(i))
			CopyBlock(f,i);
}

bool Filter::IsEqual(Filter &sec)
{
	if(no_blocks != sec.no_blocks || no_of_bits_in_last_block != sec.no_of_bits_in_last_block)
		return false;
	for(int b = 0; b < no_blocks; b++) {
		if(block_status[b] != sec.block_status[b]) {
			_int64 bstart = _int64(b) >> 16;
			_int64 bstop = bstart + (b < no_blocks - 1 ? 65535 : no_of_bits_in_last_block - 1);
			if(block_status[b] == FB_FULL && sec.block_status[b] == FB_MIXED) {	// Note: may still be equal!
				if(!sec.IsFullBetween(bstart, bstart + block_last_one[b]))
					return false;
				if(bstart + block_last_one[b] < bstop && !sec.IsEmptyBetween(bstart + block_last_one[b] + 1, bstop))
					return false;
				return true;
			}
			if(sec.block_status[b] == FB_FULL && block_status[b] == FB_MIXED) {	// Note: may still be equal!
				if(!IsFullBetween(bstart, bstart + sec.block_last_one[b]))
					return false;
				if(bstart + sec.block_last_one[b] < bstop && !IsEmptyBetween(bstart + sec.block_last_one[b] + 1, bstop))
					return false;
				return true;
			}
			return false;
		}
		if(block_status[b] == FB_FULL && block_last_one[b] != sec.block_last_one[b])
			return false;
		if(blocks[b] && blocks[b]->IsEqual(*sec.GetBlock(b)) == false)
			return false;
	}
	return true;
}

void Filter::And(Filter &f2)
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	for(int b = 0; b < mb; b++) {
		if(f2.block_status[b] == FB_EMPTY)
			ResetBlock(b);
		else if(f2.block_status[b] == FB_MIXED) {
			if(block_status[b] == FB_FULL) {
				int old_block_size = block_last_one[b];
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				block_status[b] = FB_MIXED;
				int end_block = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535);
				if(old_block_size < end_block)
					ResetBetween(b, old_block_size + 1, b, end_block);
			} else if(blocks[b]) {
				bool empty = blocks[b]->And(*(f2.GetBlock(b)));
				if(empty)
					ResetBlock(b);
			}
			if(track_changes)
				was_block_changed.set(b);
		} else {	// FB_FULL
			int end_block = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535);
			if(f2.block_last_one[b] < end_block)
				ResetBetween(b, int(f2.block_last_one[b]) + 1, b, end_block);
		}
	}
}

void Filter::Or(Filter &f2, int pack)
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	int b = (pack == -1 ? 0 : pack);
	for(; b < mb; b++) {
		if(f2.block_status[b] == FB_FULL)
			SetBetween(b, 0, b, f2.block_last_one[b]);
		else if(f2.block_status[b] != FB_EMPTY) { // else no change
			if(block_status[b] == FB_EMPTY) {
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				block_status[b] = FB_MIXED;
			} else if(blocks[b]) {
				bool full = blocks[b]->Or(*(f2.GetBlock(b)));
				if(full)
					SetBlock(b);
			} else {		// FB_FULL
				if(block_last_one[b] < (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535)) {
					int old_block_size = block_last_one[b];
					blocks[b] = block_allocator->Alloc();
					new(blocks[b]) Block(*(f2.GetBlock(b)), this);
					block_status[b] = FB_MIXED;
					SetBetween(b, 0, b, old_block_size);
				}
			}
			if(track_changes)
				was_block_changed.set(b);
		}
		if(pack != -1)
			break;
	}
}

void Filter::Not()
{
	for(int b = 0; b < no_blocks; b++) {
		if(block_status[b] == FB_FULL) {
			if(block_last_one[b] < (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535)) {
				int old_block_size = block_last_one[b];
				block_last_one[b] = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535);		// make really full
				ResetBetween(b, 0, b, old_block_size);
			} else
				block_status[b] = FB_EMPTY;
		} else if(block_status[b] == FB_EMPTY) {
			block_status[b] = FB_FULL;
			block_last_one[b] = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535);
		} else
			blocks[b]->Not();
	}
	if(track_changes)
		was_block_changed.set();
}

void Filter::AndNot(Filter &f2)			// reset all positions which are set in f2
{
	int mb = min( f2.NoBlocks(), NoBlocks() );
	for(int b = 0; b < mb; b++) {
		if(f2.block_status[b] == FB_FULL)
			ResetBetween(b, 0, b, f2.block_last_one[b]);
		else if(f2.block_status[b] != FB_EMPTY && block_status[b] != FB_EMPTY) { // else no change
			if(block_status[b] == FB_FULL) {
				int old_block_size = block_last_one[b];
				blocks[b] = block_allocator->Alloc();
				new(blocks[b]) Block(*(f2.GetBlock(b)), this);
				block_status[b] = FB_MIXED;
				blocks[b]->Not();
				int end_block = (b == no_blocks - 1 ? no_of_bits_in_last_block - 1 : 65535);
				if(old_block_size < end_block)
					ResetBetween(b, old_block_size + 1, b, end_block);
			} else if(blocks[b]) {
				bool empty = blocks[b]->AndNot(*(f2.GetBlock(b)));
				if(empty)
					ResetBlock(b);
			}
			if(track_changes)
				was_block_changed.set(b);
		}
	}
}

void Filter::SwapPack(Filter &f2, int pack)
{
	// WARNING: cannot just swap pointers, as the blocks belong to private pools!
	Block b(this);

	if(track_changes)
		was_block_changed.set(pack);
	if(f2.track_changes)
		f2.was_block_changed.set(pack);

	if(block_status[pack] == FB_MIXED) {
		// save block
		assert(shallow || blocks[pack] && blocks[pack]->Owner() == this); //shallow copy can have the original filter as the block owner

		b.CopyFrom(*(blocks[pack]), this);
	}
	if(f2.block_status[pack] == FB_MIXED) {
		assert(f2.blocks[pack] && f2.blocks[pack]->Owner() == &f2);
		if(!blocks[pack]) {
			blocks[pack] = block_allocator->Alloc();
			new(blocks[pack]) Block(*(f2.blocks[pack]), this);
		} else
			blocks[pack]->CopyFrom(*(f2.blocks[pack]), this);
	} else {
		if(blocks[pack])
			DeleteBlock(pack);
	}
	if(block_status[pack] == FB_MIXED) {
		if(!f2.blocks[pack]) {
			f2.blocks[pack] = f2.block_allocator->Alloc();
			new(f2.blocks[pack]) Block(b, &f2);
		} else
			f2.blocks[pack]->CopyFrom(b, &f2);
	} else {
		if(f2.blocks[pack])
			f2.DeleteBlock(pack);
	}
	swap(block_status[pack], f2.block_status[pack]);
	swap(block_last_one[pack], f2.block_last_one[pack]);
	if(block_status[pack] == FB_MIXED)
		assert(blocks[pack]->Owner() == this);
	if(f2.block_status[pack] == FB_MIXED)
			assert(f2.blocks[pack]->Owner() == &f2);

}


_int64 Filter::NoOnes() const
{
	if(no_blocks == 0)
		return 0;
	_int64 count = 0;
	for(int b = 0; b < no_blocks; b++) {
		if(block_status[b] == FB_FULL)
			count += int(block_last_one[b]) + 1;
		else if(blocks[b])
			count += blocks[b]->NoOnes(); // else empty
	}
	return count;
}

uint Filter::NoOnes(int b)
{
	if(no_blocks == 0)
		return 0;
	if(block_status[b] == FB_FULL)
		return uint(block_last_one[b]) + 1;
	else if(blocks[b])
		return blocks[b]->NoOnes();
	return 0;
}

uint Filter::NoOnesUncommited(int b)
{
	int uc = 0;
	if(delayed_block == b) {
		if(block_status[b] == FB_FULL && delayed_stats >= 0)
			uc = -delayed_stats - 1;
	}
	if(delayed_block_set == b) {
		if(block_status[b] == FB_EMPTY && delayed_stats_set >= 0)
			uc = delayed_stats_set + 1;
	}
	return NoOnes(b) + uc;
}

_int64 Filter::NoOnesBetween(_int64 n1, _int64 n2)	// no of 1 between n1 and n2, inclusively
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((n1 >= 0) && (n1 <= n2));
	if(n1 == n2)
		return (Get(n1) ? 1 : 0);
	int b1 = int(n1 >> 16);
	int b2 = int(n2 >> 16);
	int nn1 = int(n1 & 65535);
	int nn2 = int(n2 & 65535);
	if(b1 == b2) {
		if(block_status[b1] == FB_FULL) {
			nn2 = min(nn2, int(block_last_one[b1]));
			if(nn1 > block_last_one[b1])
				return 0;
			return nn2 - nn1 + 1;
		} 
		if(block_status[b1] == FB_EMPTY)
			return 0;
		return blocks[b1]->NoOnesBetween(nn1, nn2);
	}
	_int64 counter = 0;
	int full_pack_start = (nn1 == 0? b1 : b1 + 1);
	int full_pack_stop  = b2 - 1;
	if(nn2 == 65535 || (b2 == no_blocks - 1 && nn2 == no_of_bits_in_last_block - 1))
		full_pack_stop	= b2;
	for(int i = full_pack_start; i <= full_pack_stop; i++) {
		if(block_status[i] == FB_MIXED)
			counter += blocks[i]->NoOnes();
		else if(block_status[i] == FB_FULL)
			counter += _int64(block_last_one[i]) + 1;
	}
	if(b1 != full_pack_start) {
		if(block_status[b1] == FB_FULL) {
			if(nn1 <= block_last_one[b1])
				counter += block_last_one[b1] - nn1 + 1;
		} else if(block_status[b1] != FB_EMPTY)
			counter += blocks[b1]->NoOnesBetween(nn1, 65535);  // note that b1 is never the last block
	}
	if(b2 != full_pack_stop) {
		if(block_status[b2] == FB_FULL)
			counter += min(nn2 + 1, int(block_last_one[b2]) + 1);
		else if(block_status[b2] != FB_EMPTY)
			counter += blocks[b2]->NoOnesBetween(0, nn2);
	}
	return counter;
}

int Filter::DensityWeight()			// = 65537 for empty filter or a filter with only one nonempty block.
{									// Otherwise it is an average number of ones in nonempty blocks.
	_int64 count = 0;
	int nonempty = 0;
	if(no_blocks > 0) {
		for(int b = 0; b < no_blocks; b++) {
			if(block_status[b] == FB_FULL) {
				count += _int64(block_last_one[b]) + 1;
				nonempty++;
			} else if(blocks[b]) {
				count += blocks[b]->NoOnes();
				nonempty++;
			}
		}
	}
	if(nonempty < 2)
		return 65537;
	return int(count / double(nonempty));
}

_int64 Filter::NoObj() const
{
	_int64 res = _int64(no_blocks - (no_of_bits_in_last_block ? 1 : 0)) << 16;
	return res + no_of_bits_in_last_block;
}

int Filter::NoBlocks() const
{
	return no_blocks;
}

int Filter::NoAddBits() const
{
	return no_of_bits_in_last_block;
}


void Filter::Grow(_int64 grow_size, bool value)
{
	if(grow_size == 0)
		return;
	int new_bits_in_last = int((no_of_bits_in_last_block + grow_size) & 0xFFFF);
	if(new_bits_in_last == 0)
		new_bits_in_last = 0x10000;
	int bits_added_to_block = grow_size > 0x10000 - no_of_bits_in_last_block ? 0x10000 - no_of_bits_in_last_block
		: (int)grow_size;
	int new_blocks = no_of_bits_in_last_block == 0 ? 
		int((grow_size + 0xFFFF) >> 16) : 
		int((grow_size - (0x10000 - no_of_bits_in_last_block) + 0xFFFF) >> 16);

	if(no_blocks > 0 && track_changes)
		was_block_changed.set(no_blocks - 1);

	//add new blocks
	AddNewBlocks(new_blocks, value, new_bits_in_last);

	//grow the previously last block
	if(no_blocks > 0 && bits_added_to_block > 0) {
		if(!bit_block_pool)
			ConstructPool();
		if(value) {
			if(block_status[no_blocks - 1] == FB_EMPTY) {
				block_status[no_blocks - 1] = FB_MIXED;
				blocks[no_blocks - 1] = block_allocator->Alloc();
				new(blocks[no_blocks - 1]) Block(this, new_blocks == 0 ? new_bits_in_last : 0x10000);
				if(blocks[no_blocks - 1] == NULL)
					throw OutOfMemoryRCException();
				bool full = blocks[no_blocks - 1]->Set(no_of_bits_in_last_block, no_of_bits_in_last_block + bits_added_to_block - 1);
				if(full)
					SetBlock(no_blocks - 1);
			} else if(block_status[no_blocks - 1] == FB_MIXED) {
				blocks[no_blocks - 1]->GrowBlock(bits_added_to_block, true);
			} else if(IsFull(no_blocks - 1)) {
				block_last_one[no_blocks - 1] = (new_blocks == 0 ? new_bits_in_last - 1 : 65535);
			} else {		// FB_FULL, but not really full: 111..11000...00 with ones added to the end
				block_status[no_blocks - 1] = FB_MIXED;
				blocks[no_blocks - 1] = block_allocator->Alloc();
				new(blocks[no_blocks - 1]) Block(this, new_blocks == 0 ? new_bits_in_last : 0x10000, true);	// all ones
				if(blocks[no_blocks - 1] == NULL)
					throw OutOfMemoryRCException();
				blocks[no_blocks - 1]->Reset(int(block_last_one[no_blocks - 1]) + 1, no_of_bits_in_last_block - 1);
			}
		} else {
			if(block_status[no_blocks - 1] == FB_MIXED)
				blocks[no_blocks - 1]->GrowBlock(bits_added_to_block, false);
		}
	}
	no_blocks += new_blocks;
	no_of_bits_in_last_block = new_bits_in_last;
}

void Filter::AddNewBlocks(int new_blocks, bool value, int new_no_bits_last)
{
	if(new_blocks > 0) {
		//grow arrays
		if(track_changes)
			was_block_changed.resize(no_blocks + new_blocks, true);
		Block** tmp_blocks = new Block *[no_blocks + new_blocks];
		uchar* tmp_block_status = new uchar[no_blocks + new_blocks];
		ushort* tmp_block_size = new ushort [no_blocks + new_blocks];
		memcpy(tmp_blocks, blocks, sizeof(Block*) * no_blocks);
		memcpy(tmp_block_status, block_status, sizeof(uchar) * no_blocks);
		memcpy(tmp_block_size, block_last_one, sizeof(ushort) * no_blocks);
		delete [] blocks;
		delete [] block_status;
		delete [] block_last_one;
		blocks = tmp_blocks;
		block_status = tmp_block_status;
		block_last_one = tmp_block_size;

		for(int i = no_blocks; i < no_blocks + new_blocks; i++) {
			blocks[i] = NULL;
			if(value) {
				block_status[i] = FB_FULL;
				block_last_one[i] = (i == no_blocks + new_blocks - 1 ? new_no_bits_last - 1 : 65535);
			} else
				block_status[i] = FB_EMPTY;
		}
	}
}

// block b is copied and appended
// the original one is made empty

void Filter::MoveBitsToEnd(int block, int no_of_bits_to_move)
{
	MoveBlockToEnd(block);
	if(block == no_blocks - 2)
		return;
	if(no_of_bits_to_move < NoObj() % 65536)
		ResetBetween(no_blocks - 1, no_of_bits_to_move, no_blocks - 1, int(NoObj() % 65536));
	no_of_bits_in_last_block = no_of_bits_to_move;
	if(blocks[no_blocks - 1]) {
		blocks[no_blocks - 1]->ShrinkBlock(no_of_bits_to_move);
		if(blocks[no_blocks - 1]->NoOnes() == no_of_bits_in_last_block) {
			blocks[no_blocks - 1] = NULL;
			block_status[no_blocks - 1] = FB_FULL;
			block_last_one[no_blocks - 1] = no_of_bits_in_last_block - 1;
		} else if(blocks[no_blocks - 1]->NoOnes() == 0) {
			blocks[no_blocks - 1] = NULL;
			block_status[no_blocks - 1] = FB_EMPTY;
		}
		if(track_changes)
			was_block_changed.set(no_blocks - 1);
	}
}

void Filter::MoveBlockToEnd(int b)
{
	if(b != no_blocks - 1) {
		Grow(0x10000 - no_of_bits_in_last_block, false);
		no_of_bits_in_last_block = 0x10000;
	}

	AddNewBlocks(1, false, no_of_bits_in_last_block);
	no_blocks++;
	block_status[no_blocks - 1] = block_status[b];
	blocks[no_blocks - 1] = blocks[b];
	block_last_one[no_blocks - 1] = block_last_one[b];
	blocks[b] = NULL;
	block_status[b] = FB_EMPTY;
}

char * IBHeapAllocator::malloc(const size_type bytes)
{
	//		std::cerr<< (bytes >> 20) << " for Filter\n";
	//return (char*) Instance()->alloc(bytes, BLOCK_TEMPORARY, the_filter_block_owner);
	IBGuard guard(IBHeapAllocator::mutex);
	void* r;
	try {
		r = the_filter_block_owner->alloc(bytes,BLOCK_TEMPORARY);
	} catch (...) {
		return NULL;
	}
	return (char*)r;
}

void IBHeapAllocator::free(char * const block)
{
	//Instance()->dealloc(block, the_filter_block_owner);
	IBGuard guard(IBHeapAllocator::mutex);
	the_filter_block_owner->dealloc(block);
}

///////////////////////////////////////////////////////////////////////////////////////////

const int Filter::BlockAllocator::pool_size = 59;
const int Filter::BlockAllocator::pool_stride = 7;

Filter::BlockAllocator::BlockAllocator() : block_object_pool(sizeof (Filter::Block))
{
	free_in_pool = 0;
	pool = new Block*[pool_size];
	next_ndx = 0;
}

Filter::BlockAllocator::~BlockAllocator()
{
	delete [] pool;
}

Filter::Block* Filter::BlockAllocator::Alloc(bool sync)
{
//	return (Filter::Block* ) malloc(sizeof(Filter::Block));

	if(sync)
		block_mut.Lock();
	if(!free_in_pool) {
		for(int i =0; i< pool_size; i++) {
				pool[i] = (Filter::Block*)block_object_pool.malloc();
				if(!pool[i]) {
					block_mut.Unlock();
					throw OutOfMemoryRCException();
				}
			}
		free_in_pool = pool_size;
		next_ndx = 0;
	}
	free_in_pool--;
	Block* b = pool[next_ndx];
	next_ndx = (next_ndx+pool_stride) % pool_size;
	if(sync)
		block_mut.Unlock();
	return b;
}

void Filter::BlockAllocator::Dealloc(Block* b)
{
//	free(b); return;
	block_mut.Lock();
	block_object_pool.free(b);
	block_mut.Unlock();
}
