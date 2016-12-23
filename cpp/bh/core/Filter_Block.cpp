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

#include "Filter.h"
#include "common/bhassert.h"
#include "tools.h"
#include "bintools.h"
using namespace bh;


///////////////////////////////////////////////////////////////////////////////////////

const int Filter::Block::posOf1[] = {
		8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
		4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
};

const uint Filter::Block::lshift1[] = {
	1,2,4,8,0x10,0x20,0x40,0x80,
	0x100,0x200,0x400, 0x800,0x1000,0x2000,0x4000, 0x8000,
	0x10000,0x20000,0x40000, 0x80000, 0x100000,0x200000,0x400000, 0x800000,
	0x1000000,0x2000000,0x4000000, 0x8000000, 0x10000000,0x20000000,0x40000000, 0x80000000,
};

Filter::Block::Block(Filter* owner, int _no_obj, bool all_full)
{
	Init(owner, _no_obj, all_full);
}

void Filter::Block::Init(Filter* owner, int _no_obj, bool all_full)
{
	MEASURE_FET("Filter::Block::Init()");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(_no_obj>0 && _no_obj<=65536);

	CommonInit(owner);
	no_obj = _no_obj;
	block_size = (NoObj() + 31) / 32;
	block_table = 0;
	Clean(all_full);
}

Filter::Block::Block(Block const& block, Filter* owner)
{
	MEASURE_FET("Filter::Block::Block(...)");
	block_table = 0;
	CopyFrom(block, owner);
}

void Filter::Block::CopyFrom(Block const& block, Filter* owner)
{
	MEASURE_FET("Filter::Block::CopyFrom(...)");
	CommonInit(owner);
	block_size = block.block_size;
	no_set_bits = block.no_set_bits;
	no_obj = block.no_obj;
	if(block.block_table) {
		if(!block_table) {
			owner->bit_mut->Lock();
			block_table = (uint*)owner->bit_block_pool->malloc();
			owner->bit_mut->Unlock();
			if(!block_table)
				throw OutOfMemoryRCException();
		}
		memcpy(block_table, block.block_table, block_size * sizeof(uint));
	}
}

Filter::Block* Filter::Block::MoveFromShallowCopy(Filter* new_owner) {
	assert(owner->GetBitBlockPool() == new_owner->GetBitBlockPool());
	owner = new_owner;
	return this;
}

bool Filter::Block::IsNumberOfOnesInHeaderProper(int& tst)
{	
	int test_bits = 0;		// verify filter pack consistency
	for(int i = 0; i < no_obj; i++)
		if(Get(i)) test_bits++;

	tst = test_bits;
	return (test_bits == no_set_bits);
}

void Filter::Block::CommonInit(Filter * owner)
{
	no_set_bits = 0;
	this->owner = owner;
}

Filter::Block::~Block()
{
	if (block_table) {
		owner->bit_mut->Lock();
		owner->bit_block_pool->free(block_table);
		owner->bit_mut->Unlock();
	}
}

bool Filter::Block::Set(int n)
{
	uint  mask = lshift1[n & 31];
	uint& cur_p = block_table[n >> 5];
	if(!(cur_p & mask)) //if this operation change value
	{
		no_set_bits++;
		cur_p |= mask;
	}
	return no_set_bits == 0 || no_set_bits == no_obj;	// note that no_set_bits==0 here only when the pack is full
}

bool Filter::Block::Set(int  n1, int n2)
{
	for(int i = n1; i <= n2; i++)
		if(Set(i))			// stop the loop once the block is full
			return true;
	return false;
}

void Filter::Block::Reset()
{
	if (block_table) {
		owner->bit_mut->Lock();
		owner->bit_block_pool->free(block_table);
		owner->bit_mut->Unlock();
		block_table = NULL;
	}
	no_set_bits = 0;
}


bool Filter::Block::Reset(int n1, int n2)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n1 <= n2 && n2 < no_obj);
	int bl1 = n1/32;
	int bl2 = n2/32;
	int off1 = (n1 & 31);
	int off2 = (n2 & 31);
	if(no_set_bits == no_obj)	// just created as full
	{
		if(bl1==bl2)
		{
			for(int i = off1; i <= off2; i++)
 				block_table[bl1] &= ~lshift1[i];
		}
		else
		{
			for(int i = off1; i < 32; i++)
				block_table[bl1] &= ~lshift1[i];
			for(int i = bl1+1; i < bl2; i++)
				block_table[i] = 0;
			for(int i = 0; i <= off2; i++)
				block_table[bl2] &= ~lshift1[i];
		}
		no_set_bits = no_obj - (n2 - n1 + 1);
	}
	else
	{
		if(bl1==bl2)
		{
			for(int i = n1; i <= n2; i++)
				Reset(i);
		}
		else
		{
			for(int i = off1; i < 32; i++)
				if((block_table[bl1] & lshift1[i])) //if this operation change value
				{
					no_set_bits--;
					if(no_set_bits == 0)
						break;
					block_table[bl1] &= ~lshift1[i];
				}
				if(no_set_bits > 0)
					for(int i = bl1 + 1; i < bl2; i++)
					{
						no_set_bits -= CalculateBinSum(block_table[i]);
						block_table[i] = 0;
						if(no_set_bits == 0)
							break;
					}
					if(no_set_bits>0)
						for(int i = 0; i <= off2; i++)
							if((block_table[bl2] & lshift1[i])) //if this operation change value
							{
								no_set_bits--;
								if(no_set_bits == 0)
									break;
								block_table[bl2] &= ~lshift1[i];
							}
		}
	}
	return (no_set_bits==0);
}

bool Filter::Block::IsEmptyBetween(int n1, int n2)
{
	int bl1 = n1/32;
	int bl2 = n2/32;
	int off1 = (n1 & 31);
	int off2 = (n2 & 31);
	if(bl1 == bl2) {
		if(block_table[bl1] == 0)
			return true;
		for(int i = off1; i <= off2; i++)
			if((block_table[bl1] & lshift1[i]))
				return false;
	}
	else
	{
		int i_start = (off1 == 0? bl1 : bl1 + 1);
		int i_stop  = (off2 == 31? bl2 : bl2 - 1);
		for(int i = i_start; i <= i_stop; i++)
			if(block_table[i] != 0)
				return false;
		if(bl1 != i_start) {
			for(int i = off1; i <= 31; i++)
				if((block_table[bl1] & lshift1[i]))
					return false;
		}
		if(bl2 != i_stop) {
			for(int i = 0; i <= off2; i++)
				if((block_table[bl2] & lshift1[i]))
					return false;
		}
	}
	return true;
}

bool Filter::Block::IsFullBetween(int n1, int n2)
{
	int bl1 = n1/32;
	int bl2 = n2/32;
	int off1 = (n1 & 31);
	int off2 = (n2 & 31);
	if(bl1 == bl2)
	{
		if(block_table[bl1] == 0xFFFFFFFF)
			return true;
		for(int i = off1; i <= off2; i++)
			if((block_table[bl1] & lshift1[i]) == 0)
				return false;
	}
	else
	{
		int i_start = (off1 == 0? bl1 : bl1 + 1);
		int i_stop  = (off2 == 31? bl2 : bl2 - 1);
		for(int i = i_start; i <= i_stop; i++)
			if(block_table[i] != 0xFFFFFFFF)
				return false;
		if(bl1 != i_start) {
			for(int i = off2; i <= 31; i++)
				if((block_table[bl1] & lshift1[i]) == 0)
					return false;
		}
		if(bl2 != i_stop) {
			for(int i = 0; i <= off2; i++)
				if((block_table[bl2] & lshift1[i]) == 0)
					return false;
		}
	}
	return true;
}

int Filter::Block::NoOnesBetween(int n1, int n2)
{
	int result = 0;
	int bl1 = n1/32;
	int bl2 = n2/32;
	int off1 = (n1 & 31);
	int off2 = (n2 & 31);
	if(bl1 == bl2) {
		if(block_table[bl1] == 0)
			return 0;
		for(int i = off1; i <= off2; i++)
			result += (block_table[bl1] & lshift1[i]) ? 1 : 0;
	} else {
		int i_start = (off1 == 0? bl1 : bl1 + 1);
		int i_stop  = (off2 == 31? bl2 : bl2 - 1);
		for(int i = i_start; i <= i_stop; i++)
			result += CalculateBinSum(block_table[i]);
		if(bl1 != i_start) {
			for(int i = off1; i <= 31; i++)
				result += (block_table[bl1] & lshift1[i]) ? 1 : 0;
		}
		if(bl2 != i_stop) {
			for(int i = 0; i <= off2; i++)
				result += (block_table[bl2] & lshift1[i]) ? 1 : 0;
		}
	}
	return result;
}

int Filter::Block::BlockContinuous()
{
	int last_int = (no_obj / 32) - 1;
	int full_ints_to_check = (no_set_bits / 32);
	if(last_int > full_ints_to_check + 1 && block_table[last_int] != 0)			// check the end of block
		return -1;
	for(int i = 0; i < full_ints_to_check; i++)									// check from the beginning of block
		if(block_table[i] != 0xffffffff)
			return -1;
	for(int i = full_ints_to_check * 32; i < no_set_bits; i++)
		if(!Get(i))
			return -1;
	return no_set_bits;
}

bool Filter::Block::IsEqual(Block &b2)
{
	int mn = (no_obj - 1) / 32;
	for(int n = 0; n < mn; n++) {
		if(block_table[n] != b2.block_table[n])
			return false;
	}
	unsigned int mask = 0xffffffff >> ( 32 - (((no_obj - 1) % 32) + 1) );
	if( (block_table[mn] & mask) != (b2.block_table[mn] & mask))
		return false;
	return true;
}

bool Filter::Block::And(Block &b2)
{
	int mn = b2.NoObj() < NoObj() ? b2.NoObj() : NoObj();
	for(int n = 0; n < mn; n++)
	{
		if(Get(n) && !b2.Get(n))
			Reset(n);
	}
	return (no_set_bits==0);
}

bool Filter::Block::Or(Block &b2)
{
	int new_set_bits = 0;
	int no_positions = NoObj()/32;
	for(int b = 0; b < no_positions; b++) {
		block_table[b] |= b2.block_table[b];
		new_set_bits += CalculateBinSum(block_table[b]);
	}
	no_set_bits = new_set_bits;
	int no_all_obj = (int)NoObj();
	if(no_all_obj%32)
		for(int n = (no_all_obj / 32) * 32; n < no_all_obj; n++) {
			if(Get(n))
				no_set_bits++;
			else if(b2.Get(n))
				Set(n);				// no_set_bits increased inside
		}
	return no_set_bits==0 || no_set_bits == no_obj;			// 0 here means only a full pack, other possibilities are excluded by an upper level
}

bool Filter::Block::AndNot(Block &b2)
{
	int mn = b2.NoObj() < NoObj() ? b2.NoObj() : NoObj();
	for(int n = 0; n < mn; n++)	{
		if(Get(n) && b2.Get(n))
			Reset(n);
	}
	return (no_set_bits == 0);
}

void Filter::Block::Not()
{
	int new_set_bits;
	new_set_bits = no_obj - no_set_bits;
	int no_all_obj = (int)NoObj();
	if(no_all_obj%32)
		for(int n = (no_all_obj / 32) * 32; n < no_all_obj; n++)
			if(Get(n))
				Reset(n);
			else
				Set(n);
	for(uint b = 0; b < NoObj()/32; b++)
		block_table[b] = ~(block_table[b]);

	no_set_bits = new_set_bits;
}

void Filter::Block::Clean(bool value)
{
	if(!block_table)
	{
		owner->bit_mut->Lock();
		block_table =(uint*) owner->bit_block_pool->malloc(); //(uint*)alloc(block_size*sizeof(uint), BLOCK_UNCOMPRESSED, owner);
		owner->bit_mut->Unlock();
		if(!block_table)
			throw OutOfMemoryRCException();
	}
	if(value)
	{
		memset(block_table, 255, Filter::bitBlockSize);
		no_set_bits = no_obj;					// Note: when the block is full, the overflow occurs and no_set_bits=0.
	}
	else
	{
		memset(block_table, 0, Filter::bitBlockSize);
		no_set_bits = 0;
	}
}

//set bits to value, do not pay attention to previous values
void Filter::Block::InitBits(int n1, int n2, bool value)
{
	if (value)
		for(int i = n1; i <= n2; i++) {
				block_table[i/32] |= lshift1[i & 31];
				no_set_bits ++;
		}
	else
		for(int i = n1; i <= n2; i++)
				block_table[i/32] &= ~lshift1[i & 31];
}

void Filter::Block::GrowBlock(int grow_size, bool value)
{
	BHASSERT(NoObj() + grow_size <= 0x10000, "Filter::Block grow beyond pack size");
	InitBits(no_obj, no_obj + grow_size - 1, value);
	no_obj+= grow_size;
	block_size = (NoObj() + 31) / 32;
}

void Filter::Block::ShrinkBlock(int new_size)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_size <= no_obj);
	if(new_size == no_obj)
		return;
	Reset(new_size, std::min<int>(0xFFFF, no_obj - 1));
	no_obj = new_size;
	block_size = (NoObj() + 31) / 32;
}
