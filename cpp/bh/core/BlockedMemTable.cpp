/* Copyright (C)  2005-2009 Infobright Inc.

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

#include "BlockedMemTable.h"
#include <iostream>
using namespace std;

MemBlockManager::MemBlockManager() :
		size_limit(-1), no_threads(1), block_size(-1), hard_size_limit(-1), current_size(0)   {
}

MemBlockManager::MemBlockManager(_int64 mem_limit, int no_th, int b_size,
		_int64 mem_hard_limit) :
		size_limit(-1) {
	Init(mem_limit, no_th, b_size, mem_hard_limit);
}

MemBlockManager::MemBlockManager(const MemBlockManager& mbm) : block_size(mbm.block_size), size_limit(mbm.size_limit),
	hard_size_limit(mbm.hard_size_limit), no_threads(mbm.no_threads), current_size(0)
{
	// the copy must be empty, because the copied block would be unavailable externally
}


bool MemBlockManager::Init(_int64 mem_limit, int no_th, int b_size,
		_int64 mem_hard_limit) {
	IBGuard g(mx);
	if (size_limit != -1)
		return false;
	size_limit = mem_limit;
	no_threads = no_th;
	block_size = b_size;
	hard_size_limit = mem_hard_limit;
	current_size = 0;
	return true;
}

MemBlockManager::~MemBlockManager() {
	for (int i = 0; i < free_blocks.size(); i++)
		dealloc(free_blocks[i]);
//	assert(used_blocks.size() == 0);
	for (std::set<void*>::const_iterator it = used_blocks.begin();
			it != used_blocks.end(); ++it)
		dealloc(*it);
}

void* MemBlockManager::GetBlock() {
	assert(block_size != -1);

	if (hard_size_limit != -1 && hard_size_limit <= current_size)
		return NULL;

	{
		IBGuard g(mx);
		void* p;
		if (free_blocks.size() > 0) {
			p = free_blocks[free_blocks.size() - 1];
			free_blocks.pop_back();
		} else {
			p = alloc(block_size, BLOCK_TEMPORARY);
			current_size += block_size;
		}
		used_blocks.insert(p);
		return p;
	}
}

bool MemBlockManager::MoreBlocksAllowed() {
	if(block_size == -1)
		return false;	//uninitialized
	if (size_limit == -1)
		return true; //no limit
	if (MemoryBlocksLeft() > 0)
		return true;
	return false;
}

int MemBlockManager::MemoryBlocksLeft() {
	if (size_limit == -1)
		return 9999; //uninitialized => no limit
	_int64 s;
	mx.Lock();
	s = free_blocks.size();
	mx.Unlock();
	_int64 size_in_near_future = current_size
			+ block_size * ((no_threads >> 1) - s);
	if (size_in_near_future > size_limit)
		return 0;
	return int((size_limit - size_in_near_future) / block_size);
}

void MemBlockManager::FreeBlock(void* b) {
	if (b == NULL)
		return;
	mx.Lock();
	size_t r = used_blocks.erase(b);
	if (r == 1) {
		if(size_limit == -1) {
			//no limit - keep a few freed blocks
			if(free_blocks.size() >5) {
				dealloc(b);
				current_size -= block_size;
			} else
				free_blocks.push_back(b);
		} else
		//often freed block are immediately required by other - keep some of them above the limit in the pool
		if (current_size > size_limit + block_size * (no_threads + 1)) {
			dealloc(b);
			current_size -= block_size;
		} else
			free_blocks.push_back(b);
	} // else not found (already erased)
	mx.Unlock();
}
//--------------------------------------------------------------------------

BlockedRowMemStorage::BlockedRowMemStorage() :
		no_rows(0), no_blocks(0), current(-1), private_bman(false){
}

BlockedRowMemStorage::BlockedRowMemStorage(int row_len, MemBlockManager* mbm,
		_uint64 initial_size, int min_block_len) {
	Init(row_len, mbm, initial_size, min_block_len);
	private_bman = false;
}

BlockedRowMemStorage::BlockedRowMemStorage(int row_len, _uint64 initial_size, int min_block_len) :
	no_rows(0), no_blocks(0){
	private_bman = true;
	Init(row_len, new MemBlockManager(), initial_size, min_block_len);
}


void BlockedRowMemStorage::Init(int rowl, MemBlockManager* mbm,
		_uint64 initial_size, int min_block_len) {
	assert(no_rows == 0 && no_blocks == 0);
	CalculateBlockSize(rowl, min_block_len);
	bman = mbm;
	bman->SetBlockSize(block_size);
	row_len = rowl;
	no_blocks = 0;
	release = false;
	released_until = -2;
	current = -1;
	if (initial_size > 0)
		for (int i = 0; i < (initial_size / rows_in_block) + 1; i++) {
			void* b = bman->GetBlock();
			if (!b)
				throw OutOfMemoryRCException(
						"too large initial BlockedRowMemStorage size");
			blocks.push_back(b);
			no_blocks++;
		}
	no_rows = initial_size;
}

void BlockedRowMemStorage::Clear() {
	for (int i = 0; i < no_blocks; i++)
		bman->FreeBlock(blocks[i]); // blocks[i] may be NULL already
	blocks.clear();
	no_blocks = 0;
	no_rows = 0;
	release = false;
	current = -1;
	released_until = -2;
}

BlockedRowMemStorage::BlockedRowMemStorage(const BlockedRowMemStorage& bs) :
				row_len(bs.row_len), block_size(bs.block_size), npower(
				bs.npower), ndx_mask(bs.ndx_mask), rows_in_block(
				bs.rows_in_block), no_rows(bs.no_rows), no_blocks(bs.no_blocks), current(bs.current), release(bs.release), released_until(
				bs.released_until), private_bman(bs.private_bman) {
	if(private_bman) {
		bman = new MemBlockManager(*bs.bman); //the copy is empty
		for(int i=0; i< bs.blocks.size(); i++) {
			void* b = bman->GetBlock();
			blocks.push_back(b);
			memcpy(b, bs.blocks[i], block_size);
		}
	} else
		bman = bs.bman;
}

//! compute block_sioze so that 2^n rows fit in a block and the chosen block size >= min_block_size
void BlockedRowMemStorage::CalculateBlockSize(int row_len, int min_block_size) {
	npower = 0;
	for (rows_in_block = 1; row_len * rows_in_block < min_block_size;
			rows_in_block *= 2) {
		++npower;
	}
	ndx_mask = (1 << npower) - 1;
	block_size = rows_in_block * row_len;
}

_int64 BlockedRowMemStorage::AddRow(const void* r) {
	if ((no_rows & ndx_mask) == 0) {
		void* b = bman->GetBlock();
		blocks.push_back(b);
		no_blocks++;
	}
	memcpy(((char*) blocks[no_blocks - 1]) + (no_rows & ndx_mask) * row_len, r,
			row_len);

	return no_rows++;
}

_int64 BlockedRowMemStorage::AddEmptyRow() {
	if ((no_rows & ndx_mask) == 0) {
		void* b = bman->GetBlock();
		blocks.push_back(b);
		no_blocks++;
	}
	memset(((char*) blocks[no_blocks - 1]) + (no_rows & ndx_mask) * row_len, 0,
			row_len);

	return no_rows++;
}

void BlockedRowMemStorage::Rewind(bool rel) {
	assert(!release);
	release = rel;
	if (no_rows == 0) {
		current = -1;
	} else {
		current = 0;
	}
}

bool BlockedRowMemStorage::NextRow() {
	if (++current == no_rows) {
		current = -1;
		return false;
	}
	if (release) {
		released_until++;
		if ((current & ndx_mask) == 0) {
			bman->FreeBlock(blocks[(current >> npower) - 1]);
			blocks[(current >> npower) - 1] = NULL;
		}
	}

	return true;
}

