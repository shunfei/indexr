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

#ifndef __COMPRESS_INCALLOC_H
#define __COMPRESS_INCALLOC_H

#include <vector>
#include <iostream>
#include <stdio.h>
#include "defs.h"
#include "common/bhassert.h"

// Incremental memory allocator.
// May throw CPRS_ERR_MEM.
class IncAlloc
{
	static const int FIRSTSIZE = 16384;			// default size of the first block
	static const int ROUNDUP   = 4096;			// block size will be a multiple of this
	static const double GROWSIZE;				// how big is the next block compared to the previous one

	struct Block {
		void* mem;
		uint size;
		Block() : mem(NULL), size(0) {}
		Block(void* m, uint s) : mem(m), size(s) {}
	};

	std::vector<Block> blocks;
	uint blk;		// index of the current block == no. of blocks already filled up
	uint used;		// no. of bytes in block 'blk' already used

	uint firstsize;		// size of the first block to allocate

	// frags[s] - list of pointers to free'd fragments of size 's';
	// free'd fragments can be reused by alloc()
	static const uint MAXFRAGSIZE = 6144;
	static const uint MAXNUMFRAG  = 65536*2;		// max. no. of fragments of a given size - never used as fragments are created on demand and we have no control on their number
	std::vector<void*>* frags;

	void* _alloc(uint size);			// currently, 'size' must be at most MAXFRAGSIZE
	void* _alloc_search(uint size);		// more complicated part of _alloc, executed rarely
	void _free(void* p, uint size);		// put 'p' into frags[size]; does NOT check if p points into a block!
public:
	template<class T> void alloc(T*& p, uint n = 1)		{ p = (T*)_alloc(sizeof(T)*n); }
	template<class T> void freemem(T* p, uint n = 1)	{ _free(p, sizeof(T)*n); }

	void freeall();			// mark all blocks as free, without deallocation; clear lists of fragments
	void clear();			// physically deallocate all blocks; clear lists of fragments
	void clearfrag();		// clear lists of fragments

	void GetMemUsg(uint& memblock, uint& memalloc, uint& memused);
	void PrintMemUsg(FILE* f);
	void PrintMemUsg(std::ostream& str);

	IncAlloc(uint fsize = FIRSTSIZE);
	~IncAlloc()			{ clear(); delete[] frags; }
};


inline void* IncAlloc::_alloc(uint size)
{
	// find a fragment...
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(size && (size <= MAXFRAGSIZE));
	std::vector<void*>& frag = frags[size];
	if(frag.size()) {
		void* mem = frag.back();
		frag.pop_back();
		return mem;
	}

	// ...or take a new one from the current block
	if(blk < blocks.size() && (blocks[blk].size >= used + size)) {
		void* mem = (char*)blocks[blk].mem + used;
		used += size;
		return mem;
	}

	// ...or find/allocate a new block
	return _alloc_search(size);
}

inline void IncAlloc::_free(void* p, uint size)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(size && (size <= MAXFRAGSIZE));
	frags[size].push_back(p);

}


#endif
