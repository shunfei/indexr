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

#ifndef BLOCKEDHASHTABLE_H_
#define BLOCKEDHASHTABLE_H_

#include <vector>
#include <set>
#include <boost/scoped_ptr.hpp>
#include "system/MemoryManagement/TrackableObject.h"


class MemBlockManager : public TrackableObject
{

public:
	/*!
	 * the default constructor. No limits;
	 */
	MemBlockManager();

	/*!
	 * Create the manager
	 * \param mem_limit - the allowed summarized size of blocks in bytes. Used by BlocksAllowed() function
	 * \param no_threads - how many parallel threads will be the clients. Influences BlocksAllowed() function
	 * \param mem_hard_limit - (in bytes) after summarized size of created blocks exceeds this, further requests
	 * 	for new block will fail. Value -1 means no hard limit
	 * \param block_size - size of a block in bytes
	 */
	MemBlockManager(_int64 mem_limit, int no_threads = 1, int block_size = 4*MBYTE, _int64 mem_hard_limit=-1);

	MemBlockManager(const MemBlockManager& mbm);

	/*!
	 * Must be used on a manager object created with the parameterless constructor
	 *
	 * \param mem_limit - the allowed summarized size of blocks in MB. Used by BlocksAllowed() function
	 * \param no_threads - how many parallel threads will be the clients. Influences BlocksAllowed() function
	 * \param mem_hard_limit - (in MB) after summarized size of created blocks exceeds this, further requests
	 * 	for new block will fail. Value -1 means no hard limit
	 * \param block_size - size of a block in MB
	 * \return true: initialization OK, false: initialization unsuccessful (e.g. already initialized and used)
	 */
	bool Init(_int64 mem_limit, int no_threads, int block_size = 4*MBYTE, _int64 mem_hard_limit=-1);

	/*!
	 * Set the block size when it is calculated later, after normal initialization, but before any block has been created.
	 */
	void SetBlockSize(int bsize) { mx.Lock();
		assert((current_size==0 || block_size == bsize));
		block_size = bsize;
		mx.Unlock();
	}

	/*!
	 * Delete the manager and all the memory blocks it has created, independently if they were freed with
	 * FreeBlock() or not
	 */
	~MemBlockManager();

	/*!
	 * The memory used by allocated blocks should not exceed a limit given in constructor.
	 * Before asking for a block the clients should ask if it is allowed to get a block.
	 * \return true: a memory block can be given to a client, false: a client should not ask for more blocks
	 */
	bool MoreBlocksAllowed();

	/*!
	* The memory used by allocated blocks should not exceed a limit given in constructor.
	* Before asking for a block the clients should ask if it is allowed to get a block.
	* \return a number of memory block that can be given to a client
	*/
	int MemoryBlocksLeft();

	/*!
	 *\return the size of a block in bytes as defined in constructor or Init()
	 */
	int BlockSize() {return block_size;}

	/*!
	 *  \return the number of byte size summarized across all the existing buffers
	 */
	_int64  AllocatedSize() {return current_size;}

	/*!
	*  \return the upper limit for the memory summarized across all the potential buffers
	*/
	_int64  MaxSize() { return size_limit; }

	/*!
	 * A request for a memory block.
	 * \return address of a memory block the client can use, or 0 if no blocks can be given
	 * 	(hard limit is set and reached)
	 */
	void* GetBlock();

	/*!
	 * Return a memory block to the manager.
	 * \param b - the address of a block previously obtained through GetBlock()
	 */
	void FreeBlock(void* b);

	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_TEMPORARY;}


private:
	std::vector<void*> free_blocks;
	std::set<void*> used_blocks;
	void* make_block();
	int block_size;		//all sizes in bytes
	_int64 size_limit;		//-1 if not initialized = no limit
	_int64 hard_size_limit;
	int no_threads;
	_int64 current_size;

	IBMutex mx;

};


class BlockedRowMemStorage
{
public:
	BlockedRowMemStorage();

	/*!
	 * create the storage for rows row_len bytes long, stored in memory blocks provided by mbm. mbm will
	 * be properly initialized. initial_row_count uninitialized rows will placed in the created objects
	 */
	BlockedRowMemStorage(int row_len, MemBlockManager* mbm, _uint64 initial_row_count, int min_block_len = 4*MBYTE);

	//! with a local private MemBlockmanager
	BlockedRowMemStorage(int row_len, _uint64 initial_row_count, int min_block_len = 4*MBYTE);

	//! copy reusing the same external block manager or create a deep copy if block manager is private
	BlockedRowMemStorage(const BlockedRowMemStorage& bs);

	/*!
	 * initialize the storage for rows row_len bytes long, stored in memory blocks provided by mbm.
	 */
	void Init(int row_len, MemBlockManager* mbm, _uint64 initial_size, int min_block_len = 4*MBYTE);

	//! memory blocks are release at mbm destruction
	~BlockedRowMemStorage() {if(private_bman) delete bman;}

	//! make the storage empty
	void Clear();

	//! provide the pointer to the requested row bytes
	inline void* GetRow(_int64 r)			// keep it here (inline)
	{
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(r < no_rows);
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(released_until < r);
		_int64 b = r >> npower;
		return ((char*)blocks[b])+ row_len * (r & ndx_mask);
	}

	inline void* operator[](_uint64 idx) {return GetRow( idx);}

	_int64 NoRows()		{return no_rows;}

	//!add the row (copy bytes) and return the obtained row number. row_len bytes will be copied
	_int64 AddRow(const void* r);

	//!add a row and return the obtained row number. row_len bytes will be filled by 0
	_int64 AddEmptyRow();


	inline void	Set(_uint64 idx, const void* r) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(idx < no_rows);
		memcpy(((char*)blocks[idx >> npower])+ (idx&ndx_mask)*row_len, r, row_len);
	}


	//!rewind the built-in iterator. Must be used before any traversal.
	//! \param release = true: dealloc traversed memory blocks. After traversal the storage is unusable then
	void Rewind(bool release = false);

	//! provide the index to the current row, return -1 if iterator is not valid
	_int64 GetCurrent()  {return current;}

	//! move the built-in iterator to the next row
	//! \return true : there is the next row, false - all rows traversed and subsequent GetCurrent() will return 0
	bool NextRow();

private:
	void CalculateBlockSize(int row_len, int min_block_size = 32*MBYTE);

	MemBlockManager*	bman;
	int 		row_len;		//in bytes;
	int 		block_size;		//in bytes
	int 		npower;			// block contains 2^npower rows
	_int64		ndx_mask;		// row_num & ndx_mask == row index in a block
	int 		rows_in_block;
	_int64 		no_rows;			//number of added rows
	int			no_blocks;

	std::vector<void*> blocks;
	bool 		private_bman;

	//iterator related
	_int64 current;
	bool release;
	_int64 released_until;

};

#endif /* BLOCKEDHASHTABLE_H_ */
