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

#ifndef JOINERSORT_H_
#define JOINERSORT_H_

#include "Joiner.h"
#include "system/CacheableItem.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "ColumnBinEncoder.h"

class JoinerSortWrapper;

class JoinerSort : public TwoDimensionalJoiner
{
	/*
	* Algorithm:
	*  1. Encode both columns with a common ColumnBinEncoder.
	*  2. Insert sorted values (keys) and all dimension data into two sorters.
	*	  - omit roughly packs that are outside the scope.
	*  3. Sort them by keys.
	*  4. Traverse in parallel both sorters:
	*     - Take a row from the first one. Match it and join with the current tuples cache.
	*     - Take rows from the second sorter, putting them into cache.
	*  5. Check all additional conditions before inserting resulting rows into output multiindex.
	*/
public:
	JoinerSort( MultiIndex *_mind, RoughMultiIndex *_rmind, TempTable *_table, JoinTips &_tips)
		: TwoDimensionalJoiner( _mind, _rmind, _table, _tips),
		other_cond_exist(false), watch_traversed(false), watch_matched(false), outer_filter(NULL), outer_nulls_only(false)  {}
	~JoinerSort();

	void ExecuteJoinConditions(Condition& cond);

private:
	void AddTuples(MINewContents &new_mind, JoinerSortWrapper &sort_wrapper, 
					unsigned char *matched_row, _int64 &result_size, DimensionVector &dims_used_in_other_cond);
	_int64 AddOuterTuples(MINewContents &new_mind, JoinerSortWrapper &sort_encoder, DimensionVector &iterate_watched);

	bool other_cond_exist;				// if true, then check of other conditions is needed
	std::vector<Descriptor> other_cond;

	/////////////////////////////////////////////////////
	// Outer join part
	// If one of the following is true, we are in outer join situation:
	bool watch_traversed;		// true if we need to watch which traversed tuples are used
	bool watch_matched;			// true if we need to watch which matched tuples are used
	Filter *outer_filter;		// used to remember which traversed or matched tuples are involved in join
	bool outer_nulls_only;		// true if only null (outer) rows may exists in result

};

//////////////////////////////////////////////////

class JoinerSortWrapper : public TrackableObject
{
public:
	JoinerSortWrapper(bool _less);
	~JoinerSortWrapper();

	bool SetKeyColumns(VirtualColumn *v1, VirtualColumn *v2);		// return false if not compatible
	void SetDimensions(MultiIndex *mind, DimensionVector &dim_tr, DimensionVector &dim_match, DimensionVector &dim_other, bool count_only);
	void InitCache(_int64 no_of_rows);						// prepare cache table

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

	int KeyBytes()							{ return key_bytes; }
	int TraverseBytes()						{ return traverse_bytes; }
	int MatchBytes()						{ return match_bytes; }

	unsigned char *EncodeForSorter1(VirtualColumn *v, MIIterator &mit, _int64 outer_pos);		// for Traversed
	unsigned char *EncodeForSorter2(VirtualColumn *v, MIIterator &mit, _int64 outer_pos);		// for Matched
	bool AddToCache(unsigned char *);		// return false if there is no more space in cache (i.e. the next operation would fail)
	void ClearCache()					{ cur_cache_used = 0; }
	_int64 CacheUsed()					{ return cur_cache_used; }

	bool PackPossible(VirtualColumn *v, MIIterator &mit);	// return false if a Matched data pack defined by (v, mit) is outside Traversed statistics, given a comparison direction stored in less

	// Dimension encoding section
	bool DimEncoded(int d)				{ return (dim_size[d] > 0); }				
	_int64 DimValue(int dim, int cache_position, unsigned char *matched_buffer);	// a stored dimension value (NULL_VALUE_64 for null object)

	// Outer joins
	void WatchMatched()					{ watch_matched = true; }
	void WatchTraversed()				{ watch_traversed = true; }
	_int64 GetOuterIndex(int cache_position, unsigned char *matched_buffer);		// take it from traversed or matched, depending on the context

private:
	bool less;
	ColumnBinEncoder *encoder;

	_int64 cache_size;			// in rows
	_int64 cur_cache_used;		// in rows

	int key_bytes;				// size of key data for both sorters
	int traverse_bytes;			// total size of the first ("traverse") sorter
	int match_bytes;			// total size of the second ("match") sorter
	int cache_bytes;			// derivable: traverse_bytes - key_byts
	unsigned char *buf;			// a temporary buffer for encoding, size: max(traverse_bytes, match_bytes)
	int buf_bytes;				// buf size, derivable: max(traverse_bytes, match_bytes)

	unsigned char *min_traversed;	// minimal key set as traversed (a buffer of key_bytes size; NULL - not set yet)

	unsigned char *cache;		// a buffer for cache

	// dimension mapping section
	int no_dims;
	std::vector<bool> dim_traversed;	// true if this dim is stored in cache
	std::vector<int>  dim_size;		// byte size of dim value
	std::vector<int>  dim_offset;	// offset of dim. value in cache or matched sorter buffer (excluding the key section)

	// outer join index encoding
	int outer_offset;			// 64-bit value; the offset excludes the key section
	bool watch_traversed;		// true if we need to watch which traversed tuples are used
	bool watch_matched;			// true if we need to watch which matched tuples are used
};

#endif
