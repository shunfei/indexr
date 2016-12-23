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

#ifndef MULTIINDEX_H_INCLUDED
#define MULTIINDEX_H_INCLUDED

#include "bintools.h"
#include "Filter.h"
#include "CQTerm.h"
#include "IndexTable.h"
#include "DimensionGroup.h"

class MIIterator;
class MINewContents;					// a class to store new (future) contents of multiindex
class MIRoughSorter;

class MultiIndex
{
public:
	MultiIndex();
	MultiIndex(const MultiIndex &s);
	~MultiIndex();

	void Copy(MultiIndex &s, bool shallow = false);			// copy the object s
	void Clear();						// clear the multiindex completely (like destructor + Multiindex())

	// Construction of the index

	// max_value - upper limit of indexes of newly added objects (e.g. number of all objects in a table to be joined)
	void AddDimension_cross(_uint64 size);		// calculate a cross product of the previous value of index and the full table (trivial filter) of 'size' objects
	void AddDimension_cross(Filter& new_f);		// calculate a cross product of the previous value of index and the new set of objects (defined as Filter, to be copied inside)

	//////////// retrieve information

	int NoDimensions() const { return no_dimensions; }	// number of dimensions
	_int64 NoTuples() const { // number of all tuples
		if(!no_tuples_too_big) return no_tuples;
		throw OutOfMemoryRCException("Too many tuples.    (85)");
		return 0; 
	}			
	_int64 NoTuples(DimensionVector &dimensions, bool fail_on_overflow = true);		// for a given subset of dimensions

	bool ZeroTuples()		{ return (!no_tuples_too_big && no_tuples == 0); }
	bool TooManyTuples()	{ return no_tuples_too_big; }

	Filter* GetFilter(int dim) const		// Get the pointer to a filter attached to a dimension. NOTE: will be NULL in case of materialized MultiIndex!
	{ return no_dimensions > 0 ? group_for_dim[dim]->GetFilter(dim) : NULL; }
	Filter* GetUpdatableFilter(int dim) const		// Get the pointer to a filter, if it may be changed. NOTE: will be NULL in case of materialized MultiIndex!
	{ return no_dimensions > 0 ? group_for_dim[dim]->GetUpdatableFilter(dim) : NULL; }
	bool NullsExist(int dim)				{ return no_dimensions > 0 ? group_for_dim[dim]->NullsPossible(dim) : false; }	// return true if there exist any 0 value (always false for virtual dimensions)
	bool MarkInvolvedDimGroups(DimensionVector &v);	// if any dimension is marked, then mark the rest of this class. Return true if anything new marked.
	bool IsOrderable(int dim)				{ return no_dimensions > 0 ? group_for_dim[dim]->IsOrderable() : true; }

	_uint64 DimSize(int dim);			// the size of one dimension: NoOnes for virtual, number of materialized tuples for materialized
	_uint64 OrigSize(int dim)			{ return dim_size[dim]; }
										// the maximal size of one dimension (e.g. the size of a table, the maximal index possible)
	//////////// Locking

	void LockForGetIndex(int dim);				// must precede GetIndex(...)
	void UnlockFromGetIndex(int dim);
	void LockAllForUse();
	void UnlockAllFromUse();

	bool IteratorLock()					// register a normal iterator; false: already locked for updating
	{
		if(iterator_lock > -1)
			iterator_lock++;
		return (iterator_lock > -1);
	}
	bool IteratorUpdatingLock()			// register an updating iterator; false: already locked
	{
		if(iterator_lock == 0) {
			iterator_lock = -1;
			return true;
		}
		return false;
	}
	void IteratorUnlock()
	{
		if(iterator_lock > 0)
			iterator_lock--;
		else
			iterator_lock = 0;
	}


	/////////// operations on the index

	void MIFilterAnd(MIIterator &mit, Filter &fd);	// limit the MultiIndex by excluding all tuples which are not present in fd, in order given by mit

	bool CanBeDistinct(int dim)	const	{ return can_be_distinct[dim]; }	// true if ( distinct(orig. column) => distinct( result ) ), false if we cannot guarantee this
	bool IsForgotten(int dim)			{ return group_for_dim[dim] ? !group_for_dim[dim]->DimEnabled(dim) : false; }	// true if the dimension is forgotten (not valid for getting value)
	bool IsUsedInOutput(int dim)		{ return used_in_output[dim]; }	// true if the dimension is used in output columns
	void SetUsedInOutput(int dim)		{ used_in_output[dim] = true; }
	void ResetUsedInOutput(int dim)		{ used_in_output[dim] = false; }

	void Empty(int dim_to_make_empty = -1);	// make an index empty (delete all tuples) with the same dimensions
											// if parameter is set, then do not delete any virtual filter except this one
	void UpdateNoTuples();		// recalculate the number of tuples
	void MakeCountOnly(_int64 mat_tuples, DimensionVector& dims_to_materialize);
								// recalculate the number of tuples, assuming mat_tuples is the new material_no_tuples and the dimensions from the list are deleted

	int MaxNoPacks(int dim);	// maximal (upper approx.) number of different nonempty data packs for the given dimension
	std::string Display();		// MultiIndex structure: f - Filter, i - IndexTable

	ConnectionInfo& ConnInfo() const { return *m_conn; }

	///////////////////////////////////////////////////////////////////////////////////

	ConnectionInfo* m_conn;

	friend class MINewContents;
	friend class MIIterator;

private:
	void AddDimension();				// declare a new dimension (internal)
	void CheckIfVirtualCanBeDistinct();	// updates can_be_distinct table in case of virtual multiindex
	std::vector<int> ListInvolvedDimGroups(DimensionVector &v);	// List all internal numbers of groups touched by the set of dimensions

	int no_dimensions;
	_int64 *dim_size;		// the size of a dimension
	_uint64 no_tuples;		// actual number of tuples (also in case of virtual index); should be updated in any change of index
	bool	no_tuples_too_big;	// this flag is set if a virtual number of tuples exceeds 2^64
	std::vector<bool> can_be_distinct;	// true if the dimension contain only one copy of original rows, false if we cannot guarantee this
	std::vector<bool> used_in_output;	// true if given dimension is used for generation of output columns

	// DimensionGroup stuff
	void FillGroupForDim();			
	std::vector<DimensionGroup*> dim_groups;		// all active dimension groups
	DimensionGroup** group_for_dim;			// pointers to elements of dim_groups, for faster dimension identification
	int*			group_num_for_dim;		// an element number of dim_groups, for faster dimension identification

	// Some technical functions
	void MultiplyNoTuples(_uint64 factor);	// the same as "no_tuples*=factor", but set no_tuples_too_big whenever needed

	int iterator_lock;			// 0 - unlocked, >0 - normal iterator exists, -1 - updating iterator exists
};

/////////////////////////////////////////////////////////////////////////////////////////////
class MINewContentsRSorter;
class JoinTips;

class MINewContents					// a class to store new (future) contents of multiindex
{
public:
	
	enum { MCT_UNSPECIFIED, MCT_MATERIAL, MCT_FILTER_FORGET, MCT_VIRTUAL_DIM } content_type;

	MINewContents(MultiIndex *m, JoinTips &tips);
	~MINewContents();

	void SetDimensions(DimensionVector &dims);	// add all these dimensions as to be involved
	void Init(_int64 initial_size);				// initialize temporary structures (set approximate size)

	void SetNewTableValue(int dim, _int64 val)	// add a value (NULL_VALUE_64 is a null object index)
	{ new_value[dim] = val; }
	void CommitNewTableValues();				// move all values set by SetNew...(); roughsort if needed; if the index is larger than the current size, enlarge table automatically
	bool CommitPack(int pack);					// in case of single filter as a result: set a pack as not changed (return false if cannot do it)

	bool NoMoreTuplesPossible();				// for optimized cases: the join may be ended
	void Commit(_int64 joined_tuples);			// commit changes to multiindex - must be called at the end, or changes will be lost
	void CommitCountOnly(_int64 joined_tuples);

	int OptimizedCaseDimension()				{ return (content_type == MCT_FILTER_FORGET ? optimized_dim_stay : -1); }

private:
	void InitTnew(int dim, _int64 initial_size);		// create t_new of proper size
	void DisableOptimized();							// switch from optimized mode to normal mode

	MINewContentsRSorter *roughsorter;	// NULL if not needed

	MultiIndex		*mind;				// external pointer
	IndexTable		**t_new;			// new tables
	_int64			obj;				// a number of values set in new tables
	int				no_dims;			// a number of dimensions in mind
	_int64*			new_value;			// the buffer for new tuple values (no_dimensions, may be mostly not used)
	DimensionVector dim_involved;		// true for dimensions which are about to have new values
	bool*			nulls_possible;		// true if a null row was set anywhere for this dimension
	bool*			forget_now;			// true if the dimension should not be filled by any contents (except # of obj)
	int				ignore_repetitions_dim;		// if not -1, then row numbers repetitions may be ignored
	int				min_block_shift;
	// for optimized case
	int				optimized_dim_stay;	// the dimension which may remain a filter after join
	Filter 			*f_opt;				// new filter for optimized case
	IndexTable		*t_opt;				// an index table prepared to switch the optimized case off
	_int64			f_opt_max_ones;		// number of ones in the original filter, for optimization purposes
	_int64			max_filter_val;		// used to check if we are not trying to update positions already passed
};

#endif /* not MULTIINDEX_H_INCLUDED */

