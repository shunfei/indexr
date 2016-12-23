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

#ifndef MIUPDATINGITERATOR_H_INCLUDED
#define MIUPDATINGITERATOR_H_INCLUDED

#include "MIIterator.h"

/*! \brief Used for iteration on chosen dimensions in a MultiIndex and for
 *
 * Usage example:
 *
 * MIUpdatingIterator it(mind,...);
 * it.Rewind();
 * while(it.IsValid()) {
 *   ...
 *   if(...condition not met...)
 *       it.ResetCurrent();			// deletion may be delayed - do not access multiindex by other iterators!
 *   ...
 *   ++it;
 * }
 * it.Commit();		// must be done to ensure
 *
 */
class MIUpdatingIterator : public MIIterator
{
	friend class MIUpdatingIteratorShadow;

public:
	MIUpdatingIterator(MultiIndex* mind, DimensionVector& dimensions);
	//MIUpdatingIterator(const MIUpdatingIterator&);		// Heavy object! Do not copy it.
	~MIUpdatingIterator();

	/*! Exclude the current iterator position from the original multiindex.
	 *  Will be physically deleted not later than on Commit(),
	 *  but earlier deletion is also possible.
	 */
	void ResetCurrent();

	/*! Exclude the current packrow (defined by GetCurPackrow()) from the original multiindex.
	 *  Will be physically deleted not later than on Commit(), but usually immediately.
	 */
	void ResetCurrentPack();

	/*! Confirm deletion of all rows excluded in the original multiindex.
	 *  Must be used at the end of operations, otherwise some deletions may be lost.
	 *  For a shallow copy of a MultiIndex (parallel WHERE) no_tuples should not be recalculated
	 */
	void Commit(bool recalculate_no_tuples = true);

	// These methods are overloaded to add new functionality:

	void Rewind();
	MIUpdatingIterator& operator++()
	{
		MIIterator::operator++();
		multi_filter_pos++;
		if(next_pack_started)
			multi_filter_pack_start = multi_filter_pos;
		return *this;
	}

	void NextPackrow();

	// Only for one-dim case:
	void RewindToRow(const _int64 row);
	bool RewindToPack(const int pack);			// return true if the pack is nonempty

	//number of ones in the given pack taking into account uncommitted resets
	// \pre iterates on a 1-dimensional multiindex
	int NoOnesUncommited(uint pack);

	/*!
	 * copies filter blocks marked as changed from mui.mind to this->mind
	 * Requires calling UpdateNoTuples() before this->mind is ready for next execution stages
	 */
	void CopyChangedBlocks(MIUpdatingIterator& mui);

	void UpdateNoTuples()	{mind->UpdateNoTuples();}
	const int SingleFilterDim() const		{ return one_filter_dim; }

	//! after traversing up to n non-empty DPs the iterator becomes invalid
	//! works only if IsSingleFilter()
	void SetNoPacksToGo(int n);

	//! from this moment the MultiIndex will remember which Filter blocks were modified
	void StartTrackingChanges() { track_changes = true; mind->GetFilter(one_filter_dim)->StartTrackingChanges(); }

	// Routines for execution of OR (only in single dimension mode):
	// - make a snapshot of a pack,
	// - evaluate condition 1,
	// - swap the result with the snapshot,
	// - evaluate condition 2,
	// - make OR of the result and the snapshot
	Filter *NewPackFilter(int pack);			// create a new filter for the snapshot and initialize it
	bool SwapPackFilter(int pack, Filter *f);	// swap the contents of the given pack; do not swap and return true if the pack in the current iterator is not full
	void OrPackFilter(int pack, Filter *f);		// make OR

private:
	void PrepareUpdater();

	bool changed;							// alias necessary for MIUpdatingIteratorOnePackShadow subclass
	bool track_changes; 
	Filter *multi_dim_filter;				// internal filter of this->NoTuples() bits; a delete mask for multiindex
	_int64 multi_filter_pos;				// current position of multidimensional filter (0..no_obj-1)
	_int64 multi_filter_pack_start;			// a position of the current rowpack start in multidimensional filter (0..no_obj-1)
};

////////////////////////////////////////////////////

#endif /* MIUPDATINGITERATOR_H_INCLUDED */

