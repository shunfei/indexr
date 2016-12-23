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

#include "MIUpdatingIterator.h"

using namespace std;

MIUpdatingIterator::MIUpdatingIterator(MultiIndex* _mind, DimensionVector& dimensions)
	: MIIterator(_mind, dimensions), changed(false), track_changes(false),
	multi_dim_filter(NULL),
	multi_filter_pos(0), multi_filter_pack_start(0)
{
	mind->IteratorUnlock();		// unlock a base class lock
	bool success = mind->IteratorUpdatingLock();
	assert(success);			// Multiindex was already locked for reading or updating!
	PrepareUpdater();
}

//MIUpdatingIterator::MIUpdatingIterator(const MIUpdatingIterator& sec)
//	: MIIterator(sec, false), changed(false), track_changes(sec.track_changes),
//	multi_dim_filter(NULL),
//	multi_filter_pos(0), multi_filter_pack_start(0)
//{
//	assert(sec.SingleFilterDim() > -1);			// otherwise HEAVY OBJECT - do not copy
//	if(track_changes)
//		mind->GetFilter(one_filter_dim)->StartTrackingChanges(); //tracking is necessary in copies in the current application (parallel scan)
//}
//
MIUpdatingIterator::~MIUpdatingIterator()
{
	mind->IteratorUnlock();
	delete multi_dim_filter;
}

void MIUpdatingIterator::PrepareUpdater()
{
	if(one_filter_dim > -1 && !(it[it_for_dim[one_filter_dim]]->InternallyUpdatable()))
		one_filter_dim = -1;
	if(one_filter_dim == -1) {
		assert(multi_dim_filter == NULL);
		multi_dim_filter = new Filter(no_obj);
		multi_dim_filter->Set();
		multi_filter_pos = 0;
	}
}

void MIUpdatingIterator::ResetCurrent()
{
	changed = true;
	if(one_filter_dim > -1)
		one_filter_it->ResetCurrent();
	else
		multi_dim_filter->ResetDelayed(multi_filter_pos);
}

void MIUpdatingIterator::ResetCurrentPack()
{
	changed = true;
	if(one_filter_dim > -1)
		one_filter_it->ResetCurrentPackrow();
	else {
		multi_dim_filter->Commit();
		multi_dim_filter->ResetBetween(multi_filter_pack_start, multi_filter_pos + pack_size_left - 1);
	}
}

void MIUpdatingIterator::Commit(bool recalculate_no_tuples)
{
	if(!changed)
		return;
	if(one_filter_dim > -1) {
		one_filter_it->CommitUpdate();		// working directly on multiindex filer (special case)
		if(recalculate_no_tuples)
			mind->UpdateNoTuples();			//not for parallel WHERE - shallow copy of MultiIndex/Filter
	} else {
		multi_dim_filter->Commit();
		mind->IteratorUnlock();			// unlock to allow creating the iterator below
		{	// create scope to ensure that mit_read will be unlocked on the end
			MIIterator mit_read(*this);		// make a traversing copy of the current iterator
			mind->MIFilterAnd(mit_read, *multi_dim_filter);		// UpdateNoTuples inside
		}
		mind->IteratorUpdatingLock();	// lock it again for consistency
	}
	changed = false;
}

void MIUpdatingIterator::Rewind()
{
	Commit();
	MIIterator::Rewind();
	multi_filter_pos = 0;
	multi_filter_pack_start = 0;
}

void MIUpdatingIterator::NextPackrow()
{
	multi_filter_pos += pack_size_left;
	MIIterator::NextPackrow();
}

int MIUpdatingIterator::NoOnesUncommited(uint pack)
{
	assert(one_filter_it);
	if(one_filter_dim > -1)
		return one_filter_it->NoOnesUncommited(pack);
	else
		return -1;
}


void MIUpdatingIterator::SetNoPacksToGo(int n)
{
	assert(one_filter_it);
	one_filter_it->SetNoPacksToGo(n);
}

void MIUpdatingIterator::CopyChangedBlocks(MIUpdatingIterator& mui)
{
	assert(one_filter_it && one_filter_dim == mui.one_filter_dim);
	mind->GetFilter(one_filter_dim)->CopyChangedBlocks(*mui.mind->GetFilter(one_filter_dim));
}

void MIUpdatingIterator::RewindToRow(const _int64 row)
{
	assert(one_filter_it);
	one_filter_it->RewindToRow(row);
	valid = one_filter_it->IsValid();
	if(valid) {
		cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
		cur_pack[one_filter_dim] = int(cur_pos[one_filter_dim] >> 16);
		pack_size_left = one_filter_it->GetPackSizeLeft();
	}
}

bool MIUpdatingIterator::RewindToPack(const int pack)
{
	// if SetNoPacksToGo() has been used, then RewindToPack can be done only to the previous pack
	assert(one_filter_it);
	Commit(false);
	bool res = one_filter_it->RewindToPack(pack);
	valid = one_filter_it->IsValid();
	if(valid) {
		cur_pos[one_filter_dim] = one_filter_it->GetCurPos(one_filter_dim);
		cur_pack[one_filter_dim] = int(cur_pos[one_filter_dim] >> 16);
		pack_size_left = one_filter_it->GetPackSizeLeft();
		next_pack_started = true;
	}
	return res;
}

Filter *MIUpdatingIterator::NewPackFilter(int pack)			// create a new filter for the snapshot and initialize it
{
	assert(one_filter_it);
	Filter *f_old = mind->GetFilter(one_filter_dim);
	Filter *new_f = new Filter(f_old->NoObj());
	new_f->Or(*f_old, pack);
	return new_f;	
}

bool MIUpdatingIterator::SwapPackFilter(int pack, Filter *f)	// swap the contents of the given pack; return false without swapping if the current pack is full
{
	assert(one_filter_it);
	Commit(false);
	Filter *f_old = mind->GetFilter(one_filter_dim);
	if(f_old->IsFull(pack))
		return false;
	f_old->SwapPack(*f, pack);
	return true;
}

void MIUpdatingIterator::OrPackFilter(int pack, Filter *f)	// make OR
{
	assert(one_filter_it);
	Commit(false);
	Filter *f_old = mind->GetFilter(one_filter_dim);
	f_old->Or(*f, pack);
}
