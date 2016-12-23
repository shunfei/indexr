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

#include "MIIterator.h"
#include "core/PackOrderer.h"

using namespace std;

vector<PackOrderer> null_order; //use null_order in constructor if ordering not desired

MIIterator::MIIterator() : po(null_order)
{
	mind = NULL;
	no_dims = 0;
	mind_created_locally = false;
	cur_pos = NULL;
	cur_pack = NULL;
	it_for_dim = NULL;
	valid = false;
	omitted_factor = 1;
	next_pack_started = false;
	one_filter_dim = -1;
	one_filter_it = NULL;
	mii_type = MII_DUMMY;
}

MIIterator::MIIterator(MultiIndex* _mind, DimensionVector &_dimensions) : mind(_mind), po(null_order)
{
	no_dims = mind->NoDimensions();
	mind_created_locally = false;
	cur_pos = NULL;
	cur_pack = NULL;
	it_for_dim = NULL;
	one_filter_dim = -1;
	one_filter_it = NULL;
	dimensions = _dimensions;
	Init();
}

MIIterator::MIIterator(MultiIndex* _mind, DimensionVector &_dimensions, std::vector<PackOrderer>& _po) : mind(_mind), po(_po) // Ordered iterator
{
	no_dims = mind->NoDimensions();
	mind_created_locally = false;
	cur_pos = NULL;
	cur_pack = NULL;
	it_for_dim = NULL;
	one_filter_dim = -1;
	one_filter_it = NULL;
	dimensions = _dimensions;
	Init();
}

MIIterator::MIIterator(MultiIndex* _mind) : mind(_mind), po(null_order)
{
	if(mind == NULL) {
		mind_created_locally = true;
		mind = new MultiIndex();
		mind->AddDimension_cross(1);	// just one row
	} else
		mind_created_locally = false;
	no_dims = mind->NoDimensions();
	cur_pos = NULL;
	cur_pack = NULL;
	it_for_dim = NULL;
	one_filter_dim = -1;
	one_filter_it = NULL;
	dimensions = DimensionVector(no_dims);
	dimensions.SetAll();
	Init();
}

MIIterator::MIIterator(MultiIndex* _mind, int one_dimension, bool lock) : mind(_mind), po(null_order)
{
	no_dims = mind->NoDimensions();
	mind_created_locally = false;
	cur_pos = NULL;
	cur_pack = NULL;
	dimensions = DimensionVector(no_dims);
	one_filter_dim = -1;
	one_filter_it = NULL;
	if(one_dimension == -1)
		dimensions.SetAll();
	else
		dimensions[one_dimension] = true;
	Init(lock);
}

MIIterator::MIIterator(const MIIterator& sec, bool lock)
	: dg(sec.dg),	
	it_for_dim( NULL),
	mind( sec.mind ),
	mind_created_locally(sec.mind_created_locally),
	no_dims( sec.no_dims ),
	dimensions(sec.dimensions),
	cur_pos( NULL ),			// will be initialized below
	cur_pack( NULL ),
	valid( sec.valid ),
	omitted_factor( sec.omitted_factor ),
	no_obj( sec.no_obj ),
	pack_size_left( sec.pack_size_left ),
	next_pack_started( sec.next_pack_started ),
	mii_type( sec.mii_type ),
	po( sec.po ),
	one_filter_dim( sec.one_filter_dim )
{
	if(sec.mind_created_locally) {
		mind = new MultiIndex(*sec.mind);
		assert(no_dims == 1);
		dg[0] = mind->dim_groups[0];			// assuming that the local minds are one-dimensional
	}
	if(no_dims > 0) {
		cur_pos = new _int64 [no_dims];
		cur_pack = new int [no_dims];
		it_for_dim = new int [no_dims];
	}
	if(no_dims > 0 && lock) {
		bool success = mind->IteratorLock();
		assert(success);			// Multiindex was already locked for updating!
	}
	for(int i = 0; i < no_dims; i++) {
		it_for_dim[i] = sec.it_for_dim[i];
		cur_pos[i] = sec.cur_pos[i];
		cur_pack[i] = sec.cur_pack[i];
	}
	if(dimensions.Size() > 0)				// may be not initialized for dummy iterators
		for(int i = 0; i < no_dims; i++)
			if(dimensions[i])
				mind->LockForGetIndex(i);
	for(int i = 0; i < sec.it.size(); i++)
		it.push_back(sec.dg[i]->CopyIterator(sec.it[i]));

	one_filter_it = NULL;
	if(one_filter_dim > -1) {
		if(po.size() == 0)
			one_filter_it = (DimensionGroupFilter::DGFilterIterator*)(it[0]);
		else
			one_filter_it = (DimensionGroupFilter::DGFilterOrderedIterator*)(it[0]);
	}
}

void MIIterator::swap(MIIterator& i) 
{
	assert(po.size() == 0 && "not working due to pack orderer swap");
	using std::swap;
	swap( mind,               i.mind );
	swap( mind_created_locally, i.mind_created_locally );
	swap( no_dims,            i.no_dims );
	swap( cur_pos,            i.cur_pos );
	swap( cur_pack,           i.cur_pack );
	swap( valid,              i.valid );
	swap( it_for_dim,         i.it_for_dim );
	swap( omitted_factor,     i.omitted_factor );
	swap( no_obj,             i.no_obj );
	swap( pack_size_left,     i.pack_size_left );
	swap( dimensions,		  i.dimensions);
	swap( it,				  i.it );
	swap( dg,				  i.dg );
	swap( next_pack_started,  i.next_pack_started );
	swap( mii_type,			  i.mii_type );
	swap( po,				  i.po );
	swap( one_filter_dim,	  i.one_filter_dim );
	swap( one_filter_it,	  i.one_filter_it );
}

void MIIterator::Init(bool lock)
{
	mii_type = MII_NORMAL;
	bool* dim_group_used = new bool [mind->dim_groups.size()];
	for(int i = 0; i < mind->dim_groups.size(); i++)
		dim_group_used[i] = false;
	for(int i = 0; i < no_dims; i++)
		if(dimensions[i]) {
			dim_group_used[mind->group_num_for_dim[i]] = true;
			mind->LockForGetIndex(i);
		}

	no_obj = 1;			// special case of 0 will be tested soon
	omitted_factor = 1;
	bool zero_tuples = true;
	for(int i = 0; i < mind->dim_groups.size(); i++) {
		if(dim_group_used[i]) {
			no_obj = SafeMultiplication(no_obj, mind->dim_groups[i]->NoTuples());
			zero_tuples = false;
		} else {
			omitted_factor = SafeMultiplication(omitted_factor, mind->dim_groups[i]->NoTuples());
		}
	}
	if(zero_tuples)
		no_obj = 0;

	if(no_dims > 0) {
		it_for_dim = new int [no_dims];

		// Create group iterators: ordered filter-based first, if any
		if(po.size() > 0) {
			for(int i = 0; i < no_dims; i++)
				if(dim_group_used[mind->group_num_for_dim[i]] 
					&& mind->GetFilter(i) && po[i].Initialized()) {
						assert(mind->IsOrderable(i));
						it.push_back(mind->group_for_dim[i]->NewOrderedIterator(dimensions, &po[i]));
						dg.push_back(mind->group_for_dim[i]);						
						dim_group_used[mind->group_num_for_dim[i]] = false;
					}
		}

		// Create group iterators: other filter-based
		vector< pair<int,int> > ordering_filters;
		for(int i = 0; i < mind->dim_groups.size(); i++)
			if(dim_group_used[i] && 
				(mind->dim_groups[i]->Type() == DimensionGroup::DG_FILTER || mind->dim_groups[i]->Type() == DimensionGroup::DG_VIRTUAL))		// filters first
				ordering_filters.push_back(pair<int,int>(65537 - mind->dim_groups[i]->GetFilter(-1)->DensityWeight(), i));	// -1: the default filter for this group
		sort(ordering_filters.begin(), ordering_filters.end());	// order filters starting from the densest one (for pack decompression efficiency)

		for(uint i = 0; i < ordering_filters.size(); i++) {		// create iterators for DimensionGroup numbers from sorter
			it.push_back(mind->dim_groups[ordering_filters[i].second]->NewIterator(dimensions));
			dg.push_back(mind->dim_groups[ordering_filters[i].second]);
			dim_group_used[ordering_filters[i].second] = false;
		}

		// Create group iterators: materialized
		for(int i = 0; i < mind->dim_groups.size(); i++)
			if(dim_group_used[i]) {
				it.push_back(mind->dim_groups[i]->NewIterator(dimensions));
				dg.push_back(mind->dim_groups[i]);
			}

		for(int i = 0; i < no_dims; i++) if(dimensions[i]) {
			for(int j = 0 ; j < dg.size(); j++) {
				if(mind->group_for_dim[i] == dg[j]) {
					it_for_dim[i] = j;
					break;
				}
			}
		}

		/////////////////////////////////////////////////////
		if(lock) {
			bool success = mind->IteratorLock();
			assert(success);			// Multiindex was already locked for updating!
		}
		cur_pos = new _int64 [no_dims];
		cur_pack = new int [no_dims];
		fill(cur_pos, cur_pos + no_dims, NULL_VALUE_64);
		fill(cur_pack, cur_pack + no_dims, -1);

		//////// Check the optimized case
		if(dg.size() == 1 && dg[0]->Type() == DimensionGroup::DG_FILTER) {
			for(int i = 0; i < no_dims; i++) 
				if(dimensions[i]) {
					one_filter_dim = i;
					break;
				}
			if(po.size() == 0)
				one_filter_it = (DimensionGroupFilter::DGFilterIterator*)(it[0]);
			else
				one_filter_it = (DimensionGroupFilter::DGFilterOrderedIterator*)(it[0]);
		}
	}

	delete [] dim_group_used;

	Rewind();
	if(!IsValid())
		no_obj = 0;
}

MIIterator::~MIIterator()
{
	if(mind && no_dims > 0) {
		mind->IteratorUnlock();
	}
	for(int i = 0; i < it.size(); i++) {
		delete it[i];
	}
	if(dimensions.Size() > 0)				// may be not initialized for dummy iterators
		for(int i = 0; i < no_dims; i++)
			if(dimensions[i])
				mind->UnlockFromGetIndex(i);
	delete [] cur_pos;
	delete [] cur_pack;
	delete [] it_for_dim;
	if(mind_created_locally)
		delete mind;
}

void MIIterator::Rewind()
{
	valid = true;
	pack_size_left = 0;
	if(it.size() == 0) {
		valid = false;
		return;
	}
	for(int i = 0; i < it.size(); i++) {
		it[i]->Rewind();
		if(!it[i]->IsValid()) {
			valid = false;
			return;
		}
		dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
	}
	next_pack_started = true;
	InitNextPackrow();
}

void MIIterator::GeneralIncrement()
{
	bool done = false;			// packwise order: iterate pack-by-pack
	for(int i = 0; i < it.size(); i++) {
		bool still_in_pack = it[i]->NextInsidePack();
		if(still_in_pack)
			done = true;
		dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);

		if(done)
			break;
	}
	if(!done) {		// pack switched on every dimension - make progress in the whole pack numbering
		for(int i = 0; i < it.size(); i++) {
			it[i]->NextPackrow();
			if(it[i]->IsValid())
				done = true;
			else {
				it[i]->Rewind();		// rewind completely this dimension, increase packrow in the next one
				if(!it[i]->IsValid())
					break;
			}
			dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);

			if(done)
				break;
		}
		if(!done)
			valid = false;
	}
}

void MIIterator::NextPackrow()
{
	if(!valid)
		return;
	bool done = false;
	for(int i = 0; i < it.size(); i++) {
		it[i]->NextPackrow();
		if(it[i]->IsValid()) {
			done = true;
			dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
		} else if(i < it.size() - 1) {
			it[i]->Rewind();
			if(!it[i]->IsValid())			// possible e.g. for updating iterator
				break;						// exit and set valid = false
			dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
		}
		if(done)
			break;
	}
	if(!done)
		valid = false;
	else
		InitNextPackrow();
	assert(one_filter_dim == -1 || cur_pos[one_filter_dim] >> 16 == cur_pack[one_filter_dim]);
}

bool MIIterator::WholePack(int dim) const
{
	// the only possibility: just one virtual dimension, all other must be omitted
	return (it.size() == 1 && it[it_for_dim[dim]]->WholePack(dim));
}


int MIIterator::GetNextPackrow(int dim, int ahead) const
{
	return it[it_for_dim[dim]]->GetNextPackrow(dim, ahead);
}

void MIIterator::Skip(_int64 offset)
{
	_int64 skipped = 0;
	while(valid && skipped + pack_size_left < offset) {
		skipped += pack_size_left;
		NextPackrow();
	}
	while(valid && skipped < offset) {
		++skipped;
		++(*this);
	}
}

bool MIIterator::IsThreadSafe()
{
	for(int d = 0; d < dg.size(); d++)
		if(!dg[d]->IsThreadSafe())
			return false;
	return true;
}

bool MIIterator::BarrierAfterPackrow()
{
	if(!valid)
		return false;
	for(int i = 0; i < it.size(); i++)
		if(it[i]->BarrierAfterPackrow())
			return true;
	return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////

MIDummyIterator::MIDummyIterator(MultiIndex* _mind)
{ 
	mind = _mind;
	valid = true; 
	no_dims = mind->NoDimensions();
	mind_created_locally = false;
	it_for_dim = NULL;
	cur_pos = new _int64 [no_dims];
	cur_pack = new int [no_dims];
	pack_size_left = -1;
	omitted_factor = -1;
	no_obj = -1;
}


void MIDummyIterator::Combine(const MIIterator& sec)		// copy position from the second iterator (only the positions used)
{
	for(int i = 0; i < no_dims; i++)
		if(sec.DimUsed(i)) {
			cur_pos[i] = sec[i];
			cur_pack[i] = sec.GetCurPackrow(i);
	}
}

void MIDummyIterator::Set(int dim, _int64 val)			// set a position manually
{
	assert(dim >= 0 && dim < no_dims);
	cur_pos[dim] = val;
	if(val == NULL_VALUE_64)
		cur_pack[dim] = -1;
	else 
		cur_pack[dim] = int(val >> 16);
}

void MIDummyIterator::SetPack(int dim, int p)			// set a position manually
{
	assert(dim >= 0 && dim < no_dims);
	cur_pos[dim] = (_int64(p) << 16);
	cur_pack[dim] = p;
}

MIDummyIterator::MIDummyIterator(int dims) : MIIterator()
{
	cur_pos = new _int64 [dims];
	cur_pack = new int [dims];
	for(int i =0; i< dims; ++i)
		cur_pos[i] = cur_pack[i] = 0;
	no_dims = dims;
	valid = true;
}

bool MIDummyIterator::NullsPossibleInPack() const		
{ 
	for(int i = 0; i < no_dims; i++)
		if(cur_pos[i] == NULL_VALUE_64)
			return true; 
	return false; 
}


//////////////////////////////////////////////////////////////////////

void MIInpackIterator::GeneralIncrement()
{
	bool done = false;			// packwise order: iterate pack-by-pack
	for(int i = 0; i < it.size(); i++) {
		bool still_in_pack = it[i]->NextInsidePack();
		if(still_in_pack)
			done = true;
		dg[i]->FillCurrentPos(it[i], cur_pos, cur_pack, dimensions);
		if(done)
			break;
	}
	if(!done) 		// pack switched on every dimension
		valid = false;
}


void MIInpackIterator::swap(MIInpackIterator& i)
{
	using std::swap;
	swap( mind,               i.mind );
	swap( mind_created_locally, i.mind_created_locally );
	swap( no_dims,            i.no_dims );
	swap( cur_pos,            i.cur_pos );
	swap( cur_pack,           i.cur_pack );
	swap( valid,              i.valid );
	swap( it_for_dim,         i.it_for_dim );
	swap( omitted_factor,     i.omitted_factor );
	swap( no_obj,             i.no_obj );
	swap( pack_size_left,     i.pack_size_left );
	swap( dimensions,		  i.dimensions);
	swap( it,				  i.it );
	swap( dg,				  i.dg );
	swap( next_pack_started,  i.next_pack_started );
	swap( mii_type,			  i.mii_type );
	swap( one_filter_dim,	  i.one_filter_dim );
	swap( one_filter_it,	  i.one_filter_it );
}

int MIInpackIterator::GetNextPackrow(int dim, int ahead) const	
{ 
//	return it[it_for_dim[dim]]->GetNextPackrow(dim, ahead);

	return -1;				 // do not prefetch basing on this iterator
}
