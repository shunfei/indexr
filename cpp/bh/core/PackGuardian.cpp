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

#include "PackGuardian.h"
#include "edition/local.h"
#include "RCAttr.h"
#include "JustATable.h"
#include "edition/vc/VirtualColumn.h"
#include "MIIterator.h"

using namespace std;
/*
const _int64 DATA_PACK_STRUCT_SIZE = 8500;	// typical size of all non-data RCAttrPack structures (virt. functions, NULL mask etc.)

_int64 PackGuardian::MemoryLimit()
{
	_int64 memory_limit = 1048576;		// memory limitations will be in MB
	if ( TrackableObject::MemorySettingsScale()			== 0 )	// minimal settings
		memory_limit *= 32;
	else if ( TrackableObject::MemorySettingsScale()	== 1 ||
			  TrackableObject::MemorySettingsScale() 	== 2 )	// low settings
		memory_limit *= 128;
	else if ( TrackableObject::MemorySettingsScale() 	== 3 )	// medium settings		(2.5-5 GB RAM)
		memory_limit *= 700;
	else if ( TrackableObject::MemorySettingsScale() 	== 4 )	// high settings		(5-10 GB RAM)
		memory_limit *= 1200;
	else if ( TrackableObject::MemorySettingsScale() 	> 4 )	// very high settings	(>10 GB RAM)
		memory_limit *= 2500;
	return memory_limit;
}

PackGuardian::PackGuardian(JustATable *_t, int _n_a, ConnectionInfo *_conn, int divide_by)
	:	conn(_conn ? *_conn : ConnectionInfoOnTLS.Get())
{
	no_attr = 1;
//	n_a = new int [1];
	n_a.push_back(_n_a);
	anything_left = false;
	pstat = NULL;
	t = NULL;
	no_pack = 0;
	requested_at = NULL;
	request_counter = 0;
	if( _t && _t->TableType()==RC_TABLE) //&& n_a[0]>-1 )
	{
		assert( n_a[0]>-1 );
		t = (RCTable*)_t;
		no_pack = t->NoPacks();
		pstat = new PG_Status[ no_pack ];
		requested_at = new _int64 [ no_pack ];
		for( int i=0; i<no_pack; i++) {
			pstat[i] = PG_UNKNOWN;
			requested_at[i] = -1;
		}
	}
	pack_counter = 0;
	pack_limit = 64;			// default
	current_strategy = UNLOCK_ALL_ON_REWIND;
	old_pack_requests = 0;
	last_locked_pack = -1;
	///////// Calculation of pack limit
	if(t)
	{
		_int64 pack_size = (t->GetInternalSize(n_a[0]) + 2) * _int64(65536)
							+ DATA_PACK_STRUCT_SIZE;		// size +2 because of index table
		pack_limit = (MemoryLimit() / pack_size) / divide_by;
		if(pack_limit<4) pack_limit = 4;
	}
}

PackGuardian::PackGuardian(	JustATable *_t,	vector<int> &v_a, ConnectionInfo *_conn)
	:	conn(_conn ? *_conn : ConnectionInfoOnTLS.Get())
{
	no_attr = v_a.size();
	assert( no_attr>0 );
	//n_a = new int [ no_attr ];
	for( int i=0; i<no_attr; i++ )
		n_a.push_back(v_a[i]);
	anything_left = false;
	pstat = NULL;
	t = NULL;
	requested_at = NULL;
	no_pack = 0;
	request_counter = 0;
	old_pack_requests = 0;
	if( _t && _t->TableType()==RC_TABLE) // && n_a[0]>-1 )
	{
		assert( n_a[0]>-1 );
		t = (RCTable*)_t;
		no_pack = t->NoPacks();
		pstat = new PG_Status[ no_pack ];
		requested_at = new _int64 [ no_pack ];
		for( int i=0; i<no_pack; i++) {
			pstat[i] = PG_UNKNOWN;
			requested_at[i] = -1;
		}
	}
	pack_counter = 0;
	pack_limit = 64;			// default
	current_strategy = UNLOCK_ALL_ON_REWIND;
	last_locked_pack = -1;

	///////// Calculation of pack limit
	if(t)
	{
		_int64 row_size = 0;
		for( int i=0; i<no_attr; i++ )
			row_size += (t->GetInternalSize( n_a[i] ) + 2) * _int64(65536)
						+ DATA_PACK_STRUCT_SIZE;	// size +2 because of index table
		pack_limit = MemoryLimit() / row_size;		// i.e. pack size
		if(pack_limit<4)
			pack_limit = 4;
	}
}


void PackGuardian::AddAttrsFromCQTerm(const CQTerm& cq, std::vector<int>& attrno)
{
	assert(0);  // not used
	if(cq.IsColumn()) {
		attrno.push_back(cq.attr_id.n);
	} else if(cq.IsComplex()) {
		MysqlExpression::SetOfVars sov;
		cq.c_term->GetVars(&sov);
		for (MysqlExpression::SetOfVars::iterator it = sov.begin(); it != sov.end(); it++) {
			attrno.push_back((*it).col);
		}
	}
}

PackGuardian::PackGuardian(	JustATable *_t,	const Descriptor& desc, ConnectionInfo *_conn)
	:	conn(_conn ? *_conn : ConnectionInfoOnTLS.Get())
{
	AddAttrsFromCQTerm(desc.attr, n_a);
	AddAttrsFromCQTerm(desc.val1, n_a);
	AddAttrsFromCQTerm(desc.val2, n_a);

	no_attr = n_a.size(); //maybe 0 if constant complex term
//	assert( no_attr>0 );
//	n_a = new int [ no_attr ];
	anything_left = false;
	pstat = NULL;
	t = NULL;
	requested_at = NULL;
	no_pack = 0;
	request_counter = 0;
	old_pack_requests = 0;
	if( no_attr > 0 && _t && _t->TableType()==RC_TABLE) // && n_a[0]>-1)
	{
		assert( n_a[0]>-1 );
		t = (RCTable*)_t;
		no_pack = t->NoPacks();
		pstat = new PG_Status[ no_pack ];
		requested_at = new _int64 [ no_pack ];
		for( int i=0; i<no_pack; i++) {
			pstat[i] = PG_UNKNOWN;
			requested_at[i] = -1;
		}
	}
	pack_counter = 0;
	pack_limit = 64;			// default
	current_strategy = UNLOCK_ALL_ON_REWIND;
	last_locked_pack = -1;

	///////// Calculation of pack limit
	if(t)
	{
		_int64 row_size = 0;
		for( int i=0; i<no_attr; i++ )
			row_size += (t->GetInternalSize( n_a[i] ) + 2) * _int64(65536)
						+ DATA_PACK_STRUCT_SIZE;	// size +2 because of index table
		pack_limit = MemoryLimit() / row_size;		// i.e. pack size
		if(pack_limit<4)
			pack_limit = 4;
	}
}

PackGuardian::PackGuardian(	std::vector< std::pair<JustATable*,int> > &v_a )
	:	conn(ConnectionInfoOnTLS.Get())
{
	assert(0);		// NOT IMPLEMENTED YET
}

PackGuardian::~PackGuardian()
{
	if(t)
	{
		for(int i=0;i<no_pack;i++)
		{
			if(pstat[i]==PG_READY)
				for( int a=0; a<no_attr; a++ )
					t->UnlockPackFromUse( n_a[a], i );
		}
		delete [] pstat;
	}
//	delete [] n_a;
	delete [] requested_at;
}

PG_Status PackGuardian::PackRequest( int pack )
{
	if( t==NULL ) return PG_READY;
	assert(pack>=0 && pack<no_pack);
	if( current_strategy == LOCK_ONE) {
		if( pstat[pack]==PG_UNKNOWN ) {
			if( last_locked_pack != -1 ) {		// no pack locked yet
				for( int a=0; a<no_attr; a++ )
					t->UnlockPackFromUse( n_a[a], last_locked_pack );
				pstat[ last_locked_pack ] = PG_UNKNOWN;
			}
			for( int a=0; a<no_attr; a++ )
				t->LockPackForUse(n_a[a], pack, conn);
			last_locked_pack = pack;
			pstat[pack] = PG_READY;
		}
	}
	else if( current_strategy == UNLOCK_ALL_ON_REWIND ) {
		if(pstat[pack]==PG_UNKNOWN)
		{
			request_counter++;
			if( pack_counter>=pack_limit )
			{
				if( anything_left==false ) {		// the first case of failed locking
					ReconsiderStrategy();
					if( current_strategy == UNLOCK_ALL_ON_REWIND ) {	// no strategy change
						anything_left = true;
						return PG_LATER;
					}
					// else go forward and lock one pack more, and then follow another strategy
				}
				else
					return PG_LATER;
			}
			for( int a=0; a<no_attr; a++ )
				t->LockPackForUse(n_a[a], pack, conn);
			pstat[pack] = PG_READY;
			requested_at[pack] = request_counter;
			pack_counter++;
		}
	}
	else if( current_strategy == UNLOCK_OLDEST) {
		if(pstat[pack]==PG_UNKNOWN)
		{
			request_counter++;
			if( pack_counter>=pack_limit )
			{
				int pack_to_delete = -1;
				_int64 oldest_request = request_counter;
				for( int i=0; i<no_pack; i++ )
					if( pstat[i] == PG_READY && requested_at[i] < oldest_request ) {
						pack_to_delete = i;		// find the oldest used
						oldest_request = requested_at[i];	// note: may be -1 if switched from other mode
					}
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT(pack_to_delete != -1);
				for( int a=0; a<no_attr; a++ )
					t->UnlockPackFromUse( n_a[a], pack_to_delete );
				pstat[ pack_to_delete ] = PG_UNKNOWN;
				pack_counter--;
			}
			for( int a=0; a<no_attr; a++ )
				t->LockPackForUse(n_a[a], pack, conn);
			pstat[pack] = PG_READY;
			requested_at[pack] = request_counter;
			pack_counter++;
		}
	}
	else if( current_strategy == LOCK_OLDEST) {
		if( pstat[pack]==PG_UNKNOWN ) {
			request_counter++;
			if( pack_counter>=pack_limit )			// too many packs - lock/unlock only one
			{
				if( last_locked_pack != -1 ) {		// no pack overlocked yet
					for( int a=0; a<no_attr; a++ )
						t->UnlockPackFromUse( n_a[a], last_locked_pack );
					pstat[ last_locked_pack ] = PG_UNKNOWN;
					pack_counter--;
				}
				for( int a=0; a<no_attr; a++ )
					t->LockPackForUse(n_a[a], pack, conn);
				last_locked_pack = pack;
				pstat[pack] = PG_READY;
				pack_counter++;
			}
			else {
				for( int a=0; a<no_attr; a++ )
					t->LockPackForUse(n_a[a], pack, conn);
				pstat[pack] = PG_READY;
				requested_at[pack] = request_counter;
				pack_counter++;
			}
		}
	}
	if( pstat[pack] == PG_READY && requested_at[pack] < request_counter-10 )
		old_pack_requests++;		// this pack is requested again after some other packs was locked
	return pstat[pack];
}

void PackGuardian::UnlockOne(int pack)					// manual unlocking, may be used for some strategies
{
	if(t==NULL) return;
	if(pstat[pack]==PG_READY)
	{
		for( int a=0; a<no_attr; a++ )
			t->UnlockPackFromUse( n_a[a], pack );
		pstat[pack] = PG_UNKNOWN;
		requested_at[pack] = -1;
		pack_counter--;
		if(last_locked_pack == pack)
			last_locked_pack = -1;
	}
}

void PackGuardian::Rewind()
{
	if(t==NULL) return;
	for(int i=0;i<no_pack;i++)
	{
		if(pstat[i]==PG_READY)
		{
			for( int a=0; a<no_attr; a++ )
				t->UnlockPackFromUse( n_a[a], i );
			pstat[i] = PG_DONE;
			requested_at[i] = -1;
		}
	}
	anything_left = false;
	pack_counter = 0;
	request_counter = 0;
	old_pack_requests = 0;
	last_locked_pack = -1;
}

void PackGuardian::Clear()
{
	if(t==NULL) return;
	for(int i=0;i<no_pack;i++)
	{
		if(pstat[i]==PG_READY)
			for( int a=0; a<no_attr; a++ )
				t->UnlockPackFromUse( n_a[a], i );
		pstat[i] = PG_UNKNOWN;
		requested_at[i] = -1;
	}
	anything_left = false;
	pack_counter = 0;
	request_counter = 0;
	old_pack_requests = 0;
	last_locked_pack = -1;
}

void PackGuardian::ReconsiderStrategy()
{
	if( current_strategy == UNLOCK_OLDEST ||
		current_strategy == LOCK_OLDEST )		// cannot change these strategies
		return;
	// else: UNLOCK_ALL_ON_REWIND
	if( old_pack_requests <= request_counter/10)	// if the locking pattern is regular - switch to faster access
		current_strategy = UNLOCK_OLDEST;
}
*/
//////////////////////////////////////////////////////////

VCPackGuardian::VCPackGuardian(VirtualColumn *vc)
	:	current_strategy(LOCK_ONE), my_vc(*vc), threads(1)
{
	initialized = false;
	last_pack = NULL;
	pack_locked = NULL;
	no_dims = 0;
	dim_size = NULL;
}

VCPackGuardian::~VCPackGuardian()
{
	for(int i = 0; i< no_dims; ++i)
		delete [] last_pack[i];
	delete [] last_pack;
	delete [] dim_size;
	if(pack_locked) {
		for(int i = 0; i < no_dims; i++)
			delete [] pack_locked[i];
		delete [] pack_locked;
	}
}

void VCPackGuardian::Initialize(const MIIterator &mit, int no_th)	// mit used only for dim sizes
{
	UnlockAll();
	if(mit.Type() == MIIterator::MII_DUMMY)
		current_strategy = LOCK_ONE;			// otherwise we don't know OrigSize()

	//delete previous structures
	delete [] dim_size;
	if(pack_locked) {
		for(int i = 0; i < no_dims; i++)
			delete [] pack_locked[i];
		delete [] pack_locked;
	}
	if(last_pack) {
		for(int i = 0; i< no_dims; ++i)
			delete [] last_pack[i];
		delete [] last_pack;
	}

	threads = no_th;

	no_dims = -1;
	for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++)
		if(iter->dim > no_dims)
			no_dims = iter->dim;		// find the maximal number of dimension used
	no_dims++;
	if(no_dims > 0) {					// else constant
		if(current_strategy == LOCK_ONE) {
			last_pack = new int* [no_dims];
			for(int i = 0; i< no_dims; ++i)
				last_pack[i] = new int[threads];

			for(int i = 0; i < no_dims; i++)
				for(int j = 0; j< threads; ++j)
					last_pack[i][j] = NULL_VALUE_32;
		} else if(current_strategy == LOCK_ALL) {
			dim_size = new int [no_dims];
			for(int i = 0; i < no_dims; i++)
				dim_size[i] = bh::common::NoObj2NoPacks(mit.OrigSize(i));

			pack_locked = new char * [no_dims];
			for(int i = 0; i < no_dims; i++)
				pack_locked[i] = NULL;
			for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++) {
				if(pack_locked[iter->dim] == NULL) {		// dimensions which are not used are left NULL
					pack_locked[iter->dim] = new char [dim_size[iter->dim]];
					memset(pack_locked[iter->dim], 0, dim_size[iter->dim]);		// set all (used) dimension packs to "unlocked"
				}
			}
		}
	}
	initialized = true;
}

PG_Status VCPackGuardian::LockPackrow(const MIIterator &mit)
{
	if(!initialized)
		Initialize(mit);

	if(current_strategy == LOCK_ONE) {
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++) {
			int cur_dim = iter->dim;
			if(last_pack[cur_dim][0] != mit.GetCurPackrow(cur_dim)) {
				JustATable* tab = iter->GetTabPtr().get();
				if(last_pack[cur_dim][0] != NULL_VALUE_32)
					tab->UnlockPackFromUse(iter->col_ndx, last_pack[cur_dim][0]);
				try {
					tab->LockPackForUse(iter->col_ndx, mit.GetCurPackrow(cur_dim), my_vc.ConnInfo());
				} catch(...) {
					// unlock packs which are partially locked for this packrow
					vector<VirtualColumn::VarMap>::const_iterator it = my_vc.GetVarMap().begin();
					for(; it != iter; ++it) {
						int cur_dim = it->dim;
						if(last_pack[cur_dim][0] != mit.GetCurPackrow(cur_dim) && last_pack[cur_dim][0] != NULL_VALUE_32)
							it->GetTabPtr()->UnlockPackFromUse(it->col_ndx, mit.GetCurPackrow(cur_dim));
					}

					for(++iter; iter != my_vc.GetVarMap().end(); ++iter) {
						int cur_dim = iter->dim;
						if(last_pack[cur_dim][0] != mit.GetCurPackrow(cur_dim) && last_pack[cur_dim][0] != NULL_VALUE_32)
							iter->GetTabPtr()->UnlockPackFromUse(iter->col_ndx, last_pack[cur_dim][0]);
					}

					for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++)
						last_pack[iter->dim][0] = NULL_VALUE_32;
					throw;
				}
			}
		}
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++)
			last_pack[iter->dim][0] = mit.GetCurPackrow(iter->dim);	// must be in a separate loop, otherwise for "a + b" will not lock b
	}
	else if(current_strategy == LOCK_ALL) {
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++) {
			if(pack_locked[iter->dim][mit.GetCurPackrow(iter->dim)] == 0) {
				try {
					iter->GetTabPtr()->LockPackForUse(iter->col_ndx, mit.GetCurPackrow(iter->dim), my_vc.ConnInfo());
				} catch(...) {
					// unlock packs which are partially locked for this packrow
					vector<VirtualColumn::VarMap>::const_iterator it = my_vc.GetVarMap().begin();
					for(; it != iter; ++it) {
						if(pack_locked[it->dim][mit.GetCurPackrow(it->dim)] == 0)
							it->GetTabPtr()->UnlockPackFromUse(it->col_ndx, mit.GetCurPackrow(it->dim));
					}
					UnlockAll();
					throw;
				}
			}
		}
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++)
			pack_locked[iter->dim][mit.GetCurPackrow(iter->dim)] = 1;
	}
	return PG_READY;
}

void VCPackGuardian::UnlockAll()
{
	if(!initialized)
		return;
	if(current_strategy == LOCK_ONE) {
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++) {
			for(int i = 0; i< threads; ++i)
				if(last_pack[iter->dim][i] != NULL_VALUE_32 && iter->GetTabPtr())
				iter->GetTabPtr()->UnlockPackFromUse(iter->col_ndx, last_pack[iter->dim][i]);
		}
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++) {
			for(int i = 0; i< threads; ++i)
				last_pack[iter->dim][i] = NULL_VALUE_32;		// must be in a separate loop, otherwise for "a + b" will not unlock b
		}
	}
	else if(current_strategy == LOCK_ALL) {
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++) {
			if(iter->GetTabPtr())
				for(int pack = 0; pack < dim_size[iter->dim]; pack++)
					if(pack_locked[iter->dim][pack] == 1)
						iter->GetTabPtr()->UnlockPackFromUse(iter->col_ndx, pack);
		}
		for(vector<VirtualColumn::VarMap>::const_iterator iter = my_vc.GetVarMap().begin(); iter != my_vc.GetVarMap().end(); iter++)
			memset(pack_locked[iter->dim], 0, dim_size[iter->dim]);	// must be in a separate loop, otherwise for "a + b" will not unlock b
	}
}


