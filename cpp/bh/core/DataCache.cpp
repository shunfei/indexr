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

#include <boost/scoped_ptr.hpp>

#include "DataCache.h"
#include "core/RCAttr.h"

using namespace boost;

DataCache::~DataCache()
{
	ReleaseAll();
}

void DataCache::ReleaseAll()
{
	if(m_local) {
		std::vector<TrackableObjectPtr> packs_removed;
		{
			IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
			IBGuard m_cache_guard(m_cache_mutex);
			if (!_packs.empty()) {
				for (PackContainer::iterator it = _packs.begin(); it != _packs.end(); it++) {
					AttrPackPtr pack = static_pointer_cast<AttrPack>(it->second);
					pack->Lock();
					pack->SetOwner(NULL);
					packs_removed.push_back(pack);
				}
				_packs.clear();
			}
		}
	}
	else
		_packs.clear();
	_ftrees.clear();
	_filters.clear();
	_slices.clear();
}

void DataCache::ReleasePacks(int table)
{
	std::vector<TrackableObjectPtr> packs_removed;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		PackContainer::iterator it = _packs.begin();
		while(it != _packs.end()) {
			if(pc_table( it->first ) == table) {
				PackContainer::iterator tmp = it++;
				AttrPackPtr pack = static_pointer_cast<AttrPack>(tmp->second);
				pack->Lock();
				pack->SetOwner(0);
				_packs.erase(tmp);
				packs_removed.push_back(pack);
				m_objectsReleased++;
			} else
				it++;
		}
	}
}

void DataCache::ReleaseColumnPacks(int table, int column)
{
	std::vector<TrackableObjectPtr> packs_removed;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		PackContainer::iterator it = _packs.begin();
		while(it != _packs.end()) {
			if(pc_table( it->first ) == table && pc_column( it->first ) == column) {
				PackContainer::iterator tmp = it++;
				AttrPackPtr pack = static_pointer_cast<AttrPack>(tmp->second);
				pack->Lock();
				pack->SetOwner(0);
				_packs.erase(tmp);
				packs_removed.push_back(pack);
				m_objectsReleased++;
			} else
				it++;
		}
	}
}

void DataCache::ReleaseDPNs(int table)
{
	std::vector<TrackableObjectPtr> removed;
	{
		IBGuard m_cache_guard(m_cache_mutex);
		SliceContainer::iterator it = _slices.begin();
		while(it != _slices.end()) {
			if(it->first[0] == table) {
				SliceContainer::iterator tmp = it++;
				removed.push_back(tmp->second);
				_slices.erase(tmp);
				m_objectsReleased++;
			} else
				it++;
		}
	}
}

void DataCache::ReleaseColumnDNPs(int table, int column)
{
	std::vector<TrackableObjectPtr> removed;
	{
		IBGuard m_cache_guard(m_cache_mutex);
		SliceContainer::iterator it = _slices.begin();
		while(it != _slices.end()) {
			if(it->first[0] == table && it->first[1] == column) {
				SliceContainer::iterator tmp = it++;
				removed.push_back(tmp->second);
				_slices.erase(tmp);
				m_objectsReleased++;
			} else
				it++;
		}
	}
}

void DataCache::ReleasePackRow(int table, int pack_row)
{
	std::vector<TrackableObjectPtr> packs_removed;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		PackContainer::iterator it = _packs.begin();
		while(it != _packs.end()) {
			if(pc_table( it->first ) == table && pc_dp( it->first ) == pack_row) {
				PackContainer::iterator tmp = it++;
				AttrPackPtr pack = static_pointer_cast<AttrPack>(tmp->second);
				pack->Lock();
				pack->SetOwner(0);
				_packs.erase(tmp);
				packs_removed.push_back(pack);
				m_objectsReleased++;
			} else
				it++;
		}
	}
}

void DataCache::MoveCaches( DataCache& local )
{
	MoveCache<PackCoordinate>(local);
	MoveCache<FilterCoordinate>(local);
	MoveCache<FTreeCoordinate>(local);
	MoveCache<SpliceCoordinate>(local);
}

void DataCache::ResetAndUnlockOrDropPack(AttrPackPtr& pack)
{
	bool reset_outside = false;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		if (pack->LastLock()) {
			DropObject(pack->GetPackCoordinate());
			reset_outside = true;
		}
		else {
			pack->Unlock();
			pack.reset();
		}
	}
	if (reset_outside) {
		pack.reset();
	}
}


template<>
stdext::hash_map<PackCoordinate, TrackableObjectPtr, PackCoordinate>& DataCache::cache( void )
	{ return ( _packs ); }

template<>
stdext::hash_map<PackCoordinate, TrackableObjectPtr, PackCoordinate> const& DataCache::cache( void ) const
	{ return ( _packs ); }

template<>
stdext::hash_map<FilterCoordinate, TrackableObjectPtr, FilterCoordinate>& DataCache::cache( void )
	{	return ( _filters ); }

template<>
stdext::hash_map<FilterCoordinate, TrackableObjectPtr, FilterCoordinate> const& DataCache::cache( void ) const
	{	return ( _filters ); }

template<>
stdext::hash_map<FTreeCoordinate, TrackableObjectPtr, FTreeCoordinate>& DataCache::cache( void )
	{ return ( _ftrees ); }

template<>
stdext::hash_map<FTreeCoordinate, TrackableObjectPtr, FTreeCoordinate> const& DataCache::cache( void ) const
	{ return ( _ftrees ); }

template<>
stdext::hash_map<SpliceCoordinate, TrackableObjectPtr, SpliceCoordinate>& DataCache::cache( void )
	{ return ( _slices ); }

template<>
stdext::hash_map<SpliceCoordinate, TrackableObjectPtr, SpliceCoordinate> const& DataCache::cache( void ) const
	{ return ( _slices ); }

template<>
DataCache::IOPackReqSet& DataCache::waitIO( void )
	{ return ( _packPendingIO ); }

template<>
stdext::hash_set<FilterCoordinate, FilterCoordinate>& DataCache::waitIO( void )
	{ return ( _filterPendingIO ); }

template<>
stdext::hash_set<FTreeCoordinate, FTreeCoordinate>& DataCache::waitIO( void )
	{ return ( _ftreePendingIO ); }

template<>
stdext::hash_set<SpliceCoordinate, SpliceCoordinate>& DataCache::waitIO( void )
	{ return ( _slicePendingIO ); }

template<>
IBCond& DataCache::condition<PackCoordinate>( void )
	{ return ( _packWaitIO ); }

template<>
IBCond& DataCache::condition<FilterCoordinate>( void )
	{ return ( _filterWaitIO ); }

template<>
IBCond& DataCache::condition<FTreeCoordinate>( void )
	{ return ( _ftreeWaitIO ); }

template<>
IBCond& DataCache::condition<SpliceCoordinate>( void )
	{ return ( _sliceWaitIO ); }
