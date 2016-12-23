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

#ifndef DATACACHE_H
#define DATACACHE_H

#include <boost/shared_ptr.hpp>
#ifdef __GNUC__
	#include <ext/hash_map>
	#ifndef stdext
	#define stdext __gnu_cxx
	#endif
#else
	#include <hash_map>
#endif
#include <utility>

#include "core/RCAttr.h"
#include "core/RCAttrPack.h"
#include "core/DPN.h"
#include "system/MemoryManagement/ReleaseStrategy.h"

class RCAttr;
class IBMutex;

typedef boost::shared_ptr<TrackableObject> TrackableObjectPtr;

class DataCache
{
protected:
	struct IOPackReq {
		PackCoordinate coord; RCAttr* at;
		IOPackReq(PackCoordinate coord, RCAttr* at) : coord(coord), at(at) {}
		IOPackReq() : coord(), at(NULL) {}
		bool operator==(const IOPackReq& sec) const {return coord == sec.coord && at == sec.at;}
		size_t operator()( IOPackReq const& o ) const {
			return ( o.coord.hash() * (_uint64) at );
		}
		bool operator < ( IOPackReq const& o ) const {
			return ( coord < o.coord );
		}
	};
	typedef stdext::hash_map<PackCoordinate, TrackableObjectPtr, PackCoordinate> PackContainer;
	typedef stdext::hash_set<PackCoordinate, PackCoordinate> IOPackReqSet;
	typedef stdext::hash_map<FilterCoordinate, TrackableObjectPtr, FilterCoordinate> FilterContainer;
	typedef stdext::hash_set<FilterCoordinate, FilterCoordinate> IOFilterReqSet;
	typedef stdext::hash_map<FTreeCoordinate, TrackableObjectPtr, FTreeCoordinate> FTreeContainer;
	typedef stdext::hash_set<FTreeCoordinate, FTreeCoordinate> IOFTreeReqSet;
	typedef stdext::hash_map<SpliceCoordinate, TrackableObjectPtr, SpliceCoordinate> SliceContainer;
	typedef stdext::hash_set<SpliceCoordinate, SpliceCoordinate> IOSliceReqSet;

	PackContainer _packs;
	FilterContainer _filters;
	FTreeContainer _ftrees;
	SliceContainer _slices;
	_int64 m_cacheHits, m_cacheMisses, m_objectsReleased;	
	IOPackReqSet _packPendingIO;
	IOFilterReqSet _filterPendingIO;
	IOFTreeReqSet _ftreePendingIO;
	IOSliceReqSet _slicePendingIO;
	IBCond _packWaitIO;
	IBCond _filterWaitIO;
	IBCond _ftreeWaitIO;
	IBCond _sliceWaitIO;

	_int64 m_readWait;
	_int64 m_falseWakeup;
	_int64 m_readWaitInProgress;
	_int64 m_packLoads;
	_int64 m_packLoadInProgress;
	_int64 m_loadErrors;
	_int64 m_reDecompress;

	IBMutex m_cache_mutex;

public:

	_int64 getReadWait() { return m_readWait;}	
	_int64 getReadWaitInProgress() { return m_readWaitInProgress;}	
	_int64 getFalseWakeup() { return m_falseWakeup;}	
	_int64 getPackLoads() { return m_packLoads;}	
	_int64 getPackLoadInProgress() { return m_packLoadInProgress;}	
	_int64 getLoadErrors() { return m_loadErrors;}	
	_int64 getReDecompress() { return m_reDecompress;}	

		_int64 getCacheHits() { return m_cacheHits;}	
		_int64 getCacheMisses() { return m_cacheMisses;}	
		_int64 getReleased() { return m_objectsReleased;}	

	DataCache(bool local = true) : m_cacheHits(0), m_cacheMisses(0), m_objectsReleased(0),
		m_readWait(0), m_falseWakeup(0), m_readWaitInProgress(0), m_packLoads(0),
		m_packLoadInProgress(0), m_loadErrors(0), m_reDecompress(0),
		m_local(local) { }
	virtual ~DataCache();
	
	virtual void	ReleaseAll();

	template<typename T>
	void PutObject(T const&, TrackableObjectPtr);

	void ReleasePacks(int table);
	void ReleaseColumnPacks(int table, int column);
	void ReleasePackRow(int table, int pack_row);

	void ReleaseDPNs(int table);
	void ReleaseColumnDNPs(int table, int column);

	template<typename T>
	bool HasObject( T const& );

	template<typename T>
	void DropObject( T const& );
	template<typename T>
	void DropObjectByMM( T const& );
	template<typename T, typename U>
	void MoveObjectIfNotUsed(U const& coord_, U const& new_coord_, DataCache& new_cache);
	template<typename T>
	void MoveCache(DataCache& local);
	void MoveCaches( DataCache& );

	void ResetAndUnlockOrDropPack(AttrPackPtr& pack);

	/* Implementation of specializations of those moethods are together
	 * with implementation of respective type.
	 *
	 * i.e.:
	 * boost::shared_ptr<AttrPack> GetObject( PackCoordinate const& ) in core/RCAttrPack.cpp
	 * boost::shared_ptr<Filter> GetObject( FilterCoordinate const& ) in core/Filter.cpp
	 */
	template<typename T, typename U>
	boost::shared_ptr<T> GetObject( U const& );
	template<typename T, typename U>
	boost::shared_ptr<T> GetLockedObject(U const& );
	template<typename T, typename U, typename V>
	boost::shared_ptr<T> GetObject( U const&, V const& );
	template<typename T>
	stdext::hash_map<T, TrackableObjectPtr, T>& cache( void );
	template<typename T>
	stdext::hash_set<T, T>& waitIO( void );
	template<typename T>
	IBCond& condition( void );
	template<typename T>
	stdext::hash_map<T, TrackableObjectPtr, T> const& cache( void ) const;

private:
	/* Thos methods are implemented in different implementation files:
	 * i.e.:
	 * GetFilter in core/Filter.cpp
	 * GetAttrPack in core/RCAttrPack.cpp
	 *
	 * As a next refactoring step, those methods could be
	 * removed from this class and put into separate Fetcher methods.
	 */
	const bool m_local;

};

template<typename T>
void DataCache::PutObject( T const& coord_, TrackableObjectPtr p )
{
	TrackableObjectPtr old;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		p->SetOwner(this);
		typedef stdext::hash_map<T, TrackableObjectPtr, T> container_t;
		container_t& c( cache<T>() );
		std::pair<typename container_t::iterator,bool> result = c.insert( std::make_pair(coord_, p) );
		if(!result.second && result.first->second != p) {
			old = result.first->second;	// old object must be physically deleted after mutex unlock
			DropObject(coord_);
			result = c.insert( std::make_pair(coord_, p) );
		}
		if (T::ID==bh::COORD_TYPE::PACK) 
			p->TrackAccess();				
	}
}

template<typename T>
void DataCache::DropObject( T const& coord_ )
{
	TrackableObjectPtr removed;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		typedef stdext::hash_map<T, TrackableObjectPtr, T> container_t;
		container_t& c( cache<T>() );
		typename container_t::iterator it = c.find( coord_ );
		if(it != c.end()) {
			removed = it->second;
			if (T::ID==bh::COORD_TYPE::PACK) {
				removed->Lock();
			}
			removed->SetOwner( NULL );
			c.erase(it);
			++ m_objectsReleased;
		}
	}
}

template<typename T>
void DataCache::DropObjectByMM( T const& coord_ )
{
	TrackableObjectPtr removed;
	{
		IBGuard m_cache_guard(m_cache_mutex);
		typedef stdext::hash_map<T, TrackableObjectPtr, T> container_t;
		container_t& c( cache<T>() );
		typename container_t::iterator it = c.find( coord_ );
		if(it != c.end()) {
			removed = it->second;
			removed->SetOwner( NULL );
			c.erase(it);
			++ m_objectsReleased;
		}
	}
}

template<typename T, typename U>
void DataCache::MoveObjectIfNotUsed(U const& coord_, U const& new_coord_, DataCache& new_cache)
{
	IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
	IBGuard m_cache_guard(m_cache_mutex);
	boost::shared_ptr<T> ret = GetObject<T>(coord_);
	if(!!ret && !new_cache.HasObject(new_coord_) && !ret->IsLocked()) {
		//std::cerr << "Moving object !!!!" << std::endl;
		DropObject(coord_);
		new_cache.PutObject(new_coord_, ret);
	}
}

template<typename T>
void DataCache::MoveCache(DataCache& local)
{
	std::vector<TrackableObjectPtr> removed;
	{
		IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
		IBGuard m_local_cache_guard(local.m_cache_mutex);
		IBGuard m_cache_guard(m_cache_mutex);
		typedef stdext::hash_map<T, TrackableObjectPtr, T> container_t;
		container_t& c( cache<T>() );
		container_t& l( local.cache<T>() );
		typename container_t::iterator it = l.begin();
		while(it != l.end()) {
			typename container_t::iterator tmp( it++ );
			tmp->second->SetOwner(this);
			std::pair<typename container_t::iterator,bool> result = c.insert( std::make_pair(tmp->first, tmp->second) );
			if(!result.second && result.first->second != tmp->second) {
				removed.push_back(result.first->second);	// old object must be physically deleted after mutex unlock
				DropObject(tmp->first);
				result = c.insert( std::make_pair(tmp->first, tmp->second) );
			}
			if ( T::ID==bh::COORD_TYPE::PACK ) 
				tmp->second->TrackAccess();
			l.erase(tmp);
		}
		l.clear();
	}
}

template<typename T>
bool DataCache::HasObject(T const& coord_)
{
	IBGuard m_cache_guard(m_cache_mutex);
	typedef stdext::hash_map<T, TrackableObjectPtr, T> container_t;
	container_t const& c( cache<T>() );
	return c.find(coord_) != c.end();
}

template<typename T, typename U>
boost::shared_ptr<T> DataCache::GetObject( U const& coord_) {
	IBGuard m_cache_guard(m_cache_mutex);
	typedef stdext::hash_map<U, TrackableObjectPtr, U> container_t;
	container_t& c( cache<U>() );
	typename container_t::iterator it = c.find(coord_);

	if (it == c.end()) {
		//++ m_cacheMisses;
		return boost::shared_ptr<T>();
	} else {
		//++ m_cacheHits;
		if (U::ID==bh::COORD_TYPE::PACK) 
			it->second->TrackAccess();						
		return boost::static_pointer_cast<T>( it->second );
	}
}

template<typename T, typename U>
boost::shared_ptr<T> DataCache::GetLockedObject(U const& coord_)
{
	boost::shared_ptr<T> ret;
	IBGuard m_locking_guard(TrackableObject::GetLockingMutex());
	IBGuard m_cache_guard(m_cache_mutex);
	ret = GetObject<T>(coord_);
	if (!!ret) {
		ret->Lock();
		if (U::ID==bh::COORD_TYPE::PACK) 
			ret->TrackAccess();						
	}
	return ret;
}

template<typename T, typename U, typename V>
boost::shared_ptr<T> DataCache::GetObject( U const& coord_, V const& fetcher_ ) {
	typedef stdext::hash_map<U, TrackableObjectPtr, U> container_t;
	typedef stdext::hash_set<U,U> wait_io_t;
	typedef boost::shared_ptr<T> cacheable_t;
	container_t& c( cache<U>() );
	wait_io_t& w( waitIO<U>() );
	IBCond& cond( condition<U>() );
	bool waited = false;
	/* a scope for mutex lock */ {
		/* Lock is acquired inside */
		IBGuard m_obj_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		typename container_t::iterator it = c.find(coord_);

		if( it != c.end() ) {
			if (U::ID==bh::COORD_TYPE::PACK) {
				it->second->Lock();
				++ m_cacheHits;
				it->second->TrackAccess();
			}
			return boost::static_pointer_cast<T>( it->second );
		} else {
			if (U::ID==bh::COORD_TYPE::PACK) 
				m_cacheMisses++;
			typename wait_io_t::const_iterator rit = w.find(coord_);
			while( rit != w.end() ) {
				m_readWaitInProgress++;
				if( waited )
					m_falseWakeup++;
				else
					m_readWait++;

				m_cache_mutex.Unlock();
				cond.Wait(TrackableObject::GetLockingMutex());
				m_cache_mutex.Lock();

				waited = true;
				m_readWaitInProgress--;
				rit = w.find(coord_);
			}
			it = c.find(coord_);
			if( it != c.end()) {
				if (U::ID==bh::COORD_TYPE::PACK) {
					it->second->Lock();
					it->second->TrackAccess();
				}
				return boost::static_pointer_cast<T>( it->second );
			}
			//TrackableObject::GetLockingMutex().Unlock();
			// if we get here the obj has been loaded, used, unlocked
			// and pushed out of memory before we chould get to it after
			// waiting
		}
		w.insert( coord_ );
		m_packLoadInProgress++;
	}

	cacheable_t obj;
	try {
		obj = fetcher_();
	} catch(...) {
		IBGuard m_obj_guard(TrackableObject::GetLockingMutex());
		IBGuard m_cache_guard(m_cache_mutex);
		m_loadErrors++;
		m_packLoadInProgress--;
		w.erase(coord_);
		cond.Broadcast();
		throw;
	}


		{
			obj->SetOwner(this);
			IBGuard m_obj_guard(TrackableObject::GetLockingMutex());
			IBGuard m_cache_guard(m_cache_mutex);
			if (U::ID==bh::COORD_TYPE::PACK)  {
				m_packLoads++;
				obj->TrackAccess();
			}
			m_packLoadInProgress--;
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(c.find(coord_) == c.end());
			c.insert( std::make_pair(coord_, obj));
			w.erase(coord_);
		}
		cond.Broadcast();

	return obj;
}

#endif
