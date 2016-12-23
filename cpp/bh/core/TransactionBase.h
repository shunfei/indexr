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

#ifndef TRANSACTIONBASE_H
#define TRANSACTIONBASE_H

#include "system/BHToolkit.h"
#include "edition/core/GlobalDataCache.h"
#include "RCAttrPack.h"
#include "RCEngine.h"
#include "edition/local.h"
#ifdef __GNUC__
	#include <ext/hash_map>
	#ifndef stdext
	#define stdext __gnu_cxx
	#endif
#else
	#include <hash_map>
#endif

// NOTE: These classes are still undergoing significant changes
class RCEngine;
class RCAttr;
class AttrDataCache;
class PackAllocator;
struct cached_dpn_splice_allocator;

class TransactionBase
{
public:
	static int const NO_DECOMPOSITION = -1;
protected:
	class TableView {
	public:
		typedef std::vector<_int64> outliers_t;
		RCTablePtr table;
		uint initial_no_packs;
		uint save_result;
		outliers_t _outliers;
		outliers_t update_outliers_diff;
		bool update_delete;
		TableView() :	initial_no_packs(0), _outliers(), update_outliers_diff(), update_delete(false) {};
		TableView(RCTablePtr tab);
	};

	uint  id;
	DataCache m_localCache;
	RCEngine &m_rcengine;
	typedef stdext::hash_map<std::string, TableView> table_info_t;
	table_info_t m_modified_tables;

public:
	bool m_explicit_lock_tables;

	TransactionBase( RCEngine &r, GlobalDataCache *d)
		: m_rcengine(r), m_explicit_lock_tables(false), m_globalCache(d)
		{ id = GenerateTransactionNumber(); }
	virtual ~TransactionBase();
	
	ulong	GetID() const	{ return id; }

	RCTable* GetTable(const std::string& table_path, struct st_table_share* table_share);
	virtual RCTablePtr GetTableShared(const std::string& table_path, struct st_table_share* table_share) = 0;
	void DropTable(const std::string &);
	
	//--- for use by RCAttr during initial integration
	AttrPackPtr GetAttrPack(const PackCoordinate& coord_, PackAllocator&);
	AttrPackPtr GetAttrPackForUpdate(const PackCoordinate& pack_coord);
	int long GetInitialNoPacks( int ) const;
	boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > GetSplice(const SpliceCoordinate& coord);
	//boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > GetSplice(const SpliceCoordinate& coord, cached_dpn_splice_allocator const& a); // function used in on-demand splice loading
	boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > GetSpliceForUpdate(const SpliceCoordinate& splice_coord);
	boost::shared_ptr<FTree> GetFTreeForUpdate(const FTreeCoordinate& ftree_coord);

	template<typename T, typename U, typename V>
	boost::shared_ptr<T> GetObject( U const&, V const& );
	template<typename T, typename U, typename V>
	boost::shared_ptr<T> GetObjectForUpdate( U const&, V const& );

	template<typename T>
	void PutObject(T const&, TrackableObjectPtr);

	template<typename T>
	bool HasLocalObject(T const& coord_);

	template<typename T>
	void DropObject(T const&);

	void ResetAndUnlockOrDropPack(AttrPackPtr& pack);

	template<typename T>
	void DropLocalObject(T const&);
	//void DropLocalCache();

	void ReleasePackRow(int table, int pack_row);

	template<typename T, typename U>
	void MoveObjectFromGlobalToLocalIfNotUsed(U const& coord_, U const& new_coord_);

	virtual RCTablePtr RefreshTable(std::string const& table_path, uint packrow_size = MAX_PACK_ROW_SIZE, bool fill_up_last_packrow = false) = 0;

	bool IsTableModified(const std::string& table_path) const { return (m_modified_tables.find(table_path) != m_modified_tables.end()); }
	bool IsTableModified(int table_id) const { return !!GetModifiedTableView(table_id).table; }

	void ExplicitLockTables() { m_explicit_lock_tables = true; };
	void ExplicitUnlockTables() { m_explicit_lock_tables = false; };

	virtual void ApplyPendingChanges(THD* thd, const std::string& table_path, struct st_table_share* table_share) = 0;
	virtual void ApplyPendingChanges(THD* thd, const std::string& table_path, struct st_table_share* table_share, RCTable* tab) = 0;
	static void FlushTableBuffers(RCTable* tab);
	//void SetOutliers( std::string const&, int, int64 );
	//int long GetOutliers( std::string const&, int );
	void AddOutliers(int, int, int64);
	void AddOutliers(int, const std::vector<int64>&);
	void AddUpdateOutliersDiff(int table, int column, _int64 outliers_diff);

protected:
	GlobalDataCache* m_globalCache;
	void VerifyTablesIntegrity();

	TableView GetModifiedTableView(int table_id) const;
};

bool for_local_cache( int long, int long );

template<typename T, typename U, typename V>
boost::shared_ptr<T> TransactionBase::GetObjectForUpdate(U const& coord_, V const& f)
{
	//TODO: <michal> Refactoring
	typedef boost::shared_ptr<T> ret_t;
	ret_t ret;
	ret = m_localCache.GetObject<T>(coord_);
	if(!!ret) {
		if(ret.use_count() > 2) { // means somebody else (outside this scope) has a reference to this object. 1 ref count in cache + 1 ret
			ret_t tmp(ret);
			ret = tmp->Clone();
			m_localCache.PutObject( coord_, ret );
		}
	} else {
		ret = m_globalCache->GetObject<T>(coord_);
		if(!!ret) {
			ret_t tmp(ret);
			ret = tmp->Clone();
			m_localCache.PutObject( coord_, ret );
		}
	}
	return ret;	
}

template<typename T>
bool TransactionBase::HasLocalObject(T const& coord_)
{
	return m_localCache.HasObject<T>(coord_);
}


template<typename T, typename U, typename V>
boost::shared_ptr<T> TransactionBase::GetObject(U const& coord_, V const& fetcher)
{
	typedef boost::shared_ptr<T> ret_t;
	ret_t ret = m_localCache.GetObject<T>(coord_);
	if(!ret)
	{
		if (GetModifiedTableView(coord_[0] ).table)
			ret = m_localCache.GetObject<T>(coord_, fetcher);
		else
			ret = m_globalCache->GetObject<T>(coord_, fetcher);
	}
	return ret;
}


template<typename T>
void TransactionBase::DropObject(T const& coord_)
{
	if(m_localCache.HasObject(coord_))
		m_localCache.DropObject(coord_);
	else
		m_globalCache->DropObject(coord_);
}

template<typename T>
void TransactionBase::DropLocalObject(T const& coord_)
{
	m_localCache.DropObject(coord_);
}

template<typename T>
void TransactionBase::PutObject(T const& coord_, TrackableObjectPtr p)
{
	m_localCache.PutObject(coord_, p);
}

template<typename T, typename U>
void TransactionBase::MoveObjectFromGlobalToLocalIfNotUsed(U const& coord_, U const& new_coord_)
{
	m_globalCache->MoveObjectIfNotUsed<T>(coord_, new_coord_, m_localCache);
}

#endif
