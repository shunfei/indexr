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

#include <boost/ref.hpp>
#include <boost/filesystem/operations.hpp>

#include "system/IBFileSystem.h"

#ifndef __BH_COMMUNITY__
#include "enterprise/edition/core/RCTableImplEnt.h"
#include "enterprise/edition/system/Compactor.h"
#else
#include "core/RCTableImpl.h"
#endif
#include "core/TransactionBase.h"
//#include "core/RCTableTrans.h"
#include "core/RCEngine.h"
#include "core/DPN.h"
#include "core/tools.h"
#include "core/SplicedVector.h"
#ifdef __GNUC__
#include <unistd.h>
#endif

using namespace boost;
using namespace boost::filesystem;

TransactionBase::TableView::TableView(RCTablePtr tab)
	:	table(tab), initial_no_packs(bh::common::NoObj2NoPacks(tab->NoObj())),
	 	_outliers( table->NoAttrs(), TransactionBase::NO_DECOMPOSITION ), update_outliers_diff( table->NoAttrs(), INT_MIN ),
	 	update_delete(false)
{
}

TransactionBase::~TransactionBase()
{
	m_modified_tables.clear();
}

RCTable* TransactionBase::GetTable(const std::string& table_path, struct st_table_share* table_share) 
{
	return GetTableShared(table_path, table_share).get();
}

void TransactionBase::DropTable(const std::string &table)
{
	stdext::hash_map<std::string, TableView>::iterator iter = m_modified_tables.find(table);
	if (iter != m_modified_tables.end()) {
		m_localCache.ReleasePacks(iter->second.table->GetID());
		m_modified_tables.erase(iter);
	}
}

AttrPackPtr
TransactionBase::GetAttrPack(const PackCoordinate& pack_coord, PackAllocator &a)
{
	AttrPackPtr ret;
	{
		ret = m_localCache.GetLockedObject<AttrPack>(pack_coord);
		if (!!ret)
			return ret;
	}
	TableView tv = GetModifiedTableView(pc_table( pack_coord ) );
	if (tv.table && (uint)(pc_dp( pack_coord ) + 1) >= tv.initial_no_packs) {
		ret = m_localCache.GetObject<AttrPack>(pack_coord, bind( &AttrPackLoad, pack_coord, ref( a ) ));
	} else
		ret = m_globalCache->GetObject<AttrPack>(pack_coord, bind( &AttrPackLoad, pack_coord, ref( a ) ) );
		//ret = m_globalCache->GetAttrPack(pack_coord, bind( &AttrPackLoad, pack_coord, ref( a ) ) );

	return ret;
}

AttrPackPtr
TransactionBase::GetAttrPackForUpdate(const PackCoordinate& pack_coord)
{
	//TODO: <michal> Refactoring
	AttrPackPtr ret;
	ret = m_localCache.GetLockedObject<AttrPack>(pack_coord);
	if(!!ret) {
		if(ret.use_count() > 2) { // means somebody else (outside this scope) has a reference to this object. 1 ref count in cache + 1 ret
			AttrPackPtr tmp(ret);
			ret = tmp->Clone();
			tmp->Unlock();
			m_localCache.PutObject( pack_coord, ret );
		}
	} else {
		TableView tv = GetModifiedTableView(pc_table( pack_coord ) );
		if ((uint)(pc_dp( pack_coord ) + 1) >= tv.initial_no_packs)
			return ret;
		ret = m_globalCache->GetLockedObject<AttrPack>(pack_coord);
		if(!!ret) {
			AttrPackPtr tmp(ret);
			ret = tmp->Clone();
			tmp->Unlock();
			m_localCache.PutObject( pack_coord, ret );
		}
	}
	return ret;
}

int long TransactionBase::GetInitialNoPacks( int tabId_ ) const
{
	//<TODO>[MC] Why for session that haven't modified the table this is always 0?
	TableView tv = GetModifiedTableView(tabId_);
	return ( tv.table ? tv.initial_no_packs : 0 );
}

bool for_local_cache( int long initialNoPacks_, int long packNo_ )
{
	return ( ( initialNoPacks_ == 0 ) || ( packNo_ >= ((initialNoPacks_ - 1) / DEFAULT_SPLICE_SIZE) ) );
}

boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> >
TransactionBase::GetSplice(const SpliceCoordinate& coord)
{
	boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > ret = m_localCache.GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(coord);
	if (!!ret || (!!GetModifiedTableView(coord[0]).table && for_local_cache( GetInitialNoPacks( coord[0] ), coord[2] ) ) )
		return ret;
	return m_globalCache->GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(coord);
}

/*boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> >
TransactionBase::GetSplice(const SpliceCoordinate& coord, cached_dpn_splice_allocator const& a)
{
	boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > ret;
	{
		ret = m_localCache.GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(coord);
		if(!!ret)
			return ret;
	}

	int long initial_no_packs( GetInitialNoPacks( coord[0] ) );
	if ( !!GetModifiedTableView(coord[0]).table && for_local_cache( initial_no_packs, coord[2] ) ) {
		ret = m_localCache.GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(coord, bind(&cached_dpn_splice_allocator::fetch, ref(a), coord[2]));
	} else {
		ret = m_globalCache->GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(coord, bind(&cached_dpn_splice_allocator::fetch, ref(a), coord[2]));
	}

	return ret;
}*/

boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> >
TransactionBase::GetSpliceForUpdate(const SpliceCoordinate& splice_coord)
{
	boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > ret;
	ret = m_localCache.GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(splice_coord);
	if(!!ret) {
		if(ret.use_count() > 2) { // means somebody else (outside this scope) has a reference to this object. 1 ref count in cache + 1 ret
			boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > tmp(ret);
			ret = tmp->Clone();
			m_localCache.PutObject( splice_coord, ret );
		}
	} else {
		TableView tv = GetModifiedTableView(splice_coord[0] );
		if ((splice_coord[2]+1)*DEFAULT_SPLICE_SIZE >= int(tv.initial_no_packs))
			return ret;
		ret = m_globalCache->GetObject<Splice<DPN, DEFAULT_SPLICE_SIZE> >(splice_coord);
		if(!!ret) {
			boost::shared_ptr<Splice<DPN, DEFAULT_SPLICE_SIZE> > tmp(ret);
			ret = tmp->Clone();
			m_localCache.PutObject( splice_coord, ret );
		}
	}
	return ret;
}

boost::shared_ptr<FTree>
TransactionBase::GetFTreeForUpdate(const FTreeCoordinate& ftree_coord)
{
	boost::shared_ptr<FTree> ret;
	ret = m_localCache.GetObject<FTree>(ftree_coord);
	if(!!ret) {
		if(ret.use_count() > 2) { // means somebody else (outside this scope) has a reference to this object. 1 ref count in cache + 1 ret
			boost::shared_ptr<FTree> tmp(ret);
			ret = tmp->Clone();
			m_localCache.PutObject( ftree_coord, ret );
		}
	}
	return ret;
}

void TransactionBase::ReleasePackRow(int table, int pack_row)
{
	m_localCache.ReleasePackRow(table, pack_row);
}

void TransactionBase::ResetAndUnlockOrDropPack(AttrPackPtr& pack)
{
	if(m_localCache.HasObject(pack->GetPackCoordinate()))
		m_localCache.ResetAndUnlockOrDropPack(pack);
	else
		m_globalCache->ResetAndUnlockOrDropPack(pack);
}

TransactionBase::TableView TransactionBase::GetModifiedTableView(int table_id) const
{
	stdext::hash_map<std::string, TableView>::const_iterator iter = m_modified_tables.begin();
	stdext::hash_map<std::string, TableView>::const_iterator end = m_modified_tables.end();
	while (iter != end) {
		if (iter->second.table->GetID() == table_id)
			return iter->second;
		++ iter;
	}
	return TableView();
}

void TransactionBase::VerifyTablesIntegrity()
{
	stdext::hash_map<std::string, TableView>::iterator iter;
	for (iter = m_modified_tables.begin(); iter != m_modified_tables.end(); ++iter) {
		RCTable* tab = iter->second.table.get();
		if (tab->Verify() != 1) {
			std::string err_msg = "Data integrity is broken in table ";
			err_msg += iter->first;
			throw DatabaseRCException(err_msg.c_str());
		}
	}
}

/*void TransactionBase::DropLocalCache()
{
	m_localCache.ReleaseAll();
}*/

void TransactionBase::FlushTableBuffers(RCTable* tab)
{
	// data dir
	directory_iterator end_itr;
	IBFile fb;
	for( directory_iterator itr_db(tab->GetPath()); itr_db != end_itr; itr_db++) {
#ifdef __GNUC__
		fb.OpenReadOnly(itr_db->path().string());
#else
		fb.OpenWriteAppend(itr_db->path().string(), 0, IBFILE_OPEN_NO_TIMEOUT);
#endif
		fb.Flush();
		fb.Close();
	}
	// table dir structure
	FlushDirectoryChanges(tab->GetPath());
	// data dir structure
	//FlushDirectoryChanges(infobright_data_dir);

	// kn folder
	if ( rsi_manager ) {
		std::vector<RSIndexID> knlist = rsi_manager->GetAll2(*tab);
		std::string knf_pth = rsi_manager->GetKNFolderPath();
		for(int i = 0; i < knlist.size(); i++) {
			std::string rsi_pth = (knlist.at(i)).GetFileName(knf_pth.c_str());
			try { // TODO
#ifdef __GNUC__
				fb.OpenReadOnly(rsi_pth);
#else
				fb.OpenWriteAppend(rsi_pth, 0, IBFILE_OPEN_NO_TIMEOUT);
#endif
				fb.Flush();
				fb.Close();
			} catch(DatabaseRCException&) {
			}
		}
		// kn folder structure
		FlushDirectoryChanges(knf_pth);
	}
}

/*void TransactionBase::SetOutliers( std::string const& table_, int column_, int64 outliers_ )
{
	table_info_t::iterator it(m_modified_tables.find(table_));
	BHASSERT( it != m_modified_tables.end(), "no such table, while setting outliers" );
	BHASSERT( ( column_ >= 0 ) && ( column_ < it->second.table->NoAttrs() ), "bad column index while setting outliers" );
	BHASSERT( outliers_ >= TransactionBase::NO_DECOMPOSITION, "bad number of outliers" );
	it->second._outliers[column_] = outliers_;
}*/

void TransactionBase::AddOutliers( int table_, int column_, int64 outliers_ )
{
	stdext::hash_map<std::string, TableView>::iterator it = m_modified_tables.begin();
	stdext::hash_map<std::string, TableView>::iterator end = m_modified_tables.end();
	while (it != end) {
		if (it->second.table->GetID() == table_) {
			BHASSERT( ( column_ >= 0 ) && ( column_ < int(it->second.table->NoAttrs()) ), "bad column index while setting outliers" );
			BHASSERT( outliers_ >= TransactionBase::NO_DECOMPOSITION, "bad number of outliers" );
			if(outliers_ != TransactionBase::NO_DECOMPOSITION) {
				TableView::outliers_t& outliers( it->second._outliers );
				if ( outliers[ column_ ] == TransactionBase::NO_DECOMPOSITION )
					outliers[ column_ ] = 0;
				outliers[ column_ ] += outliers_;
			}
			return;
		}
		++ it;
	}
	BHASSERT( 0, "no such table, while setting outliers" );
}

void TransactionBase::AddOutliers(int table_, const std::vector<int64>& outliers_)
{
	int col = 0;
	BOOST_FOREACH(int64 val, outliers_)
		AddOutliers(table_, col++, val);

}

/*int long TransactionBase::GetOutliers( std::string const& table_, int column_ )
{
	table_info_t::iterator it( m_modified_tables.find( table_ ) );
	BHASSERT( it != m_modified_tables.end(), "no such table, while getting outliers" );
	BHASSERT( ( column_ >= 0 ) && ( column_ < it->second.table->NoAttrs() ), "bad column index while getting outliers" );
	return ( it->second._outliers[ column_ ] );
}*/

void TransactionBase::AddUpdateOutliersDiff(int table, int column, _int64 outliers_diff)
{
	stdext::hash_map<std::string, TableView>::iterator it = m_modified_tables.begin();
	stdext::hash_map<std::string, TableView>::iterator end = m_modified_tables.end();
	while (it != end) {
		if (it->second.table->GetID() == table) {
			BHASSERT( ( column >= 0 ) && ( column < int(it->second.table->NoAttrs()) ), "bad column index while setting outliers" );
			if (it->second.update_outliers_diff[column]==INT_MIN)
				it->second.update_outliers_diff[column] = 0;
			it->second.update_outliers_diff[column] += outliers_diff;
			return;
		}
		++it;
	}
	BHASSERT( 0, "no such table, while setting outliers" );
}
