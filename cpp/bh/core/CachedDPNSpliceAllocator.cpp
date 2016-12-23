#include <algorithm>
#include <iostream>
#include <boost/scoped_array.hpp>

#include "core/CachedDPNSpliceAllocator.h"
#include "core/RCAttr.h"
#include "edition/core/Transaction.h"
#include "loader/RCAttrLoadBase.h"

using namespace std;
using namespace boost;

template<typename T>
class constant
{
public:
	typedef T result_type;
private:
	T _constant;
public:
	constant( T constant_ ) : _constant( constant_ ) {}
	result_type operator()( void )
		{ return ( _constant ); }
	result_type const operator()( void ) const
		{ return ( _constant ); }
};

/*cached_dpn_splice_allocator::splice_ptr_t cached_dpn_splice_allocator::operator()( int const& spliceNo, Transaction* trans ) const
{
	splice_ptr_t sp;
	int spliceNo_ = spliceNo + _attr->packs_omitted / splice_t::SPLICE_SIZE;
	if ( ( process_type == ProcessType::MYSQLD ) || ( process_type == ProcessType::EMBEDDED ) ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( trans );
		if(dynamic_cast<RCAttrLoadBase*>(_attr)) {
			sp = trans->GetSpliceForUpdate( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) );
			if(!sp)
				trans->PutObject(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), sp = fetch(spliceNo_));
		} else
			sp = trans->GetSplice( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), *this );
	} else {
		sp = GlobalDataCache::GetGlobalDataCache().GetObject<splice_t>(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), bind( &cached_dpn_splice_allocator::fetch, this, spliceNo_ ) );
	}
	return ( sp );
}*/

cached_dpn_splice_allocator::splice_ptr_t cached_dpn_splice_allocator::get_from_cache( int const& spliceNo, Transaction* trans ) const
{
	splice_ptr_t sp;
	int spliceNo_ = spliceNo + _attr->packs_omitted / splice_t::SPLICE_SIZE;
	if ( ( process_type == ProcessType::MYSQLD ) || ( process_type == ProcessType::EMBEDDED ) ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( trans );
		if(dynamic_cast<RCAttrLoadBase*>(_attr)) {
			sp = trans->GetSpliceForUpdate( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) );
		} else
			sp = trans->GetSplice( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) );
	} else {
		sp = GlobalDataCache::GetGlobalDataCache().GetObject<splice_t>(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) );
	}
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !!sp );
	return ( sp );
}

void cached_dpn_splice_allocator::put_to_cache( int const& spliceNo, splice_ptr_t sp_, Transaction* trans ) const
{
	int spliceNo_ = spliceNo + _attr->packs_omitted / splice_t::SPLICE_SIZE;
	if ( ( process_type == ProcessType::MYSQLD ) || ( process_type == ProcessType::EMBEDDED ) ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( trans );
		if(dynamic_cast<RCAttrLoadBase*>(_attr)) {
			splice_ptr_t sp( trans->GetSpliceForUpdate( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) ) );
			if(!sp)
				trans->PutObject( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), sp_ );
			// <TODO>[MC] Shoudn't we replace the existing one with the one given?
		} else {
			if (trans->IsTableModified( _attr->table_number ) && for_local_cache( _initialNumberOfPacks, spliceNo_ ) )
				trans->PutObject( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), sp_ );
			else
				GlobalDataCache::GetGlobalDataCache().PutObject(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), TrackableObjectPtr( sp_ ) );
		}
	} else {
		GlobalDataCache::GetGlobalDataCache().GetObject<splice_t>(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ), constant<splice_ptr_t>( sp_ ) );
	}
}

bool cached_dpn_splice_allocator::in_cache( int const& spliceNo, Transaction* trans ) const
{
	int spliceNo_ = spliceNo + _attr->packs_omitted / splice_t::SPLICE_SIZE;
	bool inCache( false );
	if ( ( process_type == ProcessType::MYSQLD ) || ( process_type == ProcessType::EMBEDDED ) ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( trans );
		if(dynamic_cast<RCAttrLoadBase*>(_attr)) {
			inCache = trans->HasLocalObject( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) );
		} else {
			if ( trans->IsTableModified( _attr->table_number ) && for_local_cache( _initialNumberOfPacks, spliceNo_ ) )
				inCache = trans->HasLocalObject( SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) ); //<TODO>[MC] if this is for local cache why we were checking if it is in global?
			else
				inCache = GlobalDataCache::GetGlobalDataCache().HasObject(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ));
		}
	} else {
		inCache = GlobalDataCache::GetGlobalDataCache().HasObject(SpliceCoordinate( _attr->table_number, _attr->attr_number, spliceNo_ ) );
	}
	return ( inCache );
}

/*cached_dpn_splice_allocator::splice_ptr_t cached_dpn_splice_allocator::fetch( int const& spliceNo_) const
{
	int const BUF_SIZE = DPN::STORED_SIZE * splice_t::SPLICE_SIZE;
	char buf[ BUF_SIZE ];
	char* bp( buf );
	splice_ptr_t sp( splice_ptr_t( new splice_t() ) );
	int no_packs( _attr->NoPack() );
	int no_splices = ( no_packs + (splice_t::SPLICE_SIZE - 1)) / splice_t::SPLICE_SIZE; // total number of splices for whole attribute
	if ( ( ( no_packs - _attr->packs_omitted ) > 0 ) && ( spliceNo_ < no_splices ) ) {
		::memset( buf, 0, BUF_SIZE );
		try	{

			int nDPN( splice_t::SPLICE_SIZE );
			if( spliceNo_ == ( no_splices - 1 ) ) {
				/* If we load last splice it is possible it is not a full splice,
				 * we need to calculate number of dpns in last splice /
				if ( no_packs % splice_t::SPLICE_SIZE )
					nDPN = int(no_packs % splice_t::SPLICE_SIZE);
				/* In case we load last splice, the number of actual dpns to load sould be decreased by 1 due
				 * to the fact that last dpn is stored in different place.
				 /
				-- nDPN;
			}
			/* Here nDPN stores number of dpns to restore for given splice number. /

			IBFile fdpn;
			fdpn.OpenReadOnly(_attr->DPNFileName());
			fdpn.Seek( ( ( spliceNo_ * splice_t::SPLICE_SIZE ) ) * DPN::STORED_SIZE + 2 * DPN::STORED_SIZE, SEEK_SET);

			int toRead( nDPN * DPN::STORED_SIZE );
			int nRead( fdpn.Read(bp, toRead ) );

			BHASSERT( nRead == toRead, "Failed to read all DPNs" );
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT( nDPN <= splice_t::SPLICE_SIZE );

			for( int slot(0); slot < nDPN; ++ slot ) {
				_attr->RestoreDPN(bp, (*sp)[slot]);
				bp += DPN::STORED_SIZE;
			}
			if ( spliceNo_ == ( no_splices - 1 ) ) {
				fdpn.Seek( _attr->GetCurReadLocation() == 1 ? DPN::STORED_SIZE : 0, SEEK_SET );
				int nReadLast( fdpn.Read(buf, DPN::STORED_SIZE) );
				BHASSERT_WITH_NO_PERFORMANCE_IMPACT( nReadLast == DPN::STORED_SIZE );
				_attr->RestoreDPN(buf, (*sp)[nDPN]);
			}
			fdpn.Close();

		}	catch (DatabaseRCException&) {
		}
	} else {
	}

	return ( sp );
}*/

cached_dpn_splice_allocator::splice_ptr_t cached_dpn_splice_allocator::RestoreSplice(char*& bp, int no_dpns) const
{
    splice_ptr_t sp(splice_ptr_t(new splice_t()));
    for(int slot(0); slot < no_dpns; ++slot) {
        _attr->RestoreDPN(bp, (*sp)[slot]);
        bp += DPN::STORED_SIZE;
    }
    return sp;
}

boost::shared_array<char> cached_dpn_splice_allocator::ReadDPNs(int no_dpns_to_skip, int no_dpns_to_read) const
{
	int toSeek( no_dpns_to_skip * DPN::STORED_SIZE );
	int toRead( no_dpns_to_read * DPN::STORED_SIZE);

	shared_array<char> buf( new char[ (no_dpns_to_read + 1) * DPN::STORED_SIZE ] );
	::memset( buf.get(), 0, (no_dpns_to_read + 1) * DPN::STORED_SIZE );

	IBFile fdpn;
	fdpn.OpenReadOnly(_attr->DPNFileName());
	_int64 atPosition( fdpn.Seek( toSeek, SEEK_SET ) );
	BHASSERT( atPosition == toSeek, "Failed to seek into requested position!" );
	int nRead( fdpn.Read( buf.get(), toRead ) );
	BHASSERT( nRead == toRead, "Failed to read all DPNs" );

	fdpn.Seek( _attr->GetCurReadLocation() == 1 ? DPN::STORED_SIZE : 0, SEEK_SET );
	nRead = fdpn.Read(buf.get() + toRead, DPN::STORED_SIZE);
	BHASSERT( nRead == DPN::STORED_SIZE, "Failed to read last DPN" );

	fdpn.Close();
	return buf;
}

int cached_dpn_splice_allocator::RestoreCachedSplices(int old_, int newCount, int offset, splice_vector_t& vec_, Transaction* trans) const
{
	int first_not_cached = 0;
    if(_initialNumberOfPacks > splice_t::SPLICE_SIZE && in_cache(0, trans))
		first_not_cached = (_initialNumberOfPacks - 1) / splice_t::SPLICE_SIZE;

	if(first_not_cached < old_)
		first_not_cached = old_;

	//TODO :: merge the following 2 loops into one - replace in_cache call with something like get_if_in_cache(...)
	while(first_not_cached < newCount && in_cache(first_not_cached - offset, trans))
		first_not_cached++;

	for(int i = old_; i < first_not_cached && i < newCount; i++) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! vec_[i - offset] );
		vec_[i - offset] = get_from_cache(i - offset, trans);
		//put_to_cache( i - offset, vec_[ i - offset ] = splice_ptr_t( new splice_t() ), trans );
	}
	return first_not_cached;
}

void cached_dpn_splice_allocator::operator()( splice_vector_t& vec_, int old_, int new_, Transaction* trans ) const
{
	int offset( _attr->packs_omitted / splice_t::SPLICE_SIZE );
	int no_packs( _attr->NoPack() );
	old_ += offset;
	new_ += offset;
	if ( ( process_type == ProcessType::MYSQLD ) || ( process_type == ProcessType::EMBEDDED ) ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( trans );
		_initialNumberOfPacks = trans->GetInitialNoPacks( _attr->table_number );
	}
	int no_splices =  ( no_packs + ( splice_t::SPLICE_SIZE - 1 ) ) / splice_t::SPLICE_SIZE;
	if((no_packs - static_cast<int> (_attr->packs_omitted)) > 0) {

		int newCount(min(new_, no_splices));

		int first_not_cached = RestoreCachedSplices(old_, newCount, offset, vec_, trans);
		if(first_not_cached < newCount) {

			int nDPNinLastSplice(no_packs % splice_t::SPLICE_SIZE ? int(no_packs % splice_t::SPLICE_SIZE) - 1 : splice_t::SPLICE_SIZE - 1);

			int no_dpn_to_skip((first_not_cached * splice_t::SPLICE_SIZE) + 2);
			int no_dpns_to_read((((newCount - first_not_cached) - (newCount == no_splices ? 1 : 0)) * splice_t::SPLICE_SIZE)
							+ (newCount == no_splices ? nDPNinLastSplice : 0));

			shared_array<char> buf(ReadDPNs(no_dpn_to_skip, no_dpns_to_read));
			char* bp(buf.get());

			int splice_no(first_not_cached);
			for(; splice_no < newCount && splice_no < (no_splices - 1); ++splice_no)
				put_to_cache(splice_no - offset, vec_[splice_no - offset] = RestoreSplice(bp, splice_t::SPLICE_SIZE), trans);

			if(splice_no == (no_splices - 1))
				put_to_cache(splice_no - offset, vec_[splice_no - offset] = RestoreSplice(bp, nDPNinLastSplice + 1), trans);

		}
	}
	for ( int i( no_splices ); i < new_; ++ i ) {
		if ( ! vec_[i - offset] ) {
			/* We have exactly 128 packs, last pack is not full.
			 * We insert 1 row, and commit.
			 * This insert of 1 row will allocate whole new empty splice though it will be never used for this operation.
			 * This empty splice is inserted into cache.
			 * `else' below is necessary to keep consistency between RCAttr and cache afterwards.
			 */
			if ( !in_cache( i - offset, trans ) )
				put_to_cache( i - offset, vec_[ i - offset ] = splice_ptr_t( new splice_t() ), trans );
			else {
				BHASSERT(false, "Empty splice in cache." );
				vec_[ i - offset ] = get_from_cache( i - offset, trans );
			}
		}
	}
}
