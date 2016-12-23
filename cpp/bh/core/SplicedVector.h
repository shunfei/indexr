#ifndef CORE_SPLICEDVECTOR_H_INCLUDED
#define CORE_SPLICEDVECTOR_H_INCLUDED 1

#include <vector> /* for std::vector */
#include <boost/shared_array.hpp> /* for boost::shared_array */

#include "common/bhassert.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "core/tools.h"
#include "system/ConnectionInfo.h"

//namespace core
//{

inline int splice_count( int const& size_, int const& spliceSize_ )
{
	return ( ( size_ / spliceSize_ ) + ( ( size_ % spliceSize_ ) ? 1 : 0 ) );
}

static int const DEFAULT_SPLICE_SHIFT = 7;
static int const DEFAULT_SPLICE_SIZE = ( 1 << DEFAULT_SPLICE_SHIFT );

template<typename T, int const SPLICE_SIZE_>
class Splice : public TrackableObject
{
public:
	static int const SPLICE_SIZE = SPLICE_SIZE_;
private:
	T* _data; //[SPLICE_SIZE];
public:
	explicit Splice( void ) : _data( static_cast<T*>( alloc( SPLICE_SIZE * sizeof ( T ), BLOCK_TEMPORARY ) ) ) {
		for ( int i( 0 ); i < SPLICE_SIZE; ++ i )
			new ( _data + i ) T();
	}
	virtual ~Splice( void ) {
		for ( int i( 0 ); i < SPLICE_SIZE; ++ i ) {
			try {
				_data[i].~T();
			} catch ( ... ) {
				// Catching exception called from destrctor. This is bad!
			}
		}
		dealloc( _data );
	}
	inline T const& operator[]( int64 const& index_ ) const {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( index_ < SPLICE_SIZE );
		return ( _data[ index_ ] );
	}
	inline T& operator[]( int64 const& index_ ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( index_ < SPLICE_SIZE );
		return ( _data[ index_ ] );
	}
	TRACKABLEOBJECT_TYPE TrackableType( void ) const {
		return ( TO_SPLICE );
	}

	std::auto_ptr<Splice<T, SPLICE_SIZE_> > Clone() const {
		std::auto_ptr<Splice<T, SPLICE_SIZE_> > splice(new Splice<T, SPLICE_SIZE_>());
		std::transform( _data, _data + SPLICE_SIZE_, splice->_data, copy_object<T>() );
		return splice;
	}
};

template<typename T, int const SPLICE_SIZE>
struct system_heap_allocator
{
	typedef Splice<T, SPLICE_SIZE> splice_t;
	typedef boost::shared_ptr<splice_t> splice_ptr_t;
	splice_ptr_t operator()( int const& ) const {
		return ( splice_ptr_t( new splice_t() ) );
	}
	template<typename splice_vector_t, typename dummy>
	void operator()( splice_vector_t& vec_, int old_, int new_, dummy = dummy() ) const {
		for ( int i( old_ ); i < new_; ++ i )
			vec_[ i ] = (*this)( i );
	}
};

template<typename T, typename allocator_t = system_heap_allocator<T, DEFAULT_SPLICE_SIZE>, int const SPLICE_SIZE_ = DEFAULT_SPLICE_SIZE>
class SplicedVector
{
private: /* for UT, without UT it is redundant */
	typedef T value_type;
	typedef Splice<T, SPLICE_SIZE_> splice_t;
	typedef boost::shared_ptr<splice_t> splice_ptr_t;
	typedef std::vector<splice_ptr_t> data_t;
	typedef allocator_t allocator_type;
	mutable data_t _data;
	int _size;
	allocator_type _allocator;
	//bool _onDemand;
public:
	static int const SPLICE_SIZE = SPLICE_SIZE_;
	SplicedVector( int const& size_ = 0, allocator_type allocator_ = allocator_type(), bool onDemand_ = false )
		: _data(), _size(), _allocator( allocator_ )/*, _onDemand( onDemand_ )*/ {
		resize( size_ );
	}
	void resize( int const& size_, Transaction* trans = GetCurrentTransaction()) {
		int newSpliceCount = splice_count( size_, SPLICE_SIZE_ );
		int oldSpliceCount = splice_count( _size, SPLICE_SIZE_ );
		if ( newSpliceCount != oldSpliceCount ) {
			_data.resize( newSpliceCount );
			//if ( ! _onDemand ) {
				_allocator( _data, oldSpliceCount, newSpliceCount, trans );
			//}
		}
		_size = size_;
	}
	inline T& operator[]( int64 const& index_ ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( index_ < _size );
		int64 spliceIdx( index_ >> DEFAULT_SPLICE_SHIFT );
//		if ( _onDemand && ! _data[spliceIdx] )
//			_data[spliceIdx] = _allocator( spliceIdx, 0 );
		return ( (*_data[spliceIdx])[index_ & ( SPLICE_SIZE_ - 1 )] );
	}
	inline T const& operator[]( int64 const& index_ ) const {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( index_ < _size );
		int64 spliceIdx( index_ >> DEFAULT_SPLICE_SHIFT );
//		if ( _onDemand && ! _data[spliceIdx] )
//			_data[spliceIdx] = _allocator( spliceIdx, 0 );
		return ( (*_data[spliceIdx])[index_ & ( SPLICE_SIZE_ - 1 )] );
	}
	bool empty( void ) const {
		return ( !_size );
	}
	int size( void ) const {
		return ( _size );
	}
	int64 capacity( void ) const {
		return ( _data.size() * SPLICE_SIZE_ );
	}
	void set_splice( int const& spliceIndex_, splice_ptr_t splice_ ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( spliceIndex_ < _data.size() );
		_data[spliceIndex_] = splice_;
	}
	void clear( void ) {
		_data.clear();
		_size = 0;
	}
	void add_splice( splice_ptr_t splice_ ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( !! splice_ );
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! ( _size & (SPLICE_SIZE_ - 1) ) );
		_data.push_back( splice_ );
		_size += SPLICE_SIZE_;
	}
};

//}

#endif /* not CORE_SPLICEDVECTOR_H_INCLUDED */

