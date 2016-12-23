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

#ifndef CORE_TOOLS_H_INCLUDED
#define CORE_TOOLS_H_INCLUDED

#include <cstdio>
#include <set>
#include <vector>
#include <string>
#include <boost/shared_array.hpp>
#include <boost/static_assert.hpp>

#include "common/bhassert.h"
#include "system/ib_system.h"

#if !defined(_countof)

template <typename _CountofType, size_t _SizeOfArray>
char (*__countof_helper( _CountofType (&_Array)[_SizeOfArray]))[_SizeOfArray];
#define _countof(_Array) sizeof(*__countof_helper(_Array))

#endif

namespace object_id_helper
{

template<typename T, int const no_dims>
struct hasher
	{ static size_t calc( T const* ); };

template<typename T>
struct hasher<T, 1>
{
	static size_t calc( T const* v )
		{ return ( *v ); }
};

template<typename T>
struct hasher<T, 2>
{
	static size_t calc( T const* v )
		{ return ( ( v[0] << 4 ) ^ v[1] ); }
};

template<typename T>
struct hasher<T, 3>
{
	static size_t calc( T const* v )
		{ return ( ( v[0] << 6 ) ^ ( v[1] << 4 ) ^ v[2] ); }
};

template<typename T>
struct hasher<T, 4>
{
	static size_t calc( T const* v )
		{ return ( ( v[0] << 8 ) ^ ( v[1] << 6 ) ^ ( v[2] << 4 ) ^ v[3] ); }
};


template<typename T>
struct hasher<T, 5>
{
	static size_t calc( T const* v )
		{ return ( ( v[0] << 8 ) ^ ( v[1] << 6 ) ^ ( v[2] << 4 ) ); }
};
template<typename T>
struct range_eq
{
	bool operator()( T f1, T l1, T f2 ) const {
		return ( std::equal( f1, l1, f2 ) );
	}
};

template<typename T>
struct range_less
{
	bool operator()( T f1, T l1, T f2 ) const {
		std::pair<T, T> m( std::mismatch( f1, l1, f2 ) );
		return ( ( m.first != ( l1 ) ) && ( *m.first < *m.second ) );
	}
};

struct empty {};

}

template<int const id, typename T,
	int const no_dims,
	template<typename>class cmp_algo = object_id_helper::range_eq,
	typename U = object_id_helper::empty>
class ObjectId : public U
{
	T _coord[no_dims];
public:
	static int const ID = id;
	ObjectId( void ) : _coord() {
		std::fill( _coord, _coord + no_dims, 0 );
	}
	ObjectId( T const& a0 ) : _coord() {
		BOOST_STATIC_ASSERT( no_dims == 1 );
		_coord[0] = a0;
	}
	ObjectId( T const& a0, T const& a1 ) : _coord() {
		BOOST_STATIC_ASSERT( no_dims == 2 );
		_coord[0] = a0;
		_coord[1] = a1;
	}
	ObjectId( T const& a0, T const& a1, T const& a2 ) : _coord() {
		BOOST_STATIC_ASSERT( no_dims == 3 );
		_coord[0] = a0;
		_coord[1] = a1;
		_coord[2] = a2;
	}
	ObjectId( T const& a0, T const& a1, T const& a2, T const& a3 ) : _coord() {
		BOOST_STATIC_ASSERT( no_dims == 4 );
		_coord[0] = a0;
		_coord[1] = a1;
		_coord[2] = a2;
		_coord[3] = a3;
	}
	ObjectId( T const& a0, T const& a1, T const& a2, T const& a3, T const& a4 ) : _coord() {
		BOOST_STATIC_ASSERT( no_dims == 5 );
		_coord[0] = a0;
		_coord[1] = a1;
		_coord[2] = a2;
		_coord[3] = a3;
		_coord[4] = a4;
	}
	bool operator == ( ObjectId const& oid ) const {
		return ( object_id_helper::range_eq<T const*>()( _coord, _coord + (no_dims==5? 3: no_dims), oid._coord ) );
	}
	bool operator < ( ObjectId const& oid ) const {
		return ( object_id_helper::range_less<T const*>()( _coord, _coord + (no_dims==5? 3: no_dims), oid._coord ) );
	}
	size_t hash( void ) const {
		return ( object_id_helper::hasher<T, no_dims>::calc( _coord ) );
	}
	bool operator() ( ObjectId const& oid1, ObjectId const& oid2 ) const {
		return ( cmp_algo<T const*>()( oid1._coord, oid1._coord + (no_dims==5? 3: no_dims), oid2._coord ) );
	}
	T const& operator[]( int const& idx_ ) const {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( idx_ < no_dims );
		return ( _coord[idx_] );
	}
	T& operator[]( int const& idx_ ) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT( idx_ < no_dims );
		return ( _coord[idx_] );
	}
	size_t operator()( ObjectId const& oid ) const {
		return ( oid.hash() );
	}
};

namespace bh
{

struct COORD
{
	static int const TABLE;
	static int const COLUMN;
	static int const DP;
};

struct COORD_TYPE
{
	static int const PACK = 0;
	static int const FILTER = 1;
	static int const FTREE = 2;
	static int const SPLICE = 3;
	static int const RCATTR = 4;
	static int const BUFFER_BLOCK = 5;
	static int const COORD_UNKNOWN = 999;
};

}

#ifdef __GNUC__
typedef ObjectId<bh::COORD_TYPE::PACK, int, 5> PackCoordinate;
typedef ObjectId<bh::COORD_TYPE::FILTER, int, 1> FilterCoordinate;
typedef ObjectId<bh::COORD_TYPE::FTREE, int, 2> FTreeCoordinate;
typedef ObjectId<bh::COORD_TYPE::SPLICE, int, 3> SpliceCoordinate;
typedef ObjectId<bh::COORD_TYPE::RCATTR, int, 2> RCAttrCoordinate;
typedef ObjectId<bh::COORD_TYPE::BUFFER_BLOCK, int, 2> BufferBlockCoordinate;
#else
struct BucketInfo
{
	const static  size_t bucket_size = 4;
	const static size_t min_buckets = 8;
};
typedef ObjectId<bh::COORD_TYPE::PACK, int, 5, object_id_helper::range_less, BucketInfo> PackCoordinate;
typedef ObjectId<bh::COORD_TYPE::FILTER, int, 1, object_id_helper::range_less, BucketInfo> FilterCoordinate;
typedef ObjectId<bh::COORD_TYPE::FTREE, int, 2, object_id_helper::range_less, BucketInfo> FTreeCoordinate;
typedef ObjectId<bh::COORD_TYPE::SPLICE, int, 3, object_id_helper::range_less, BucketInfo> SpliceCoordinate;
typedef ObjectId<bh::COORD_TYPE::RCATTR, int, 2, object_id_helper::range_less, BucketInfo> RCAttrCoordinate;
typedef ObjectId<bh::COORD_TYPE::BUFFER_BLOCK, int, 2, object_id_helper::range_less, BucketInfo> BufferBlockCoordinate;
#endif

inline int pc_table( PackCoordinate const& pc )
	{ return ( pc[bh::COORD::TABLE] ); }

inline int pc_column( PackCoordinate const& pc )
	{ return ( pc[bh::COORD::COLUMN] ); }

inline int pc_dp( PackCoordinate const& pc )
	{ return ( pc[bh::COORD::DP] ); }


/* Create a coordinate type whereby objects of different logical
 * types can be compared and stored in the same datastructure.
 * 
 * Short name of TrackableObjectCoordinate
 */
#ifdef __GNUC__
class TOCoordinate {
#else 
class TOCoordinate : public BucketInfo {
#endif
	public:
	// this ID is bh::COORD_TYPE
	int ID;
	
	// since union is not possible.
	// maybe modify ObjectId<> to change its number of dimensions at run time instead
	struct {
		PackCoordinate pack;
		RCAttrCoordinate rcattr;
		BufferBlockCoordinate block;
	} co;

	TOCoordinate() : ID(bh::COORD_TYPE::COORD_UNKNOWN) {}
	TOCoordinate( PackCoordinate &_p ) : ID(bh::COORD_TYPE::PACK) { co.pack = _p; }
	TOCoordinate( RCAttrCoordinate &_r ) : ID(bh::COORD_TYPE::RCATTR) { co.rcattr = _r; }
	TOCoordinate( BufferBlockCoordinate &_r ) : ID(bh::COORD_TYPE::BUFFER_BLOCK) { co.block = _r; }
	TOCoordinate( const TOCoordinate &_t ) : ID(_t.ID) { co.pack = _t.co.pack; co.rcattr = _t.co.rcattr; co.block = _t.co.block; }
	
	bool operator == ( TOCoordinate const& oid ) const {
		if( oid.ID != ID ) return false;
		switch( ID ) {
			case bh::COORD_TYPE::PACK:
				return co.pack == oid.co.pack;
			case bh::COORD_TYPE::RCATTR:
				return co.rcattr == oid.co.rcattr;
			case bh::COORD_TYPE::BUFFER_BLOCK:
				return co.block == oid.co.block;
			default:
				BHASSERT(false,"Undefined coordinate usage");
				return false;
		};
	}
	bool operator < ( TOCoordinate const& oid ) const {
		if( oid.ID < ID ) return false;
		else if( oid.ID > ID ) return true;
		else switch( ID ) {
			case bh::COORD_TYPE::PACK:
				return oid.co.pack < co.pack;
			case bh::COORD_TYPE::RCATTR:
				return oid.co.rcattr < co.rcattr;
			case bh::COORD_TYPE::BUFFER_BLOCK:
				return oid.co.block < co.block;
			default:
				BHASSERT(false,"Undefined coordinate usage");
				return false;
		};
	}
	
	size_t hash( void ) const {
		switch( ID ) {
			case bh::COORD_TYPE::PACK:
				return co.pack.hash();
			case bh::COORD_TYPE::RCATTR:
				return co.rcattr.hash();
			case bh::COORD_TYPE::BUFFER_BLOCK:
				return co.block.hash();
			default:
				BHASSERT(false,"Undefined coordinate usage");
				return false;
		};
	}
	
	bool operator() ( TOCoordinate const& oid1, TOCoordinate const& oid2 ) const {
                return oid1 < oid2;
	}
	
	int const& operator[]( int const& idx_ ) const {
		switch( ID ) {
			case bh::COORD_TYPE::PACK:
				return co.pack[idx_];
			case bh::COORD_TYPE::RCATTR:
				return co.rcattr[idx_];
			case bh::COORD_TYPE::BUFFER_BLOCK:
				return co.block[idx_];
			default:
				BHASSERT(false,"Undefined coordinate usage");
				return co.rcattr[idx_];
		};
	}
	
	int& operator[]( int const& idx_ ) {
		switch( ID ) {
			case bh::COORD_TYPE::PACK:
				return co.pack[idx_];
			case bh::COORD_TYPE::RCATTR:
				return co.rcattr[idx_];
			case bh::COORD_TYPE::BUFFER_BLOCK:
				return co.block[idx_];
			default:
				BHASSERT(false,"Undefined coordinate usage");
				return co.rcattr[idx_];
		};
	}

	size_t operator()( TOCoordinate const& oid ) const {
		switch( oid.ID ) {
			case bh::COORD_TYPE::PACK:
				return oid.co.pack(oid.co.pack);
			case bh::COORD_TYPE::RCATTR:
				return oid.co.rcattr(oid.co.rcattr);
			case bh::COORD_TYPE::BUFFER_BLOCK:
				return oid.co.block(oid.co.block);
			default:
				BHASSERT(false,"Undefined coordinate usage");
				return co.rcattr(oid.co.rcattr);
		};
	}

};

/* Release (close) a FILE* on return from a function (i.e. on destruction of this object) */
class CloseFILE
{
	FILE* f;
public:
	CloseFILE(FILE* _f)		{ f = _f; }
	~CloseFILE()				{ fclose(f); }
};

template<typename T>
struct copy_object
{
	T operator()( T const& obj )
		{	return ( obj ); }
};

//T - class type
//A - parameter type
template<class T, typename A>
class MemberFunctionExecutor
{
public:
	MemberFunctionExecutor(T& obj, void (T::*on_start)(int), void (T::*on_end)(int), A argument)
		: argument(argument), obj(&obj)
	{
		this->on_start = on_start;
		this->on_end = on_end;
		((this->obj)->*on_start)(argument);
	}

	~MemberFunctionExecutor()
	{
		(obj->*on_end)(argument);
	}

private:
	A argument;
	T* obj;
	void (T::*on_start)(A);
	void (T::*on_end)(A);
};

template<typename T>
class TemporalValueReplacement
{
public:
	TemporalValueReplacement(T& to_replace, T temp_value)
		: orig_value(to_replace), orig_value_ptr(&to_replace)
	{
		*orig_value_ptr = temp_value;
	}

	~TemporalValueReplacement()
	{
		*orig_value_ptr = orig_value;
	}
private:
	T orig_value;
	T* orig_value_ptr;
};

template<typename T>
class NullaryFunctionObject
{
public:
	NullaryFunctionObject(T = T()) {}
	void operator()() const {};
};

#include <boost/function/function0.hpp>
class FunctionExecutor
{
typedef boost::function0<void> F;
public:
    FunctionExecutor(F call_in_constructor, F call_in_deconstructor)
        : call_in_constructor(call_in_constructor), call_in_deconstructor(call_in_deconstructor)
    {
    	if(!this->call_in_constructor.empty())
    		this->call_in_constructor();
    }

    virtual ~FunctionExecutor()
    {
    	if(!call_in_deconstructor.empty())
    		call_in_deconstructor();
    }

private:
	boost::function0<void> call_in_constructor;
	boost::function0<void> call_in_deconstructor;
};

void MakeStringUnique(std::string& id);

//#define PROFILE_LOCK_WAITING
#ifdef PROFILE_LOCK_WAITING

class LockProfiler
{
	public:
		LockProfiler() {
			//pthread_mutex_init(&m,NULL);
		}
		~LockProfiler() {
			map<string,_uint64> :: iterator it;
			for(it = lock_waiting.begin(); it!= lock_waiting.end(); it++) {
				cerr << it->first << "\t" << it->second << endl;
			}
			lock_waiting.clear();
		}
		void Record(string where, _uint64 usec) {
			//pthread_mutex_lock(&m);
            IBGuard guard(lock_profiler_mutex);
			lock_waiting[where]+= usec;
			//pthread_mutex_unlock(&m);
		}

	private:
        IBMutex lock_profiler_mutex;
		//pthread_mutex_t m;
		map<string,_uint64> lock_waiting;


};

extern LockProfiler lock_profiler;

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

#define pthread_mutex_lock(m) \
{ \
struct timeval start_time, t2;	\
_uint64 sec, usec; 		\
gettimeofday(&start_time, NULL); 		\
pthread_mutex_lock(m);	\
gettimeofday(&t2, NULL); \
sec = usec = 0;			\
sec = (t2.tv_sec - start_time.tv_sec); \
if (t2.tv_usec < start_time.tv_usec) {  \
        sec--; \
        t2.tv_usec += 1000000l; \
}; \
usec = (t2.tv_usec - start_time.tv_usec); \
if (usec >= 1000000l) \
{ \
        usec -=  1000000l; \
        sec++; \
}; \
usec += sec * 1000000; \
lock_profiler.Record(string(__FILE__) + TOSTRING(__LINE__) , usec);\
}


#define pthread_cond_wait(c,m) \
({ \
int r; \
struct timeval start_time, t2;	\
_uint64 sec, usec; 		\
gettimeofday(&start_time, NULL); 		\
r = pthread_cond_wait(c,m);	\
gettimeofday(&t2, NULL); \
sec = usec = 0;			\
sec = (t2.tv_sec - start_time.tv_sec); \
if (t2.tv_usec < start_time.tv_usec) {  \
        sec--; \
        t2.tv_usec += 1000000l; \
}; \
usec = (t2.tv_usec - start_time.tv_usec); \
if (usec >= 1000000l) \
{ \
        usec -=  1000000l; \
        sec++; \
}; \
usec += sec * 1000000; \
lock_profiler.Record(string(__FILE__) + TOSTRING(__LINE__) , usec);\
r;\
})

#endif


/* Use objects of this class to enter critical section and automatically leave it when returning from a function.
   To do this, declare a local object: "(...) LockCS _cs(&critical_section); (...)".
   Its destructor will be invoked automatically. */
/******
 * replaced by IBGuard
class LockCS
{
	pthread_mutex_t *cs;
public:
	LockCS(pthread_mutex_t* cs_)		{ cs = cs_; pthread_mutex_lock(cs); }
	~LockCS()							{ pthread_mutex_unlock(cs); }
};
********/

template<typename SourceType>
void store_bytes(char*& destination, const SourceType& source)
{
	memcpy(destination, (unsigned char*)&source, sizeof(SourceType));
	destination += sizeof(SourceType);
}

template<typename DestinationType>
void unstore_bytes(DestinationType& destination, char*& source)
{
	memcpy((unsigned char*)&destination, source, sizeof(DestinationType));
	source += sizeof(DestinationType);
}

std::vector<std::string> get_stack( int );
void dump_stack( char const*, int, char const*, std::string const&, int = 64 );
#define DUMP_STACK( msg ) dump_stack( __FILE__, __LINE__, __PRETTY_FUNCTION__, msg )


#endif
