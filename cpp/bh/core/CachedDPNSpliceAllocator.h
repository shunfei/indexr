#ifndef CORE_CACHED_DPN_SPLICE_ALLOCATOR
#define CORE_CACHED_DPN_SPLICE_ALLOCATOR 1

#include <boost/shared_array.hpp>

#include "core/DPN.h"
#include "core/SplicedVector.h"

class RCAttr;
class Transaction;

struct cached_dpn_splice_allocator
{
	RCAttr* _attr;
	mutable int long _initialNumberOfPacks;
	typedef Splice<DPN, DEFAULT_SPLICE_SIZE> splice_t;
	typedef boost::shared_ptr<splice_t> splice_ptr_t;
	typedef std::vector<splice_ptr_t> splice_vector_t;
	cached_dpn_splice_allocator( RCAttr* attr_ ) : _attr( attr_ ), _initialNumberOfPacks( 0 ) {}
	//splice_ptr_t operator()(const int & spliceNo_, Transaction*) const; // on-demand splice loading operator
	void operator ()(splice_vector_t& , int, int, Transaction*) const;
	//splice_ptr_t fetch(const int & spliceNo_) const; // function used in on-demand splice loading
	splice_ptr_t get_from_cache(const int & spliceNo_, Transaction*) const;
	void put_to_cache(const int& , splice_ptr_t, Transaction*) const;
	bool in_cache(const int&, Transaction*) const;
private:
	splice_ptr_t RestoreSplice(char*& bp, int no_dpns) const;
	boost::shared_array<char> ReadDPNs(int no_dpns_to_skip, int no_dpns_to_read) const;
	int RestoreCachedSplices(int old_, int newCount, int offset, splice_vector_t& vec_, Transaction*) const;
};

#endif /* not CORE_CACHED_DPN_SPLICE_ALLOCATOR */

