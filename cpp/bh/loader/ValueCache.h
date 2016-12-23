#ifndef LOADER_VALUECACHE_H_INCLUDED
#define LOADER_VALUECACHE_H_INCLUDED  1

#include <vector>

#include "loader/NewValuesSetBase.h"

class ValueCache : public NewValuesSetBase
{
	typedef std::vector<int> values_t; /* offsets in this->data buffer, no need for int64 here */
	typedef std::vector<bool> nulls_t;
private:
	void* data;
	_int64 size;
	_int64 capacity;
	int value_count;
	values_t values;
	nulls_t nulls;
	int expected_size;
	bool expected_null;
	AttrPackType pack_type;
	DTCollation col;
	bool needs_UTFCollation;
public:
	ValueCache(_int64, _int64, AttrPackType packType, DTCollation);
	~ValueCache();
	void* Prepare(_int64);
	void Commit(_int64);
	void Commit();
	void SetNull(int, bool);
	void ExpectedNull(bool);
	bool ExpectedNull() const;
	int ExpectedSize() const;
	void ExpectedSize(int);
	void* PreparedBuffer();
	virtual bool 	IsNull(int ono);
	virtual _uint64 SumarizedSize();
	virtual uint	Size(int ono) const;
	virtual void 	GetIntStats(_int64& min, _int64& max, _int64& sum);
	virtual void 	GetRealStats(double& min, double& max, double& sum);
	virtual void 	GetStrStats(RCBString& min, RCBString& max, ushort& maxlen);
	virtual char*	GetDataBytesPointer(int ono);
	char const*	GetDataBytesPointer(int ono) const;
	virtual int		NoValues();
	int compare(int ono_a, int ono_b);
	void Nullify();

private:
	void Realloc(_int64);
};

typedef boost::shared_ptr<ValueCache> ValueCachePtr;
typedef std::vector<ValueCachePtr> ValueCachePackRow;
typedef std::vector<uint> order_t;

void Partition(ValueCachePackRow const& in, int in_offset, order_t const& order, int count, ValueCachePackRow& out);

#endif /* #ifndef LOADER_VALUECACHE_H_INCLUDED */

