#include <cstdio>

#include "loader/ValueCache.h"
#include "common/bhassert.h"
#include "types/RCDataTypes.h"

using namespace std;

ValueCache::ValueCache(_int64 valueCount, _int64 initialCapacity, AttrPackType packType, DTCollation col_)
	: data(NULL), size(0), capacity(0), value_count(valueCount), values(), nulls(), expected_size(0), expected_null(false), pack_type(packType), col(col_)
{
	Realloc(initialCapacity);
	values.reserve(valueCount);
	nulls.reserve(valueCount);
	needs_UTFCollation = RequiresUTFConversions(col);
}

ValueCache::~ValueCache()
{
	if (data)
		free(data);
}

void* ValueCache::Prepare(_int64 valueSize)
{
	_int64 newSize(size + valueSize);
	if (newSize > capacity) {
		int newCapacity(capacity);
		while (newSize > newCapacity)
			newCapacity <<= 1;
		int vals(static_cast<int>(values.size()));
		if ((capacity > 0) && (vals > 0)) { /* second allocation, first reallocation */
			int valSize(size / vals);
			newCapacity = max(newCapacity, valSize * value_count);
		}
		Realloc(newCapacity);
	}
	return ( static_cast<char*>( data ) + size );
}

void* ValueCache::PreparedBuffer()
{
	return ( static_cast<char*>( data ) + size );
}

int ValueCache::ExpectedSize() const
{
	return (expected_size);
}

void ValueCache::ExpectedSize(int expectedSize)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((size + expectedSize) <= capacity);
	expected_size = expectedSize;
}

void ValueCache::Commit(_int64 valueSize)
{
	values.push_back(size);
	nulls.push_back(false);
	size += valueSize;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( size <= capacity );
	expected_size = 0;
	expected_null = false;
	return;
}

void ValueCache::Commit()
{
	values.push_back(size);
	nulls.push_back(expected_null);
	size += expected_size;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( size <= capacity );
	expected_size = 0;
	expected_null = false;
	return;
}

void ValueCache::Realloc(_int64 newCapacity)
{
	data = realloc( data, newCapacity );
	capacity = newCapacity;
}

_uint64 ValueCache::SumarizedSize()
{
	return (size);
}

void ValueCache::SetNull(int ono, bool null)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < values.size());
	nulls[ono] = null;
}

void ValueCache::ExpectedNull(bool null)
{
	expected_null = null;
}

bool ValueCache::ExpectedNull() const
{
	return (expected_null);
}

bool ValueCache::IsNull(int ono)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < values.size());
	return (nulls[ono]);
}

uint ValueCache::Size(int ono) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < values.size());
	int end(ono < (values.size() - 1) ? values[ono + 1] : size);
	return (end - values[ono]);
}

void ValueCache::GetIntStats(_int64& min, _int64& max, _int64& sum)
{
	min = PLUS_INF_64;
	max = MINUS_INF_64;
	sum = 0;
	_int64 v = 0;
	for(int i(0), count(values.size()); i < count; ++i) {
		if(!nulls[i]) {
			v = *(int64*)GetDataBytesPointer(i);
			sum += v;
			if(min > v)
				min = v;
			if(max < v)
				max = v;
		}
	}
}

void ValueCache::GetRealStats(double& min, double& max, double& sum)
{
	min = PLUS_INF_64;
	max = MINUS_INF_64;
	sum = 0;
	double v = 0;
	for(int i(0), count(values.size()); i < count; ++i) {
		if(!nulls[i]) {
			v = *(double*)GetDataBytesPointer(i);
			sum += v;
			if(min > v)
				min = v;
			if(max < v)
				max = v;
		}
	}
}

void ValueCache::GetStrStats(RCBString& min, RCBString& max, ushort& maxlen)
{
	min = RCBString();
	max = RCBString();
	maxlen = 0;

	RCBString v = 0;
	for(int i(0), count(values.size()); i < count; ++ i) {
		if (!nulls[i]) {
			v.val = GetDataBytesPointer(i);
			v.len = Size(i);
			if(v.len > maxlen)
				maxlen = v.len;

			if(min.IsNull())
				min = v;
			else if(RequiresUTFConversions(col)) {
				if(CollationStrCmp(col, min, v) > 0)
					min = v;
			} else if(min > v)
				min = v;

			if(max.IsNull())
				max = v;
			else if(RequiresUTFConversions(col)) {
				if(CollationStrCmp(col, max, v) < 0)
					max = v;
			} else if(max < v)
				max = v;
		}
	}
}

char* ValueCache::GetDataBytesPointer(int ono)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < values.size());
	return (static_cast<char*>(data) + values[ono]);
}

char const* ValueCache::GetDataBytesPointer(int ono) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < values.size());
	return (static_cast<char*>(data) + values[ono]);
}

int ValueCache::NoValues()
{
	return (values.size());
}

int ValueCache::compare(int ono_a, int ono_b)
{
	if( nulls[ono_a] == 1) {
		if( nulls[ono_b] == 1 )
			return 0;
		else
			return -1;
	}
	if( nulls[ono_b] == 1 )
		return 1;

	if( pack_type == PackN ) {
		_uint64 vx = *(_uint64*)GetDataBytesPointer(ono_a);
		_uint64 vy = *(_uint64*)GetDataBytesPointer(ono_b);

		if( vx < vy ) return -1;
		if( vx > vy ) return 1;
		return 0;
	}

	if( needs_UTFCollation ) {
		RCBString a(GetDataBytesPointer(ono_a),Size(ono_a)),
				  b(GetDataBytesPointer(ono_b),Size(ono_b));
		return CollationStrCmp(col, a, b);
	} else {
		int len_a = Size(ono_a);
		int len_b = Size(ono_b);
		int ret = strncmp(GetDataBytesPointer(ono_a), GetDataBytesPointer(ono_b),
						std::min(len_a,len_b));

		if( ret == 0 ) {
			if( len_a == len_b ) return 0;
			return (len_a < len_b ? -1 : 1);
		}
		return ret;
	}
}

void ValueCache::Nullify()
{
	for(int i = 0; i < values.size(); i++)
		nulls[i] = true;
}

void Partition(ValueCachePackRow const& in, int in_offset, order_t const& order, int count, ValueCachePackRow& out)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(in.size() == out.size());
	for ( int col(0), col_count(in.size()); col < col_count; ++ col) {
		_int64 memory_needed(0);
		ValueCache const& vcIn(*in[col]);
		for ( int i(0); i < count; ++ i) {
			memory_needed += vcIn.Size(order[i + in_offset]);
		}
		ValueCache& vcOut(*out[col]);
		vcOut.Prepare(memory_needed);
		for ( int i(0); i < count; ++ i) {
			void const* inB(vcIn.GetDataBytesPointer(order[i + in_offset]));
			int s(vcIn.Size(order[i + in_offset]));
			void* outB(vcOut.PreparedBuffer());
			::memcpy(outB, inB, s);
			vcOut.Commit(s);
		}
	}
	return;
}

