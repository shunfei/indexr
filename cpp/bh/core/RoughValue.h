#ifndef __ROUGH_QUERY_H__
#define __ROUGH_QUERY_H__

#include "common/CommonDefinitions.h"

class RoughValue
{
	_int64 min;
	_int64 max;
public:
	RoughValue(_int64 min = MINUS_INF_64, _int64 max = PLUS_INF_64);
	_int64 GetMin() const { return min; }
	_int64 GetMax() const { return max; }
};

#endif
