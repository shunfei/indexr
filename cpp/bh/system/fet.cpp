/*
 * fet.cpp
 *
 *  Created on: Nov 22, 2010
 *      Author: michal
 */

#include "fet.h"

#ifdef FUNCTIONS_EXECUTION_TIMES



void NotifyDataPackLoad(const PackCoordinate& coord)
{
	static IBMutex mutex;
	IBGuard guard(mutex);
	count_distinct_dp_loads.insert(coord);
}

void NotifyDataPackDecompression(const PackCoordinate& coord)
{
	static IBMutex mutex;
	IBGuard guard(mutex);
	count_distinct_dp_decompressions.insert(coord);
}

#endif
