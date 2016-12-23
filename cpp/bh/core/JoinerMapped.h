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

#ifndef JOINERMAPPED_H_
#define JOINERMAPPED_H_

#include "Joiner.h"

class JoinerMapFunction;

class JoinerMapped : public TwoDimensionalJoiner
{
	/*
	* There are two sides of join:
	* - "traversed", which is analyzed by a function generator creating join function F : attr_values -> row_number,
	* - "matched", which is scanned to create resulting pairs: (n1, F(key(n1))).
	* Assumptions: only for the easiest cases, i.e. integers without encoding, one equality condition, "traversed" has unique values.
	*
	* Algorithm:
	* 1. determine all traversed and matched dimensions,
	* 2. create mapping function F by scanning the "traversed" dimension,
	* 3. scan the "matched" dimension, get the key values and calculate "traversed" row number using F,
	* 4. submit all the joined tuples as the result of join.
	*
	*/
public:
	JoinerMapped(MultiIndex *_mind, RoughMultiIndex *_rmind, TempTable *_table, JoinTips &_tips);

	void ExecuteJoinConditions(Condition& cond);

private:
	JoinerMapFunction* GenerateFunction(VirtualColumn *vc);
	void ExecuteJoin();

	/////////////// dimensions description ///////////////
	DimensionVector traversed_dims;	// the mask of dimension numbers of traversed dimensions to be put into the join result
	DimensionVector matched_dims;		// the mask of dimension numbers of matched dimensions to be put into the join result
};

////////////////////////////////////////////////////////////////////////////////////////////

class JoinerMapFunction				// a superclass for all implemented types of join mapping
{
public:
	virtual ~JoinerMapFunction()				{}
	virtual _int64 F(_int64 key_val) = 0;		// mapping itself: transform key value into row number
	virtual bool ImpossibleValues(_int64 local_min, _int64 local_max) = 0;	// true if the given interval is out of scope of the function
	virtual bool CertainValues(_int64 local_min, _int64 local_max) = 0;		// true if the given interval is fully covered by the function values (all rows will be joined)
	virtual _int64 DistinctVals() = 0;			// number of distinct vals found (upper approx.)
};

////////////////////////////////////////////////////////////////////////////////////////////

class OffsetMapFunction : public JoinerMapFunction
{
public:
	OffsetMapFunction() : key_status(NULL), key_table_min(0), key_min(PLUS_INF_64), key_max(MINUS_INF_64), 
		key_continuous_max(MINUS_INF_64), dist_vals_found(0) { memset(offset_table, 0, offset_table_size); }
	~OffsetMapFunction();

	bool Init(VirtualColumn *vc, MIIterator &mit);

	_int64 F(_int64 key_val);			// mapping itself: transform key value into row number
	bool ImpossibleValues(_int64 local_min, _int64 local_max)	{ return local_min > key_max || local_max < key_min; }
	bool CertainValues(_int64 local_min, _int64 local_max)		{ return local_min >= key_min && local_max <= key_continuous_max; }
	_int64 DistinctVals()				{ return dist_vals_found; }

	unsigned char* key_status;			// every key value has its own status here: 255 - not exists, other - row offset is stored in offset_table
	static const int offset_table_size = 255;
	_int64 offset_table [offset_table_size]; // a table of all offsets for this join

	_int64 key_table_min;				// approximated min (used as an offset in key_status table)
	_int64 key_min;						// real min encountered in the table (for ImpossibleValues)
	_int64 key_max;						// real max encountered in the table (for ImpossibleValues)
	_int64 key_continuous_max;			// all values between the real min and this value are guaranteed to exist

	_int64 dist_vals_found;
};


#endif