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

#ifndef GROUPBYWRAPPER_H_
#define GROUPBYWRAPPER_H_

#include "GroupTable.h"
#include "TempTable.h"
#include "PackGuardian.h"
#include "edition/vc/VirtualColumn.h"
#include "GroupDistinctCache.h"
#include "system/RCSystem.h"
#include "system/ConnectionInfo.h"
#include "core/tools.h"

class DistinctWrapper						// a class for remembering the status of distinct aggregations
{											// (rows omitted)
public:
	DistinctWrapper();
	~DistinctWrapper();

	void Initialize(int _no_attr);
	void InitTuples(_int64 _no_obj, GroupTable *gt);	// gt is needed for column width
	void DeclareAsDistinct(int attr);

	bool AnyOmitted();

	void SetAsOmitted(int attr, _int64 obj)				{ f[attr]->Set(obj); }
	Filter *OmittedFilter(int attr)						{ return f[attr]; }

	GroupDistinctCache *gd_cache;

private:
	int no_attr;
	_int64 no_obj;

	Filter **f;
	bool *is_dist;
};

///////////////////////////////////////////////////////////////////////////////////////////

class GroupByWrapper
{
public:
	GroupByWrapper(int attrs_size, bool just_distinct, ConnectionInfo& conn);
	GroupByWrapper(GroupByWrapper &sec);
	~GroupByWrapper();

	void AddGroupingColumn( int attr_no, int orig_no, TempTable::Attr &a, ushort max_size = 0 );
	void AddAggregatedColumn( 	int orig_no, TempTable::Attr &a,
								_int64 max_no_vals = 0,
								_int64 min_v = MINUS_INF_64, _int64 max_v = PLUS_INF_64,
								ushort max_size = 0);

	// Optimizations:
	bool AddPackIfUniform(int attr_no, MIIterator &mit);
	void AddAllGroupingConstants(MIIterator &mit);			// set buffers for all constants
	void AddAllAggregatedConstants(MIIterator &mit);		// set buffers for all constants in aggregations
	void AddAllCountStar(_int64 row, MIIterator &mit, _int64 val);		// set all count(*) values to val, or to 0 in case of nulls
	bool AggregatePackInOneGroup(int attr_no, MIIterator &mit, _int64 uniform_pos, _int64 rows_in_pack, _int64 factor);
	bool AttrMayBeUpdatedByPack(int i, MIIterator &mit);
	bool PackWillNotUpdateAggregation(int i, MIIterator &mit);		// ...because min/max worse than already found
	bool DataWillNotUpdateAggregation(int i);						// as above, for the whole dataset
	void InvalidateAggregationStatistics()					{ gt->InvalidateAggregationStatistics(); }

	void DefineAsEquivalent(int i,int j)					{ attr_mapping[i] = attr_mapping[j]; }

	void PutGroupingValue(int gr_a, MIIterator &mit)			{ gt->PutGroupingValue(gr_a, mit); }
	bool PutAggregatedNull(int gr_a, _int64 pos);
	bool PutAggregatedValue(int gr_a, _int64 pos, MIIterator &mit, _int64 factor = 1);
	// return value: true if value checked in, false if not (DISTINCT buffer overflow)
	// functionalities around DISTINCT
	bool PutAggregatedValueForCount(int gr_a, _int64 pos, _int64 factor);	// a simplified version for counting only
	bool PutCachedValue(int gr_a);							// current value from distinct cache
	bool CacheValid(int gr_a);								// true if there is a value cached for current row
	void UpdateDistinctCaches();							// take into account which values are already counted
	void OmitInCache(int attr, _int64 obj_to_omit);
	void DistinctlyOmitted(int attr, _int64 obj);
	bool AnyOmittedByDistinct()								{ return distinct_watch.AnyOmitted(); }
	_int64 ApproxDistinctVals(int gr_a, MultiIndex *mind = NULL);

	int NoAttr() 						{ return no_attr; }
	int NoGroupingAttr() 				{ return no_grouping_attr; }
	bool DistinctAggr(int col)			{ return gt->AttrDistinct(col); }
	void Initialize(_int64 upper_approx_of_groups, bool parallel_allowed);
	_int64 NoGroups()					{ return no_groups; }
	void AddGroup()						{ no_groups++; }
	void ClearNoGroups()				{ no_groups = 0; }
	_int64 UpperApproxOfGroups()		{ return gt->UpperApproxOfGroups(); }

	// a position in the current GroupTable, row==NULL_VALUE_64 if not found
	bool FindCurrentRow(_int64 &row) { return gt->FindCurrentRow(row); }
	_int64 GetValue64(int col, _int64 row)	{ return gt->GetValue64(col, row);	}

	// iteration through resulting rows
	void RewindRows()					{ gt->RewindRows(); }
	_int64 GetCurrentRow()				{ return gt->GetCurrentRow(); }
	void NextRow()						{ gt->NextRow(); }
	bool RowValid()						{ return gt->RowValid(); }

	RCBString GetValueT(int col, _int64 row);

	void Clear();								// reset all contents of the grouping table and statistics
	void ClearUsed()							{ gt->ClearUsed(); }
	void ClearDistinctBuffers();				// reset distinct buffers and distinct cache
	void RewindDistinctBuffers();				// reset distinct buffers, rewind distinct cache to use it contents
	void SetDistinctTuples(_int64 no_tuples)	{ distinct_watch.InitTuples(no_tuples, gt); }
	bool IsFull() 								{ return gt->IsFull(); }
	void SetAsFull() 							{ gt->SetAsFull(); }
	bool NoMoreGroups()							{ return no_more_groups; }
	void SetNoMoreGroups()						{ no_more_groups = true; }
	void FillDimsUsed(DimensionVector &dims);		// set true on all dimensions used
	int AttrMapping(int a)						{ return attr_mapping[a]; }
	bool IsCountOnly(int a = -1);				// true, if an attribute is count(*)/count(const), or if there is no columns except constants and count (when no attr specified)
	bool IsCountDistinctOnly()					{ return 	no_grouping_attr == 0 &&
															no_aggregated_attr == 1 &&
															gt->AttrOper(0) == GT_COUNT_NOT_NULL &&
															gt->AttrDistinct(0); }
	bool MayBeParallel()						{ return gt->MayBeParallel(); }
	bool IsOnePass()							{ return gt->IsOnePass(); }
	int MemoryBlocksLeft()						{ return gt->MemoryBlocksLeft(); }	// no place left for more packs (soft limit)
	void Merge(GroupByWrapper &sec);
	void SetThNo(int _n)						{ th_no = _n; }

	////////////////// A filter of rows to be aggregated

	void InitTupleLeft(_int64 n);
	bool AnyTuplesLeft(_int64 from, _int64 to);
	bool AnyTuplesLeft()						{ return (tuple_left != NULL) && !tuple_left->IsEmpty(); }
	_int64 TuplesLeftBetween(_int64 from, _int64 to);
	void CommitResets()							{ if(tuple_left) tuple_left->Commit(); }
	void TuplesResetAll()						{ if(tuple_left) tuple_left->Reset(); }
	void TuplesResetBetween(_int64 from, _int64 to)		{ if(tuple_left) tuple_left->ResetBetween(from, to); }
	void TuplesReset(_int64 pos)				{ if(tuple_left) tuple_left->ResetDelayed(pos); }
	bool TuplesGet(_int64 pos)					{ return (tuple_left == NULL) || tuple_left->Get(pos); }
	_int64 TuplesNoOnes()						{ return (tuple_left == NULL ? 0 : tuple_left->NoOnes()); }

	////////////////// Locking packs etc.

	void LockPack(int i, MIIterator &mit);
	void ResetPackrow();
	bool ColumnNotOmitted(int a)				{ return pack_not_omitted[a]; }
	void OmitColumnForPackrow(int a)			{ pack_not_omitted[a] = false; }
	VirtualColumn* SourceColumn(int a)			{ return virt_col[a]; }

	VirtualColumn* GetColumn(int i) {assert(i < no_attr); return virt_col[i];}
	/////////////////
	DistinctWrapper	distinct_watch;

	// Statistics:
	_int64 packrows_omitted;
	_int64 packrows_part_omitted;
	ConnectionInfo&	m_conn;

private:
	enum GBInputMode {  GBIMODE_NOT_SET,
						GBIMODE_NO_VALUE,							// e.g. count(*)
						GBIMODE_AS_INT64, 
						GBIMODE_AS_TEXT};

	int				attrs_size;
	int				no_grouping_attr;
	int				no_aggregated_attr;
	int				no_attr;
	bool			no_more_groups;			// true if we know that all groups are already found

	_int64			no_groups;				// number of groups found

	VirtualColumn**	virt_col;				// a table of grouping/aggregated columns
	GBInputMode*	input_mode;				// text/numerical/no_value

	bool*			is_lookup;				// is the original column a lookup one?

	int*			attr_mapping;			// output attr[j] <-> gt group[attr_mapping[j]]
	bool*			pack_not_omitted;		// pack status for columns in current packrow
	_int64*			dist_vals;				// distinct values for column - upper approximation

	GroupTable*		gt;
	int				th_no;

	Filter*			tuple_left;				// a mask of all rows still to be aggregated
};

#endif /*GROUPBYWRAPPER_H_*/
