/* Copyright (C)  2005-2009 Infobright Inc.

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

#ifndef AGGREGATIONALGORITHM_H_
#define AGGREGATIONALGORITHM_H_

#include "TempTable.h"
#include "GroupByWrapper.h"
#include "Query.h"

class AggregationAlgorithm
{
public:
	AggregationAlgorithm(TempTable *tt) :	t(tt), m_conn(&(tt->m_conn)), mind(tt->GetMultiIndexP()), 
											factor(1), packrows_found(0) 
	{}

	void Aggregate(bool just_distinct, _int64& limit, _int64& offset, ResultSender* sender);

	bool AggregateRough(GroupByWrapper &gbw, MIIterator &mit, bool &packrow_done, bool &part_omitted, bool &ag_not_changeabe, bool &stop_all, _int64 &uniform_pos, _int64 rows_in_pack, _int64 local_factor, int just_one_aggr = -1);
	void MultiDimensionalGroupByScan(GroupByWrapper &gbw, _int64& limit, _int64& offset, std::map<int,std::vector<PackOrderer::OrderingInfo> > &oi, ResultSender* sender, bool limit_less_than_no_groups);
	void MultiDimensionalDistinctScan(GroupByWrapper &gbw, MIIterator &mit);
	void AggregateFillOutput(GroupByWrapper &gbw, _int64 gt_pos, _int64 &omit_by_offset);

	// Return code for AggregatePackrow: 0 - success, 1 - stop aggregation (finished), 5 - pack already aggregated (skip)
	int AggregatePackrow(GroupByWrapper &gbw, MIIterator *mit, _int64 cur_tuple);

	bool ParallelAllowed() {return t->GetQuery()->GetParallelAggr();}

private:
	// just pointers:
	TempTable *t;
	ConnectionInfo*	m_conn;
	MultiIndex *mind;

	_int64 factor;				// multiindex factor - how many actual rows is processed by one iterator step

	// Some statistics for display:
	_int64 packrows_found;			// all packrows, except these completely omitted (as aggregated before)
};

class AggregationWorker
{
public:
	AggregationWorker(GroupByWrapper &gbw, AggregationAlgorithm* _aa) : gb_main(&gbw), aa(_aa) {}

	bool MayBeParallel(MIIterator &mit)		{ return false; }
	void Init(MIIterator &mit)				{ }
	// Return code for AggregatePackrow: 0 - success, 1 - stop aggregation (finished), 2 - killed, 3 - overflow, 4 - other error, 5 - pack already aggregated (skip)
	int AggregatePackrow(MIInpackIterator &lmit, _int64 cur_tuple) { return aa->AggregatePackrow(*gb_main, &lmit, cur_tuple); }
	void Commit(bool do_merge = true)		{ gb_main->CommitResets(); }
	void ReevaluateNumberOfThreads(MIIterator &mit) {}
	int ThreadsUsed()						{ return 1; }
	void Barrier()	{}

protected:
	GroupByWrapper *gb_main;
	AggregationAlgorithm* aa;
};

#endif
