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

#include "Joiner.h"
#include "JoinerHash.h"
#include "JoinerSort.h"
#include "JoinerMapped.h"
#include "Query.h"
#include "edition/vc/VirtualColumn.h"
#include "vc/ConstColumn.h"

using namespace std;

TwoDimensionalJoiner::TwoDimensionalJoiner(	MultiIndex *_mind, // multi-index to be updated
							RoughMultiIndex *_rmind,
							TempTable *_table,
							JoinTips &_tips)
	:	tips(_tips), m_conn(ConnectionInfoOnTLS.Get())
{
	mind = _mind;
	rmind = _rmind;
	table = _table;
	why_failed = NOT_FAILED;
}

TwoDimensionalJoiner::~TwoDimensionalJoiner()
{
	// Note that mind and rmind are external pointers
}


JoinAlgType TwoDimensionalJoiner::ChooseJoinAlgorithm(MultiIndex& mind, Condition& cond)
{
	JoinAlgType join_alg = JTYPE_GENERAL;

	if(cond[0].IsType_JoinSimple() && cond[0].op == O_EQ) {
		if(cond.Size() == 1)
			join_alg = JTYPE_MAP;		// available types checked inside
		else
			join_alg = JTYPE_HASH;
	} else  {
		if(cond[0].IsType_JoinSimple() &&
		    (cond[0].op == O_MORE_EQ || cond[0].op == O_MORE || cond[0].op == O_LESS_EQ || cond[0].op == O_LESS))
			join_alg = JTYPE_SORT;
	}
	return join_alg; 
}

JoinAlgType TwoDimensionalJoiner::ChooseJoinAlgorithm(JoinFailure join_result, JoinAlgType prev_type, size_t desc_size)
{
	if(join_result == FAIL_1N_TOO_HARD)
		return JTYPE_HASH;
	if(join_result == FAIL_WRONG_SIDES)
		return prev_type;
	// the easiest strategy: in case of any problems, use general joiner
	return JTYPE_GENERAL;
}

TwoDimsJoinerAutoPtr TwoDimensionalJoiner::CreateJoiner(JoinAlgType join_alg_type, MultiIndex& mind, RoughMultiIndex& rmind,
														JoinTips &tips, TempTable *table)
{
	switch( join_alg_type ) {
		case JTYPE_HASH:
			return TwoDimsJoinerAutoPtr(new JoinerHash(		&mind, &rmind, table, tips));
		case JTYPE_SORT:
			return TwoDimsJoinerAutoPtr(new JoinerSort(	 	&mind, &rmind, table, tips));
		case JTYPE_MAP:
			return TwoDimsJoinerAutoPtr(new JoinerMapped( 	&mind, &rmind, table, tips));
		case JTYPE_GENERAL:
			return TwoDimsJoinerAutoPtr(new JoinerGeneral(	&mind, &rmind, table, tips));
		default:
			BHERROR("Join algorithm not implemented");
	}
    return TwoDimsJoinerAutoPtr(0);
}


JoinTips::JoinTips(MultiIndex& mind)
{
	limit = -1;
	count_only = false;
	for(int i = 0; i < mind.NoDimensions(); i++) {
		forget_now.push_back(mind.IsForgotten(i));
		distinct_only.push_back(false);
		null_only.push_back(false);
	}
}

JoinTips::JoinTips(const JoinTips& sec)
{
	limit = sec.limit;
	count_only = sec.count_only;
	forget_now = sec.forget_now;
	distinct_only = sec.distinct_only;
	null_only = sec.null_only;
}
