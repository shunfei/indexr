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

//////////////////////////////////////////////////////////////////////////////////////////////////
// This is a part of RCBase implementation concerned with the join conditions execution
//////////////////////////////////////////////////////////////////////////////////////////////////

#include "edition/local.h"
#include "Query.h"
#include "ParametrizedFilter.h"
#include "Joiner.h"
#include "edition/vc/VirtualColumn.h"
#include "TempTable.h"

using namespace std;

void ParameterizedFilter::PrepareJoiningStep(Condition& join_desc, Condition& desc, int desc_no, MultiIndex &mind)
{
	// join parameters based on the first joining condition
	DimensionVector dims1(mind.NoDimensions());
	desc[desc_no].DimensionUsed(dims1);
	mind.MarkInvolvedDimGroups(dims1);
	DimensionVector cur_outer_dim(desc[desc_no].right_dims);
	bool outer_present = !cur_outer_dim.IsEmpty();

	// add join (two-table) conditions first
	for(uint i = desc_no; i < desc.Size(); i++) {
		if(!desc[i].done && !desc[i].IsDelayed() && desc[i].IsType_JoinSimple()) {
			DimensionVector dims2(mind.NoDimensions());
			desc[i].DimensionUsed(dims2);
			if(desc[i].right_dims == cur_outer_dim &&
				(outer_present || dims1.Includes(dims2))) {
					// can be executed together if all dimensions of the other condition are present in the base one
					// or in case of outer join
					join_desc.AddDescriptor(desc[i]);
					desc[i].done = true;		// setting in advance, as we already copied the descriptor to be processed
			}
		}
	}

	// add the rest of conditions (e.g. one-dimensional outer conditions), which are not "done" yet
	for(uint i = desc_no; i < desc.Size(); i++) {
		if(!desc[i].done && !desc[i].IsDelayed()) {
			DimensionVector dims2(mind.NoDimensions());
			desc[i].DimensionUsed(dims2);
			if(desc[i].right_dims == cur_outer_dim &&
				(outer_present || dims1.Includes(dims2))) {
					// can be executed together if all dimensions of the other condition are present in the base one
					// or in case of outer join
					join_desc.AddDescriptor(desc[i]);
					desc[i].done = true;		// setting in advance, as we already copied the descriptor to be processed
			}
		}
	}
}

void ParameterizedFilter::DescriptorJoinOrdering()
{
	// calculate join weights
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done && descriptors[i].IsType_JoinSimple())
			descriptors[i].evaluation = EvaluateConditionJoinWeight(descriptors[i]);
	}

	// descriptor ordering by evaluation weight - again
	for(uint k = 0; k < descriptors.Size(); k++) {
		for(uint i = 0; i < descriptors.Size() - 1; i++) {
			uint j = i + 1;			// bubble sort
			if( descriptors[i].evaluation > descriptors[j].evaluation &&
				descriptors[j].right_dims == descriptors[i].right_dims) {	// do not change outer join order, if incompatible

					descriptors[i].swap(descriptors[j]);
			}
		}
	}
}

void ParameterizedFilter::UpdateJoinCondition(Condition& cond, JoinTips& tips)

{
	// Calculate joins (i.e. any condition using attributes from two dimensions)
	// as well as other conditions (incl. one-dim) flagged as "outer join"

	DimensionVector all_involved_dims(mind->NoDimensions());
	RoughSimplifyCondition(cond);
	for(uint i = 0; i < cond.Size(); i++)
		cond[i].DimensionUsed(all_involved_dims);
	bool is_outer = cond[0].IsOuter();

	// Apply conditions
	int conditions_used = cond.Size();
	JoinAlgType join_alg = JTYPE_NONE;
	TwoDimensionalJoiner::JoinFailure join_result = TwoDimensionalJoiner::NOT_FAILED;
	join_alg = TwoDimensionalJoiner::ChooseJoinAlgorithm(*mind, cond);

	/////////////////// Joining itself  ////////////////////
	do {
		TwoDimsJoinerAutoPtr joiner = TwoDimensionalJoiner::CreateJoiner(join_alg, *mind, *rough_mind,
																		 tips, table);
		if(join_result == TwoDimensionalJoiner::FAIL_WRONG_SIDES) // the previous result, if any
			joiner->ForceSwitchingSides();

		joiner->ExecuteJoinConditions(cond);
		join_result = joiner->WhyFailed();

		if(join_result != TwoDimensionalJoiner::NOT_FAILED)
			join_alg = TwoDimensionalJoiner::ChooseJoinAlgorithm(join_result, join_alg, cond.Size());
	} while(join_result != TwoDimensionalJoiner::NOT_FAILED);

	for(int i = 0; i < conditions_used; i++)
		cond.EraseFirst();			// erase the first condition (already used)
	mind->UpdateNoTuples();

	/////////////////// display results (the last alg.) ///////////
	DisplayJoinResults(all_involved_dims, join_alg, is_outer, conditions_used);
}

void ParameterizedFilter::DisplayJoinResults(DimensionVector &all_involved_dims, JoinAlgType join_performed,
							bool is_outer, int conditions_used)
{
	if(rccontrol.isOn()) {
		_int64 tuples_after_join = mind->NoTuples(all_involved_dims);

		char buf[30];
		if(conditions_used > 1)
			sprintf(buf, "%d cond. ", conditions_used);
		else
			strcpy(buf, "");
		if(is_outer)
			strcat( buf, "outer ");
		else
			strcat( buf, "inner ");

		char buf_dims[500];
		buf_dims[0] = '\0';
		bool first = true;
		for(int i = 0; i < mind->NoDimensions(); i++)
			if(all_involved_dims[i]) {
				if(first) {
					if(all_involved_dims.NoDimsUsed() == 1)
						sprintf(buf_dims, "...-%d", i);
					else
						sprintf(buf_dims, "%d", i);
					first = false;
				} else
					sprintf(buf_dims + strlen(buf_dims), "-%d", i);
			}

		rccontrol.lock(mind->m_conn->GetThreadID())
			<< "Tuples after " << buf << "join " << buf_dims
			<<	(join_performed == JTYPE_SORT ?		" [sort]: " :
				(join_performed == JTYPE_MAP ?		" [map]:  " :
				(join_performed == JTYPE_HASH ?		" [hash]: " :
				(join_performed == JTYPE_GENERAL ?	" [loop]: " :
													" [????]: " ))))
			<< tuples_after_join
			<< " \t" << mind->Display()
			<< unlock;
	}
}

void ParameterizedFilter::RoughSimplifyCondition(Condition& cond)
{
	for(uint i = 0; i < cond.Size(); i++) {
		Descriptor& desc = cond[i];
		if(desc.op == O_FALSE || desc.op == O_TRUE || !desc.IsType_OrTree())
			continue;
		DimensionVector all_dims(mind->NoDimensions());		// "false" for all dimensions
		desc.DimensionUsed(all_dims);
		desc.ClearRoughValues();
		MIIterator mit(mind, all_dims);
		while(mit.IsValid()) {
			desc.RoughAccumulate(mit);
			mit.NextPackrow();
		}
		desc.SimplifyAfterRoughAccumulate();
	}
}
