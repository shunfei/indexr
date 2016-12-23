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

#include "core/Query.h"
#include "core/MIUpdatingIterator.h"
#include "edition/vc/VirtualColumn.h"
#include "core/RoughMultiIndex.h"
#include "core/ParametrizedFilter.h"

using namespace std;

void ParameterizedFilter::ApplyDescriptor(int desc_number, _int64 limit)
// desc_number = -1 => switch off the rough part
{
	Descriptor& desc = descriptors[desc_number];
	if(desc.op == O_TRUE) {
		desc.done = true;
		return;
	}
	if(desc.op == O_FALSE) {
		mind->Empty();
		desc.done = true;
		return;
	}

	DimensionVector dims(mind->NoDimensions());
	desc.DimensionUsed(dims);
	mind->MarkInvolvedDimGroups(dims);		// create iterators on whole groups (important for multidimensional updatable iterators)
	int no_dims = dims.NoDimsUsed();
	if(no_dims == 0 && !desc.IsDeterministic())
		dims.SetAll();
	// Check the easy case (one-dim, parallelizable)
	int one_dim = -1;
	RSValue* rf = NULL;
	if(no_dims == 1) {
		for(int i = 0; i < mind->NoDimensions(); i++) {
			if(dims[i]) {
				if(mind->GetFilter(i))
					one_dim = i;		// exactly one filter (non-join or join with forgotten dims)
				break;
			}
		}
	}
	if(one_dim != -1)
		rf = rough_mind->GetLocalDescFilter(one_dim, desc_number, true);	// "true" here means that we demand an existing local rough filter

	RSValue cur_roughval;
	MIUpdatingIterator mit(mind, dims);

	_uint64 passed = 0;
	int pack = -1;
	while(mit.IsValid()) {
		if(limit != -1 && rf) {		// rf - not null if there is one dim only (otherwise packs make no sense)
			if(passed >= limit) {
				mit.ResetCurrentPack();
				mit.NextPackrow();
				continue;
			}
			if(mit.PackrowStarted()) {
				if(pack != -1)
					passed += mit.NoOnesUncommited(pack);
				pack = mit.GetCurPackrow(one_dim);
			}
		}

		if(rf && mit.GetCurPackrow(one_dim) >= 0)
			cur_roughval = rf[mit.GetCurPackrow(one_dim)];
		else
			cur_roughval = RS_SOME;
		if(cur_roughval == RS_NONE) {
			mit.ResetCurrentPack();
			mit.NextPackrow();
		} else if(cur_roughval == RS_ALL) {
			mit.NextPackrow();
		} else {// RS_SOME or RS_UNKNOWN
//			if(mit.IsSingleFilter()) {
//			if(0) {
//				sh_mit.RewindToRow(mit[one_dim]);
//				assert(sh_mit.IsValid() && mit.IsValid());
//				desc.EvaluatePack(sh_mit);
//				mit.NextPackrow();
//			} else
				desc.EvaluatePack(mit);
		}
		if(mind->m_conn->killed())
			throw KilledRCException();
	}
	mit.Commit();

	desc.done = true;
	if(one_dim != -1 && mind->GetFilter(one_dim)) {		// update global rough part
		Filter *f = mind->GetFilter(one_dim);
		for(int p = 0; p < rough_mind->NoPacks(one_dim); p++)
			if(f->IsEmpty(p)) {
				rough_mind->SetPackStatus(one_dim, p, RS_NONE);
			}
	}
	desc.UpdateVCStatistics();
	return;
}
