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

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// This is a part of RCBase implementation concerned with the query optimization and rough set mechanisms
/////////////////////////////////////////////////////////////////////////////////////////////////////////

#include "edition/local.h"
#include "Query.h"
#include "ParametrizedFilter.h"
#include "RoughMultiIndex.h"
#include "vc/SingleColumn.h"
#include "core/PackOrderer.h"
#include "vc/InSetColumn.h"

using namespace std;

double ParameterizedFilter::EvaluateConditionNonJoinWeight(Descriptor &d, bool for_or)
{
	// Interpretation of weight:
	// an approximation of logarithm of answer size
	// (in case of for_or: an approximation of (full_table - answer) size)
	// 0 -> time is very short (constant).
	// high weight -> schedule this query to be executed later
	double eval = 0.0;
	_uint64 no_distinct, no_distinct2;
	_uint64 answer_size;

	if(d.IsTrue() || d.IsFalse())
		eval = 0;				// constant time
	else if(d.IsType_AttrValOrAttrValVal()) {		// typical condition: attr=val
		if(!d.encoded) {
			return log(1 + double(d.attr.vc->NoTuples())) + 5;			// +5 as a penalty for complex expression
		}
		SingleColumn* col = static_cast<SingleColumn*>(d.attr.vc);
		answer_size = col->ApproxAnswerSize(d);
		if(for_or)
			answer_size = d.attr.vc->NoTuples() - answer_size;
		_int64 no_in_values = 1;
		if(d.op == O_IN || d.op == O_NOT_IN) {
			MultiValColumn* iscol = static_cast<MultiValColumn*>(d.val1.vc);
			no_in_values = iscol->NoValues(NULL);
		}
		eval = log(1 + double(answer_size));		// approximate size of the result
		if(no_in_values > 1)
			eval += log(double(no_in_values)) * 0.5;	// INs are potentially slower (many comparisons needed)
		if(col->Type().IsString() && !col->Type().IsLookup())
			eval += 0.5;							// strings are slower
		if(col->Type().IsFloat())
			eval += 0.1;							// floats are slower
		if(d.op == O_LIKE || d.op == O_NOT_LIKE)
			eval += 0.2;							// these operators need more work
	} else if(d.IsType_AttrAttr()) {				// attr=attr on the same table
		_uint64 no_obj = d.attr.vc->NoTuples();		// changed to uint64 to prevent negative logarithm for NULL_VALUE_64
		if(!d.encoded)
			return log(1 + double(2 * no_obj)) + 5;		// +5 as a penalty for complex expression
		else if(d.op == O_EQ) {
			no_distinct = d.attr.vc->GetApproxDistVals(false);
			if(no_distinct == 0)
				no_distinct = 1;
			no_distinct2 = d.val1.vc->GetApproxDistVals(false);
			if(no_distinct2 == 0)
				no_distinct2 = 1;
			if(no_distinct2 > no_distinct)
				no_distinct = no_distinct2;					// find the attribute with smaller abstract classes
			if(for_or)
				eval = log(1 + (no_obj - double(no_obj) / no_distinct));
			else
				eval = log(1 + double(no_obj) / no_distinct);	// size of the smaller abstract class
		} else {
			eval = log(1 + double(no_obj) / 2);	// other operators filter potentially a half of objects
		}
		eval += 1;			// add to compensate opening two packs
	} else if(d.IsType_OrTree() && !d.IsType_Join()) {
		eval = d.tree->root->EvaluateConditionWeight(this, for_or);
	} else {	// expressions and other types, incl. joins (to be calculated separately)
		if(d.IsType_IBExpression())
			return log(1 + double(d.attr.vc->NoTuples())) + 2;		// +2 as a penalty for IB complex expression
		eval = 99999;
	}
	return eval;
}

double ParameterizedFilter::EvaluateConditionJoinWeight(Descriptor &d)
{
	double eval = 0.0;
	int dim1 = d.attr.vc->GetDim();
	int dim2 = d.val1.vc->GetDim();
	/*
	if(dim1 == 0 && dim2 == 2) return 1.11;
	if(dim1 == 1 && dim2 == 6) return 1.12;
	if(dim1 == 1 && dim2 == 2) return 1.13;
	if(dim1 == 5 && dim2 == 7) return 1.14;
	if(dim1 == 4 && dim2 == 5) return 1.15;
	if(dim1 == 3 && dim2 == 4) return 1.16;
	*/
	if(dim1 > -1 && dim2 > -1) {
		_uint64 no_obj1 = mind->OrigSize(dim1);		// changed to uint64 to prevent negative logarithm for NULL_VALUE_64
		_uint64 no_obj2 = mind->OrigSize(dim2);
		if(mind->GetFilter(dim1))
			no_obj1 = mind->DimSize(dim1);
		if(mind->GetFilter(dim2))
			no_obj2 = mind->DimSize(dim2);
		if(no_obj1 == 0 || no_obj2 == 0)
			return 1;

		_uint64 no_distinct1, no_distinct2;
		double r_select1 = 0.5 + 0.5 * d.attr.vc->RoughSelectivity(); 	// 1 if no rough exclusions, close to 0.5 if good rough performance
		double r_select2 = 0.5 + 0.5 * d.val1.vc->RoughSelectivity();
		double c_select1 = double(no_obj1) / mind->OrigSize(dim1);	// condition selectivity (assuming 1:n)
		double c_select2 = double(no_obj2) / mind->OrigSize(dim2);
		double bigger_table_size = (double)max(no_obj1, no_obj2);

		if(d.op == O_EQ) {
			no_distinct1 = d.attr.vc->GetApproxDistVals(false);
			no_distinct2 = d.val1.vc->GetApproxDistVals(false);
			if(no_distinct1 >= 0.99 * mind->DimSize(dim1)) {	// potentially 1:n join
				eval = double(no_obj2) * r_select2 * c_select1
					 + double(no_obj1) / 100;
				eval = log(1 + eval);		// result will be of size of the second table filtered by the value of the first one
				if(no_obj2 > no_obj1 && mind->DimSize(dim1) <= 65536)	// bonus for potential small-large join
					eval -= 1;
			} else if(no_distinct2 >= 0.99 * mind->DimSize(dim2)) { // potentially n:1 join
				eval = double(no_obj1) * r_select1 * c_select2
					 + double(no_obj2) / 100;
				eval = log(1 + eval);		// result will be of size of the first table filtered by the value of the second one
				if(no_obj1 > no_obj2 && mind->DimSize(dim2) <= 65536)	// bonus for potential small-large join
					eval -= 1;
			} else {
				// approximated size: (no_of_classes) * (ave_class_size1 * ave_class_size2) = no_of_classes * (no_obj1 / no_dist1) * (no_obj2 / no_dist2)
				//                    where no_of_classes = min(no_dist1,no_dist2) , because they must have common values
				no_distinct1 = (_uint64)(no_distinct1 * double(no_obj1) / mind->DimSize(dim1));
				no_distinct2 = (_uint64)(no_distinct2 * double(no_obj2) / mind->DimSize(dim2));
				_int64 no_of_classes = min(no_distinct1, no_distinct2);

				eval = log(double(no_of_classes < 1 ? 1 : no_of_classes))	// instead of multiplications
					+ (log(1 + double(no_obj1)) - log(double(no_distinct1)))
					+ (log(1 + double(no_obj2)) - log(double(no_distinct2)));
			}
		} else
			eval = log(1 + double(no_obj1)) + log(1 + double(no_obj2));			// these join operators generate potentially large result

		eval += (eval - log(1 + bigger_table_size)) * 2;	// bonus for selective strength
		if(!mind->IsUsedInOutput(dim1))						// bonus for not used (cumulative, if both are not used)
			eval *= 0.75;
		if(!mind->IsUsedInOutput(dim2))
			eval *= 0.75;

		eval += 20;
	} else	// other types of joins
		eval = 99999;
	return eval;
}


///////////////////////////////////////////////////////////////////////

bool ParameterizedFilter::RoughUpdateMultiIndex()
{
	MEASURE_FET("ParameterizedFilter::RoughUpdateMultiIndex(...)");
	bool is_nonempty = true;
	bool false_desc = false;
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done && descriptors[i].IsInner() && (descriptors[i].IsTrue()))
			descriptors[i].done = true;
		if(descriptors[i].IsFalse() && descriptors[i].IsInner()) {
			rough_mind->MakeDimensionEmpty();
			false_desc = true;
		}
	}
	// one-dimensional conditions
	if(!false_desc) {
		// init by previous values of mind (if any nontrivial)
		for(int i = 0; i < mind->NoDimensions(); i++) {
			Filter* loc_f = mind->GetFilter(i);
			rough_mind->UpdateGlobalRoughFilter(i, loc_f);	// if the filter is nontrivial, then copy pack status
		}
		for(uint i = 0; i < descriptors.Size(); i++) {
			if(!descriptors[i].done && !descriptors[i].IsDelayed() && descriptors[i].IsInner() &&
				descriptors[i].GetJoinType() == DT_NON_JOIN) {
					DimensionVector dims(mind->NoDimensions());
					descriptors[i].DimensionUsed(dims);
					int dim = dims.GetOneDim();
					if(dim == -1)
						continue;
					RSValue* rf = rough_mind->GetLocalDescFilter(dim, i);	// rough filter for a descriptor
					descriptors[i].ClearRoughValues();						// clear accumulated rough values for descriptor
					MIIterator mit(mind, dim);
					while(mit.IsValid()) {
						int p = mit.GetCurPackrow(dim);
						if(p >= 0 && rf[p] != RS_NONE)
							rf[p] = descriptors[i].EvaluateRoughlyPack(mit);		// rough values are also accumulated inside
						mit.NextPackrow();
						if(mind->m_conn->killed()) throw KilledRCException();
					}
					bool this_nonempty = rough_mind->UpdateGlobalRoughFilter(dim, i); // update the filter using local information
					is_nonempty = (is_nonempty && this_nonempty);
					descriptors[i].UpdateVCStatistics();
					descriptors[i].SimplifyAfterRoughAccumulate();					// simplify tree if there is a roughly trivial leaf
			}
		}
	}

	// Recalculate all multidimensional dependencies only if there are 1-dim descriptors which can benefit
	// from the projection
	if(DimsWith1dimFilters())
		RoughMakeProjections();

	///////////////////////////////////////////////////////////////////////////////////
	// Displaying statistics

	if(rccontrol.isOn()) {
		int pack_full = 0, pack_some = 0, pack_all = 0;
		rccontrol.lock(mind->m_conn->GetThreadID()) << "Packs/packrows after KN evaluation:" << unlock;
		for(int dim = 0; dim < rough_mind->NoDimensions(); dim++) {
			pack_full = 0;
			pack_some = 0;
			pack_all  = rough_mind->NoPacks(dim);
			for(int b = 0; b < pack_all; b++) {
				if(rough_mind->GetPackStatus(dim, b) == RS_ALL)		
					pack_full++;
				else if(rough_mind->GetPackStatus(dim, b) != RS_NONE)	
					pack_some++;
			}
			rccontrol.lock(mind->m_conn->GetThreadID()) << "(t" << dim << ") Pckrows: " << pack_all << ", susp. " << pack_some 
												<< " (" << pack_all - (pack_full + pack_some) << " empty " << pack_full 
												<< " full). Conditions: " << rough_mind->NoConditions(dim) << unlock;
		}
	}
	return is_nonempty;
}

bool ParameterizedFilter::PropagateRoughToMind()
{
	MEASURE_FET("ParameterizedFilter::PropagateRoughToMind(...)");
	bool is_nonempty = true;
	for(int i = 0; i < rough_mind->NoDimensions(); i++) {		// update classical multiindex
		Filter *f = mind->GetUpdatableFilter(i);
		if(f) {
			for(int b = 0; b < rough_mind->NoPacks(i); b++) {
				if(rough_mind->GetPackStatus(i, b) == RS_NONE)
					f->ResetBlock(b);
			}
			if(f->IsEmpty())
				is_nonempty = false;
		}
	}
	return is_nonempty;
}

void ParameterizedFilter::RoughUpdateJoins()
{
	MEASURE_FET("ParameterizedFilter::RoughUpdateJoins(...)");
	bool join_or_delayed_present = false;
	set<int> dims_to_be_suspect;
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done && descriptors[i].IsDelayed()) {
			join_or_delayed_present = true;
			break;
		}
		if(!descriptors[i].done && descriptors[i].IsOuter())  {
			for(int j = 0; j < descriptors[i].right_dims.Size(); j++)
				if(descriptors[i].right_dims[j])
					dims_to_be_suspect.insert(j);
		}
		if(!descriptors[i].done&& descriptors[i].IsType_Join() && descriptors[i].IsInner())  {
			descriptors[i].DimensionUsed(descriptors[i].left_dims);
			for(int j = 0; j < descriptors[i].left_dims.Size(); j++)
				if(descriptors[i].left_dims[j])
					dims_to_be_suspect.insert(j);
		}
	}
	if(join_or_delayed_present)
		rough_mind->MakeDimensionSuspect();			// no RS_ALL packs
	else if(dims_to_be_suspect.size()) {
		set<int>::iterator it;
		for(it = dims_to_be_suspect.begin(); it != dims_to_be_suspect.end(); it++)
			rough_mind->MakeDimensionSuspect(*it); // no RS_ALL packs
	}
}

void ParameterizedFilter::RoughMakeProjections()
{
	for(int dim = 0; dim < mind->NoDimensions(); dim++)
		RoughMakeProjections(dim, false);

	rough_mind->UpdateReducedDimension();
}

void ParameterizedFilter::RoughMakeProjections(int to_dim, bool update_reduced)
{
	MEASURE_FET("ParameterizedFilter::RoughMakeProjections(...)");
	int total_excluded = 0;
	vector<int> dims_reduced = rough_mind->GetReducedDimensions();
	vector<int>::const_iterator dim1;
	for(dim1 = dims_reduced.begin(); dim1 != dims_reduced.end(); ++dim1) {
		if(*dim1 == to_dim)
			continue;
		// find all descriptors which may potentially influence other dimensions
		vector<Descriptor> local_desc;
		for(uint i = 0; i < descriptors.Size(); i++) {
			if(descriptors[i].done || descriptors[i].IsDelayed())
				continue;
			if(descriptors[i].IsOuter())
				return;					// do not make any projections in case of outer joins present
			if(!descriptors[i].attr.vc || descriptors[i].attr.vc->GetDim() == -1 || !descriptors[i].val1.vc || descriptors[i].val1.vc->GetDim() == -1)
				continue; //only SingleColumns processed here
			DimensionVector dims(mind->NoDimensions());
			descriptors[i].DimensionUsed(dims);
			if(descriptors[i].attr.vc && dims[*dim1] && dims[to_dim] && dims.NoDimsUsed() == 2)
				local_desc.push_back(descriptors[i]);
		}

		// make projection to another dimension
		for(int i = 0; i < local_desc.size(); i++) {
			// find the other dimension
			Descriptor& ld = local_desc[i];
			DimensionVector dims(mind->NoDimensions());
			ld.DimensionUsed(dims);
			PackOrderer po;
			VirtualColumn* matched_vc;
			MIDummyIterator local_mit(mind);
			if(*dim1 == ld.attr.vc->GetDim()) {
				po.Init(ld.attr.vc, PackOrderer::RangeSimilarity, rough_mind->GetRSValueTable(ld.attr.vc->GetDim()));
				matched_vc = ld.val1.vc;
			} else {
				po.Init(ld.val1.vc, PackOrderer::RangeSimilarity, rough_mind->GetRSValueTable(ld.val1.vc->GetDim()));
				matched_vc = ld.attr.vc;
			}
			// for each dim2 pack, check whether it may be joined with anything nonempty on dim1
			for(int p2 = 0; p2 < rough_mind->NoPacks(to_dim); p2++) {
				if(rough_mind->GetPackStatus(to_dim, p2) != RS_NONE) {
					bool pack_possible = false;
					local_mit.SetPack(to_dim, p2);

//					if(po.IsValid())
						po.RewindToMatch(matched_vc, local_mit);
//					else
//						po.RewindToCurrent();

					while(po.IsValid()) {
//						for(int p1 = 0; p1 < rmind.NoPacks(dim1); p1++)
//							if(rmind.GetPackStatus(dim1, p1) != RS_NONE) {
						if(rough_mind->GetPackStatus(*dim1, po.Current()) != RS_NONE) {
							local_mit.SetPack(*dim1, po.Current());	// set a dummy position, just for transferring pack number
							if(ld.attr.vc->RoughCheck(local_mit, ld) != RS_NONE) {
								pack_possible = true;
								break;
							}
						}
						++po;
					}
					if(!pack_possible) {
						rough_mind->SetPackStatus(to_dim, p2, RS_NONE);
						total_excluded++;
					}
				}
			}
		}
		if(update_reduced)
			rough_mind->UpdateReducedDimension(*dim1);
		local_desc.clear();
	}
	if(total_excluded > 0) {
		rccontrol.lock(mind->m_conn->GetThreadID()) << "Packrows excluded by rough multidimensional projections: " << total_excluded << unlock;
		rough_mind->UpdateLocalRoughFilters(to_dim);

		for(dim1 = dims_reduced.begin(); dim1 != dims_reduced.end(); ++dim1) {
			Filter *f = mind->GetUpdatableFilter(*dim1);
			if(f) {
				for(int b = 0; b < rough_mind->NoPacks(*dim1); b++) {
					if(rough_mind->GetPackStatus(*dim1, b) == RS_NONE)
						f->ResetBlock(b);
				}
			}
		}
		mind->UpdateNoTuples();
	}
}

bool ParameterizedFilter::DimsWith1dimFilters()
{
	DimensionVector dv1(mind->NoDimensions()), dv2(mind->NoDimensions());
	bool first = true;
	for(uint i = 0; i < descriptors.Size(); i++) {
		if(!descriptors[i].done && descriptors[i].IsInner() && !descriptors[i].IsType_Join() && !descriptors[i].IsDelayed()) {
			if(first) {
				descriptors[i].DimensionUsed(dv1);
				first = false;
			} else {
				descriptors[i].DimensionUsed(dv2);
				if(!dv1.Intersects(dv2))
					return true;
			}
		}
	}
	return false;
}
