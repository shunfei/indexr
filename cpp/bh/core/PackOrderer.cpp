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

#include "core/PackOrderer.h"
#include "core/MIIterator.h"

using namespace std;

float PackOrderer::basic_sorted_percentage = 10.0;	// ordering on just sorted_percentage% of packs

PackOrderer::PackOrderer()
{
	curvc= 0;
	ncols = 0;
	packs.push_back(vector<PackPair>());
	otype.push_back(NotSpecified);
	packs_ordered_up_to = 0;
	packs_passed = 0;
}

PackOrderer::PackOrderer(const PackOrderer& p)
{

	if(visited.get())
		visited = auto_ptr<Filter>(new Filter(*p.visited.get()));
	packs = p.packs;
	curndx = p.curndx;
	prevndx = p.prevndx;
	curvc = p.curvc;
	ncols = p.ncols;
	natural_order = p.natural_order;
	dimsize = p.dimsize;
	lastly_left = p.lastly_left;
	mmtype = p.mmtype;
	otype = p.otype;
	packs_ordered_up_to = p.packs_ordered_up_to;
	packs_passed = p.packs_passed;
}

//PackOrderer::PackOrderer(vector<VirtualColumn*> vcs, vector<OrderType> orders, RSValue* r_filter)
//
//{
//	Init(vcs, orders, r_filter);
//}

PackOrderer::PackOrderer(VirtualColumn* vc, RSValue* r_filter, OrderType order)
{
	Init(vc, order, r_filter);
}

PackOrderer::CompIntLess PackOrderer::comp_int_less;
PackOrderer::CompIntGreater PackOrderer::comp_int_greater;
PackOrderer::CompPackNumber PackOrderer::comp_pack_number;

bool PackOrderer::Init(vector<VirtualColumn*> vcs, vector<OrderType> orders, int no_unsorted_cols, bool one_group)
{
	packs_ordered_up_to = BH_INT_MAX;
	if(Initialized())
		return false;

	if(vcs.size() == 0)
		return false;

	packs.clear();
	otype.clear();

	curvc = 0;
	int dim = vcs[0]->GetDim();
	assert(dim != -1); //works only for vcolumns based on a single table

	dimsize = int((vcs[0]->GetMultiIndex()->OrigSize(dim) + 0xffff) >> 16);
	if(dimsize < 2 || !vcs[0]->GetMultiIndex()->IsOrderable(dim))
		return false;
	if(vcs.size() > 1)
		visited = auto_ptr<Filter>(new Filter(dimsize));

	struct OrderStat os(no_unsorted_cols, int(vcs.size()));
	bool covering_found = false;
	if(one_group) {
		if(vcs.size() == 1 && no_unsorted_cols == 0 && orders[0] == Covering) { // one column only
			basic_sorted_percentage = 10.0;
			covering_found = true;
		} else {
			int weight_sum = 0;
			for(int i = 0; i < orders.size(); i++) {
				if(orders[i] == Covering) {
					weight_sum += 2;
					covering_found = true;
				} else
					weight_sum++;
			}
			basic_sorted_percentage = weight_sum / float(2 * (vcs.size() + no_unsorted_cols)) * 15;
		}
	} 
	if(!covering_found) {
		if(vcs.size() == 1 && no_unsorted_cols == 0) // one column only
			basic_sorted_percentage = 5.0;
		else
			basic_sorted_percentage = vcs.size() / float(vcs.size() + no_unsorted_cols) * 10;
	}
	for(int i = 0; i < vcs.size(); i++) {
		assert(vcs[i]->GetDim() == dim); //works only for vcolumns based on a single table
		InitOneColumn(vcs[i], orders[i], NULL, os);
		if(i > 0)
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(natural_order[i] || natural_order[i-1] || packs[i].size() == packs[i-1].size());
	}
	return true;
}

bool PackOrderer::Init(VirtualColumn* vc, OrderType order, RSValue* r_filter)
{
	assert(vc->GetDim() != -1); //works only for vcolumns based on a single table

	if(Initialized())
		return false;

	packs.clear();
	otype.clear();

	curvc = 0;

	InitOneColumn(vc, order, r_filter, OrderStat(0,1));

	return true;
}


void PackOrderer::InitOneColumn(VirtualColumn* vc, OrderType otype, RSValue* r_filter, struct OrderStat os) {

	++ncols;
	this->otype.push_back(otype);

	MinMaxType mmtype;
	if(vc->Type().IsFixed() || vc->Type().IsDateTime()) {
		mmtype = MMT_Fixed;
	} else
		mmtype = MMT_String;

	this->mmtype.push_back(mmtype);
	this->packs.push_back(vector<PackPair>());

	lastly_left.push_back(true);

	prevndx.push_back(INIT_VAL);
	curndx.push_back(INIT_VAL);

	vector<PackPair>& packs_one_col = this->packs[ncols-1];
	int d = vc->GetDim();
	MIIterator mit(vc->GetMultiIndex(), d);

	MMTU mid(0);
	while(mit.IsValid()) {
		int pack = mit.GetCurPackrow(d);
		if(!r_filter || r_filter[pack] != RS_NONE) {
			if(mmtype == MMT_Fixed) {
				if(vc->GetNoNulls(mit) == mit.GetPackSizeLeft()) {
					mid.i = PLUS_INF_64;
				} else {
					_int64 min = vc->GetMinInt64(mit);
					_int64 max = vc->GetMaxInt64(mit);
					switch(otype) {
						case RangeSimilarity:
							mid.i = (min == NULL_VALUE_64 ? MINUS_INF_64 : (max - min)/2);
							break;
						case MinAsc:
						case MinDesc:
						case Covering:
							mid.i = min;
							break;
						case MaxAsc:
						case MaxDesc:
							mid.i = max;
							break;
						case NotSpecified: 
							break;
					}
				}
				packs_one_col.push_back(make_pair(mid, pack));
			} else {
				// not implemented for strings & doubles/floats
				//keep the original order
				if(r_filter) {
					//create a list with natural order. For !r_filter just use ++ to visit each pack
					mid.i = mit.GetCurPackrow(d);
					packs_one_col.push_back(make_pair(mid, pack));
				} else break;	// no need for iterating mit
			}
		}
		mit.NextPackrow();
	}

	if(packs_one_col.size() == 0 && !r_filter)
		natural_order.push_back(true);
	else
		natural_order.push_back(false);

	if(mmtype == MMT_Fixed) {
		switch(otype) {
			case RangeSimilarity:
			case MinAsc:
			case MaxAsc:
				sort(packs_one_col.begin(), packs_one_col.end(), comp_int_less);
				break;
			case Covering:
				ReorderForCovering(packs_one_col, vc);
				break;
			case MinDesc:
			case MaxDesc:
				sort(packs_one_col.begin(), packs_one_col.end(), comp_int_greater);
				break;
			case NotSpecified: 
				break;
		}
	}

	// Resort in natural order, leaving only the beginning ordered
	vector<PackPair>::iterator packs_one_col_begin = packs_one_col.begin();
	float sorted_percentage = basic_sorted_percentage / (os.neutral + os.ordered ? os.neutral + os.ordered : 1);			//todo: using os.sorted
	packs_ordered_up_to = (_int64)(packs_one_col.size() * (sorted_percentage/100.0));
	if(packs_ordered_up_to > 1000)								// do not order too much packs (for large cases)
		packs_ordered_up_to = 1000;
	packs_one_col_begin += packs_ordered_up_to;					// ordering on just sorted_percentage% of packs
	sort(packs_one_col_begin, packs_one_col.end(), comp_pack_number);
}

void PackOrderer::ReorderForCovering(vector<PackPair> &packs_one_col, VirtualColumn *vc)
{
	sort(packs_one_col.begin(), packs_one_col.end(), comp_int_less);

	int d = vc->GetDim();
	MIDummyIterator mit(vc->GetMultiIndex());
	// Algorithm: store at the beginning part all the packs which covers the whole scope
	int j = 0;					// j is a position of the last swapped pack (the end of "beginning" part)
	int i_max;					// i_max is a position of the best pack found, i.e. min(i_max) <= max(j), max(i_max)->max
	int i = 1;				// i is the current seek position
	while(i < packs_one_col.size()) {
		mit.SetPack(d, packs_one_col[j].second);		// vector of pairs: <mid, pack>
		_int64 max_up_to_now = vc->GetMaxInt64(mit);	// max of the last pack from the beginning
		if(max_up_to_now == PLUS_INF_64)
			break;
		// Find max(max) of packs for which min<=max_up_to_now
		i_max = -1;
		_int64 best_local_max = max_up_to_now;
		while(i < packs_one_col.size() && packs_one_col[i].first.i <= max_up_to_now) {
			mit.SetPack(d, packs_one_col[i].second);
			_int64 local_max = vc->GetMaxInt64(mit);
			if(local_max > best_local_max) {
				best_local_max = local_max;
				i_max = i;
			}
			i++;
		}
		if(i_max == -1 && i < packs_one_col.size())		// suitable pack not found
			i_max = i;									// just get the next good one
		j++;
		if(i_max > j) {
			std::swap(packs_one_col[j].first, packs_one_col[i_max].first);
			std::swap(packs_one_col[j].second, packs_one_col[i_max].second);
		}
	}
	// Order the rest of packs in a semi-covering way
	int step = int(packs_one_col.size() - j) / 2;
	while(step > 1 && j < packs_one_col.size() - 1) {
		for(i = j + step; i < packs_one_col.size(); i += step) {
			j++;
			std::swap(packs_one_col[j].first, packs_one_col[i].first);
			std::swap(packs_one_col[j].second, packs_one_col[i].second);
		}
		step = step / 2;
	}
}

void PackOrderer::NextPack()
{
	packs_passed++;
	if(natural_order[curvc]) {
		// natural order traversing all packs
		if(curndx[curvc] < dimsize - 1 && curndx[curvc] != END)
			++curndx[curvc];
		else
			curndx[curvc] = END;
	} else
		switch(otype[curvc]) {
			case RangeSimilarity: {
				if(lastly_left[curvc]) {
					if(prevndx[curvc] < packs[curvc].size() - 1) {
						lastly_left[curvc] = !lastly_left[curvc];
						int tmp = curndx[curvc];
						curndx[curvc] = prevndx[curvc] + 1;
						prevndx[curvc] = tmp;
					} else {
						if(curndx[curvc] > 0)
							--curndx[curvc];
						else
							curndx[curvc] = END;
					}
				} else if(prevndx[curvc] > 0) {
					lastly_left[curvc] = !lastly_left[curvc];
					int tmp = curndx[curvc];
					curndx[curvc] = prevndx[curvc]-1;
					prevndx[curvc] = tmp;
				} else {
					if(curndx[curvc] < packs[curvc].size() - 1)
						++curndx[curvc];
					else
						curndx[curvc] = END;
				}
				break;
			}
			default:
				//go along packs from 0 to packs[curvc].size() - 1
				if(curndx[curvc] != END){
					if(curndx[curvc] < (int)packs[curvc].size() - 1)
						++curndx[curvc];
					else {
						curndx[curvc] = END;
					}
				}
				break;
		}
}

PackOrderer& PackOrderer::operator++()
{
	assert(Initialized());
	if(ncols == 1) {
		NextPack();
	} else {
			curvc = (curvc+1)%ncols;

			do {
				NextPack();
			} while(curndx[curvc]!= END && visited->Get(natural_order[curvc] ? curndx[curvc] : packs[curvc][curndx[curvc]].second));

			if(curndx[curvc]!= END ) {
				visited->Set(natural_order[curvc] ? curndx[curvc] : packs[curvc][curndx[curvc]].second);
			}
	}
	return *this;
}

void PackOrderer::Rewind() 
{
	packs_passed = 0;
	for(curvc = 0; curvc< ncols; curvc++)
		RewindCol();
	curvc = 0;
	if(visited.get())
		visited->Reset();
}

void PackOrderer::RewindCol() {
	if(!natural_order[curvc] && packs[curvc].size() == 0)
		curndx[curvc] = END;
	else
		curndx[curvc] = prevndx[curvc] = INIT_VAL;
}


void PackOrderer::RewindToMatch(VirtualColumn* vc, MIIterator& mit)
{
	assert(vc->GetDim() != -1);
	assert(otype[curvc] == RangeSimilarity);
	assert(ncols == 1); //not implemented otherwise

	if(mmtype[curvc] == MMT_Fixed) {
		_int64 mid = MINUS_INF_64;
		if(vc->GetNoNulls(mit) != mit.GetPackSizeLeft()) {
			_int64 min = vc->GetMinInt64(mit);
			_int64 max = vc->GetMaxInt64(mit);
			mid = (max-min)/2;
		}
		vector<PackPair>::iterator it = lower_bound(packs[curvc].begin(), packs[curvc].end(), make_pair(mid,0), comp_int_less);
		if(it == packs[curvc].end())
			curndx[curvc] = int(packs[curvc].size() - 1);
		else
			curndx[curvc] = int(distance(packs[curvc].begin(), it));
	} else
		// not implemented for strings & doubles/floats
		curndx[curvc] = 0;

	if(packs[curvc].size() == 0 && !natural_order[curvc])
		curndx[curvc] = END;
	prevndx[curvc] = curndx[curvc];
}

void PackOrderer::RewindToCurrent()
{
	for(curvc = 0; curvc< ncols; curvc++)
		RewindToCurrentCol();
	if(visited.get())
		visited->Reset();
}

void PackOrderer::RewindToCurrentCol()
{
	assert(otype[curvc] == RangeSimilarity); //otherwise this operation is unsound

	if(curndx[curvc] == INIT_VAL || curndx[curvc] == END )
		curndx[curvc] = 0; //from the start
	if(packs[curvc].size() == 0 && !natural_order[curvc])
		curndx[curvc] = END;
	prevndx[curvc] = curndx[curvc];
}

/*!
 * Based on the given set of virtual columns (all from the same dimension) with information how they will be used (e.g. calculate MIN)
 * this function should find the best ordering of packrows for this dimension.
 *
 * Currently handled:  mixture of MAX, MIN, COUNT(DISTINCT)
 * TODO:
 * -smallest range first (for joins),
 *  ...
 */
void PackOrderer::ChoosePackOrderer(PackOrderer& po, const vector<OrderingInfo>& oi, bool one_group)
{
	vector<OrderingInfo>::const_iterator oi_it;
	vector<VirtualColumn*> vcs;
	vector<OrderType> orders;
	int unsorted_cols = 0;
	for(oi_it = oi.begin(); oi_it != oi.end(); oi_it++) {
		if(oi_it->op == MAX) {
			vcs.push_back(oi_it->vc);
			orders.push_back(MaxDesc);
		} else if(oi_it->op == MIN) {
			vcs.push_back(oi_it->vc);
			orders.push_back(MinAsc);
		} else if(oi_it->op == COUNT && oi_it->distinct) {
			vcs.push_back(oi_it->vc);
			orders.push_back(Covering);
		} else
			unsorted_cols++;
	}
	po.Init(vcs, orders, unsorted_cols, one_group);
}
