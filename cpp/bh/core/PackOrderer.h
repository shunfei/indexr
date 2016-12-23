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

#ifndef PACKORDERER_H_
#define PACKORDERER_H_

#include "common/CommonDefinitions.h"
#include <vector>
#include "edition/vc/VirtualColumn.h"

/*!
 *
 */

class PackOrderer
{
public:
	enum OrderType {RangeSimilarity,			// ...
					MinAsc,						// ascending by pack minimum
					MinDesc,					// descending by pack minimum 
					MaxAsc,						// ascending by pack maximum
					MaxDesc, 					// descending by pack maximum
					Covering,					// start with packs which quickly covers the whole attribute domain (MinAsc, then find the next minimally overlapping pack)
					NotSpecified};
	enum State {INIT_VAL = -1, END = -2};

	struct OrderStat {
		OrderStat(int n, int o) : neutral(n), ordered(o) {};
		int neutral;
		int ordered;
	};

	//! Uninitialized object, use Init() before usage
	PackOrderer() ;

	PackOrderer(const PackOrderer&);

	/*!
	 * Create an orderer for given column,
	 * \param vc datapacks of this column will be ordered
	 * \param r_filter use only pack not eliminated by this rough filter
	 * \param order how to order datapacks
	 */
	PackOrderer(VirtualColumn* vc, RSValue* r_filter, OrderType order);
//	PackOrderer(std::vector<VirtualColumn*> vcs, std::vector<OrderType> orders, RSValue* r_filter);

	~PackOrderer() {};

	//! Initialized Orderer constructed with a default constructor,
	//! ignored if used on a initialized orderer
	//! \return true if successful, otherwise false
	bool Init(VirtualColumn* vc, OrderType order, RSValue* r_filter = NULL);

	bool Init(std::vector<VirtualColumn*> vcs, std::vector<OrderType> orders, int no_unsorted_cols, bool one_group);

	/*!
	 * Reset the iterator, so it will start from the first pack in the given sort order
	 */
	void Rewind();

	/*!
	 * Reset the iterator to the position associated with given datapack from the given column
	 */
	void RewindToMatch(VirtualColumn* vc, MIIterator& mit);

	/*!
	 * Reset the iterator, so it will start from the current position
	 */
	void RewindToCurrent();

	/*!
	 * the current datapack number in the ordered sequence
	 * \return pack number or -1 if end of sequence reached
	 */
	int Current() {return curndx[curvc] < 0 ? -1 : natural_order[curvc] ? curndx[curvc] : packs[curvc][curndx[curvc]].second;}

	/*!
	 * Advance to the next position in the ordered sequence, return the next datapack number
	 */
	PackOrderer& operator++();

	//! Is the end of sequence reached?
	bool IsValid()				{ return curndx[curvc] >= 0; }
	uint NoPacks()				{ return uint(packs[0].size()); }
	bool Initialized()			{ return ncols > 0 ;}
	bool NaturallyOrdered()		{ return packs_passed >= packs_ordered_up_to; }		// true if the current position and the rest of packs are in ascending order
	OrderType GetOrderType()	{ return otype[0]; } //FIX - regard the whole vector otype

	struct OrderingInfo {
		OrderingInfo(VirtualColumn* vc,ColOperation op, bool dist) : vc(vc), op(op), distinct(dist) {}
		VirtualColumn* vc;
		ColOperation op; // distinguishes aggregations only, may need extensions for other operations
		bool distinct;	 // aggregation with distinct
	};

	/*!
	 * analyze the given ordering info (= list of columns + how they are to be used)
	 * and assign to po a properly chosen PackOrderer
	 * one_group is a flag indicating that there is trivial GROUP BY
	 */
	static void ChoosePackOrderer(PackOrderer& po, const std::vector<OrderingInfo>&, bool one_group);

private:
	enum MinMaxType {MMT_Fixed, MMT_Float, MMT_Double, MMT_String};

	typedef union MMTU {
		_int64 i;
		double d;
		float f;
		char c[8];
		MMTU(_int64 i) :i(i) {}
	} MMTU;

	typedef std::pair<MMTU,int> PackPair;

	class CompIntLess {
	public:
		inline bool operator() (const PackPair& v1, const PackPair& v2) const { return v1.first.i < v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second); }
	};

	class CompIntGreater {
	public:
		inline bool operator() (const PackPair& v1, const PackPair& v2) const { return v1.first.i > v2.first.i || (v1.first.i == v2.first.i && v1.second < v2.second); }
	};

	class CompPackNumber {
	public:
		inline bool operator() (const PackPair& v1, const PackPair& v2) const { return v1.second < v2.second; }
	};

	void InitOneColumn(VirtualColumn* vc, OrderType otype, RSValue* r_filter, struct OrderStat os);
	void NextPack();
	void RewindCol();
	void RewindToCurrentCol();
	void ReorderForCovering(std::vector<PackPair> &packs_one_col, VirtualColumn *vc);

	static CompIntLess    comp_int_less;	//implemented comparison - sorting on packs[n].first.i ascending
	static CompIntGreater comp_int_greater;	//implemented comparison - sorting on packs[n].first.i desc
	static CompPackNumber comp_pack_number;	//implemented comparison - sorting on pack number (natural ordering)
	static float basic_sorted_percentage;

	std::vector<std::vector<PackPair> > packs;
	std::vector<int> curndx;			//Current() == packs[curvc][curndx[curvc]]
	std::vector<int> prevndx;		// i = o.Current(); ++o; prevndx[curvc] = i ;
	int curvc;			//
	int ncols;
	std::vector<bool> natural_order;
	int dimsize;
	std::vector<bool> lastly_left;	//if the last ++ went to the left
	std::vector<MinMaxType> mmtype;
	std::vector<OrderType> otype;

	std::auto_ptr<Filter> visited;	//which pack was visited already; can be implemented by Filter class to save space
	_int64 packs_ordered_up_to;		// how many packs are actually reordered (the rest of them is left in natural order
	_int64 packs_passed;			// how many packs are already processed
};


#endif /* PACKORDERER_H_ */
