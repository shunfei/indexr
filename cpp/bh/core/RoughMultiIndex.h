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

#ifndef _ROUGHMULTIINDEX_H_
#define _ROUGHMULTIINDEX_H_

#include "bintools.h"
#include "Filter.h"
#include "CQTerm.h"

class RoughMultiIndex
{
public:
	RoughMultiIndex(std::vector<int> no_of_packs);				// initialize by dimensions definition (i.e. pack numbers)
	RoughMultiIndex(const RoughMultiIndex&);				
	~RoughMultiIndex();

	RSValue GetPackStatus(int dim, int pack)									{ return rf[dim][pack]; };
	void SetPackStatus(int dim, int pack, RSValue v)							{ rf[dim][pack]=v; };

	//////////////////////////////////////////////////////////////////////////////////////////////////////////
	int NoDimensions()															{ return no_dims; };
	int NoPacks(int dim)														{ return no_packs[dim]; };
	int NoConditions(int dim)													{ return int(local_desc[dim].size()); };
	int GlobalDescNum(int dim,int local_desc_num)								{ return (local_desc[dim])[local_desc_num]->desc_num; }

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Example of query processing steps:
	// 1. Add new rough filter (RF) by GetLocalDescFilter for a condition. Note that it will have RS_NONE if global RF is RC_NONE.
	// 2. Update local RF for all non-RS_NONE packs.
	// 3. Update global RF by UpdateGlobalRoughFilter, RC_NONEto optimize access for next conditions.

	RSValue* GetRSValueTable(int dim)											{ return rf[dim]; };
	RSValue* GetLocalDescFilter(int dim, int desc_num, bool read_only = false);
																	// if not exists, create one (unless read_only is set)
	void ClearLocalDescFilters();									// clear all desc info, for reusing the rough filter e.g. in subqueries

	bool UpdateGlobalRoughFilter(int dim, int desc_num);			// make projection from local to global filter for dimension
																	// return false if global filter is empty
	void UpdateGlobalRoughFilter(int dim, Filter* loc_f);			// if the filter is nontrivial, then copy pack status
	void UpdateLocalRoughFilters(int dim);							// make projection from global filters to all local for the given dimension
	void MakeDimensionSuspect(int dim = -1);						// RS_ALL -> RS_SOME for a dimension (or all of them)
	void MakeDimensionEmpty(int dim = -1);

	/////////////////////////////////////////////////////////////////////////////////
	std::vector<int> GetReducedDimensions();	// find dimensions having more omitted packs than recorded in no_empty_packs
	void UpdateReducedDimension(int d);	// update no_empty_packs
	void UpdateReducedDimension();		// update no_empty_packs for all dimensions

private:
	int no_dims;
	int* no_packs;			// number of packs in each dimension

	RSValue** rf;		// rough filters for packs (global)

	int* no_empty_packs;	// a number of (globally) empty packs for each dimension,
							// used to check whether projections should be made (and updated therein)

	class RFDesc			// rough description of one condition (descriptor)
	{
	public:
		RFDesc(int packs, int d);
		RFDesc(const RFDesc &);
		~RFDesc();

		int desc_num;		// descriptor number
		int no_packs;		// table size (for copying)
		RSValue* desc_rf;	// rough filter for desc; note that all values are interpretable here
	};

	std::vector<RFDesc*>* local_desc;		// a table of vectors of conditions for each dimension
};

//////////////////////////////////////////////////////////////////////////////////////////////////

class RoughValueTree
{
public:
	RoughValueTree() : v(RS_NONE), left(NULL), right(NULL)		{}
	~RoughValueTree()											{ delete left; delete right; }

	RSValue v;
	RoughValueTree*	left;
	RoughValueTree*	right;
};

#endif

