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


#ifndef SORTERWRAPPER_H_
#define SORTERWRAPPER_H_

#include "Sorter3.h"
#include "ColumnBinEncoder.h"
#include "MultiIndex.h"
#include "MIIterator.h"
#include "edition/vc/VirtualColumn.h"

//////////////////////////////////////////////////////
//
/*
	----  Usage example:  ----

	SorterWrapper swrap(mind);

	for(int i = 0; i < no_cols; i++)
		swrap.AddSortedColumn(virt_col[i], sort_order[i], in_output[i]);

	swrap.InitSorter();

	MIIterator it(mind);
	while(it.IsValid()) {
		swrap.PutValues(it);
		++it;
	}

	// Note that the values are sorted somewhere between PutValues and FetchNextRecord, depending on algorithm

	cout << "Sorted values: " << endl;
	while(swrap.FetchNextRow()) {
		for(int i = 0; i < no_cols; i++) {
			if(swrap.IsNull(i))
				cout << "Null ";
			else
				cout << swrap.GetValue64(i) << " ";
		}
		cout << endl;
	}
	-----  End of example  -----
*/
///////////////////////////////////////////////////////

class SorterWrapper
{
public:
	SorterWrapper(MultiIndex &_mind, _int64 _limit);
	~SorterWrapper();

	void AddSortedColumn(VirtualColumn *col, int sort_order, bool in_output);

	void InitSorter(MultiIndex &_mind);		// MultiIndex is needed to check which dimensions we should virtualize

	bool InitPackrow(MIIterator &mit);		// return true if the packrow may be skipped because of limit/statistics

	bool PutValues(MIIterator &mit);		// set values for all columns based on multiindex position
	// NOTE: data packs must be properly locked by InitPackrow!
	// Return false if no more values are needed (i.e. limit already reached)

	// Must be executed before the first GetValue.
	// Return true if OK, false if there is no more data
	bool FetchNextRow();

	_int64 GetValue64(int col, bool &is_null)
	{ return scol[col].GetValue64(cur_val, cur_mit, is_null); }

	RCBString GetValueT(int col)
	{ return scol[col].GetValueT(cur_val, cur_mit); }

	void SortRoughly(std::vector<PackOrderer> &po);

private:
	Sorter3 *s;
	MultiindexPositionEncoder *mi_encoder;	// if null, then no multiindex position encoding is needed
	_int64 limit;
	_int64 no_of_rows;			// total number of rows to be sorted (upper limit)
	_int64 no_values_encoded;	// values already put to the sorter
	int rough_sort_by;			// the first nontrivial sort column
	int buf_size;				// total number of bytes in sorted rows
	unsigned char *cur_val;		// a pointer to the current (last fetched) output row
	unsigned char *input_buf;	// a buffer of buf_size to prepare rows
	MIDummyIterator cur_mit;	// a position of multiindex for implicit (virtual) columns

	std::vector<ColumnBinEncoder> scol;		// encoders for sorted columns

	// this is a temporary input description of sorting columns
	class SorterColumnDescription
	{
	public:
		SorterColumnDescription(VirtualColumn *_col, int _sort_order, bool _in_output)
			: col(_col), sort_order(_sort_order), in_output(_in_output) {}
		VirtualColumn *col;
		int sort_order;
		bool in_output;
	};
	void InitOrderByVector(std::vector<int> &order_by);	// a method refactored from InitSorter

	std::vector<SorterColumnDescription> input_cols;		// encoders for sorted columns
};

#endif /* SORTERWRAPPER_H_ */
