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

/////////////////////////////////////////////////////////////////////////////////////////////////////
// This is a part of TempTable implementation concerned with the query execution low-level mechanisms
/////////////////////////////////////////////////////////////////////////////////////////////////////
#include <boost/shared_ptr.hpp>
#include <boost/assign.hpp>

#include "edition/local.h"
#include "TempTable.h"
#include "PackGuardian.h"
#include "system/TextUtils.h"
#include "types/DataConverter.h"
#include "system/RCSystem.h"
#include "system/ConnectionInfo.h"
#include "common/DataFormat.h"
#include "exporter/DataExporter.h"
#include "types/ValueParserForText.h"
#include "system/IOParameters.h"
#include "edition/vc/VirtualColumn.h"
#include "SorterWrapper.h"
#include "system/fet.h"
#include "common/bhassert.h"

#include "RCEngine.h"

using namespace std;
using namespace boost;
using namespace boost::assign;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

bool TempTable::OrderByAndMaterialize(vector<SortDescriptor> &ord, _int64 limit, _int64 offset, ResultSender* sender)	// Sort MultiIndex using some (existing) attributes in some tables
{	// "limit=10; offset=20" means that the first 10 positions of sorted table will contain objects 21...30.
	MEASURE_FET("TempTable::OrderBy(...)");
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(limit >= 0 && offset >= 0);
	no_obj = limit;
	if((int)ord.size() == 0 || filter.mind->NoTuples() < 2 || limit == 0) {
		ord.clear();
		return false;
	}

	// Prepare sorter
	std::vector<VirtualColumn*> vc_for_prefetching;
	SorterWrapper sorted_table(*(filter.mind), limit + offset);
	int sort_order = 0;
	for(int j = 0; j < attrs.size(); j++) {
		if(attrs[j]->alias != NULL) {
			VirtualColumn* vc = attrs[j]->term.vc;
			assert(vc);
			sort_order = 0;
			for(int i = 0; i < ord.size(); i++)
				if(ord[i].vc == vc) {
					sort_order = (ord[i].dir == 0 ? (i + 1) : -(i + 1));
					ord[i].vc = NULL;		// annotate this entry as already added
				}
			sorted_table.AddSortedColumn(vc, sort_order, true);
			vc_for_prefetching.push_back(vc);
		}
	}
	//////////////////////////////////

	for(int i = 0; i < ord.size(); i++) {		// find all columns not added yet (i.e. not visible in output)
		if(ord[i].vc != NULL) {
			sort_order = (ord[i].dir == 0 ? (i + 1) : -(i + 1));
			sorted_table.AddSortedColumn(ord[i].vc, sort_order, false);
			vc_for_prefetching.push_back(ord[i].vc);
		}
	}
	sorted_table.InitSorter(*(filter.mind));

	// Put data
	vector<PackOrderer> po(filter.mind->NoDimensions());
	sorted_table.SortRoughly(po);
	DimensionVector all_dims(filter.mind->NoDimensions());
	all_dims.SetAll();
	MIIterator it(filter.mind, all_dims, po);
	_int64 local_row = 0;
	bool continue_now = true;

	for(int i = 0; i < ord.size(); i++)
		if(ord[i].vc != NULL)
			ord[i].vc->InitPrefetching(it);
	ord.clear();

	for(int i = 0; i< vc_for_prefetching.size(); i++) {
		vc_for_prefetching[i]->InitPrefetching(it);
	}

	while(it.IsValid() && continue_now) {
		if(m_conn.killed()) throw KilledRCException();
		if(it.PackrowStarted()) {
			bool skip_packrow = sorted_table.InitPackrow(it);
			if(skip_packrow) {
				local_row += it.GetPackSizeLeft();
				it.NextPackrow();
				continue;
			}
		}
		continue_now = sorted_table.PutValues(it);		// return false if a limit is already reached (min. values only)
		++it;

		local_row++;
		if(local_row % 100000000 == 0)
			rccontrol.lock(m_conn.GetThreadID()) << "Preparing values to sort (" << int(local_row / double(filter.mind->NoTuples()) * 100) << "% done)." << unlock;
	}

	// Create output
	for(uint i = 0; i < NoAttrs(); i++) {
		if(attrs[i]->alias != NULL) {
			if(sender)
				attrs[i]->CreateBuffer(no_obj > RESULT_SENDER_CACHE_SIZE ? RESULT_SENDER_CACHE_SIZE : no_obj, &m_conn, no_obj > RESULT_SENDER_CACHE_SIZE);
			else
				attrs[i]->CreateBuffer(no_obj, &m_conn);
		}
	}

	_int64 global_row = 0;
	local_row = 0;
	RCBString val_s;
	_int64 val64;
	_int64 offset_done = 0;
	_int64 produced_rows = 0;
	bool null_value;
	bool valid = true;
	do {				// outer loop - through streaming buffers (if sender != NULL)
		do {
			valid = sorted_table.FetchNextRow();
			if(valid && global_row >= offset) {
				int col = 0;
				if(m_conn.killed()) throw KilledRCException();
				for(attrs_t::iterator attr = attrs.begin(), end = attrs.end(); attr != end; ++attr) {
					if((*attr)->alias != NULL) {
						switch((*attr)->TypeName()) {
							case RC_STRING:
							case RC_VARCHAR:
							case RC_BIN:
							case RC_BYTE:
							case RC_VARBYTE:
								val_s = sorted_table.GetValueT(col);
								(*attr)->SetValueString(local_row, val_s);
								break;
							default:
								val64 = sorted_table.GetValue64(col, null_value);	// works also for constants
								if(null_value)
									(*attr)->SetValueInt64(local_row, NULL_VALUE_64);
								else
									(*attr)->SetValueInt64(local_row, val64);
								break;
						}
						col++;
					}
				}
				local_row++;
				++produced_rows;
				if((global_row - offset + 1) % 100000000 == 0)
					rccontrol.lock(m_conn.GetThreadID()) << "Retrieving sorted rows (" << int((global_row - offset) / double(limit - offset) * 100) << "% done)." << unlock;
			} else if(valid)
				++offset_done;
			global_row++;
		} while(valid && global_row < limit + offset && !(sender && local_row >= RESULT_SENDER_CACHE_SIZE));		// a limit for streaming buffer
		// Note: what about SetNoMaterialized()? Only no_obj is set now.
		if(sender) {
			TempTable::RecordIterator iter = begin();	
			for(int i = 0; i < local_row; i++) {
				sender->Send(iter);
				++iter;
			}
			local_row = 0;
		}
	} while(valid && global_row < limit + offset);
	rccontrol.lock(m_conn.GetThreadID()) << "Sorted rows retrieved." << unlock;

	for(int i = 0; i< vc_for_prefetching.size(); i++) {
		vc_for_prefetching[i]->StopPrefetching();
	}

	return true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void TempTable::FillMaterializedBuffers(_int64 local_limit, _int64 local_offset, ResultSender* sender, bool pagewise)
{
	RCBString vals;
	no_obj = local_limit;
	int page_size = CalculatePageSize();
	if(sender || pagewise)
		page_size = min(page_size, RESULT_SENDER_CACHE_SIZE);		// a number of rows to be sent at once
	if(!filter.mind->ZeroTuples()) {
		if(no_materialized == 0) {
			//////// Column statistics ////////////////////////
			if(m_conn.DisplayAttrStats()) {
				for(uint j = 0; j < NoAttrs(); j++) {
					if(attrs[j]->term.vc)
						attrs[j]->term.vc->DisplayAttrStats();
				}
				m_conn.SetDisplayAttrStats(false); // already displayed
			}
			///////////////////////////////////////////////////

			for(uint i = 0; i < NoAttrs(); i++)
				attrs[i]->CreateBuffer(no_obj, &m_conn, sender || pagewise);
		}
		bool has_intresting_columns = false;
		for(attrs_t::const_iterator attr = attrs.begin(), end = attrs.end(); attr != end; ++attr) { /* materialize dependent tables */
			if(((*attr)->mode == LISTING) && (*attr)->term.vc && (*attr)->alias) { // constant value, the buffer is already filled in
				has_intresting_columns = true;
				break;
			}
		}

		MIIterator it(filter.mind);
		if(pagewise && local_offset < no_materialized)
			local_offset = no_materialized;   //continue filling
		
		if(local_offset > 0)
			it.Skip(local_offset);
		_int64 row = local_offset;

		if(!has_intresting_columns || !it.IsValid())
			return;

		for(attrs_t::iterator attr = attrs.begin(), end = attrs.end(); attr != end; ++attr) /* fill buffers in attrs */
			if((*attr)->term.vc)
				(*attr)->term.vc->InitPrefetching(it);

		// Semantics of variables:
		// row		- a row number in orig. tables
		// no_obj	- a number of rows to be actually sent (offset already omitted)
		// start_row, page_end - in terms of orig. tables
		while(it.IsValid() && row < no_obj + local_offset) { /* go thru all rows */
			bool outer_iterator_updated = false;
			MIIterator page_start(it);
			_int64 start_row = row;
			_int64 page_end = (((row - local_offset) / page_size) + 1) * page_size + local_offset;
			// where the current TempTable buffer ends, in terms of multiindex rows (integer division)
			if(page_end > no_obj + local_offset)
				page_end = no_obj + local_offset;
			_int64 no_rows_inserted = 0;				// number of rows to be sent in streaming mode
			_int64 table_output_row;
			if(sender)
				table_output_row = 0;		// in case of streaming, start always from the beginning
			else if(lazy)
				table_output_row = no_materialized;
			else
				table_output_row = start_row - local_offset;	/* actual row number in output buffers */

			for(attrs_t::iterator attr = attrs.begin(), end = attrs.end(); attr != end; ++attr) { /* fill buffers in attrs */
				VirtualColumn* vc = (*attr)->term.vc;
				if((((*attr)->mode == LISTING) && vc && ((*attr)->alias)) || !vc->IsConst()) { // constant value, the buffer is already filled in
					MIIterator i(page_start);
					bool first_row_for_vc = true;
					_int64 r = start_row;
					_int64 output_row = table_output_row;
					do { /* go thru all rows (fitting into page size) in given column in given packrow */
						if(i.PackrowStarted() || first_row_for_vc) {
							vc->LockSourcePacks(i);
							first_row_for_vc = false;
						}
						switch((*attr)->TypeName()) {
							case RC_STRING:
							case RC_VARCHAR:
								vc->GetValueString(vals, i);
								(*attr)->SetValueString(output_row, vals);
								break;
							case RC_BIN:
							case RC_BYTE:
							case RC_VARBYTE:
								if(!vc->IsNull(i))
									vc->GetNotNullValueString(vals, i);
								else
									vals = RCBString();
								(*attr)->SetValueString(output_row, vals );
								break;
							default:
								(*attr)->SetValueInt64(output_row, vc->GetValueInt64(i));
								break;
						}
						++i;			// local multiindex iterator
						++r;			// global output row index
						++output_row;	// local output row index
						if(attr == attrs.begin())
							++no_materialized;
					} while(i.IsValid()	&& r < page_end);
					if(!outer_iterator_updated) {
						it.swap(i); /* update global iterator - once */
						row = r;
						outer_iterator_updated = true;
						no_rows_inserted = output_row;
					}
					vc->UnlockSourcePacks();
				}
			}
			if(sender) {
				if(m_conn.killed()) 
					throw KilledRCException();
				TempTable::RecordIterator iter = begin();	
				for(_int64 i = 0; i < no_rows_inserted; i++) {
					sender->Send(iter);
					++iter;
				}
			}
			if(lazy)
				break;
		}
	}
}


std::vector<ATI> TempTable::GetATIs(bool orig)
{
	vector<ATI> deas;
	for(uint i = 0; i < NoAttrs(); i++) {
		if(!IsDisplayAttr(i))
			continue;
		deas += ATI(attrs[i]->TypeName(), !attrs[i]->Type().GetNullsMode(), orig ? attrs[i]->orig_precision : attrs[i]->Type().GetPrecision(), attrs[i]->Type().GetScale(), false, false, attrs[i]->Type().GetCollation());
	}
	return deas;
}


void TempTable::VerifyAttrsSizes()	// verifies attr[i].field_size basing on the current multiindex contents
{
	for(uint i = 0; i < attrs.size(); i++)
		if(ATI::IsStringType(attrs[i]->TypeName()))
			attrs[i]->OverrideStringSize(attrs[i]->term.vc->MaxStringSize());
}

