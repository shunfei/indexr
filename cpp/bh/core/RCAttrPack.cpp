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

#include "RCAttrPack.h"
#include "RCAttr.h"
#include "core/tools.h"
#include "DataCache.h"
#include "loader/RCAttrLoadBase.h"
#include "edition/core/Transaction.h"

using namespace boost;

//CRITICAL_SECTION rc_save_pack_cs;

AttrPack::AttrPack(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner)
	:	m_prev_pack(NULL), m_next_pack(NULL), pack_no(pack_no), is_only_compressed(false),
		inserting_mode(inserting_mode), no_compression(no_compression), attr_type(attr_type)
{
	nulls = 0;
	compressed_buf = 0;
	is_empty = true;
	previous_size = 0;
	_logical_coord.ID=bh::COORD_TYPE::PACK;
	_logical_coord.co.pack = pc;
	//SynchronizedDataCache::GlobalDataCache().PutAttrPackRO(table, column, dp, this);
}

AttrPack::AttrPack(const AttrPack &ap) :
	TrackableObject( ap ),
		pack_no(ap.pack_no),
		no_nulls(ap.no_nulls),
		no_obj(ap.no_obj),
		is_empty(ap.is_empty),
		compressed_up_to_date(ap.compressed_up_to_date),
		saved_up_to_date(ap.saved_up_to_date),
		comp_buf_size(ap.comp_buf_size),
		comp_null_buf_size(ap.comp_null_buf_size),
		comp_len_buf_size(ap.comp_len_buf_size),
		is_only_compressed(ap.is_only_compressed),
		previous_size(ap.previous_size),
		inserting_mode(ap.inserting_mode),
		no_compression(ap.no_compression),
		attr_type(ap.attr_type)
{
	nulls = 0;
	compressed_buf = 0;
	
	if(ap.compressed_buf) {
		compressed_buf = (uchar*)alloc(comp_buf_size, BLOCK_COMPRESSED);
		memcpy(compressed_buf, ap.compressed_buf, comp_buf_size);
	}

	if(ap.nulls) {
		nulls = (uint*)alloc(8192, BLOCK_UNCOMPRESSED);
		memcpy(nulls, ap.nulls, 8192);
	}
	m_lock_count = 1;

}

void AttrPack::Release() 
{ 
	if(owner) owner->DropObjectByMM(GetPackCoordinate()); 
}

AttrPack::~AttrPack()
{
	dealloc(nulls);
	dealloc(compressed_buf);
	nulls = 0;
	compressed_buf = 0;
}

bool AttrPack::UpToDate()				// return 1 iff there is no need to save
{
	if(!compressed_up_to_date)
		saved_up_to_date = false;
	return saved_up_to_date;
}

bool AttrPack::ShouldNotCompress()
{
	return (no_compression || (inserting_mode == 1 && no_obj < 0x10000));
}

