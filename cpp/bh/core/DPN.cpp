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

#include "DPN.h"

// Redundant, declared elsewhere
#define PF_NULLS_ONLY -1
#define PF_NO_OBJ -2

std::auto_ptr<DPN> DPN::CloneWithoutPack() const
{
	return std::auto_ptr<DPN>(new DPN(copy_object<DPN>()(*this)));
}

// Not tested
void DPN::CopyToBuffer(char* buf, AttrPackType packtype)
{
	if(no_nulls == (uint) (no_objs) + 1) {
		*((int*) (buf)) = PF_NULLS_ONLY;
		*((ushort*) (buf + 34)) = 0; // no_nulls (0 is special value here)
	} else {
		*((int*) (buf)) = pack_file;
		*((ushort*) (buf + 34)) = (ushort) no_nulls;
	}
	*((uint*) (buf + 4)) = pack_addr;

	*((_uint64*) (buf + 8)) = local_min;
	*((_uint64*) (buf + 16)) = local_max;
	if(packtype == PackN)
		*((_uint64*) (buf + 24)) = sum_size;
	else
		*((_uint64*) (buf + 24)) = sum_size;

	*((ushort*) (buf + 32)) = no_objs;
	*((uchar*) (buf + 36)) = repetition_found;
	*((uint*)(buf + 37)) = natural_save_size;
}

// Not tested
void DPN::RestoreFromBuffer(char* buf, AttrPackType packtype)
{
	is_stored = true;
	pack_file = *((int*) buf);
	pack_addr = *((uint*) (buf + 4));
	local_min = *((_uint64*) (buf + 8));
	local_max = *((_uint64*) (buf + 16));
	if(packtype == PackN)
		sum_size = *((_uint64*) (buf + 24));
	else
		sum_size = ushort(*((_uint64*) (buf + 24)));
	no_objs = *((ushort*) (buf + 32));
	no_nulls = *((ushort*) (buf + 34));
	if(pack_file == PF_NULLS_ONLY)
		no_nulls = no_objs + 1;
	if(pack_file == PF_NULLS_ONLY || (packtype == PackN && local_min == local_max && no_nulls == 0)) {
		pack_mode = PACK_MODE_TRIVIAL; // trivial data (only nulls or all values are the same), no physical pack
		is_stored = false;
	} else if(pack_file == PF_NO_OBJ) {
		pack_mode = PACK_MODE_EMPTY; // empty pack, no objects
		is_stored = false;
	} else {
		pack_mode = PACK_MODE_UNLOADED; // non trivial pack - data on disk
		is_stored = true;
	}
	natural_save_size = *((uint*) (buf + 37));
}

