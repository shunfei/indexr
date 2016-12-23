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

#ifndef _CORE_RSIMAP_H_
#define _CORE_RSIMAP_H_

#include "../common/CommonDefinitions.h"
#include "../types/RCDataTypes.h"
#include "RSI_Framework.h"
#include "system/MemoryManagement/MMGuard.h"
/////////////////////////////////////////////////////////////////////////////
// RSIndex_CMap    - histograms for text packs
//
// Usage:
//
//	RSIndex_CMap h;
//	h.Load(fhandle);
//	if(h.NoObj()<current_no_obj) then only a part of it may be safely used, i.e.
//		only between 0 and (h.NoObj()>>16 - 1) packs are up to date.
//
//	result = h.IsValue( value,value,pack,min_pack,max_pack);	// check if a value (or interval) is present in a pack
//
// Updating:
//
//	h.ClearPack(pack);		// clear all packs which are changed
//	h.Update(new_no_obj);	// register new number of objects
//	for(all changed packs)
//		h.PutValue(value,pack,min_pack,max_pack);
//	h.Save(fhandle);
//

class RSIndex_CMap : public RSIndex
{
public:
	RSIndex_CMap(int no_pos = 64);				// create an empty index
	virtual ~RSIndex_CMap();

	void	Clear();				// make the index empty

	_int64	NoObj()		{ return no_obj; }
	bool	UpToDate(_int64 cur_no_obj, int pack);		// true, if this pack is up to date (i.e. either cur_no_obj==no_obj, or pack is not the last pack in the index)
	int		NoPositions()		{ return no_positions; }

	// creation, update

	void	Create(_int64 _no_obj,int no_pos = 64);			// create a new histogram based on _no_obj objects;
	void	Update(_int64 _new_no_obj);		// enlarge buffers for the new number of objects;
										// note that all of the last pack must be analyzed by PutValue(),
										// because pack min and max might change. ClearPack may be needed.
	void	ClearPack(int pack);			// reset the information about the specified pack
										// and prepare it for update (i.e. analyzing again all values)
	//void	SetPack(int pack);			// set the information about the specified pack
										// can be used for fakes
	void	PutValue(RCBString& v, int pack); // set information that value v does exist in this pack

	// reading histogram information
	// Note: this function is thread-safe, i.e. it only reads the data
	RSValue IsValue(RCBString min_v, RCBString max_v, int pack);
					// Results:		RS_NONE - there is no objects having values between min_v and max_v (including)
					//				RS_SOME - some objects from this pack do have values between min_v and max_v
					//				RS_ALL	- all objects from this pack do have values between min_v and max_v

	RSValue	IsLike(RCBString pattern, int pack, char escape_character);

	//bool	Intersection(int pack, RSIndex_CMap *sec, int pack2);
					//	Return true if there is any common value possible for two packs from two different columns (histograms).

	int		NoOnes(int pack, uint pos);	// a number of ones in the pack on a given position 0..no_positions
	bool	IsSet(int pack, unsigned char c, uint pos);
	bool	IsAnySet(int pack, unsigned char first, unsigned char last, uint pos); // true, if there is at least one 1
					// in [first, last]

	// Loading/saving

	void	Load(IBFile* frs_index, int current_read_loc);		// fd - file descriptor, open for reading, loc - 0 or 1
	void	LoadLastOnly(IBFile* frs_index, int current_read_loc); // fd - file descriptor, open for reading
	void	Save(IBFile* frs_index, int current_save_loc);		// fd - file descriptor, open for writing

	//void	Display(int pack, unsigned char c);
	void	AppendKN(int pack, RSIndex_CMap*, uint no_values_to_add);	//append a given KN to KN table, extending the number of objects

	ushort	GetRepLength() { return no_positions*32; }
	void 	CopyRepresentation(void *buf, int pack);

protected:
	char	*GetRepresentation(int pack) { BHASSERT(pack <= end_pack, "Invalid pack request"); return (char *)(PackBuf(pack)); }

private:
	void	Alloc(_int64);
	uint*	PackBuf(_int64);
	void	Deallocate();

	void 	AppendKNs(int no_new_packs);	//append new KNs
	void	Set(int pack, unsigned char c, uint pos);
	//void	UnSet(int pack, unsigned char c, uint pos);

private:
	_int64	no_obj;			// timeliness signature: a number of objects for which the index is up-to-date
	int	no_pack;			// number of packs
	int	start_pack;			// number of the first pack stored in memory
	int	end_pack;			// number of the last pack stored in memory
	_int64 no_pack_declared;	// current size of the buffer (usually more than used, to avoid too frequent reallocations)
	int	no_positions;		// number of string positions to describe one pack (default: 64 positions)
	typedef std::vector<uint*> cmap_buffers_t;
	static _int64 const MAX_CMAP_BLOCK_SIZE = (1l << 28l); // 256MB
	_int64 total_size;
	cmap_buffers_t cmap_buffers;
};

#endif   //_CORE_RSIMAP_H_

