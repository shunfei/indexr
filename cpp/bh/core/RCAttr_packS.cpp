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

#include "RCAttr.h"
#include "RCAttrPack.h"
#include "bintools.h"
#include "compress/BitstreamCompressor.h"
#include "compress/TextCompressor.h"
#include "compress/PartDict.h"
#include "compress/NumCompressor.h"
#include "system/TextUtils.h"
#include "system/IBStream.h"
#include "WinTools.h"
#include "tools.h"
#include "ValueSet.h"
#include "system/MemoryManagement/MMGuard.h"
//#include <iostream>

using namespace std;

AttrPackS::AttrPackS(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner)
	:	AttrPack(pc, attr_type, inserting_mode, no_compression, owner), ver(0), previous_no_obj(0), data(0), index(0), lens(0), decomposer_id(0), use_already_set_decomposer(false), outliers(0)
{
	Construct();
}

AttrPackS::AttrPackS(const AttrPackS &aps)
	:	AttrPack(aps), ver(aps.ver), previous_no_obj(aps.previous_no_obj), data(0), index(0), lens(0), decomposer_id(aps.decomposer_id), use_already_set_decomposer(aps.use_already_set_decomposer), outliers(aps.outliers)
{
	try {
		assert(!is_only_compressed);
		max_no_obj = aps.max_no_obj;
		//data_id = aps.data_id;
		data_id = 0;
		data_full_byte_size = aps.data_full_byte_size;
		len_mode = aps.len_mode;
		binding = false;
		last_copied = no_obj - 1;
		optimal_mode = aps.optimal_mode;

		lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
		memcpy(lens, aps.lens, len_mode * no_obj * sizeof(char));

		index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
		data = 0;

		//int ds = (int) rc_msize(aps.data) / sizeof(char*);
		if(data_full_byte_size) {
			data_id = 1;
			data = (uchar**)alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
			*data = 0;
			int sum_size = 0;
			for (uint i = 0; i < aps.no_obj; i++) {
				sum_size += aps.GetSize(i);
			}
			if(sum_size > 0) {
				data[0] = (uchar*) alloc(sum_size, BLOCK_UNCOMPRESSED);
			} else
				data[0] = 0;
			sum_size = 0;
			for (uint i = 0; i < aps.no_obj; i++) {
				int size = aps.GetSize(i);
				if(!aps.IsNull(i) && size != 0) {
					memcpy(data[0] + sum_size, (uchar*)aps.index[i], size);
					index[i] = data[0] + sum_size;
					sum_size += size;
				} else
					index[i] = 0;
			}
		}
	} catch (...) {
		Destroy();
		throw;
	}
}

std::auto_ptr<AttrPack> AttrPackS::Clone() const
{
	return std::auto_ptr<AttrPack>(new AttrPackS(*this));
}

void AttrPackS::Prepare(int no_nulls)
{
	Lock();
	Construct();
	this->no_obj = no_nulls;
	for(int i = 0; i < no_nulls; i++)
		SetNull(i);						// no_nulls set inside
	is_empty = false;
	Unlock();
}

void AttrPackS::LoadData(IBStream* fcurfile)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackS::LoadData(...)");
	NotifyDataPackLoad(GetPackCoordinate());
#endif
	Lock();
	try {
		Construct();
		no_nulls = 0;
		optimal_mode = 0;
		uchar prehead[9];
		int buf_used = fcurfile->ReadExact(prehead, 9);
		// WARNING: if the first 4 bytes are NOT going to indicate file size, change CheckPackFileSize in RCAttr !
		uint total_size = previous_size = *((uint *)prehead);
		uchar tmp_mode = prehead[4];
		no_obj = *((ushort*)(prehead + 5));
		previous_no_obj = ++no_obj;
		no_nulls = *((ushort*)(prehead + 7));
		previous_no_obj -= no_nulls;
		int header_size;

		if(tmp_mode >= 8 && tmp_mode <=253) {
			ver = tmp_mode;
			uchar head[10];
			buf_used = fcurfile->ReadExact(head, 10);
			optimal_mode = head[0];
			decomposer_id = *((uint*)(head + 1));
			no_groups = head[5];
			data_full_byte_size = *((uint*)(head + 6));
			header_size = 19;
		} else {
			ver = 0;
			optimal_mode = tmp_mode;
			uchar head[4];
			buf_used = fcurfile->ReadExact(head, 4);
			data_full_byte_size = *((uint*)(head));
			header_size = 13;
		}
		if(!IsModeValid()) {
			rclog << lock << "Error: Unexpected byte in data pack (AttrPackS)." << unlock;
			throw DatabaseRCException("Unexpected byte in data pack (AttrPackS).");
		} else {
			if(IsModeNoCompression()) {
				LoadUncompressed(fcurfile);
			} else {
				// normal compression
				index = 0;
				if(IsModeCompressionApplied()) // 0,1	= Huffmann 16-bit compression of remainings with tree (0 - flat null encoding, 1 - compressed nulls)
				{
					comp_buf_size = total_size - header_size;
					dealloc(compressed_buf);
					compressed_buf = 0;
					compressed_buf = (uchar*) alloc((comp_buf_size + 1) * sizeof(uchar), BLOCK_COMPRESSED);

					if(!compressed_buf) {
						rclog << lock << "Error: out of memory (" << comp_buf_size + 1 << " bytes failed). (32)" << unlock;
						throw OutOfMemoryRCException();
					}
					buf_used = fcurfile->ReadExact((char*)compressed_buf, comp_buf_size);
				} else {
					BHASSERT(0, "The process reached to an invalid code path! (LoadData)");
				}

				compressed_up_to_date = true;
				saved_up_to_date = true;
			}
		}
		max_no_obj = no_obj;
		is_empty = false;
#ifdef FUNCTIONS_EXECUTION_TIMES
		NoBytesReadByDPs += total_size;
#endif
	} catch (DatabaseRCException&) {
		Unlock();
		throw;
	}

	Unlock();
	//std::cerr << "Optimal mode: " << optimal_mode << std::endl;
}

void AttrPackS::Destroy()
{
	if(data) {
		int ds = (int) rc_msize(data) / sizeof(char*);
		for (int i = 0; i < ds && i < data_id; i++) {
			dealloc(data[i]);
			data[i] = 0;
		}
		dealloc(data);
		data = 0;
	}

	dealloc(index);
	index = 0;

	dealloc(lens);
	lens = 0;
	is_empty = true;

	dealloc(nulls);
	dealloc(compressed_buf);
	compressed_buf = 0;
	nulls = 0;
	Instance()->AssertNoLeak(this);
	//BHASSERT(m_sizeAllocated == 0, "TrackableObject accounting failure");
}

AttrPackS::~AttrPackS()
{
	DestructionLock();
	Destroy();
}

void AttrPackS::Construct()
{
	data = 0;
	index = 0;
	lens = 0;
	max_no_obj = no_obj = 0;
	compressed_up_to_date = false;
	saved_up_to_date = false;
	no_groups = 1;

	if(attr_type == RC_BIN)
		len_mode = sizeof(uint);
	else
		len_mode = sizeof(ushort);
	//cout << "len_mode = " << len_mode << endl;

	last_copied = -1;
	binding = false;
	nulls = 0;
	no_nulls = 0;
	data_full_byte_size = 0;
	compressed_buf = 0;
	data_id = 0;
	optimal_mode = 0;
	comp_buf_size = comp_null_buf_size = comp_len_buf_size = 0;
}

void AttrPackS::Expand(int no_obj)
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	BHASSERT(this->no_obj + no_obj <= MAX_NO_OBJ, "Expression evaluation failed on MAX_NO_OBJ!");

	uint new_max_no_obj = this->no_obj + no_obj;
	index = (uchar**)rc_realloc(index, new_max_no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
	lens = rc_realloc(lens, len_mode * new_max_no_obj * sizeof(char), BLOCK_UNCOMPRESSED);

	memset((char*)lens + (this->no_obj * len_mode), 0, (no_obj * len_mode));

	int cs = data ? (int)rc_msize(data) : 0;
	data = (uchar**)rc_realloc(data, cs + (no_obj * sizeof(uchar*)), BLOCK_UNCOMPRESSED);

	memset((char*)data + cs, 0, no_obj * sizeof(uchar*));
	cs /= sizeof(uchar*);

	for(uint o = this->no_obj; o < new_max_no_obj; o++)
		index[o] = data[cs++];

	max_no_obj = new_max_no_obj;
}

void AttrPackS::BindValue(bool null, uchar* value, uint size)
{
	is_empty = false;
	if(!binding) {
		binding = true;
		last_copied = (int) no_obj - 1;
	}

	if(null) {
		SetNull(no_obj);
		index[no_obj] = 0;
	} else if(size == 0) {
		SetSize(no_obj, 0);
		index[no_obj] = 0;
	} else {
		data_full_byte_size += size;
		SetSize(no_obj, size);
		index[no_obj] = value;
	}
	no_obj++;
	compressed_up_to_date = false;
}

void AttrPackS::CopyBinded()
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	if(binding) {
		int size_sum = 0;
		for(uint i = last_copied + 1; i < no_obj; i++)
			size_sum += GetSize(i);
		if(size_sum > 0) {
			data[data_id] = (uchar*)alloc(size_sum, BLOCK_UNCOMPRESSED);
			size_sum = 0;
			for(uint i = last_copied + 1; i < no_obj; i++) {
				int size = GetSize(i);
				if(!IsNull(i) && size != 0) {
					uchar* value = (uchar*&)index[i];
					memcpy(data[data_id] + size_sum, value, size);
					index[i] = data[data_id] + size_sum;
					size_sum += size;
				} else
					index[i] = 0;
			}
		} else
			data[data_id] = 0;
		data_id++;
	}
	last_copied = no_obj -1;
	binding = false;
}


void AttrPackS::SetNull(int ono)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
	if(nulls == 0) {
		nulls = (uint*) alloc(8192, BLOCK_UNCOMPRESSED);
		memset(nulls, 0, 8192);
	}
	// x & 31 == x % 32
	int mask = (uint) (1) << (ono & 31);
	if((nulls[ono >> 5] & mask) == 0) {
		nulls[ono >> 5] |= mask; // set the i-th bit of the table
		no_nulls++;
	} else {
		BHASSERT(0, "Expression evaluation failed!");
	}
}

void AttrPackS::SetSize(int ono, uint size)
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	if(len_mode == sizeof(ushort))
		((ushort*)lens)[ono] = (ushort)size;
	else
		((uint*)lens)[ono] = (uint)size;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

void AttrPackS::Uncompress(DomainInjectionManager& dim)
{
	switch (ver) {
	case 0:
		UncompressOld();
		break;
	case 8:
		Uncompress8(dim);
		break;
	default:
		rclog << lock << "ERROR: wrong version of data pack format" << unlock;
		BHASSERT(0, "ERROR: wrong version of data pack format");
		break;
	}
}

void AttrPackS::AllocNullsBuffer()
{
	if(no_nulls > 0) {
		if(!nulls)
			nulls = (uint*) alloc(2048 * sizeof(uint), BLOCK_UNCOMPRESSED);
		if(no_obj < 65536)
			memset(nulls, 0, 8192);
	}
}

void AttrPackS::AllocBuffersButNulls()
{
	if(data == NULL) {
		data = (uchar**) alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
		*data = 0;
		if(data_full_byte_size) {
			*data = (uchar*) alloc(data_full_byte_size, BLOCK_UNCOMPRESSED);
			if(!*data)
				rclog << lock << "Error: out of memory (" << data_full_byte_size << " bytes failed). (40)" << unlock;
		}
		data_id = 1;
	}
	assert(!lens && !index);
	lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
	index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
}

/*void AttrPackS::Uncompress8()
{
	AllocNullsBuffer();
	uint *cur_buf = (uint*) compressed_buf;
	// decode nulls
	uint null_buf_size = 0;
	if(no_nulls > 0) {
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
			BitstreamCompressor bsc;
			bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
			// For tests:
			uint nulls_counted = 0;
			for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				rclog << lock << "Error: AttrPackT::Uncompress uncompressed wrong number of nulls." << unlock;
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
	}

	cur_buf = (uint*) ((char*) cur_buf + no_groups);  // one compression type so far so skip
	uint* block_lengths = cur_buf;						// lengths of blocks
	cur_buf += no_groups;
	std::vector<ushort*> lengths;
	std::vector<char*> data_blocks;
	std::vector<char**> indexes;
	NumCompressor<ushort> nc;
	TextCompressor tc;

	for (int g=0; g<no_groups; g++) {
		uint block_data_size = *cur_buf;
		uint block_comp_len_buf_size = *(cur_buf+1);
		ushort* lens = (ushort*) alloc((1 << 16) * sizeof(ushort), BLOCK_TEMPORARY);
		if((_uint64) * (cur_buf + 2) != 0) {
			// decompress lenghts
			nc.Decompress(lens, (char*) (cur_buf + 3), block_comp_len_buf_size, no_obj - no_nulls, *(uint*) (cur_buf + 2));

			// decompress data
			char* block_data = (char*)alloc(block_data_size, BLOCK_TEMPORARY);
			char** block_index = (char**)alloc((no_obj-no_nulls) * sizeof(char*), BLOCK_TEMPORARY);
			cur_buf = (uint*) ((char*) (cur_buf) + 12 + block_comp_len_buf_size);
			int zlo = 0;
			for(uint obj = 0; obj < no_obj-no_nulls; obj++)
				if(lens[obj] == 0)
					zlo++;
			int objs = no_obj - no_nulls - zlo;
			if(objs)
				tc.Decompress(block_data, block_data_size, (char*)cur_buf, block_lengths[g]-12-block_comp_len_buf_size, block_index, lens, objs);
			cur_buf = (uint*)((char*)cur_buf + (block_lengths[g]-12-block_comp_len_buf_size));

			lengths.push_back(lens);
			data_blocks.push_back(block_data);
			indexes.push_back(block_index);
		}
		else {
			ushort* tmp = lens;
			for (uint o = 0; o < no_obj - no_nulls; o++)
				*(tmp++) = 0;
		}
		data_full_byte_size += block_data_size;

	}

	// merge blocks
	AllocBuffersButNulls();
	uchar* cur_pos = *data;
	uint* index_ids = new uint[no_groups];
	for (int g=0; g<no_groups; g++)
		index_ids[g] = 0;
	int oid = 0;
	for (int o=0; o<no_obj; o++)
		if (!IsNull(o)) {
			index[o] = cur_pos;
			uint len = 0;
			for (int g=0; g<no_groups; g++)
				if (lengths[g][oid]>0) {
					memcpy(cur_pos, indexes[g][index_ids[g]++], lengths[g][oid]);
					cur_pos += lengths[g][oid];
					len += lengths[g][oid];
				}
			SetSize(o, len);
			if (len==0)
				index[o] = NULL;
			oid++;
		}

	// free temp buffers
	delete[] index_ids;
	for (int g=0; g<no_groups; g++) {
		dealloc(indexes[g]);
		dealloc(data_blocks[g]);
		dealloc(lengths[g]);
	}

	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;
}*/


void AttrPackS::Uncompress8(DomainInjectionManager& dim)
{
	DomainInjectionDecomposer& decomposer = dim.Get(decomposer_id);	
	AllocNullsBuffer();
	uint *cur_buf = (uint*) compressed_buf;
	// decode nulls
	uint null_buf_size = 0;
	if(no_nulls > 0) {
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
			BitstreamCompressor bsc;
			CprsErr res = bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
			if(res != CPRS_SUCCESS) {
				std::stringstream msg_buf;
				msg_buf << "Decompression of nulls failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw DatabaseRCException(msg_buf.str());
			}
			// For tests:
			uint nulls_counted = 0;
			for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				rclog << lock << "Error: AttrPackT::Uncompress uncompressed wrong number of nulls." << unlock;
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
	}

	char*	block_type = (char*) cur_buf;
	cur_buf = (uint*) ((char*) cur_buf + no_groups);  // one compression type so far so skip
	uint* block_lengths = cur_buf;						// lengths of blocks
	cur_buf += no_groups;

	//NumCompressor<ushort> nc;
	//TextCompressor tc;
	
	std::vector<boost::shared_ptr<DataBlock> > blocks;

	char* cur_bufc = (char*) cur_buf;	
	for (int g=0; g<no_groups; g++) {
		int bl_nobj = *(int*) cur_bufc;
		boost::shared_ptr<DataBlock> block;
		switch(*block_type) {
			case BLOCK_BINARY	:	
				block = boost::shared_ptr<DataBlock>(new BinaryDataBlock(bl_nobj));
				break;
			case BLOCK_NUMERIC_UCHAR	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<uchar>(bl_nobj));				
				break;
			case BLOCK_NUMERIC_USHORT	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<ushort>(bl_nobj));				
				break;
			case BLOCK_NUMERIC_UINT	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<uint>(bl_nobj));				
				break;
			case BLOCK_NUMERIC_UINT64	:
				block = boost::shared_ptr<DataBlock>(new NumericDataBlock<uint64>(bl_nobj));				
				break;
			case BLOCK_STRING	:
				block = boost::shared_ptr<DataBlock>(new StringDataBlock(bl_nobj));				
				break;
			default:
				BHASSERT(0, "Wrong data block type in decomposed data pack");
				break;
		}
		block->Decompress((char*) cur_bufc, *block_lengths);
		blocks.push_back(block);
		cur_bufc += (*block_lengths);
		block_lengths++;
		block_type++;
	}

	//data_full_byte_size = decomposer.GetComposedSize(blocks);
	AllocBuffersButNulls();

	StringDataBlock block_out(no_obj - no_nulls);
	decomposer.Compose(blocks, block_out, *((char**)(data)), data_full_byte_size, outliers);
	
	uint oid = 0;
	for(uint o = 0; o < no_obj; o++) 		
		if (!IsNull(o)) {
			index[o] = (uchar*) block_out.GetIndex(oid);
			SetSize(o, block_out.GetLens(oid));
			oid++;
		} else {
			SetSize(o, 0);
			index[o] = 0;
			//lens[o] = 0;
		}	

	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;
}

void AttrPackS::UncompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackS::Uncompress()");
	FETOperator feto1(string("aS[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
	NotifyDataPackDecompression(GetPackCoordinate());
#endif
	if(IsModeNoCompression())
		return;

	is_only_compressed = false;
	if(optimal_mode == 0 && ATI::IsBinType(attr_type)) {
		rclog << lock << "Error: Compression format no longer supported." << unlock;
		return;
	}

	AllocBuffers();
	MMGuard<char*> tmp_index((char**)alloc(no_obj * sizeof(char*), BLOCK_TEMPORARY), *this);
	////////////////////////////////////////////////////////
	int i; //,j,obj_317,no_of_rep,rle_bits;
	uint *cur_buf = (uint*) compressed_buf;

	///////////////////////////////////////////////////////
	// decode nulls
	char (*FREE_PLACE)(reinterpret_cast<char*> (-1));

	uint null_buf_size = 0;
	if(no_nulls > 0) {
		null_buf_size = (*(ushort*) cur_buf);
		if(!IsModeNullsCompressed()) // flat null encoding
			memcpy(nulls, (char*) cur_buf + 2, null_buf_size);
		else {
			BitstreamCompressor bsc;
			CprsErr res = bsc.Decompress((char*) nulls, null_buf_size, (char*) cur_buf + 2, no_obj, no_nulls);
			if(res != CPRS_SUCCESS) {
				std::stringstream msg_buf;
				msg_buf << "Decompression of nulls failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw DatabaseRCException(msg_buf.str());
			}
			// For tests:
			uint nulls_counted = 0;
			for(i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				rclog << lock << "Error: AttrPackT::Uncompress uncompressed wrong number of nulls." << unlock;
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
		for(i = 0; i < (int) no_obj; i++) {
			if(IsNull(i))
				tmp_index[i] = NULL; // i.e. nulls
			else
				tmp_index[i] = FREE_PLACE; // special value: an object awaiting decoding
		}
	} else
		for(i = 0; i < (int) no_obj; i++)
			tmp_index[i] = FREE_PLACE;

	///////////////////////////////////////////////////////
	//	<null_buf_size><nulls><lens><char_lib_size><huffmann_size><huffmann><rle_bits><obj>...<obj>
	if(optimal_mode == 0) {
		rclog << lock << "Error: Compression format no longer supported." << unlock;
	} else {
		comp_len_buf_size = *cur_buf;
		if((_uint64) * (cur_buf + 1) != 0) {
			NumCompressor<uint> nc;
			MMGuard<uint> cn_ptr((uint*) alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);
			CprsErr res = nc.Decompress(cn_ptr.get(), (char*) (cur_buf + 2), comp_len_buf_size - 8, no_obj - no_nulls, *(uint*) (cur_buf + 1));
			if(res != CPRS_SUCCESS) {
				std::stringstream msg_buf;
				msg_buf << "Decompression of lengths of string values failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw DatabaseRCException(msg_buf.str());
			}

			int oid = 0;
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetSize(o, (uint) cn_ptr[oid++]);
		} else {
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetSize(o, 0);
		}

		cur_buf = (uint*) ((char*) (cur_buf) + comp_len_buf_size);
		TextCompressor tc;
		int dlen = *(int*) cur_buf;
		cur_buf += 1;
		int zlo = 0;
		for(uint obj = 0; obj < no_obj; obj++)
			if(!IsNull(obj) && GetSize(obj) == 0)
				zlo++;
		int objs = no_obj - no_nulls - zlo;

		if(objs) {
			MMGuard<ushort> tmp_len((ushort*) alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);
			for(uint tmp_id = 0, id = 0; id < no_obj; id++)
				if(!IsNull(id) && GetSize(id) != 0)
					tmp_len[tmp_id++] = GetSize(id);
			CprsErr res = tc.Decompress((char*)*data, data_full_byte_size, (char*) cur_buf, dlen, tmp_index.get(), tmp_len.get(), objs);
			if(res != CPRS_SUCCESS) {
				std::stringstream msg_buf;
				msg_buf << "Decompression of string values failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw DatabaseRCException(msg_buf.str());
			}
		}
	}

	for(uint tmp_id = 0, id = 0; id < no_obj; id++) {
		if(!IsNull(id) && GetSize(id) != 0)
			index[id] = (uchar*) tmp_index[tmp_id++];
		else {
			SetSize(id, 0);
			index[id] = 0;
		}
		if(optimal_mode == 0)
			tmp_id++;
	}

	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;
#ifdef FUNCTIONS_EXECUTION_TIMES
	SizeOfUncompressedDP += (1 + rc_msize(*data) + rc_msize(nulls) + rc_msize(lens) + rc_msize(index));
#endif
}

//void AttrPackS::Decompose(uint& size, std::vector<uchar**>& indexes, std::vector<ushort*>& lengths, std::vector<uint>& maxes, std::vector<uint>& sumlens)
//{
//	no_groups = 2;
//	size = no_obj - no_nulls;
//	uchar** index1 = (uchar**) alloc(size * sizeof(uchar*), BLOCK_TEMPORARY);
//	ushort* lens1 = (ushort*) alloc(size * sizeof(ushort), BLOCK_TEMPORARY);
//	uchar** index2 = (uchar**) alloc(size * sizeof(uchar*), BLOCK_TEMPORARY);
//	ushort* lens2 = (ushort*) alloc(size * sizeof(ushort), BLOCK_TEMPORARY);
//	int onn = 0;
//	uint maxv = 0;
//	uint cv = 0;
//	uint sumlens1 =0, sumlens2 = 0;
//	for(uint o = 0; o < no_obj; o++) {
//		if(!IsNull(o)) {
//			cv = GetSize(o);
//			index1[onn] = index[o];
//			lens1[onn] = (cv+1)/2;
//			index2[onn] = index1[onn]+lens1[onn];
//			lens2[onn] = cv/2;
//			sumlens1 += lens1[onn];
//			sumlens2 += lens2[onn];
//			if(cv > maxv)
//				maxv = cv;
//			onn++;
//		}
//	}
//	assert(onn==size);
//	indexes.push_back(index1);
//	indexes.push_back(index2);
//	lengths.push_back(lens1);
//	lengths.push_back(lens2);
//	maxes.push_back((maxv+1)/2);
//	maxes.push_back(maxv/2);
//	sumlens.push_back(sumlens1);
//	sumlens.push_back(sumlens2);
//}

CompressionStatistics AttrPackS::Compress(DomainInjectionManager& dim)
{
	if (!use_already_set_decomposer) {
		if (dim.HasCurrent()) {
			ver = 8;
			decomposer_id = dim.GetCurrentId();
		} else {
			ver = 0;
			decomposer_id = 0;
		}
	}

	switch (ver) {
		case 0:
			return CompressOld();
		case 8:
			return Compress8(dim.Get(decomposer_id));
		default:
			rclog << lock << "ERROR: wrong version of data pack format" << unlock;
			BHASSERT(0, "ERROR: wrong version of data pack format");
			break;
	}
	return CompressionStatistics();
}

CompressionStatistics AttrPackS::Compress8(DomainInjectionDecomposer& decomposer)
{
	MEASURE_FET("AttrPackS::Compress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aS[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	//SystemInfo si;
	//uint nopf_start = SystemInfo::NoPageFaults();
	//uint nopf_end = 0;

	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress nulls:

	comp_len_buf_size = comp_null_buf_size = comp_buf_size = 0;
	MMGuard<uchar> comp_null_buf;
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((comp_null_buf_size + 2) * sizeof(uchar), BLOCK_TEMPORARY), *this);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
			BitstreamCompressor bsc;
			CprsErr res = bsc.Compress((char*) comp_null_buf.get(), comp_null_buf_size, (char*) nulls, no_obj, no_nulls);
			if(comp_null_buf[cnbl] != 0xBA) {
				rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (T f)." << unlock;
				BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (T f)!");
			}
			if(res == CPRS_SUCCESS)
				SetModeNullsCompressed();
			else if(res == CPRS_ERR_BUF) {
				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} else {
				std::stringstream msg_buf;
				msg_buf << "Compression of nulls failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw InternalRCException(msg_buf.str());
			}
		}
	}
	comp_buf_size += (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0);

	// Decomposition
	StringDataBlock wholeData(no_obj-no_nulls);
	for(uint o = 0; o < no_obj; o++)
		if(!IsNull(o))
			wholeData.Add(GetVal(o), GetSize(o));
	std::vector<boost::shared_ptr<DataBlock> > decomposedData;
	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_outliers = stats.previous_no_outliers = outliers;
	decomposer.Decompose(wholeData, decomposedData, stats);
	outliers = stats.new_no_outliers;

	no_groups = ushort(decomposedData.size());
	comp_buf_size += 5 * no_groups; // types of blocks conversion + size of compressed block
	MMGuard<uint> comp_buf_size_block((uint*)alloc(no_groups * sizeof(uint), BLOCK_TEMPORARY), *this);

	// Compression
	for(int b = 0; b < no_groups; b++) {
		decomposedData[b]->Compress(comp_buf_size_block.get()[b]);
		comp_buf_size += comp_buf_size_block.get()[b];
		// TODO: what about setting comp_len_buf_size???
	}

	// Allocate and fill the compressed buffer
	MMGuard<uchar> new_compressed_buf((uchar*) alloc(comp_buf_size * sizeof(uchar), BLOCK_COMPRESSED), *this);
	char* p = (char*)new_compressed_buf.get();

	////////////////////////////////	Nulls
	if(no_nulls > 0) {
		*((ushort*) p) = (ushort) comp_null_buf_size;
		p += 2;
		memcpy(p, comp_null_buf.get(), comp_null_buf_size);
		p += comp_null_buf_size;
	}

	////////////////////////////////	Informations about blocks
	for(int b = 0; b < no_groups; b++)
		*(p++) = decomposedData[b]->GetType();		// types of block's compression
	memcpy(p, comp_buf_size_block.get(), no_groups * sizeof(uint)); // compressed block sizes
	p += no_groups * sizeof(uint);
	
	for(int b = 0; b < no_groups; b++) {
		decomposedData[b]->StoreCompressedData(p, comp_buf_size_block.get()[b]);
		p += comp_buf_size_block.get()[b];
	}

	dealloc(compressed_buf);
	compressed_buf = new_compressed_buf.release();

	SetModeDataCompressed();
	compressed_up_to_date = true;
	return stats;
}

CompressionStatistics AttrPackS::CompressOld()
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aS[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	//SystemInfo si;
	//uint nopf_start = SystemInfo::NoPageFaults();
	//uint nopf_end = 0;

	//////////////////////////////////////////////////////////////////////////////////////////////////
	// Compress nulls:

	comp_len_buf_size = comp_null_buf_size = comp_buf_size = 0;
	MMGuard<uchar> comp_null_buf;
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			comp_null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf_size = ((no_obj + 7) / 8);
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((comp_null_buf_size + 2) * sizeof(uchar), BLOCK_TEMPORARY), *this);

			uint cnbl = comp_null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
			BitstreamCompressor bsc;
			CprsErr res = bsc.Compress((char*) comp_null_buf.get(), comp_null_buf_size, (char*) nulls, no_obj, no_nulls);
			if(comp_null_buf[cnbl] != 0xBA) {
				rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (T f)." << unlock;
				BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (T f)!");
			}
			if(res == CPRS_SUCCESS)
				SetModeNullsCompressed();
			else if(res == CPRS_ERR_BUF) {
				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				comp_null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} else {
				std::stringstream msg_buf;
				msg_buf << "Compression of nulls failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw InternalRCException(msg_buf.str());
			}
		}
	}


	MMGuard<uint> comp_len_buf;

	NumCompressor<uint> nc(ShouldNotCompress());
	MMGuard<uint> nc_buffer((uint*)alloc((1 << 16) * sizeof(uint), BLOCK_TEMPORARY), *this);

	int onn = 0;
	uint maxv = 0;
	uint cv = 0;
	for(uint o = 0; o < no_obj; o++) {
		if(!IsNull(o)) {
			cv = GetSize(o);
			*(nc_buffer.get() + onn++) = cv;
			if(cv > maxv)
				maxv = cv;
		}
	}

	if(maxv != 0) {
		comp_len_buf_size = onn * sizeof(uint) + 28;
		comp_len_buf = MMGuard<uint>((uint*)alloc(comp_len_buf_size / 4 * sizeof(uint), BLOCK_TEMPORARY), *this);
		uint tmp_comp_len_buf_size = comp_len_buf_size - 8;
		CprsErr res = nc.Compress((char*)(comp_len_buf.get() + 2), tmp_comp_len_buf_size, nc_buffer.get(), onn, maxv);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Compression of lengths of string values failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
			throw InternalRCException(msg_buf.str());
		}
		comp_len_buf_size = tmp_comp_len_buf_size + 8;
	} else {
		comp_len_buf_size = 8;
		comp_len_buf = MMGuard<uint>((uint*) alloc(sizeof(uint) * 2, BLOCK_TEMPORARY), *this);
	}

	*comp_len_buf.get() = comp_len_buf_size;
	*(comp_len_buf.get() + 1) = maxv;

	TextCompressor tc;
	int zlo = 0;
	for(uint obj = 0; obj < no_obj; obj++)
		if(!IsNull(obj) && GetSize(obj) == 0)
			zlo++;
	int dlen = data_full_byte_size + 10;
	MMGuard<char> comp_buf((char*)alloc(dlen * sizeof(char), BLOCK_TEMPORARY), *this);

	if(data_full_byte_size) {
		int objs = (no_obj - no_nulls) - zlo;

		MMGuard<char*> tmp_index((char**)alloc(objs * sizeof(char*), BLOCK_TEMPORARY), *this);
		MMGuard<ushort> tmp_len((ushort*)alloc(objs * sizeof(ushort), BLOCK_TEMPORARY), *this);

		int nid = 0;
		for(int id = 0; id < (int) no_obj; id++) {
			if(!IsNull(id) && GetSize(id) != 0) {
				tmp_index[nid] = (char*) index[id];
				tmp_len[nid++] = GetSize(id);
			}
		}

		CprsErr res = tc.Compress(comp_buf.get(), dlen, tmp_index.get(), tmp_len.get(), objs, ShouldNotCompress() ? 0 : TextCompressor::VER);
		if(res != CPRS_SUCCESS) {
			std::stringstream msg_buf;
			msg_buf << "Compression of string values failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
			throw InternalRCException(msg_buf.str());
		}

	} else {
		dlen = 0;
	}

	comp_buf_size = (comp_null_buf_size > 0 ? 2 + comp_null_buf_size : 0) + comp_len_buf_size + 4 + dlen;

	MMGuard<uchar> new_compressed_buf((uchar*) alloc(comp_buf_size * sizeof(uchar), BLOCK_COMPRESSED), *this);
	uchar* p = new_compressed_buf.get();

	////////////////////////////////Nulls
	if(no_nulls > 0) {
		*((ushort*) p) = (ushort) comp_null_buf_size;
		p += 2;
		memcpy(p, comp_null_buf.get(), comp_null_buf_size);
		p += comp_null_buf_size;
	}

	////////////////////////////////	Lens

	if(comp_len_buf_size)
		memcpy(p, comp_len_buf.get(), comp_len_buf_size);

	p += comp_len_buf_size;

	*((int*) p) = dlen;
	p += sizeof(int);
	if(dlen)
		memcpy(p, comp_buf.get(), dlen);

	dealloc(compressed_buf);
	compressed_buf = new_compressed_buf.release();

	SetModeDataCompressed();
	compressed_up_to_date = true;
	CompressionStatistics stats;
	stats.previous_no_obj = previous_no_obj;
	stats.new_no_obj = NoObjs();
	return stats;
}

void AttrPackS::StayCompressed()
{
	if(!compressed_up_to_date)
		saved_up_to_date = false;
	BHASSERT(compressed_buf!=NULL && compressed_up_to_date, "Compression not up-to-date in StayCompressed");


	if(data) {
		int ds = (int)rc_msize(data) / sizeof(char*);
		for(int i = 0; i < ds && i < data_id; i++)
			dealloc(data[i]);
		dealloc(data);
		data = 0;
	}

	dealloc(nulls);
	dealloc(index);
	dealloc(lens);
	index = 0;
	lens = 0;
	nulls = 0;
	is_only_compressed = true;
}

//int AttrPackS::Save(IBFile* fcurfile)
//{
//	if(compressed_buf==NULL || !compressed_up_to_date)
//	{
//		Compress();
//	}
//	uint total_size=0;
//	uchar head[13];
//
//	head[4]=optimal_mode;
//	if(!IsModePackVirtual())
//	{
//		*((ushort*)(head+5))=(ushort)(no_obj-1);
//		*((ushort*)(head+7))=(ushort)(no_nulls);
//	}
//	else
//		*((uint *)(head+5))=0;
//
//	*((uint*)(head+9))=data_full_byte_size;
//	//EnterCriticalSection(&rc_save_pack_cs);
//	if(IsModeCompressionApplied())
//	{
//		total_size = 13 + comp_buf_size ;
//		*((uint*)head)=total_size;
//		fcurfile->Write((char*)head,13);
//		fcurfile->Write((char*)compressed_buf, comp_buf_size);
//	}
//	else if(IsModePackVirtual())		// optimal_mode==255, 254
//	{
//		total_size=13;
//		*((uint*)head)=total_siz;e
//		fcurfile->Write((char*)head,13);
//	} else if(IsModeNoCompression()) {
//		SaveUncompressed(head, fcurfile);
//	} else {
//		assert("unexpected PackS mode");
//		rclog << lock << "unexpected PackS mode " << optimal_mode << " << unlock";
//	}
//	//LeaveCriticalSection(&rc_save_pack_cs);
//	previous_size = TotalSaveSize();
//	saved_up_to_date = true;
//	return 0;
//}

int AttrPackS::Save(IBStream* fcurfile, DomainInjectionManager& dim)
{
	MEASURE_FET("AttrPackS::Save(...)");
	if(compressed_buf == NULL || !compressed_up_to_date) {
		Compress(dim);
	}
	uint total_size = 0;
	uchar head[19];
	int head_size;
	switch(ver) {
		case 0:
			head_size = 13;
			head[4] = optimal_mode;
			break;
		case 8:
			head_size = 19;
			head[4] = 8; // version
			break;
		default:
			rclog << lock << "ERROR: wrong version of data pack format" << unlock;
			BHASSERT(0, "ERROR: wrong version of data pack format");
			break;
	}

	*((ushort*) (head + 5)) = (ushort) (no_obj - 1);
	*((ushort*) (head + 7)) = (ushort) (no_nulls);

	switch(ver) {
		case 0:
			*((uint*) (head + 9)) = data_full_byte_size;
			break;
		case 8:
			head[9] = optimal_mode;
			*((uint *) (head + 10)) = decomposer_id;
			head[14] = (uchar)no_groups;
			*((uint *) (head + 15)) = data_full_byte_size;
			break;
	}

	if(IsModeCompressionApplied()) {
		total_size = head_size + comp_buf_size;
		*((uint*) head) = total_size;
		fcurfile->WriteExact((char*) head, head_size);
		fcurfile->WriteExact((char*) compressed_buf, comp_buf_size);
	} else if(IsModeNoCompression()) {
		SaveUncompressed(head, fcurfile);
	} else {
		assert("unexpected PackS mode");
		rclog << lock << "unexpected PackS mode " << optimal_mode << " << unlock";
	}
	previous_size = TotalSaveSize();
	saved_up_to_date = true;
	previous_no_obj = no_obj-no_nulls;
	return 0;
}

uint AttrPackS::TotalSaveSize()
{
	uint tot = 13;
	if(ver == 8)
		tot = 19;
	if(this->IsModeCompressionApplied())
		tot += comp_buf_size;
	else if(IsModeNoCompression()) {
		uint string_size = 0;
		for(uint i = 0; i < no_obj; i++) {
			if(!IsNull(i))
				string_size += GetSize(i);
		}
		tot += (data ? string_size : 0) + (nulls ? (no_obj + 7) / 8 : 0) + (lens ? len_mode * no_obj : 0);
	}

	return tot;
}

void AttrPackS::SaveUncompressed(uchar* head, IBStream* fcurfile)
{
	uint total_size = 0;
	uint string_size = 0;
	for(uint i = 0; i < no_obj; i++) {
		if(!IsNull(i))
			string_size += GetSize(i);
	}
	total_size = TotalSaveSize();
	*((uint*) head) = total_size;
	fcurfile->WriteExact((char*) head, 13);
	if(data) {
		MMGuard<char> tmpbuf((char*)alloc(string_size * sizeof(char), BLOCK_TEMPORARY), *this);
		char* tmpptr = tmpbuf.get();
		for(uint i = 0; i < no_obj; i++) {
			if(!IsNull(i)) {
				memcpy(tmpptr, GetVal(i), GetSize(i));
				tmpptr += GetSize(i);
			}
		}
		fcurfile->WriteExact(tmpbuf.get(), string_size);
	}

	if(nulls)
		fcurfile->WriteExact((char*) nulls, (no_obj + 7) / 8);
	if(lens)
		fcurfile->WriteExact((char*) lens, len_mode * no_obj);
}

void AttrPackS::LoadUncompressed(IBStream* fcurfile)
{
	AllocBuffers();
	if(data)
		fcurfile->ReadExact((char*) *data, data_full_byte_size);
	if(nulls)
		fcurfile->ReadExact((char*) nulls, (no_obj + 7) / 8);
	if(lens)
		fcurfile->ReadExact((char*) lens, len_mode * no_obj);
	int sumlen = 0;
	for(uint i = 0; i < no_obj; i++) {
		if(!IsNull(i) && GetSize(i) != 0) {
			index[i] = ((uchar*) (*data)) + sumlen;
			sumlen += GetSize(i);
		} else {
			SetSize(i, 0);
			index[i] = 0;
		}
	}
}

void AttrPackS::AllocBuffers()
{
	if(data == NULL) {
		data = (uchar**) alloc(sizeof(uchar*), BLOCK_UNCOMPRESSED);
		*data = 0;
		if(data_full_byte_size) {
			*data = (uchar*) alloc(data_full_byte_size, BLOCK_UNCOMPRESSED);
			if(!*data)
				rclog << lock << "Error: out of memory (" << data_full_byte_size << " bytes failed). (40)" << unlock;
		}
		data_id = 1;
	}
	assert(!lens && !index);
	lens = alloc(len_mode * no_obj * sizeof(char), BLOCK_UNCOMPRESSED);
	index = (uchar**)alloc(no_obj * sizeof(uchar*), BLOCK_UNCOMPRESSED);
	if(no_nulls > 0) {
		if(!nulls)
			nulls = (uint*) alloc(2048 * sizeof(uint), BLOCK_UNCOMPRESSED);
		if(no_obj < 65536)
			memset(nulls, 0, 8192);
	}
}

