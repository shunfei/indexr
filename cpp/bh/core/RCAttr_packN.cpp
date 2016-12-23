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
#include "compress/NumCompressor.h"
#include "WinTools.h"
#include "system/MemoryManagement/MMGuard.h"

using namespace std;

AttrPackN::AttrPackN(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner)
	:	AttrPack(pc, attr_type, inserting_mode, false, owner), data_full(0)
{
	no_obj = 0;
	no_nulls = 0;
	max_val = 0;
	bit_rate = 0;
	value_type = UCHAR;
	comp_buf_size = 0;
	compressed_buf = 0;
	nulls = 0;
	compressed_up_to_date = false;
	saved_up_to_date = false;
	optimal_mode = 0;
}


AttrPackN::AttrPackN(const AttrPackN &apn)
	:	AttrPack(apn)
{
	assert(!is_only_compressed);
	value_type = apn.value_type;
	bit_rate = apn.bit_rate;
	max_val = apn.max_val;
	optimal_mode = apn.optimal_mode;
	data_full = 0;
	if(no_obj && apn.data_full) {
		data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);
		memcpy(data_full, apn.data_full, value_type * no_obj);
	}
}

std::auto_ptr<AttrPack> AttrPackN::Clone() const
{
	return std::auto_ptr<AttrPack>(new AttrPackN(*this) );
}

AttrPackN::ValueType AttrPackN::ChooseValueType(int bit_rate)
{
	if(bit_rate <= 8)
		return UCHAR;
	else if(bit_rate <= 16)
		return USHORT;
	else if(bit_rate <= 32)
		return UINT;
	else
		return UINT64;
}

void AttrPackN::Prepare(uint new_no_obj, _uint64 new_max_val)
{
	Lock();
	this->no_obj=new_no_obj;
	//no_nulls = new_no_obj;
	this->no_nulls = 0;
	this->max_val=new_max_val;
	bit_rate = GetBitLen(max_val);
	data_full = 0;
	value_type = UCHAR;
	if(bit_rate) {
		value_type = ChooseValueType(bit_rate);
		if(new_no_obj)
			data_full = alloc(value_type * new_no_obj, BLOCK_UNCOMPRESSED);
	}
	comp_buf_size=0;
	compressed_buf=NULL;
	nulls=NULL;
	compressed_up_to_date = false;
	saved_up_to_date = false;
	optimal_mode = 0;
	is_empty = false;
	Unlock();
}

void AttrPackN::Destroy()
{
	dealloc(data_full);
	data_full = 0;
	is_empty = true;
	dealloc(nulls);
	dealloc(compressed_buf);
	compressed_buf = 0;
	nulls = 0;
}

AttrPackN::~AttrPackN()
{
	DestructionLock();
	Destroy();
}

void AttrPackN::SetNull(int i)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
	if(nulls == NULL) {
		nulls = (uint*) alloc(2048 * sizeof(uint), BLOCK_UNCOMPRESSED);
		memset(nulls, 0, 8192);
	}
	int mask = (uint) (1) << (i % 32);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT((nulls[i>>5] & mask)==0);
	nulls[i >> 5] |= mask; // set the i-th bit of the table
	no_nulls++;
}


////////////////////////////////////////////////////////////////////////////
//	Save format:
//
//	<total_byte_size>		- uint, the size of the data on disk (bytes), including header and dictionaries
//	<mode>					- uchar, see Compress method definition. Special values: 255 - nulls only, 254 - empty (0 objects)
//	<no_obj>				- ushort-1, i.e. "0" means 1 object
//	<no_nulls>				- ushort(undefined if nulls only)
//	<max_val>				- T_uint64, the maximal number to be encoded. E.g. 0 - only nulls and one more value.
//	<...data...>			- depending on compression mode

void AttrPackN::LoadData(IBStream* fcurfile)
	//: AttrPack(attr, pack_no)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto("AttrPackN::LoadData(...)");
	NotifyDataPackLoad(GetPackCoordinate());
#endif
	Lock();
	try {
		uint buf_used;
		comp_buf_size = comp_null_buf_size = comp_len_buf_size = 0;
		compressed_buf = 0;
		nulls = 0;
		data_full = 0;
		// read header of the pack.		WARNING: if the first 4 bytes are NOT going to indicate file size, change CheckPackFileSize in RCAttr !
		uchar head[17];
		uint total_size;
		buf_used = fcurfile->ReadExact(head, 17);
		if(buf_used < 17) {
			rclog << lock << "Error: Cannot load or wrong header of data pack (AttrPackN)." << unlock;
			throw DatabaseRCException("Cannot load or wrong header of data pack (AttrPackN).");
		}
		total_size = previous_size = *((uint*) head);
		optimal_mode = head[4];
		no_obj = no_nulls = 0;
		no_obj = *((ushort*) (head + 5));
		no_obj++;
		no_nulls = *((ushort*) (head + 7));
		max_val = *((_uint64*) (head + 9));

		bit_rate = CalculateBinSize(max_val);
		value_type = ChooseValueType(bit_rate);

		if(IsModeNoCompression()) {
			if(no_nulls) {
				nulls = (uint*) alloc(2048 * sizeof(uint), BLOCK_UNCOMPRESSED);
				if(no_obj < 65536)
					memset(nulls, 0, 8192);
				fcurfile->ReadExact((char*)nulls, (no_obj+7)/8);
			}
			if(bit_rate > 0 && value_type * no_obj) {
				data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);
				fcurfile->ReadExact((char*)data_full, value_type * no_obj);
			}
			compressed_up_to_date = false;
		} else {
			if(total_size < 17) {
				std::string msg("Wrong header of data pack in file: ");
				msg += fcurfile->name;
				rclog << lock << msg << unlock;
				throw DatabaseRCException(msg);
			}

			comp_buf_size = total_size - 17;
			compressed_buf = (uchar*) alloc((comp_buf_size + 1) * sizeof(uchar), BLOCK_COMPRESSED);

			if(!compressed_buf) {
				rclog << lock << "Error: out of memory (" << comp_buf_size + 1 << " bytes failed). (28)" << unlock;
				throw OutOfMemoryRCException();
			}
			buf_used = fcurfile->ReadExact((char*) compressed_buf, comp_buf_size);
			if(buf_used > comp_buf_size) {
				throw SystemRCException("File read error of data pack.");
			}
			compressed_up_to_date = true;
		}
		saved_up_to_date = true;
		is_empty = false;
#ifdef FUNCTIONS_EXECUTION_TIMES
		NoBytesReadByDPs += total_size;
#endif
	} catch (RCException&) {
		Unlock();
		throw;
	}
	Unlock();
}

int AttrPackN::Save(IBStream* fcurfile, DomainInjectionManager& dim)
{
	MEASURE_FET("AttrPackN::Save(...)");
	SetModeDataCompressed();
	if(compressed_buf == 0 || !compressed_up_to_date)
		Compress(dim);
	uint total_size = 0;
	uchar head[17];
	head[4] = optimal_mode;

	*((ushort*) (head + 5)) = (ushort) (no_obj - 1);
	*((ushort*) (head + 7)) = (ushort) no_nulls;
	*((_uint64*) (head + 9)) = max_val;
	//		 optimal_mode & 0x0F = 0000 for flat data, here or in Compress
	previous_size = total_size = TotalSaveSize();
	*((uint*) head) = total_size;
	//EnterCriticalSection(&rc_save_pack_cs);
	fcurfile->WriteExact((char*) head, 17);
	if(IsModeNoCompression()) {
		if(no_nulls)
			fcurfile->WriteExact((char*) nulls, (no_obj + 7) / 8);
		if(data_full)
			fcurfile->WriteExact((char*) data_full, value_type * no_obj);
	} else
		fcurfile->WriteExact((char*) compressed_buf, comp_buf_size);
	//LeaveCriticalSection(&rc_save_pack_cs);

	saved_up_to_date = true;
	return 0;
}

uint AttrPackN::TotalSaveSize()
{
	// header size
	uint total_size = 17;
//	if(!IsModeNullsCompressed() && no_nulls > 0)
//		total_size += (no_obj + 7) / 8 + 2;
////	if(optimal_mode % 16 == 0 && bit_rate > 0 && (optimal_mode & 0x20) == 0 && data_full)
//	if( !IsModeDataCompressed() && data_full)
//		total_size += (uint) rc_msize(data_full);
////	if((optimal_mode & 0x1F) != 0 || optimal_mode & 0x20)
	if(IsModeCompressionApplied())
		total_size += comp_buf_size; // including nulls
	else {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsModeNoCompression()); //other mode combinations not tested
		total_size += (no_nulls ? ((no_obj + 7) / 8 + 2) : 0) + (data_full ? value_type * no_obj : 0);
	}
	return total_size;
}


////////////////////////////////////////////////////////////////////////////


void AttrPackN::SetVal64(uint n, const _uint64 &val_code)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
	compressed_up_to_date = false;
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(data_full && n < no_obj);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n < no_obj);
	if(data_full) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(n <= no_obj);

		if(value_type == UINT64)		((_uint64*)data_full)[n]	= _uint64(val_code);
		else if(value_type == UCHAR)	((uchar*)data_full) [n]	= uchar(val_code);
		else if(value_type == USHORT)	((ushort*)data_full)[n]	= ushort(val_code);
		else if(value_type == UINT)		((uint*)data_full)  [n]	= uint(val_code);
	}
}

template <AttrPackN::ValueType VT>
void AttrPackN::AssignToAll(_int64 offset, void*& new_data_full)
{
	typedef typename ValueTypeChooser<VT>::Type Type;
	Type v = (Type)(offset);
	Type* values = (Type*)(new_data_full);
	InitValues(v, values);
}

template <typename T>
void AttrPackN::InitValues(T value, T*& values)
{
    for(uint i = 0; i < no_obj; i++) {
        if(!IsNull(i)) {
            values[i] = value;
        }
    }
}

void AttrPackN::InitValues(ValueType value_type, _int64 value, void*& values)
{
	switch(value_type) {
		case UCHAR :
			InitValues((uchar)value, (uchar*&)values);
			break;
		case USHORT :
			InitValues((ushort)value, (ushort*&)values);
			break;
		case UINT :
			InitValues((uint)value, (uint*&)values);
			break;
		case UINT64 :
			InitValues((_uint64)value, (_uint64*&)values);
			break;
		default :
			BHERROR("Unexpected Value Type");
	}
}


template <typename S, typename D>
void AttrPackN::CopyValues(D*& new_values, _int64 offset)
{
	S* values = (S*)(data_full);
    for(uint i = 0; i < no_obj; i++) {
        if(!IsNull(i))
            new_values[i] = D(values[i] + offset);
    }
}

void AttrPackN::CopyValues(ValueType value_type, ValueType new_value_type, void*& new_values, _int64 offset)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type >= value_type);
	if(value_type == UCHAR) {
		if(new_value_type == UCHAR)
			CopyValues<uchar, uchar>((uchar*&)(data_full), offset);
		else if(new_value_type == USHORT)
			CopyValues<uchar, ushort>((ushort*&)new_values, offset);
		else if(new_value_type == UINT)
			CopyValues<uchar, uint>((uint*&)new_values, offset);
		else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type == UINT64);
			CopyValues<uchar, _uint64>((_uint64*&)new_values, offset);
		}
	} else if(value_type == USHORT) {
		if(new_value_type == USHORT)
			CopyValues<ushort, ushort>((ushort*&)new_values, offset);
		else if(new_value_type == UINT)
			CopyValues<ushort, uint>((uint*&)new_values, offset);
		else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type == UINT64);
			CopyValues<ushort, _uint64>((_uint64*&)new_values, offset);
		}
	} else if(value_type == UINT) {
		if(new_value_type == UINT)
			CopyValues<uint, uint>((uint*&)new_values, offset);
		else {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(new_value_type == UINT64);
			CopyValues<uint, _uint64>((_uint64*&)new_values, offset);
		}
	} else if(value_type == UINT64) {
		CopyValues<_uint64, _uint64>((_uint64*&)new_values, offset);
	}
}

void AttrPackN::Expand(uint new_no_obj, _uint64 new_max_val, _int64 offset)		// expand data by adding empty values (if necessary) or changing bit rate (if necessary)
{
	BHASSERT(!is_only_compressed, "The pack is compressed!");
	BHASSERT(new_no_obj >= no_obj, "Counter of new objects must be greater or equal to the counter total number of objects!");

	if(max_val <= new_max_val)
		max_val = new_max_val;
	else
		saved_up_to_date = false;

	int new_bit_rate = GetBitLen(max_val);
	ValueType new_value_type = ChooseValueType(new_bit_rate);

	if(new_bit_rate > 0 && (new_bit_rate > bit_rate || new_no_obj > no_obj || offset != 0)) {

		if(value_type != new_value_type) {

			MMGuard<void> new_data_full(alloc(new_value_type * new_no_obj, BLOCK_UNCOMPRESSED), *this);
			void* p(new_data_full.get());
			if(bit_rate == 0)
				InitValues(new_value_type, offset, p);
			else
				CopyValues(value_type, new_value_type, p, offset);

			dealloc(data_full);
			data_full = new_data_full.release();

		} else {
			if(new_value_type * new_no_obj)
				data_full = rc_realloc(data_full, new_value_type * new_no_obj, BLOCK_UNCOMPRESSED);
			else
				data_full = 0;

			if(bit_rate == 0)
				InitValues(value_type, offset, data_full);
			else
				CopyValues(value_type, new_value_type, data_full, offset);
		}
		bit_rate = new_bit_rate;
		value_type = new_value_type;
	}

	if(new_no_obj > no_obj) {
		no_obj = new_no_obj;
		compressed_up_to_date = false;
	}
}

void AttrPackN::StayCompressed()	// Remove full data (conserve memory) and stay with compressed buffer only.
{
	if(!compressed_up_to_date)
		saved_up_to_date = false;
	BHASSERT(compressed_buf!=NULL && compressed_up_to_date, "Compression not up-to-date in StayCompressed");

	dealloc(data_full);
	dealloc(nulls);

	data_full	= NULL;
	nulls		= NULL;
	is_only_compressed = true;
}

///////////////////////////////////////////////////////////////////////////////////////
//
//	Compression protocol < OLD! Encoding for version 2.12 SP2 and earlier >:
//
//  General notation remark:	<val> - 2-level value in flat version or Huffmann version (depending on mode)
//								<jump> - flat 3-bit or Huffmann encoding of jump length (minimum: 1)
//								<diff> - flat 3-bit or Huffmann encoding of difference: 0 -> set NULL, 1 -> diff=0, 2 -> diff=1, 3 -> diff=-1, ... k -> diff=(k/2)*(-1)^(k%2)
//								<rle> - number of repetitions, encoded on "rle_bits" bits; value 0 means 1 repetition etc.
//  Compression modes:
//		 mode%2		= 0 for flat value encoding and 1 for Huff. val. enc.
//		(mode/2)%2	= 1 for RLE enabled, otherwise 0
//		(mode/4)%2	= 1 for jump mode, otherwise 0
//		(mode/8)%2	= 1 for combined mode, otherwise 0
//		(mode/16)%2	= 1 for compressed nulls, otherwise 0 (flat or no nulls at all)
//
//  Parameter "rle_bits": number of bits to encode number of repetitions
//
//  Encoding:
//	[x0000 - RLE off, jump off, combined off]
//	<nulls><val>...<val>
//
//	[x0010 - RLE on, jump off, combined off]
//	<nulls>
//	<rle_bits>					- uchar, RLE bit depth, 0 = no RLE compression
//	1<val>						- new value of attr.
//	0<rle>						- how many times we should repeat the last value; if rle_bits=0 then bit 0 indicates one repetition.
//
//	[x0110 - RLE on, jump on, combined off]
//	<nulls>
//	<rle_bits>					- uchar, RLE bit depth, 0 = no RLE compression
//	1<val>						- new value of attr.
//	0<rle><jump>				- get a value from position (current-jump-1)
//								  and repeat it (rle+1) times; if rle_bits=0 then this is just one repetition.
//
//	[x1000 - RLE off, jump off, combined on]
//	<nulls>
//	1<val>						- new value of attr.
//	0<diff>						- get the last nonzero value and add the encoded difference
//
//	[x1010 - RLE on, jump off, combined on]
//	<nulls>
//	<rle_bits>					- uchar, RLE bit depth, 0 = no RLE compression
//	11<val>						- new value of attr.
//	10<diff>					- get the last nonzero value and add the encoded difference
//  01<rle>						- get the last nonzero value, add 1 and put here, creating an increasing sequence of (rle+1) numbers (e.g. last=7 and code "01<4>" produce sequence "8,9,10,11,12")
//								  if rle_bits=0 then code "01" means the same as "10<+1>"
//  00<rle><diff>				- get the last nonzero value, add the encoded difference and repeat the result (rle+2) times
//								  if rle_bits=0 then code "00<diff>" means two identical values encoded by "diff"

template<typename etype> void AttrPackN::RemoveNullsAndCompress(NumCompressor<etype> &nc, char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv)
{
    MMGuard<etype> tmp_data;
	if(no_nulls > 0) {
		tmp_data = MMGuard<etype>((etype*) (alloc((no_obj - no_nulls) * sizeof(etype), BLOCK_TEMPORARY)), *this);
		for(uint i = 0, d = 0; i < no_obj; i++) {
			if(!IsNull(i))
				tmp_data[d++] = ((etype*) (data_full))[i];
		}
	} else
		tmp_data = MMGuard<etype>((etype*)data_full, *this, false);

	CprsErr res = nc.Compress(tmp_comp_buffer, tmp_cb_len, tmp_data.get(), no_obj - no_nulls, (etype) (maxv));
	if(res != CPRS_SUCCESS) {
		std::stringstream msg_buf;
		msg_buf << "Compression of numerical values failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
		throw InternalRCException(msg_buf.str());
	}
}


template<typename etype> void AttrPackN::DecompressAndInsertNulls(NumCompressor<etype> & nc, uint *& cur_buf)
{
    CprsErr res = nc.Decompress(data_full, (char*)((cur_buf + 3)), *cur_buf, no_obj - no_nulls, (etype) * (_uint64*)((cur_buf + 1)));
	if(res != CPRS_SUCCESS) {
		std::stringstream msg_buf;
		msg_buf << "Decompression of numerical values failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
		throw DatabaseRCException(msg_buf.str());
	}
    etype *d = ((etype*)(data_full)) + no_obj - 1;
    etype *s = ((etype*)(data_full)) + no_obj - no_nulls - 1;
    for(int i = no_obj - 1;d > s;i--){
        if(IsNull(i))
            --d;
        else
            *(d--) = *(s--);
    }
}

CompressionStatistics AttrPackN::Compress(DomainInjectionManager& dim)		// Create new optimal compressed buf. basing on full data.
{
	MEASURE_FET("AttrPackN::Compress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	std::stringstream s1;
	s1 << "aN[" << pc_column( GetPackCoordinate() ) << "].Compress(...)";
	FETOperator fet1(s1.str());
#endif

	/////////////////////////////////////////////////////////////////////////
	uint *cur_buf = NULL;
	uint buffer_size = 0;
	MMGuard<char> tmp_comp_buffer;

	uint tmp_cb_len = 0;
	SetModeDataCompressed();

	_uint64 maxv = 0;
	if(data_full) {		// else maxv remains 0
		_uint64 cv = 0;
		for(uint o = 0; o < no_obj; o++) {
			if(!IsNull(o)) {
				cv = (_uint64)GetVal64(o);
				if(cv > maxv)
					maxv = cv;
			}
		}
	}

	if(maxv != 0) {
		//BHASSERT(last_set + 1 == no_obj - no_nulls, "Expression evaluation failed!");

		if(value_type == UCHAR) {
			NumCompressor<uchar> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uchar) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else if(value_type == USHORT) {
			NumCompressor<ushort> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(ushort) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else if(value_type == UINT) {
			NumCompressor<uint> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(uint) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		} else {
			NumCompressor<_uint64> nc(ShouldNotCompress());
			tmp_cb_len = (no_obj - no_nulls) * sizeof(_uint64) + 20;
			if(tmp_cb_len)
				tmp_comp_buffer = MMGuard<char>((char*)alloc(tmp_cb_len * sizeof(char), BLOCK_TEMPORARY), *this);
			RemoveNullsAndCompress(nc, tmp_comp_buffer.get(), tmp_cb_len, maxv);
		}
		buffer_size += tmp_cb_len;
	}
	buffer_size += 12;
	//delete [] nc_buffer;

	////////////////////////////////////////////////////////////////////////////////////
	// compress nulls
	uint null_buf_size = ((no_obj+7)/8);
	MMGuard<uchar> comp_null_buf;
	//IBHeapAutoDestructor del((void*&)comp_null_buf, *this);
	if(no_nulls > 0) {
		if(ShouldNotCompress()) {
			comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
			null_buf_size = ((no_obj + 7) / 8);
			ResetModeNullsCompressed();
		} else {
			comp_null_buf = MMGuard<uchar>((uchar*)alloc((null_buf_size + 2) * sizeof(char), BLOCK_TEMPORARY), *this);
			uint cnbl = null_buf_size + 1;
			comp_null_buf[cnbl] = 0xBA; // just checking - buffer overrun
			BitstreamCompressor bsc;
			CprsErr res = bsc.Compress((char*)comp_null_buf.get(), null_buf_size, (char*) nulls, no_obj, no_nulls);
			if(comp_null_buf[cnbl] != 0xBA) {
				rclog << lock << "ERROR: buffer overrun by BitstreamCompressor (N f)." << unlock;
				BHASSERT(0, "ERROR: buffer overrun by BitstreamCompressor (N f).");
			}
			if(res == CPRS_SUCCESS)
				SetModeNullsCompressed();
			else if(res == CPRS_ERR_BUF) {
				comp_null_buf = MMGuard<uchar>((uchar*)nulls, *this, false);
				null_buf_size = ((no_obj + 7) / 8);
				ResetModeNullsCompressed();
			} else {
				std::stringstream msg_buf;
				msg_buf << "Compression of nulls failed for column " << (GetCoordinate()[bh::COORD::COLUMN] + 1) << ", pack " << (GetCoordinate()[bh::COORD::DP] + 1) << " (error " << res << ").";
				throw InternalRCException(msg_buf.str());
			}
		}
		buffer_size += null_buf_size + 2;
	}
	dealloc(compressed_buf);
	compressed_buf = 0;
	compressed_buf= (uchar*)alloc(buffer_size*sizeof(uchar), BLOCK_COMPRESSED);

	if(!compressed_buf) rclog << lock << "Error: out of memory (" << buffer_size << " bytes failed). (29)" << unlock;
	memset(compressed_buf, 0, buffer_size);
	cur_buf = (uint*)compressed_buf;
	if(no_nulls > 0) {
		if(null_buf_size > 8192)
			throw DatabaseRCException("Unexpected bytes found (AttrPackN::Compress).");
#ifdef _MSC_VER
		__assume(null_buf_size <= 8192);
#endif
		*(ushort*) compressed_buf = (ushort) null_buf_size;
#pragma warning(suppress: 6385)
		memcpy(compressed_buf + 2, comp_null_buf.get(), null_buf_size);
		cur_buf = (uint*) (compressed_buf + null_buf_size + 2);
	}

	////////////////////////////////////////////////////////////////////////////////////

	*cur_buf = tmp_cb_len;
	*(_uint64*) (cur_buf + 1) = maxv;
	memcpy(cur_buf + 3, tmp_comp_buffer.get(), tmp_cb_len);
	comp_buf_size = buffer_size;
	compressed_up_to_date = true;

	CompressionStatistics stats;
	stats.new_no_obj = NoObjs();
	return stats;
}

void AttrPackN::Uncompress(DomainInjectionManager& dim)		// Create full_data basing on compressed buf.
{
	MEASURE_FET("AttrPackN::Uncompress()");
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator feto1(string("aN[") + boost::lexical_cast<string>(pc_column( GetPackCoordinate())) + "].Compress(...)");
	NotifyDataPackDecompression(GetPackCoordinate());
#endif
	is_only_compressed = false;
	if(IsModeNoCompression())
		return;
	assert(compressed_buf);
	uint *cur_buf=(uint*)compressed_buf;
	if(data_full == NULL && bit_rate > 0 && value_type * no_obj)
		data_full = alloc(value_type * no_obj, BLOCK_UNCOMPRESSED);

//	int cur_val = -1, prev_val = -1;

//	int i_bit=0;		// buffer position
//	int last_val[]={-1,-1,-1,-1,-1,-1,-1,-1};

	///////////////////////////////////////////////////////////////
	// decompress nulls

	if(no_nulls > 0) {
		uint null_buf_size = 0;
		if(nulls == NULL)
			nulls = (uint*) alloc(2048 * sizeof(uint), BLOCK_UNCOMPRESSED);

		if(no_obj < 65536)
			memset(nulls, 0, 8192);
		null_buf_size = (*(ushort*) cur_buf);
		if(null_buf_size > 8192)
			throw DatabaseRCException("Unexpected bytes found in data pack (AttrPackN::Uncompress).");
		if(!IsModeNullsCompressed() ) // no nulls compression
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
#if defined(_DEBUG) || (defined(__GNUC__) && !defined(NDEBUG))
			uint nulls_counted = 0;
			for(uint i = 0; i < 2048; i++)
				nulls_counted += CalculateBinSum(nulls[i]);
			if(no_nulls != nulls_counted)
				throw DatabaseRCException("AttrPackN::Uncompress uncompressed wrong number of nulls.");
#endif
		}
		cur_buf = (uint*) ((char*) cur_buf + null_buf_size + 2);
	}

	////////////////////////////////////////////////////////////////////////////////////
	if(!IsModeValid()) {
		rclog << lock << "Unexpected byte in data pack (AttrPackN)." << unlock;
		throw DatabaseRCException("Unexpected byte in data pack (AttrPackN).");
	} else {
		if(IsModeDataCompressed() && bit_rate > 0 && *(_uint64*) (cur_buf + 1) != (_uint64) 0) {
			if(value_type == UCHAR) {
				NumCompressor<uchar> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			} else if(value_type == USHORT) {
				NumCompressor<ushort> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			} else if(value_type == UINT) {
				NumCompressor<uint> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			} else {
				NumCompressor<_uint64> nc;
				DecompressAndInsertNulls(nc, cur_buf);
			}
		} else if(bit_rate > 0) {
			for(uint o = 0; o < no_obj; o++)
				if(!IsNull(int(o)))
					SetVal64(o, 0);
		}
	}

	compressed_up_to_date = true;	
	dealloc(compressed_buf);
	compressed_buf=0;
	comp_buf_size=0;

#ifdef FUNCTIONS_EXECUTION_TIMES
	SizeOfUncompressedDP += (rc_msize(data_full) + rc_msize(nulls));
#endif
}
