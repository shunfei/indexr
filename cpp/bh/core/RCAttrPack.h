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

#ifndef RCATTRPACK_H_INCLUDED
#define RCATTRPACK_H_INCLUDED

#include "core/tools.h"
#include "types/RCDataTypes.h"
#include "CQTerm.h"
#include "ftree.h"
#include "Filter.h"
#include "system/fet.h"

#include "system/MemoryManagement/TrackableObject.h"
#include "system/IBStream.h"
#include "domaininject/DomainInjection.h"

class RCAttr;
class DataCache;
template <class T>	class NumCompressor ;

enum PackMode {PM_TRIVIAL = 0, PM_UNLOADED = 1, PM_LOADED = 2, PM_NO_DATA = 3};

// table of modes:
//  0 - trivial data: all values are derivable from the statistics, or nulls only,
//		the pack physically doesn't exist, only statistics
//  1 - unloaded (on disc)
//  2 - loaded to memory
//  3 - no data yet, empty pack


class AttrPack : public TrackableObject
{
	friend class DataCache;
public:
	AttrPack(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner);
	AttrPack(const AttrPack &ap);
	virtual std::auto_ptr<AttrPack> Clone() const = 0;
	virtual void LoadData(IBStream* fcurfile) = 0;


	AttrPack* m_prev_pack;
	AttrPack* m_next_pack;

	bool IsDataPack() const { return true; };
	int Collapse() { return 0; };

	virtual AttrPackType GetPackType() const = 0;

	virtual void Uncompress(DomainInjectionManager& dim) = 0;			// Create full_data basing on compressed buf.
	virtual CompressionStatistics Compress(DomainInjectionManager& dim) = 0; // Create optimal compressed buf. basing on full data.
	virtual void StayCompressed() = 0;		// Remove full data (conserve memory) and stay with compressed buffer only.

	bool IsEmpty() const {return is_empty;}
	virtual bool IsCompressed() = 0;
	virtual int Save(IBStream* fcurfile, DomainInjectionManager& dim) = 0;		// save all pack data (parameters, dictionaries, compressed data)

	virtual bool UpToDate();					// return 1 iff there is no need to save
	virtual uint TotalSaveSize() = 0;		// number of bytes occupied by data and dictionaries on disk

	bool IsNull(int i) const
	{
		if(no_nulls == no_obj) return true;
		if(! nulls) return false;
		return ((nulls[i>>5]&((uint)(1)<<(i%32)))!=0);		// the i-th bit of the table
	}
	virtual void SetNull(int n) = 0;
	uint NoNulls()	{return no_nulls;}
	uint NoObjs()	{return no_obj;}

	//TrackableObject functionality
	TRACKABLEOBJECT_TYPE TrackableType() const {return TO_PACK;}
	uint PreviousSaveSize() {return previous_size;}

	PackCoordinate GetPackCoordinate() const { return _logical_coord.co.pack; }
	virtual void Release();
public:
	virtual ~AttrPack();
protected:
	virtual void Destroy() = 0;
	bool ShouldNotCompress();

protected:
	int		pack_no;
	uint	no_nulls;
	uint	no_obj;
	bool	is_empty;

	bool  compressed_up_to_date;			// true means that flat data was encoded in "compressed_buf" and didn't change
	bool  saved_up_to_date;				// true means that the contents of table in memory didn't change (comparing to disk)

	uchar *compressed_buf;

	uint* nulls;
	uint  comp_buf_size, comp_null_buf_size, comp_len_buf_size;

	bool is_only_compressed; // true means that compressed pack resides in a memory (uncompressed part is deleted)
	uint previous_size;
	int inserting_mode;
	bool no_compression;
	AttributeType attr_type;
	//PackCoordinate pc;
};

//////////////////////////////////////////////////////////////////////////////////////////////////

class AttrPackN : public AttrPack
{
public:
    AttrPackN(PackCoordinate pc, AttributeType attr_type, int inserting_mode, DataCache* owner);
	AttrPackN(const AttrPackN &apn);
    virtual std::auto_ptr<AttrPack> Clone() const;
	void LoadData(IBStream *fcurfile);
    void Prepare(uint new_no_obj, _uint64 new_max_val);
    void SetVal64(uint n, const _uint64 & val_code2);
    _int64 GetVal64(int n);
    double GetValD(int n);
    void SetNull(int n);
    void Expand(uint new_no_obj, _uint64 new_max_val, _int64 offset = 0);
    void Uncompress(DomainInjectionManager& dim);
    CompressionStatistics Compress(DomainInjectionManager& dim);
    void StayCompressed();
    bool IsCompressed()	{ return is_only_compressed; }

	int Save(IBStream *fcurfile, DomainInjectionManager& dim);
    uint TotalSaveSize();

    virtual AttrPackType GetPackType() const { return PackN; }

public:
    ~AttrPackN();
protected:
    void Destroy();

private:
    enum ValueType{ UCHAR = 1, USHORT = 2, UINT = 4, UINT64 = 8};
    template<ValueType VT> class ValueTypeChooser
    {
		public:
        typedef typename boost::mpl::if_c<VT == UINT64, uint64,
			typename boost::mpl::if_c<VT == UINT, uint,
				typename boost::mpl::if_c<VT == USHORT,ushort,uchar> ::type> ::type>::type Type;
    };

    template <typename T> void SetValues(_int64 offset);
	void InitValues(ValueType new_value_type, _int64 offset, void*& new_data_full);
	void CopyValues(ValueType value_type, ValueType new_value_type, void*& new_data_full, _int64 offset);
	template <typename S, typename D> void CopyValues(D*& new_values, _int64 offset);
	template <ValueType VT> void AssignToAll(_int64 value, void*& new_data_full);
	template <typename T> void InitValues(T value, T*& values);
	static ValueType ChooseValueType(int bit_rate);
    template<typename etype> void DecompressAndInsertNulls(NumCompressor<etype> & nc, uint *& cur_buf);
    template<typename etype> void RemoveNullsAndCompress(NumCompressor<etype> &nc, char* tmp_comp_buffer, uint & tmp_cb_len, _uint64 & maxv);

private:
    void*		data_full;
	ValueType	value_type;
	int			bit_rate;
	_uint64		max_val;
	uchar		optimal_mode; //see below

	bool IsModeNullsCompressed() const {return optimal_mode & 0x10;}
	//! Pack not existing physically, does not cover situation when min==max, then data_full is null
	bool IsModeDataCompressed() const {return optimal_mode & 0x20;}
	bool IsModeCompressionApplied() const {return IsModeDataCompressed() || IsModeNullsCompressed();}
	bool IsModeNoCompression() const {return optimal_mode & 0x40;}
//	void SetModeNoCompression() {optimal_mode = 0x40;}		//not used, replaced by CompressCopy inside the Compressor to keep old format
	void ResetModeNoCompression() {optimal_mode &= 0xBF;}
	void SetModeDataCompressed() {ResetModeNoCompression(); optimal_mode |= 0x20; }
	void SetModeNullsCompressed() {ResetModeNoCompression(); optimal_mode |= 0x10;}
	void ResetModeNullsCompressed() {optimal_mode &= 0xEF;}
	void ResetModeDataCompressed() {optimal_mode &= 0xDF;}
	bool IsModeValid() const {return ((optimal_mode & 0xCF) == 0) || IsModeNoCompression();}

};

inline _int64 AttrPackN::GetVal64(int n)
{
	assert(!is_only_compressed);
	if(data_full == NULL) return 0;
	if(value_type == UINT)			return (_int64)((uint*)data_full)[n];
	else if(value_type == UINT64)	return ((_int64*)data_full)[n];
	else if(value_type == USHORT)	return (_int64)((ushort*)data_full)[n];
	return (_int64)((uchar*)data_full)[n];
}

inline double AttrPackN::GetValD(int n)
{
	assert(!is_only_compressed);
	if(data_full == NULL) return 0;
	assert(value_type == UINT64);
	return ((double*)data_full)[n];
}


//////////////////////////////////////////////////////////////////////////////////////////////////

class AttrPackS : public AttrPack
{
public:
	AttrPackS(PackCoordinate pc, AttributeType attr_type, int inserting_mode, bool no_compression, DataCache* owner);
	AttrPackS(const AttrPackS &aps);
	virtual std::auto_ptr<AttrPack> Clone() const;
	void LoadData(IBStream* fcurfile);
	void Prepare(int no_nulls);
	void Expand(int no_obj);
	void BindValue(bool null, uchar* value = 0, uint size = 0);
	void CopyBinded();
	bool HasBinded() const { return binding; }
	//void AddValue(bool null, uchar* value = 0, uint size = 0);

	int		GetSize(int ono) const;
	char*	GetVal(int ono) { BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < (int)no_obj); return (char*)index[ono]; }

	void SetNull(int ono);

	void Uncompress(DomainInjectionManager& dim);
	//void Uncompress8();
	void Uncompress8(DomainInjectionManager& dim);
	void UncompressOld();

	CompressionStatistics Compress(DomainInjectionManager& dim);
	CompressionStatistics Compress8(DomainInjectionDecomposer& decomposer);
	CompressionStatistics CompressOld();
	void StayCompressed();
	bool IsCompressed() { return is_only_compressed; /*(data == NULL);*/ };
	int Save(IBStream* fcurfile, DomainInjectionManager& dim);

	uint TotalSaveSize();

	virtual AttrPackType GetPackType() const { return PackS; }

	uint PreviousNoObj() const { return previous_no_obj; }
	bool IsDecomposed() const { return ver == 8; }
	void SetDecomposerID(uint decomposer_id) { this->decomposer_id = decomposer_id; use_already_set_decomposer = true; ver = decomposer_id != 0 ? 8 : 0;  }
	uint GetDecomposerID() const { return decomposer_id; }

	uint GetNoOutliers() const { return outliers; };

public:
    ~AttrPackS();
protected:
    void Destroy();

private:
	void Construct();
	void SetSize(int ono, uint size);

	uchar ver;
	uint 	previous_no_obj;
	uint	max_no_obj;

	uchar**	data;
	uchar**	index;
	void*	lens;

	int		data_id;
//	ushort	values_psbs;
	uint	data_full_byte_size;

	ushort	len_mode;						//RC_BIN - sizeof(ushort), otherwise - sizeof(ushort)
	//uchar*	value_t;

	bool	binding;
	int		last_copied;

//	uchar	predictor_hist[32];
	int		optimal_mode;
	uint decomposer_id;
	ushort	no_groups;	// for decomposition

	bool use_already_set_decomposer;

	uint outliers;

	bool IsModeNullsCompressed() const {return optimal_mode & 0x1;}
	//! Pack not existing physically, does not cover situation when min==max, then data_full is null
	bool IsModeDataCompressed() const {return optimal_mode & 0x2;}
	bool IsModeCompressionApplied() const {return IsModeDataCompressed() || IsModeNullsCompressed();}
	bool IsModeNoCompression() const {return optimal_mode & 0x4;}
//	void SetModeNoCompression() {optimal_mode = 0x4;} //not used, replaced by CompressCopy inside the Compressor to keep old format
	void SetModeDataCompressed() {ResetModeNoCompression() ; optimal_mode |= 0x2;}
	void SetModeNullsCompressed() {ResetModeNoCompression() ; optimal_mode |= 0x1;}
	void ResetModeNullsCompressed() {optimal_mode &= 0xFE;}
	void ResetModeDataCompressed() {optimal_mode &= 0xFD;}
	void ResetModeNoCompression() {optimal_mode &= 0xB;}
	bool IsModeValid() const {return ((optimal_mode & 0xFC) == 0) || IsModeNoCompression();}

	void SaveUncompressed(uchar *h, IBStream* fcurfile);
	void LoadUncompressed(IBStream* fcurfile);
	void AllocBuffers();

	void AllocNullsBuffer();
	void AllocBuffersButNulls();

	void Decompose(uint& size, std::vector<uchar**>& indexes, std::vector<ushort*>& lengths, std::vector<uint>& maxes, std::vector<uint>& sumlens);
};

inline int AttrPackS::GetSize(int ono) const
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(!is_only_compressed);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ono < (int)no_obj);
	if(len_mode == sizeof(ushort))
		return ((ushort*)lens)[ono];
	else
		return ((uint*)lens)[ono];
}

#endif /* RCATTRPACK_H_INCLUDED */

