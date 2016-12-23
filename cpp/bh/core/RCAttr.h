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

#ifndef _RCATTR_H_
#define _RCATTR_H_

#ifdef MSC_VER
#include <io.h>
#include <FCNTL.H>
#include <SYS\STAT.H>
#endif
#include <iostream>
#include <vector>
#include <boost/bind.hpp>

#include "RCAttrPack.h"
#include "system/RCSystem.h"
#include "common/CommonDefinitions.h"
#include "bintools.h"
#include "ftree.h"
#include "Filter.h"
#include "CompiledQuery.h"
#include "CQTerm.h"
#include "RoughMultiIndex.h"
#include "MIUpdatingIterator.h"
#include "RSI_CMap.h"
#include "RSI_Histogram.h"
#include "RCAttrTypeInfo.h"
#include "types/RCDataTypes.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "system/IBFile.h"
#include "system/IBFileSystem.h"
#include "core/PhysicalColumn.h"
#include "system/ib_system.h"
#include "tools.h"
#include "core/SplicedVector.h"
#include "core/DPN.h"
#include "core/CachedDPNSpliceAllocator.h"
#include "domaininject/DomainInjection.h"
#include "common/bhassert.h"
#include "vc/TypeCastColumn.h"

#define MAX_NO_OBJ (1 << 16)
#define TEMP_VALUE_BUF_SIZE 32

#ifndef MAX_PATH
#define MAX_PATH 256
#endif

#define RCATTR_HEADER_TOP_LENGTH 46

//#define VERSION_ATTR_DES_9      "RSc9"
//#define VERSION_ATTR_DES_10     "RS10"
//#define VERSION_ATTR_DES_LENGTH 4
//#define VERSION_ATTR_DES_ID_9   9
//#define VERSION_ATTR_DES_ID_10  10
//#define VERSION_ATTR_DES_ID_INVALID 0

//#define BH_FILE_ERROR			1
//#define RCATTR_BADFORMAT		2

#define SESSION_READ			0
#define SESSION_WRITE			1

#define FILE_READ_SESSION		0
#define FILE_SAVE_SESSION		1

#define PM_TRIVIAL			0	        //  0 - trivial data: all values are derivable from the statistics, or nulls only,
							//  The pack physically doesn't exist, only statistics
#define PM_UNLOADED_TO_DISK		1		//  1 - unloaded (on disc)
#define PM_LOADED_IN_MEM		2		//  2 - loaded to memory
#define PM_EMPTY			3	        //  3 - no data yet, empty pack

#define CURRENT_FILE_FORMAT		0

/////////////////////////////////////////////////////////////////////////////////////////////////
// DPN file format ("TA00000DPN.ctb"), revised for release 3.2:
//
//	<DPN_pack_nA>		37		- DPN description block, the last pack, version A
//	<DPN_pack_nB>		37		- DPN description block, the last pack, version B
//	<DPN_pack_1>		37		- DPN description block, the first pack
//	...
//	<DPN_pack_n-1>		37		- DPN description block, the one before last pack
//
//	DPN description block format:
//	<pack_file>			4		- file number for actual data (i.e. an identifier of large data file)
//								  Special values for no storage needed: PF_NULLS_ONLY - only nulls, PF_NO_OBJ - no objects (empty pack)
//	<pack_addrs>		4		- file offset for actual data in the large data file
//	<local_min>			8		- min of data pack values:
//								  for int/decimal/datetime - as int_64,
//								  for double - as double,
//								  for string - as the first 8 bytes of minimal string
//	<local_max>			8		- max, as above
//	<local_sum/size>	8		- sum of values of data pack (NULL_VALUE_64 means "not known or overflow")
//								  as int64 for fixed precision columns, as double for floating point.
//								  In case of strings, this value stores the longest string size of data pack.
//	<loc_no_obj>		2		- number of objects-1 (i.e. 0 means 1 object etc.), 0 objects are stored as pak_file = PF_NO_OBJ
//	<loc_no_nulls>		2		- number of nulls,
//								  0 means either no nulls, or all nulls (but in this case pak_file = PF_NULLS_ONLY)
//	<special_flags>		1		- default 0, but the following bits may be set:
//								  SEQUENTIAL_VALUES - the pack is derivable as x_n = (local_min + n), no data needed


namespace bh
{
	#define CLMD_HEADER_LEN            46
	#define CLMD_FORMAT_BUF_LEN        4

	// File format string identifiers
	#define CLMD_FORMAT_RSC9           "RSc9"
	#define CLMD_FORMAT_RSC10          "RS10"

	// File format numeric ids: 9, 10
	#define CLMD_FORMAT_RSC9_ID        9
	#define CLMD_FORMAT_RSC10_ID       10
	#define CLMD_FORMAT_INVALID_ID     0

        // TODO: Need more refactor
	#define BH_FILE_ERROR              1
	#define RCATTR_BADFORMAT           2

	// Convert string to numeric id(9/10) from string (RSc9 or RS10)
	#define FILEFORMAT(buf_format) \
	((!strncmp(buf_format, CLMD_FORMAT_RSC9, CLMD_FORMAT_BUF_LEN)) ? \
		(CLMD_FORMAT_RSC9_ID) : \
		((!strncmp(buf_format, CLMD_FORMAT_RSC10, CLMD_FORMAT_BUF_LEN)) ? \
			(CLMD_FORMAT_RSC10_ID) : \
			(CLMD_FORMAT_INVALID_ID) \
	))
};
/******************************************
 * ABSwitch implementation
 ******************************************/
typedef enum ABSwtich_tag { ABS_A_STATE, ABS_B_STATE }  ABSwitch;

#define INVALID_TRANSACTION_ID   0xFFFFFFFF

#define AB_SWITCH_FILE_NAME   "ab_switch"

class ABSwitcher
{
public:
    ABSwitch GetState(std::string const& path);
    int FlipState(std::string const& path);

    static const char* SwitchName(ABSwitch value );

private:
    void GenerateName(std::string& name, std::string const& path);

};


class NewValuesSet;
class DataPack;
class TransactionBase;
//   Attribute (universal class)

// ENCODING LEVELS
// 0 - text values of attributes
//     NULL represented as '\0' string or null pointer
// 1 - int encoded:
//		RC_INT,RC_NUM   - int64 value, NULL_VALUE_64 for null, may be also treated as int (NULL_VALUE)
//						  decimals: the value is shifted by precision, e.g. "583880"=583.88 for DEC(10,3)
//		RC_TIME, RC_DATE, RC_YEAR - bitwise 64-bit encoding as DATETIME
//		RC_STRING, RC_VARCHAR:
//				lookup	- value from the dictionary as int64, NULL_VALUE_64 for null
//			non-lookup	- text value as RCBString
// 2 - locally encoded (to be read from packs):
//		string, non-lookup - as level 1;
//		other			- uint64, relatively to min value in pack, nulls encoded in a separate bit mask.

#define CURRENT_FILE_FORMAT	0

class RetVal {
	public:
	uint no_obj;
	uint no_nulls;

	RetVal(uint no_obj = 0, uint no_nulls = 0)
		:no_obj(no_obj), no_nulls(no_nulls)
	{
	}
};

template<class T, class A> A convert(T obj)
{
	if(typeid(T) == typeid(A))
		return *(A*)&obj;
	return 0;
};

class AttrPack;
class PackOrderer;
class PackAllocator;


class RCAttr: public TrackableObject, public PhysicalColumn {
	USE_THIS_MACRO_TO_DO_RC_TABLE_A_FRIEND;
	friend class PackOrderer;
	// Added to allow PackGuardian to quicksort the pack
	// This is temporary until the Attribute packs are refactored.
	friend class MaxPredicate;
	friend class MinPredicate;
	friend class SimpleCompactor;
	friend class ConditionEncoder;

public:
	//TODO: Need more refactoring to have functions like Load/save(tablename, attrname);
	void	Load(std::string const& cmd_file);
	bool	Save(std::string const& cmd_file, FTree* dic, _int64 comprs_size);

//////////////////////////
// Methods declared in PhysicalColumn.h or RCAttr-specific

	// Create a new attribute (empty)
	RCAttr(char	*a_name, int a_num,	int	t_num, AttributeType a_type, int a_field_size,
		int a_dec_places, std::string const& a_path, unsigned int param,	unsigned int s_id = 0, DTCollation collation = DTCollation());

	// Connect to an attribute (on disk); Connection mode: 0/1 - read only (queries),
	// write session (s_id - Session Identifier)
	RCAttr(int a_num, int t_num, std::string const& a_path, int conn_mode = 0, unsigned int s_id = 0, DTCollation collation = DTCollation())
		throw(DatabaseRCException);

	// Create a remote attribute
	RCAttr(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, std::string const& path_);

	TRACKABLEOBJECT_TYPE TrackableType() const { return TO_TEMPORARY; }

	const RCDataType& ValuePrototype(bool lookup_to_num) const
	{
		if(( Type().IsLookup() && lookup_to_num ) || ATI::IsNumericType(TypeName()))
			return RCNum::NullValue();
		if(ATI::IsStringType(TypeName()))
			return RCBString::NullValue();
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(ATI::IsDateTimeType(TypeName()));
		return RCDateTime::NullValue();
	}

	virtual inline _int64 GetValueInt64(_int64 obj) const
	{
		if(obj == NULL_VALUE_64)
			return NULL_VALUE_64;
		assert(no_obj >= obj);
		assert(!pack_info_collapsed);
		int pack = (int)(obj >> 16);
		const DPN &dpn = dpns[pack];
		if(dpn.is_stored) {
			assert(dpn.pack);
			assert(pack_type == PackN);
			assert(dpn.pack->IsLocked()); // assuming it is already loaded and locked
			int inpack = int(obj & 65535);
			if(dpn.pack->IsNull(inpack))
				return NULL_VALUE_64;
			_int64 res = ((AttrPackN*)dpn.pack.get())->GetVal64(inpack);	// 2-level encoding
			// Natural encoding
			if (ATI::IsRealType(TypeName()))
				return res;
			res += dpn.local_min;
			return res;
		}
		if(dpn.pack_file == PF_NULLS_ONLY)
			return NULL_VALUE_64;
		// the only possibility: uniform
		assert(dpn.local_min == dpn.local_max);
		return dpn.local_min;
	}

	// Get value which we already know as not null
	virtual inline _int64 GetNotNullValueInt64(_int64 obj) const
	{
		int pack = (int)(obj >> 16);	// simplified version: all packs are full
		const DPN &dpn = dpns[pack];
		if(dpn.is_stored) {
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dpn.pack->IsLocked());
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(dpn.pack);
			
			_int64 res = ((AttrPackN*)dpn.pack.get())->GetVal64((int)(obj&65535));	// 2-level encoding
			// Natural encoding
			if(ATI::IsRealType(TypeName()))
				return res;
			res += dpn.local_min;
			return res;
		}
		// the only possibility: uniform
		return dpn.local_min;
	}

	inline bool IsNull(_int64 obj) const
	{
		if(obj == NULL_VALUE_64)
			return true;
		assert(no_obj >= (_int64)obj);
		assert(!pack_info_collapsed);
		int pack = (int)(obj >> 16);
		const DPN &dpn = dpns[pack];

		if ( Type().GetNullsMode() == NO_NULLS || dpn.no_nulls == 0)
			return false;

		if (dpn.is_stored) {
			assert(dpn.pack->IsLocked());	// assuming the pack is already loaded and locked
			return dpn.pack->IsNull((int)(obj&65535));				// return 1 if the value of attr. is null
		}

		if (dpn.pack_file == PF_NULLS_ONLY)
			return true;
		// Nulls only
		if ((pack_type == PackS) && dpn.pack_mode == PACK_MODE_TRIVIAL) {
			assert(0);
			return true;
		}
		assert(0);		// neither loaded nor null only
		return true;
	};

	/*! \brief Get a non null-terminated String from a column
		 *
		 * \pre necessary datapacks (containing rows pointed by \e mit) are loaded and locked
		 *
		 * Can be used to retrieve binary data
		 *
		 * \param row identifies a row
		 * \param bin_as_hex in case of a binary column, present the value as hexadecimal number
		 * \return RCBString object - a string representation of an object from the column
		 */
	RCBString GetValueString(const _int64 row);
	RCBString GetNotNullValueString(const _int64 row);

	void GetValueString(_int64 row, RCBString& s) { s = GetValueString(row); }
	void GetNotNullValueString(_int64 row, RCBString& s) { s = GetNotNullValueString(row); }

	//RCBString GetMinString(int pack) const;

	// The last stage of conserving memory: delete all data, must be reloaded from disk on next use
	int Collapse();			
	virtual void	LockPackForUse(unsigned pack_no, ConnectionInfo& conn);
	void 			LockPackForUse(unsigned pack_no, Transaction& trans, boost::function0<void> notify_pack_load = boost::function0<void>());
	virtual void	LockPackForUse(unsigned pack_no);
	virtual void	UnlockPackFromUse(unsigned pack_no);

	void			LockLastPackForUse(Transaction& trans);
	void			UnlockLastPackFromUse();
	void Release();
	uint AttrNo()						{ return attr_number; }
	enum phys_col_t ColType() {return RCATTR;}

//	int OrigSize()		 const 			{ return field_size;	}	// as defined in DEC(X,*) or CHAR(X) or...
//	void SetOrigSize(unsigned short int a_field_size) { field_size = a_field_size; }	// as defined in DEC(X,*) or CHAR(X) or...
//	int Precision()		 const			{ return dec_places;	}	// as defined in DEC(*,Y)
//	void SetPrecision(int a_dplace)     { dec_places = a_dplace;}
	// Total width (excl. terminating null), including the sign, decimal point etc.
//	int ColumnSize()	 const;
	// Maximal byte width for one value

	inline AttrPackType PackType()const		{ return pack_type;		}
	void SetPackType(AttrPackType a_pack_type) { pack_type = a_pack_type; }

	PackOntologicalStatus GetPackOntologicalStatus(int pack_no);
	time_t UpdateTime()  const;

	// Transaction management
	uint GetSessionId()	        const	{ return read_session_id; }
	void SetSessionId(uint a_sid)		{ read_session_id = 0;	  }
	uint GetSaveSessionId()     const	{ return save_session_id; }
	void SetSaveSessionId(uint a_ssid)  { save_session_id = a_ssid;}
	//uint GetLastSaveSessionId() const	{ return last_save_session_id; }
	// confirm the last opened session (switch read location)
	void CommitSaveSession(bool same_file = false);
	void Rollback(uint s_id = 0);
	void DeleteRSIs();

	// the natural size of data (i.e. without any compression)
	_int64 NaturalSize()				{ return natural_size;	  }
	// the natural size of data (i.e. without any compression)
	void SetNaturalSize(_int64 na_size)	{ natural_size = na_size; }
	// the compressed size of the attribute (for e.g. calculating compression ratio); NOTE: may be slightly approximated
	_int64 CompressedSize()				{ return compressed_size; };
	void SetCompressedSize(_int64 a_cs) { compressed_size = a_cs; }

	_int64 NoObj()       const			{ return no_obj;		}
	void SetNoObj(_int64 a_no_obj)		{ no_obj = a_no_obj;	}
	int NoPack()         const			{ return no_pack;		}
	void SetNoPack(int a_no_pack)		{ no_pack = a_no_pack;	}
	_int64 NoNulls()     const			{ return no_nulls;		}
	void SetNoNulls(_int64 nnulls)		{ no_nulls = nnulls;	}
	unsigned char FileFormat()			{ return file_format;	}
	void SetFileFormat(unsigned char a_fm) { file_format = a_fm;}

	//! Set Unique status ( \sa SetUnique, \sa SetUniqueUpdated ) and write it to disk without any synchronisation
	//void WriteUnique(RSValue v);

	_uint64 GetNaturalSizeSaved() const { return natural_size_saved;}
	void SetNaturalSizeSaved(_uint64 a_nss) { natural_size_saved = a_nss;}

	uint GetNoPackPtr()					{ return no_pack_ptr; }
	void SetNoPackPtr(uint a_npp)		{ no_pack_ptr = a_npp; }

	int GetLastPackIndex()				{ return last_pack_index; }
	void SetLastPackIndex(int a_lpi)	{ last_pack_index = a_lpi; }

	_int64 GetMinInt64() const			{ return i_min;	}
	void SetMinInt64(_int64 a_imin)		{ i_min = a_imin; }

	_int64 GetMaxInt64() const			{ return i_max; }
	void SetMaxInt64(_int64 a_imax)		{ i_max = a_imax; }

	unsigned int GetPackOffset()		{ return packs_offset;	}
	void SetPackOffset(unsigned int a_po){ packs_offset = a_po;	}

	unsigned int GetDictOffset()		{ return dict_offset;	}
	void SetDictOffset(unsigned int a_do){ dict_offset = a_do;	}

	unsigned int GetSpOffset()			{ return special_offset;}
	void SetSpOffset(uint a_so)			{ special_offset = a_so;}

	int GetTotalPackFile()				{ return total_p_f;     }
	void SetTotalPackFile(int a_tpf)	{ total_p_f = a_tpf;    }

	bool GetPackInfoCollapsed()			{ return pack_info_collapsed; }
	void SetPackInfoCollapsed(bool a_pic){ pack_info_collapsed = a_pic;}

	int GetCurSaveLocation()			{ return current_save_loc; }
	void SetCurSaveLocation(int a_csl)  { current_save_loc = a_csl;}

	int GetCurReadLocation()			{ return current_read_loc; }
	void SetCurReadLocation(int a_crl)  { current_read_loc = a_crl;}

	int GetSaveFileLoc(int i) const		{ return savefile_loc[i]; }
	void SetSaveFileLoc(int i, int a_sfl){ savefile_loc[i] = a_sfl;}

	int GetSavePosLoc(int i)			{ return savepos_loc[i]; }
	void SetSavePosLoc(int i, int a_spl){ savepos_loc[i] = a_spl;}

	int GetInsertingMode()				{return inserting_mode;}
	bool GetNoCompression()				{return no_compression;}

	DomainInjectionManager& GetDomainInjectionManager() { return dom_inj_mngr; }

	// Original 0-level value (text, not null-terminated) and its length; binary data types may be displayed as hex
	void			GetValueBin(_int64 obj, int& size, char* val_buf);

	// size of original 0-level value (text/binary, not null-terminated)
	int				GetLength(_int64 obj);

	// lookup_to_num=true to return a number instead of string
	RCValueObject	GetValue(_int64 obj, bool lookup_to_num = false);	//should be removed to get rid of RCValueObject class
	RCDataType& 	GetValue(_int64 obj, RCDataType& value, bool lookup_to_num = false);

	_int64			GetNoNulls(int pack);
	bool 			RoughNullsOnly() const		{return no_obj == no_nulls;}
	unsigned int	GetNoValues(int pack);
	_int64			GetSum(int pack, bool &nonnegative);
	ushort			GetActualSize(int pack);
	_int64			GetMinInt64(int pack);
	_int64			GetMaxInt64(int pack);
	RCBString 		GetMaxString(int pack);
	RCBString 		GetMinString(int pack);

	std::auto_ptr<DPN>		GetDPNWithoutDataPack(const DataPackId& dpid);
	std::auto_ptr<DataPack>	GetDataPack(const DataPackId& dpid, ConnectionInfo& conn);

	RCBString		GetPrefix(int pack);

	RCBString		DecodeValue_S(_int64 code);
	int				EncodeValue_S(RCBString &v)	{ return EncodeValue_T(v); }
	int				EncodeValue_T(const RCBString& rcbs, int new_val=0, BHReturnCode* bhrc = 0);		// 1-level code value for a given 0-level (text) value, if new_val=1 then add to dictionary if not present
	// no changes for REAL; rounded=true iff v has greater precision than the column and the returned result is rounded down
	_int64			EncodeValue64(RCDataType* v, bool& rounded, BHReturnCode* bhrc = 0);				// as above
	_int64			EncodeValue64(const RCValueObject& v, bool& rounded, BHReturnCode* bhrc = 0);

	// Query execution
	void EvaluatePack(MIUpdatingIterator &mit, int dim, Descriptor& desc);
	bool TryToMerge(Descriptor &d1,Descriptor &d2);   // true, if d2 is no longer needed

	_uint64 ApproxDistinctVals(bool incl_nulls, Filter* f, RSValue* rf, bool outer_nulls_possible);		// provide the best upper approximation of number of diff. values (incl. null, if flag set)
	_uint64 ExactDistinctVals(Filter* f);			// provide the exact number of diff. non-null values, if possible, or NULL_VALUE_64
	_uint64 ApproxAnswerSize(Descriptor& d);	// provide the most probable approximation of number of objects matching the condition
	ushort MaxStringSize(Filter* f = NULL);			// maximal byte string length in column
	bool IsDistinct(Filter* f);
	_int64 RoughMin(Filter *f, RSValue* rf = NULL);		// for numerical: best rough approximation of min for a given filter (or global min if filter is NULL) or rough filter
	_int64 RoughMax(Filter *f, RSValue* rf = NULL);		// for numerical: best rough approximation of max for a given filter (or global max if filter is NULL) or rough filter

	// Rough queries and indexes
	// Note that you should release all indexes after using a series of RoughChecks!
	RSValue RoughCheck(int pack, Descriptor& d, bool additional_nulls_possible);	// check whether any value from the pack may meet the condition
	RSValue RoughCheck(int pack1, int pack2, Descriptor& d);	// check whether any pair from two packs of two different attr/tables may meet the condition
	RSValue RoughCheckBetween(int pack, _int64 min, _int64 max);	// check whether any value from the pack may meet the condition "... BETWEEN min AND max"

	void RoughStats(double &hist_density,int &trivial_packs,double &span);		// calculate the number of 1's in histograms and other KN stats
	void DisplayAttrStats(Filter *f);							// filter is for # of objects in packs
	double RoughSelectivity();
	void GetTextStat(TextStat &s, Filter *f = NULL);

	std::vector<_int64> GetListOfDistinctValuesInPack(int pack);

	void ReleaseRSI(RSIndex *rsi);		// Release an one-dimensional index for this attribute
	RSIndex_Hist *LoadRSI_Hist();
	RSIndex_CMap *LoadRSI_CMap();
//	bool VerifyRSI(); 					// Repairs index directory: false if the repair was needed and done.

	uint packs_omitted;

	SplicedVector<DPN, cached_dpn_splice_allocator> dpns;
	static IBMutex m_dpns_mutex;
	//std::vector<ushort> packs_locks;
	// Session and saving information
	int current_state;					// 0 - opened as read only, 1 - writing session, TODO: define explicit constants

	// DPN::local_min => Statistics (in 1-level codes) for data packs (NOTE: these two 64-bit values are used also by TEXT and RC_BIN, as a kind of optimization/indexing)
	// DPN::local_max => 2-level values start at 0 (or 1, if 0 is null value)
										// level_1_value = local_min + level_2_value + (-1 if nulls present)
	// DPN::sum_size => // for every PackN, sum of 1-level non-null values (note: may be overflowed!)
	                  // for every PackS, maximum byte size of string value in the data pack
	// DPN::no_objs => // for every pack, number of objects - 1 (i.e. 0 means 1 object etc.)
	// DPN::no_nulls =>	// for every pack, number of nulls (cannot be short, because it may be between 0 and 65536 (including!)).
	// DPN::pack_addr => // Address (byte) of the beginning of i-th pack in the file
	// DPN::pack_file => // Number of file where i-th pack is located.
	// DPN::pack => Packs itself (when loaded). Encoded internally as 2-level codes
	double rough_selectivity;			// a probability that simple condition "c = 100" needs to open a data pack, providing KNs etc.

	// Global column info
	int attr_number;					// index of the column in table (for file naming purposes etc.)
	int table_number;					// index of the table (for RSI etc.)
	//char path[MAX_PATH];				// current path (terminated by '/'), determining the directory where TAxxxxx.ctb may be found
	std::string path;

	// Rough set indexes
	RSIndex_Hist* rsi_hist_update;
	RSIndex_CMap* rsi_cmap_update;

	int file;
	size_t file_size_limit;				// limit for TA000x0000x.ctb file size - safely far from 2 GB

	//pthread_mutex_t cs;
    IBMutex dpns_load_mutex;

	boost::shared_ptr<FTree> dic;		// Dictionary for symbolic values

	void UpdateRSI_PP(RCAttr *sec);		// make the index up to date
	void SaveHeader();
	bool OldFormat();
	void UpgradeFormat();
	virtual ~RCAttr();
	void	DoEditionSpecificCleanUp();

	void Drop();
	IBFile *GetPackFile(int pack_no);
	inline void LoadPackInfo(Transaction& trans)		{ if(pack_info_collapsed) LoadPackInfo_physical(&trans); }
	inline void LoadPackInfo()							{ if(pack_info_collapsed) LoadPackInfo_physical(GetCurrentTransaction()); }
	unsigned int LoadPackSize(int n) ;
	std::string AttrPackFileName(int n_pack);
	std::string AttrFileName(int ftype=0, bool oppositFile=false) const;
	bool ShouldExist(int pack_no);
	boost::shared_ptr<FTree> LoadLookupDictFromFile(); //bool include_restore_dpn) ;
	std::string DPNFileName() const;			// get data pack node file (TA00000DPN.ctb)
	void RestoreDPN(char* buf, uint pack_no);
	void RestoreDPN(char* buf, DPN&);

	std::string DomainInjectionFileName(int ftype) const;	// get the domain injection dictionary name (TA00000DI.ctb)

protected:
	//virtual void LoadPack(int n);		// load pack in uncompressed mode	
	void StoreDPN(uint pack_no, char* buf);
	void SaveRSI();

	void LoadDictionaries(const char* buf_ptr);

	PackAllocator *GetAllocator() { return m_allocator;	}

private:
	//RCAttr metadata
	unsigned char	file_format;
	unsigned char	inserting_mode;	//0 - normal (compression), 1 - LastPackNotCompressed
	// SQL-like (internal) type of attribute: RC_STRING, RC_VARCHAR, RC_INT, RC_DATE, ...
	bool			no_compression; // for dataprocessor - no compression mode instead of lookup

	_int64			no_obj;
	_int64			no_nulls;

	// Number of 64k packs.
	int				no_pack;
	// Beginning of a dictionary (numerical or symbolic).
	unsigned int	dict_offset;

	// Beginning of a pack table.
	unsigned int	packs_offset;
	// Beginning of a special section (optimization directions etc.), or 0.
	uint			special_offset;

	// Total number of pack files
	int				total_p_f;

	// Location of the next save position
	// in the both saving locations
	int				savefile_loc[2];
	unsigned int	savepos_loc[2];
	int				current_read_loc;
	int				current_save_loc;
	// id of the last confirmed session (not used and =0 for saving mode)
	// id of the current saving session (or 0xFFFFFFFF if the session is not opened)
	unsigned int	read_session_id;
	unsigned int	save_session_id;
	//unsigned int	last_save_session_id;

	// Derived metadata
	int				last_pack_index;
	uint			no_pack_ptr;

	// Global min and max (1-level value) of the column, redundant (derivable from local)
	_int64			i_min;
	_int64			i_max;

	_uint64			natural_size;
	_uint64			natural_size_saved;
	_uint64			compressed_size;

	// If RC_STRING or RC_VARCHAR then use dictionary representation
	// If text then no dictionary representation
//	int				string_no_dict;
	AttrPackType	pack_type;
	bool			pack_info_collapsed;
	bool			last_pack_locked;
	PackAllocator   *m_allocator;

	bool 	LoadRSI_PP(RCAttr *sec);
	void 	LoadAllDPN(Transaction* trans);
	void 	CreateDPNFile();

	void 	LoadPackInfo_physical(Transaction* trans);	// = LoadPackInfo(), but if really needed
	_int64 	ComputeCompressedSize();		// the compressed size of the attribute (for e.g. calculating compression ratio); NOTE: may be slightly approximated
	void 	ComputeNaturalSize();			// the natural size of data (i.e. without any compression)
public:
	static std::string AttrPackFileNameDirect(int attr_number, int n_file, const std::string& path);
private:
	uint	LoadHeader(const char* a_buf);
	uint	LoadColumnInfo(const char* a_buf);
	uint	LoadSessionInfo(const char* cur_buf, uint actual_size, IBFile* fcmd_file);

	void EvaluatePack_IsNull(MIUpdatingIterator &mit, int dim);
	void EvaluatePack_NotNull(MIUpdatingIterator &mit, int dim);
	void EvaluatePack_Like(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_Like_UTF(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_InString(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_InString_UTF(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_InNum(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_BetweenString(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_BetweenString_UTF(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_BetweenInt(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_BetweenReal(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_AttrAttr(MIUpdatingIterator &mit, int dim, Descriptor& d);
	void EvaluatePack_AttrAttrReal(MIUpdatingIterator &mit, int dim, Descriptor& d);

	void CalcPackFileSizeLimit(int a_field_size);
	bool HasUnCommitedSession();
	unsigned int ReadUnCommitedSessionID();
	std::vector<int> GetEmptiedFiles(boost::shared_ptr<Filter> del_mask);

	inline _int64 GetTable64Loaded( uint pack, int n )	// low-level, fast: no pack loading etc.
	{
		assert( pack_type == PackN && dpns[pack].pack_file!=PF_NULLS_ONLY &&
				(dpns[pack].no_nulls!=0 || dpns[pack].local_min!=dpns[pack].local_max) && dpns[pack].pack_mode==PACK_MODE_IN_MEMORY );
		if( dpns[pack].pack->IsNull(n) )
			return NULL_VALUE_64;
		_int64 res = ((AttrPackN*)dpns[pack].pack.get())->GetVal64( n );	// 2-level encoding
		// Natural encoding
		if (ATI::IsRealType(TypeName()))
			return res;
		res += dpns[pack].local_min;
		return res;
	} ;		// value of RC_NUM or RC_INT or RC_STRING object in 1-level encoding (decimals are precision-shifted)

protected:
	DomainInjectionManager dom_inj_mngr;
};


class PackAllocator {
  RCAttr *attr;
  AttrPackType pt;
public:
  PackAllocator(RCAttr *a, AttrPackType p) : attr(a), pt(p)  {}

  AttrPackPtr lockedAlloc(const PackCoordinate& pack_coord);
  RCAttr* GetAttrPtr() {return attr;}
  bool isLoadable( int pack );

  IBFile *getFile( int pack );

  int NoOfPacks() const { return attr->NoPack(); }

  DomainInjectionManager& GetDomainInjectionManager() { return attr->GetDomainInjectionManager(); }
};

AttrPackPtr AttrPackLoad(const PackCoordinate& coord_, PackAllocator &a);


class DataPackLock : public FunctionExecutor
{
public:
	DataPackLock(RCAttr& attr, const DataPackId& id)
		:	FunctionExecutor(
				boost::bind(&RCAttr::LockPackForUse, 	boost::ref(attr), (int)id),
				boost::bind(&RCAttr::UnlockPackFromUse, boost::ref(attr), (int)id)
			)
	{}
};


#endif
