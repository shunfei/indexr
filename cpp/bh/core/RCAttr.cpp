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

#include <boost/bind.hpp>

#include "common/CommonDefinitions.h"
#include "RCAttr.h"
#include "RCAttrPack.h"
#include "RCAttrTypeInfo.h"
#include "system/fet.h"
#include "system/ConnectionInfo.h"
#include "types/ValueParserForText.h"
#include "DataPackImpl.h"
#include "DPN.h"
#include "common/DataFormat.h"
#include "edition/core/Transaction.h"
#include "common/ProcessType.h"

#include "util/BHString.h"
#include "system/IBFile.h"

using namespace bh;
using namespace std;
using namespace boost;

enum { VAL_BUF_MIN_COUNT = 24 };

IBMutex RCAttr::m_dpns_mutex;

void RCAttr::CalcPackFileSizeLimit(int a_field_size)
{
	// Determine pack file size limit
	// Smaller file => more system overload and size limitations (file naming!).
	// Larger size  => harder to backup; cannot exceed 2.1 GB!
	if(a_field_size > 16000)
		file_size_limit = 1000000000;
	else
		// Default size: the safe distance from 2 GB
		file_size_limit = 2000000000 - 2 * 65536 * size_t(a_field_size);

	try {
		string conferror;
		size_t new_file_size_limit = 1000000 * size_t(ConfMan.GetValueInt("brighthouse/ClusterSize", conferror));
		if(new_file_size_limit < file_size_limit)
			file_size_limit = new_file_size_limit;
	} catch (...) {
		// Just use the default values
	}
	if(file_size_limit < 10000000)
		file_size_limit = 10000000;
}

RCAttr::RCAttr(char *a_name, int a_num, int t_num, AttributeType a_type, int a_field_size, int a_dec_places, std::string const& a_path, uint param, uint s_id, DTCollation collation)
	:	packs_omitted(0), dpns( 0, cached_dpn_splice_allocator( this ) ), path( add_dir_sep( a_path ) ), last_pack_locked(false)
{

	_logical_coord.ID=bh::COORD_TYPE::RCATTR;
	_logical_coord.co.rcattr[0] = t_num;
	_logical_coord.co.rcattr[1] = a_num;

	SetFileFormat(10);
	CalcPackFileSizeLimit(a_field_size);

	SetNoPack(0);
	SetNoPackPtr(0);
	SetTotalPackFile(1);

	SetSaveFileLoc(0, 0);
	SetSaveFileLoc(1, 1);
	SetSavePosLoc(0, 0);
	SetSavePosLoc(1, 0);
	SetCurReadLocation(0);
	SetCurSaveLocation(0);

	SetSessionId(0);
	SetSaveSessionId(s_id);
        // Address (byte) of the beginning of i-th pack in the file
	rsi_hist_update = NULL;
	rsi_cmap_update = NULL;

	SetLastPackIndex(0);
	SetNoObj(0);

	NullMode nulls;
	bool is_lookup;

	is_lookup = (param & 0x00000002) != 0;
    // automatic LOOKUP for small columns - DISABLED AS TOO RISKY
//    if((a_type == RC_STRING || a_type == RC_VARCHAR) && a_field_size <= 2)
//		is_lookup = true;
	if(a_type != RC_STRING && a_type != RC_VARCHAR)
		is_lookup = false;				// note: is_lookup  =>  RC_STRING || RC_VARCHAR
    if(param & 0x00000001)
    	nulls = NO_NULLS;
	else
    	nulls = AS_MISSED;

    if(param & 0x00000004)
    	inserting_mode = 1;
    else
    	inserting_mode = 0;

    no_compression = false;

	if (a_field_size <= 0)
	{
		unsigned short int fsize;
                // maximum size of double for gcvt(...,15,...)
        if(a_type==RC_REAL) fsize = 23;
        else if(a_type==RC_FLOAT) fsize = 15;
		else if(a_type==RC_BYTEINT) fsize = 4;
		else if(a_type==RC_SMALLINT) fsize = 6;
		else if(a_type==RC_MEDIUMINT) fsize = 8;
		else if(a_type==RC_INT) fsize = 11;
		else if(a_type==RC_BIGINT) fsize = 20;
		else if(a_type==RC_NUM) fsize = 18;
		else if(a_type==RC_DATE) fsize = 10;
		else if(a_type==RC_TIME) fsize = 10;
		else if(a_type==RC_YEAR) fsize = 4;
		else if(a_type == RC_DATETIME || a_type == RC_TIMESTAMP) fsize = 19;
		a_field_size  = fsize;
	}

	ct.Initialize(a_type, nulls, is_lookup, a_field_size, a_dec_places, collation);

	if(Type().IsLookup() || ATI::IsNumericType(a_type) || ATI::IsDateTimeType(a_type))
		SetPackType(PackN);
	else
		SetPackType(PackS);

	m_allocator = new PackAllocator(this,PackType());
	
	int val_buf_size = Type().GetDisplaySize() + 1;
	if (val_buf_size < VAL_BUF_MIN_COUNT)
		val_buf_size=VAL_BUF_MIN_COUNT;		// minimal RC_NUM buffer size

	SetMinInt64(0);
	SetMaxInt64(0);
	current_state = 1;				// read/write session

	SetNoNulls(0);
	SetNaturalSizeSaved(0);
	SetCompressedSize(0);
	rough_selectivity = -1;
	SetUnique(true);				// all non-null values are different (because there are no values at all)
	SetUniqueUpdated(true);		// true if the information in "is_unique" is up to date

	attr_number = a_num;	// column number (for file naming purposes etc.)
	table_number = t_num;

	SetName(a_name);
	SetDescription(NULL);


	SetDictOffset(0);
	SetPackOffset(0);

	if(Type().IsLookup()) {
		dic = shared_ptr<FTree>(new FTree());
		dic->Init(Type().GetPrecision());
		GlobalDataCache::GetGlobalDataCache().PutObject(FTreeCoordinate(table_number, attr_number), dic); // we do not have 
	}

	SetPackInfoCollapsed((GetDictOffset() || NoPack()));
	file = FILE_READ_SESSION;
	ComputeNaturalSize();

	if(process_type != ProcessType::DATAPROCESSOR)
		dom_inj_mngr.SetPath(DomainInjectionFileName(file));
}

RCAttr::RCAttr(int a_num, AttributeType a_type, int a_field_size, int a_dec_places, uint param, DTCollation collation, bool compress_lookups, string const& path_)
	:	packs_omitted(0), dpns( 0, cached_dpn_splice_allocator( this ) ), path( add_dir_sep( path_ ) ), last_pack_locked(false)
{
	_logical_coord.ID=bh::COORD_TYPE::RCATTR;
	_logical_coord.co.rcattr[0] = 0;
	_logical_coord.co.rcattr[1] = a_num;

	SetFileFormat(10);
	file_size_limit = 0;

	SetNoPack(0);
	SetNoPackPtr(0);
	SetTotalPackFile(1);

	SetSaveFileLoc(0, 0);
	SetSaveFileLoc(1, 1);
	SetSavePosLoc(0, 0);
	SetSavePosLoc(1, 0);
	SetCurReadLocation(0);
	SetCurSaveLocation(0);

	SetSessionId(0);
	SetSaveSessionId(INVALID_TRANSACTION_ID);
        // Address (byte) of the beginning of i-th pack in the file
	rsi_hist_update = NULL;
	rsi_cmap_update = NULL;

	SetLastPackIndex(0);
	SetNoObj(0);

	NullMode nulls;
	bool is_lookup;

	is_lookup = (param & 0x00000002) != 0;
	    // automatic LOOKUP for small columns - DISABLED AS TOO RISKY
//    if((a_type == RC_STRING || a_type == RC_VARCHAR) && a_field_size <= 2)
//		is_lookup = true;
	if(a_type != RC_STRING && a_type != RC_VARCHAR)
		is_lookup = false;				// note: is_lookup  =>  RC_STRING || RC_VARCHAR
	if(param & 0x00000001)
    	nulls = NO_NULLS;
	else
    	nulls = AS_MISSED;

    if(param & 0x00000004)
    	inserting_mode = 1;
    else
    	inserting_mode = 0;

    no_compression = false;
	if (is_lookup) {
		// convert lookup to no compression mode in dataprocessor
		no_compression = !compress_lookups;
		is_lookup = false;
	}

	if (a_field_size <= 0)
	{
		unsigned short int fsize;
                // maximum size of double for gcvt(...,15,...)
        if(a_type==RC_REAL) fsize = 23;
        else if(a_type==RC_FLOAT) fsize = 15;
		else if(a_type==RC_BYTEINT) fsize = 4;
		else if(a_type==RC_SMALLINT) fsize = 6;
		else if(a_type==RC_MEDIUMINT) fsize = 8;
		else if(a_type==RC_INT) fsize = 11;
		else if(a_type==RC_BIGINT) fsize = 20;
		else if(a_type==RC_NUM) fsize = 18;
		else if(a_type==RC_DATE) fsize = 10;
		else if(a_type==RC_TIME) fsize = 10;
		else if(a_type==RC_YEAR) fsize = 4;
		else if(a_type == RC_DATETIME || a_type == RC_TIMESTAMP) fsize = 19;
		a_field_size  = fsize;
	}

	ct.Initialize(a_type, nulls, is_lookup, a_field_size, a_dec_places, collation);

	if(Type().IsLookup() || ATI::IsNumericType(a_type) || ATI::IsDateTimeType(a_type))
		SetPackType(PackN);
	else
		SetPackType(PackS);

	m_allocator = new PackAllocator(this,PackType());

	int val_buf_size = Type().GetDisplaySize() + 1;
	if (val_buf_size < VAL_BUF_MIN_COUNT)
		val_buf_size=VAL_BUF_MIN_COUNT;		// minimal RC_NUM buffer size

	SetMinInt64(0);
	SetMaxInt64(0);
	current_state = 1;				// read/write session

	SetNoNulls(0);
	SetNaturalSizeSaved(0);
	SetCompressedSize(0);
	rough_selectivity = -1;
	SetUnique(true);				// all non-null values are different (because there are no values at all)
	SetUniqueUpdated(true);		// true if the information in "is_unique" is up to date

	attr_number = a_num;	// column number (for file naming purposes etc.)
	table_number = 0;

	SetName(NULL);
	SetDescription(NULL);

	SetDictOffset(0);
	SetPackOffset(0);

	if(Type().IsLookup()) {
		dic = shared_ptr<FTree>(new FTree());
		dic->Init(Type().GetPrecision());
		GlobalDataCache::GetGlobalDataCache().PutObject(FTreeCoordinate(table_number, attr_number), dic); // we do not have
	}

	SetPackInfoCollapsed((GetDictOffset() || NoPack()));
	file = FILE_READ_SESSION;
	ComputeNaturalSize();

	// prepare for load
	pack_info_collapsed = false;
}

bool RCAttr::HasUnCommitedSession()
{
	return (DoesFileExist(AttrFileName(FILE_SAVE_SESSION)));
}

unsigned int RCAttr::ReadUnCommitedSessionID()
{
	IBFile fattr_save;
	unsigned int session_id = INVALID_TRANSACTION_ID;
	unsigned char a_file_format;
	char* tmp_buf = new char [10000];

	string attrf_name(AttrFileName(FILE_SAVE_SESSION));
	fattr_save.OpenReadOnly(attrf_name);
	fattr_save.Read(tmp_buf, CLMD_HEADER_LEN);
	a_file_format = FILEFORMAT(tmp_buf);
	if (a_file_format == CLMD_FORMAT_INVALID_ID)
	{
		fattr_save.Close();
		delete [] tmp_buf;
		rclog << lock << "Unsupported version (not an attribute), or file does not exist: " << attrf_name << unlock;
		string mess = (string)"Unsupported version (not an attribute), or file does not exist: " + attrf_name;
		throw DatabaseRCException(mess.c_str());
	}

	uint sp_offset = *((uint*)(tmp_buf + 42));
	if (sp_offset > 0)										// Note that it is assumed that the first special block contain session info
	{
		fattr_save.Seek(sp_offset + 22, SEEK_SET);
		fattr_save.Read(tmp_buf, 46);
		session_id = *((uint*)(tmp_buf + 1));
	}
	fattr_save.Close();

	delete [] tmp_buf;
	return session_id;
}

RCAttr::RCAttr(int a_num, int t_num, string const& a_path, int conn_mode, uint s_id, DTCollation collation) throw(DatabaseRCException)
	:	packs_omitted(0), dpns( 0, cached_dpn_splice_allocator( this ) ), path( add_dir_sep( a_path ) ), last_pack_locked(false)
{
	ct.Initialize(RC_INT, AS_MISSED, false, 0, 0, collation);
	rsi_hist_update = NULL;
	rsi_cmap_update = NULL;
	attr_number = a_num;				// index of the column in table (for file naming purposes etc.)
	table_number = t_num;

	_logical_coord.ID=bh::COORD_TYPE::RCATTR;
	_logical_coord.co.rcattr[0] = t_num;
	_logical_coord.co.rcattr[1] = a_num;

	current_state = conn_mode;			// conn_mode is used to determine initial state; current_state may change

	// Determine currently opened save session_id and save_loc (if any)
	SetSessionId(0);
	SetSaveSessionId(INVALID_TRANSACTION_ID);		// this value indicates that there is no session opened
	SetCurSaveLocation(0);

	int new_save_session = 0;			// this flag will indicate that this is a new session and we should switch save locations

	//IBFile fattr_read;
	string attrf_name(AttrFileName(FILE_SAVE_SESSION));

	// Normal read session
	int open_fname = FILE_READ_SESSION;

	if(current_state == SESSION_WRITE) {

		if (HasUnCommitedSession()) {
			// There is a load applied but not yet committed
			SetSaveSessionId(ReadUnCommitedSessionID());

			if (GetSaveSessionId() == s_id) {
				// Continue previous save session since this has same session id as current
				open_fname = FILE_SAVE_SESSION;
			}
			else {
				rclog << lock << "Error - previous session (id=" << GetSaveSessionId() << ") was not properly committed!" << unlock;
				new_save_session = 1;
			}
		}
		else {
			new_save_session = 1;
		}

		if (new_save_session) {
			SetSaveSessionId(s_id);
			// open read file to copy (by saving attr.) to the second file type
			open_fname = FILE_READ_SESSION;
		}
	}

    no_compression = false;

	// packs
	rough_selectivity = -1;

	file = open_fname;

	// Load file with attribute description
	Load(AttrFileName(open_fname));

	SetCurReadLocation(GetCurSaveLocation());

	if (new_save_session == 1)
		SetCurSaveLocation(1 - GetCurSaveLocation());
	BHASSERT(GetCurSaveLocation() == 0 || GetCurSaveLocation() == 1, "Invalid current save location!");

	CalcPackFileSizeLimit(Type().GetPrecision());		// Determine pack file size limit
	ComputeNaturalSize();

	// need to add it manually since Load above can reset collation
	ct.SetCollation(collation);
	m_allocator = new PackAllocator(this,PackType());
	if(process_type != ProcessType::DATAPROCESSOR) {
		dom_inj_mngr.Init(DomainInjectionFileName(open_fname));
		if (new_save_session)
			dom_inj_mngr.SetPath(DomainInjectionFileName(1));
	}
}

RCAttr::~RCAttr()
{
	BHASSERT(rsi_hist_update == NULL && rsi_cmap_update == NULL,
						"Invalid Histogram or CMAP object. Either one was not released/freed properly!");

	DoEditionSpecificCleanUp();
	//UnlockLastPackFromUse();

	delete m_allocator;
}

//int RCAttr::ColumnSize() const
//{
//	return ATI::TextSize(TypeName(), Type().GetPrecision(), Precision());
//}

void RCAttr::ComputeNaturalSize()
{
	AttributeType a_type = TypeName();
	_uint64 na_size;

	na_size = (Type().GetNullsMode()  != NO_NULLS ? 1 : 0) * NoObj() / 8;
	if(a_type == RC_STRING || a_type == RC_BYTE || a_type == RC_DATE)
		na_size += Type().GetPrecision() * NoObj();
	else if(a_type == RC_TIME || a_type == RC_YEAR || a_type == RC_DATETIME || a_type == RC_TIMESTAMP)
		na_size += Type().GetDisplaySize() * NoObj();
	else if(a_type == RC_NUM)
		na_size += (Type().GetPrecision() + (Type().GetScale() ? 1 : 0)) * NoObj();
	else if(ATI::IsRealType(a_type))
		na_size += 8 * NoObj();
	else if(a_type == RC_FLOAT)
		na_size += 4 * NoObj();
	else if(a_type == RC_INT)
		na_size += 4 * NoObj();
	else if(a_type == RC_BIGINT)
		na_size += 8 * NoObj();
	else if(a_type == RC_MEDIUMINT)
		na_size += 3 * NoObj();
	else if(a_type == RC_SMALLINT)
		na_size += 2 * NoObj();
	else if(a_type == RC_BYTEINT)
		na_size += 1 * NoObj();
	else if(a_type == RC_VARCHAR)
		na_size += (_int64)GetNaturalSizeSaved();
	else if(a_type == RC_VARBYTE || a_type == RC_BIN)
		na_size += (_int64)GetNaturalSizeSaved();
	SetNaturalSize(na_size);
}

void RCAttr::SaveHeader()
{
	_int64 css = ComputeCompressedSize();

	if (current_state == SESSION_READ)
		file = FILE_READ_SESSION;
	if(current_state == SESSION_WRITE)
		file = FILE_SAVE_SESSION;
	BHASSERT(FileFormat() == CLMD_FORMAT_RSC10_ID, "Invalid Attribute data file format!");
	//Save(AttrFileName(file), &dic, css);
	Save(AttrFileName(file), dic.get(), css);
	dom_inj_mngr.Save();
}

//GenerateColumnMetaDataFileName()
string RCAttr::AttrFileName(int ftype, bool oppositeName/*=false*/) const
{
	char fnm[] = { "TA00000.ctb" };

	if(ftype == 1) {
		fnm[1] = 'S'; // save file:   TS000...
	} else {
		ABSwitcher absw;
		ABSwitch cur_ab_switch_state = absw.GetState(path);

		if(oppositeName) {
			if(cur_ab_switch_state == ABS_A_STATE)
				fnm[1] = 'B';
		} else {
			if(cur_ab_switch_state == ABS_B_STATE)
				fnm[1] = 'B';
		}
	}

	BHASSERT(ftype!=2, "Trying to open file using invalid session mode!");
	fnm[6]=(char)('0'+attr_number%10);
	fnm[5]=(char)('0'+(attr_number/10)%10);
	fnm[4]=(char)('0'+(attr_number/100)%10);
	fnm[3]=(char)('0'+(attr_number/1000)%10);
	fnm[2]=(char)('0'+(attr_number/10000)%10);
	string filename(path);
	filename += fnm;
	return filename;
}

string RCAttr::DomainInjectionFileName(int ftype) const
{
	char fnm[] = {"TA00000DI.ctb"};
    ABSwitcher absw;
    ABSwitch cur_ab_switch_state = absw.GetState(path);
    if (ftype==0) {
    	if (cur_ab_switch_state==ABS_B_STATE)
    		fnm[1] = 'B';
    }
    else {
    	if (cur_ab_switch_state==ABS_A_STATE)
    		fnm[1] = 'B';
    }
	fnm[6]=(char)('0'+attr_number%10);
	fnm[5]=(char)('0'+(attr_number/10)%10);
	fnm[4]=(char)('0'+(attr_number/100)%10);
	fnm[3]=(char)('0'+(attr_number/1000)%10);
	fnm[2]=(char)('0'+(attr_number/10000)%10);
	string filename(path);
	filename += fnm;
	return filename;
}

unsigned int RCAttr::LoadPackSize(int n)
{
	unsigned int res = 0;
	IBFile fattr;
	try {
		fattr.OpenReadOnly(AttrPackFileName(n));
		fattr.Seek(dpns[n].pack_addr,SEEK_SET);
		if(PackType()==PackS){
			uchar header[13];		
			fattr.Read(header,13);			
			res = *((uint*) header);
		} else {
			uchar header[17];		
			fattr.Read(header,17);					
			res = *((uint*) header);
		}		
		fattr.Close();
	} catch (...) {
		res = 0;
	}
	return res;
}

_int64 RCAttr::ComputeCompressedSize()
{
	_int64 tsize = 0;
	_int64 size = 0;

	if(GetFileSize(DPNFileName(), size))
		tsize = size;
	// for all pack file
	for(int p_f = 0; p_f < GetTotalPackFile(); p_f++) {
		if(GetFileSize(RCAttr::AttrPackFileNameDirect(attr_number, p_f, path), size))
			tsize += size;
	}
	return tsize;
}

void RCAttr::UpgradeFormat()
{
	if(file!=0)
	{
		rclog << lock << "Error: cannot upgrade. RCAttr constructor connects to working file." << unlock;
		throw;
	}
	LoadPackInfo();
	if (PackType()==PackS)
		for (int i = 0; i < NoPack(); i++)
		{
			DPN& dpn( dpns[i] );
			dpn.local_min = 0;
			dpn.local_max = -1;
		}
	SetFileFormat(10);
	CreateDPNFile();
	string fn(AttrFileName(file));
	string backup_name(fn);
	backup_name += "bck";
    try {
	    RenameFile(fn,backup_name);
	} catch(...) {
		rclog << lock << "Internal upgrade error: unable to rename " << fn << " to " << backup_name << "." << unlock;
		throw;
	}
	SaveHeader();
	try {
        RemoveFile(backup_name);
	} catch(...) {
		rclog << lock << "Internal upgrade error: unable to remove " << fn << " backup file (change *.bck to *.ctb before next startup)." << unlock;
		throw;
	}
}

void RCAttr::CreateDPNFile()
{
	if(NoPack() == 0)
		return;
	const int buf_size = 10000;
	char buf[buf_size];
	IBFile fdpn;

	fdpn.OpenCreateEmpty(DPNFileName());

	ushort buffer_pos = 0;
	for(uint slot = 0; slot < (uint)NoPack() + 1; slot++) {
		if(buffer_pos + 37 > buf_size) {
			fdpn.WriteExact(buf, buffer_pos);
			buffer_pos = 0;
		}
		if(slot < 2) 
			StoreDPN(NoPack() - 1, buf + buffer_pos);		// double slot for last pack info for rollback handling
		else 
			StoreDPN(slot - 2, buf + buffer_pos);
		buffer_pos += 37;
	}
	fdpn.WriteExact(buf, buffer_pos);
	fdpn.Close();
}

void RCAttr::StoreDPN(uint pack_no, char* buf)
{
	DPN const& dpn(dpns[pack_no]);
	if(dpn.no_nulls == (uint) (dpn.no_objs) + 1) {
		*((int*) (buf)) = PF_NULLS_ONLY;
		*((ushort*) (buf + 34)) = 0; // no_nulls (0 is special value here)
	} else {
		*((int*) (buf)) = dpn.pack_file;
		*((ushort*) (buf + 34)) = (ushort) dpn.no_nulls;
	}
	*((uint*) (buf + 4)) = dpn.pack_addr;

	*((_uint64*) (buf + 8)) =	dpn.local_min;
	*((_uint64*) (buf + 16)) =	dpn.local_max;

	*((_uint64*) (buf + 24)) =	dpn.sum_size;

	*((ushort*) (buf + 32)) =	dpn.no_objs;
	*((uchar*) (buf + 36)) = 0;
}

string RCAttr::AttrPackFileName(int n_pack)
{
	LoadPackInfo();
	return AttrPackFileNameDirect(attr_number, dpns[n_pack].pack_file, path);
}

string RCAttr::AttrPackFileNameDirect(int attr_number, int n_file, const std::string& path)
{
	BHASSERT(n_file>=0, "Invalid input pack number!");
	char fnm[] = {"TA00000000000000.ctb"};
	fnm[15]=(char)('0'+n_file%10);
	fnm[14]=(char)('0'+(n_file/10)%10);
	fnm[13]=(char)('0'+(n_file/100)%10);
	fnm[12]=(char)('0'+(n_file/1000)%10);
	fnm[11]=(char)('0'+(n_file/10000)%10);
	fnm[10]=(char)('0'+(n_file/100000)%10);
	fnm[9]=(char)('0'+(n_file/1000000)%10);
	fnm[8]=(char)('0'+(n_file/10000000)%10);
	fnm[7]=(char)('0'+(n_file/100000000)%10);
	fnm[6]=(char)('0'+attr_number%10);
	fnm[5]=(char)('0'+(attr_number/10)%10);
	fnm[4]=(char)('0'+(attr_number/100)%10);
	fnm[3]=(char)('0'+(attr_number/1000)%10);
	fnm[2]=(char)('0'+(attr_number/10000)%10);
	string filename(path);
	filename += fnm;
	return filename;
}

string RCAttr::DPNFileName() const
{
	char fnm[] = {"TA00000DPN.ctb"};
	fnm[6]=(char)('0'+attr_number%10);
	fnm[5]=(char)('0'+(attr_number/10)%10);
	fnm[4]=(char)('0'+(attr_number/100)%10);
	fnm[3]=(char)('0'+(attr_number/1000)%10);
	fnm[2]=(char)('0'+(attr_number/10000)%10);
	string filename(path);
	filename += fnm;
	return filename;
}

time_t RCAttr::UpdateTime() const
{
	string fname = AttrFileName(current_state);
    int result = (int)GetFileTime(fname);
	if (result < 0) {
		rclog << "Error: " << fname << " status can not be obtained" << endl;
		return 0;
	}
	return (time_t) result;
}

void RCAttr::CommitSaveSession(bool same_file)
{
  //WaitForSaveThreads();
	SetSaveSessionId(INVALID_TRANSACTION_ID);
	string fname0( AttrFileName(0, !same_file) ); // AB Switch: generate alternative name
	string fname1( AttrFileName(1) );
	// if save file is not accessible for writing (or not exists), skip the attribute
	// WARNING: dangerous code  --> if(access(fname1,W_OK|F_OK)!=0) return;
    try {
	    RemoveFile(fname0);
	    RenameFile(fname1, fname0);
    } catch(...) {
		rclog << lock << Name() << ": CommitWork failed! Database may be corrupted!" << unlock;
		throw;
	}
	file = 0;
}

void RCAttr::Rollback(uint s_id)
{
	DeleteRSIs();
	if(GetSaveSessionId() != INVALID_TRANSACTION_ID) {
		if(s_id == 0 || s_id == GetSaveSessionId()) {
			RemoveFile(AttrFileName(1));
			SetSaveSessionId(INVALID_TRANSACTION_ID);
		} else
			rclog << lock << "Rollback error - invalid save session_id = " << GetSaveSessionId() << unlock;
	}
}

void RCAttr::DeleteRSIs()
{
	if (process_type == ProcessType::DATAPROCESSOR) {
		delete rsi_hist_update;
		delete rsi_cmap_update;
	} else {
		if(rsi_manager && rsi_hist_update) {
			rsi_manager->UpdateIndex(rsi_hist_update, 0, true);
		}
		if(rsi_manager && rsi_cmap_update) {
			rsi_manager->UpdateIndex(rsi_cmap_update, 0, true);
		}
	}
	rsi_hist_update = 0;
	rsi_cmap_update = 0;
}


void RCAttr::LoadPackInfo_physical(Transaction* trans)
{
#ifdef FUNCTIONS_EXECUTION_TIMES
	FETOperator fet("RCAttr::LoadPackInfo()");
#endif
    IBGuard guard(dpns_load_mutex);

	if(!GetPackInfoCollapsed())
	{
		//pthread_mutex_unlock(&cs);
		return;
	}
	if((uint)NoPack() > GetNoPackPtr())
		SetNoPackPtr((uint)NoPack());
	// load packs descriptions (file of a pack, localization in file, statistics, ...)
	if (FileFormat() != 9)
		LoadAllDPN(trans);
	if (FileFormat() == 9 || GetDictOffset() != 0)
	{
		IBFile fattr;
		char* buf_ptr = NULL;
		string fn(AttrFileName(file));
		try
		{
			fattr.OpenReadOnly(fn);
			int const SIZE_OF_DPN = 
				sizeof ( static_cast<DPN*>( NULL )->pack_file )
				+ sizeof ( static_cast<DPN*>( NULL )->pack_addr )
				+ sizeof ( static_cast<DPN*>( NULL )->local_min )
				+ sizeof ( static_cast<DPN*>( NULL )->local_max )
				+ sizeof ( static_cast<DPN*>( NULL )->sum_size )
				+ sizeof ( static_cast<DPN*>( NULL )->no_objs )
			/* read comments in RCAttr.h for DPN::no_nulls and look at RestoreDPN implementation,
			 * that is why sizeof ( static_cast<DPN*>( NULL )->no_nulls ) cannot be used here,
			 * no_nulls are stored on disk as ushort, but represented as uint at runtime in engine. */
				+ sizeof ( ushort );

			/* Ensure that on disk physical DPN layout wont change unexpectedely. */
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT( SIZE_OF_DPN == 36 );

			int fin_size = (int)fattr.Seek(0, SEEK_END);
			buf_ptr = new char [(fin_size - GetPackOffset()) + 1];
			fattr.Seek(GetPackOffset(), SEEK_SET);
			fattr.Read(buf_ptr, fin_size - GetPackOffset());
			if (FileFormat() == 9)
			{
				dpns.resize(GetNoPackPtr(), trans);
				char* buf = buf_ptr;
				for(int i=0;i<NoPack();i++)
				{
					RestoreDPN(buf, i);
					buf += SIZE_OF_DPN;
				}
			}
			if (GetDictOffset() != 0 && (fin_size - GetDictOffset())) {
				if(!Type().IsLookup())
					LoadDictionaries(buf_ptr);
				else 
					//[mk]
					{						
						if(process_type == ProcessType::BHLOADER || process_type == ProcessType::DIM)
							dic = GlobalDataCache::GetGlobalDataCache().GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this));
						else
							dic = trans->GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this));
					}
					// [emk]

			}
				
			delete [] buf_ptr;
			fattr.Close();
		} catch (DatabaseRCException&) {
			delete [] buf_ptr;
			SetPackInfoCollapsed(false);
			//pthread_mutex_unlock(&cs);
			rclog << lock << "Internal error: unable to open/read " << fn << " file." << unlock;
			throw;	//newly added
		}

		//try {
		//	if (Type().IsLookup() && GetDictOffset() != 0)			
		//	if(process_type != ProcessType::BHLOADER)
		//	dic = ConnectionInfoOnTLS->GetTransaction()->GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this, true));
		//	else
		//	dic = GlobalDataCache::GetGlobalDataCache().GetObject<FTree>(FTreeCoordinate(table_number, attr_number), bind(&RCAttr::LoadLookupDictFromFile, this, true));
		//} 
		//catch (DatabaseRCException&)
		//{
		//	SetPackInfoCollaped(false);			
		//	rclog << lock << "Internal error: unable to open/read " << AttrFileName(file) << " file." << unlock;
		//	throw;
		//}
	}
	if(NoPack()>0 && dpns.empty())
		rclog << lock << "Error: out of memory (" << NoPack() << " bytes failed). (22)" << unlock;

	SetPackInfoCollapsed(false);

}

void RCAttr::LoadDictionaries(const char* buf_ptr)
{
	AttributeType a_type = TypeName();
	// load dictionary for the whole attribute
	const char* buf = buf_ptr + (GetDictOffset() - GetPackOffset());
	// dictionary for symbolic (text) values - min, max, FTree object
	if(Type().IsLookup())
	{
		SetMinInt64(*((_uint64*)(buf)));
		SetMaxInt64(*((_uint64*)(buf+8)));
		buf+=16;
		if(!dic)			
			dic = shared_ptr<FTree>(new FTree());			
		dic->Init((uchar*&)buf);		// note that dic.Init will shift buf to the end of minicompressor stream
	}
	// dictionary for numerical values - min, max
	else if(
		ATI::IsNumericType(a_type) ||
		ATI::IsDateTimeType(a_type) || ATI::IsDateTimeNType(a_type))
	{
		SetMinInt64(*((_uint64*)(buf)));
		SetMaxInt64(*((_uint64*)(buf+8)));
		buf+=16;
	}
	else
	{
		// ... other types of dictionary ...
	}
}

shared_ptr<FTree> RCAttr::LoadLookupDictFromFile() // bool include_restore_dpn)
{
	string fn;
	if (GetDictOffset() != 0)
	{
		IBFile fattr;
		char* buf_ptr = NULL;
		try {
			fattr.OpenReadOnly(fn = AttrFileName(file));
			int fin_size = (int)fattr.Seek(0, SEEK_END);
			buf_ptr = new char [(fin_size - GetPackOffset()) + 1];
			fattr.Seek(GetPackOffset(), SEEK_SET);
			fattr.ReadExact(buf_ptr, fin_size - GetPackOffset());
			if(GetDictOffset() != 0 && (fin_size - GetDictOffset())) 
				LoadDictionaries(buf_ptr);
			delete [] buf_ptr;
			fattr.Close();
		} catch (DatabaseRCException&) {
			delete [] buf_ptr;
			//rclog << lock << "Internal error: unable to load column dictionary from " << fn << ". " << GetErrorMessage(errno) << unlock;
			throw;
		}
	}
	return dic;
}

void RCAttr::LoadAllDPN(Transaction* trans)
{
	if(NoPack() == 0)
		return;
	try {
		IBFile fdpn;
		fdpn.OpenReadOnly(DPNFileName());
		fdpn.Close();
		dpns.resize(NoPack(), trans); /* FIXME */
	} catch (DatabaseRCException&) {
		rclog << lock << "Internal error: unable to open " << DPNFileName() << " file." << unlock;
		throw; //newly added
	}
	return;
}

void RCAttr::RestoreDPN(char* buf, DPN& dpn)
{
	dpn.pack.reset();
	dpn.pack_file = *((int*)buf);
	dpn.pack_addr = *((uint*)(buf+4));
	dpn.local_min = *((_uint64*)(buf+8));
	dpn.local_max = *((_uint64*)(buf+16));
	if(PackType() == PackN)
		dpn.sum_size = *((_uint64*)(buf+24));
	else
		dpn.sum_size = ushort(*((_uint64*)(buf+24)));
	dpn.no_objs = *((ushort*)(buf+32));
	dpn.no_nulls = *((ushort*)(buf+34));
	if(dpn.pack_file == PF_NULLS_ONLY)
		dpn.no_nulls = dpn.no_objs + 1;
	if(dpn.pack_file == PF_NULLS_ONLY ||
		(PackType() == PackN && dpn.local_min == dpn.local_max && dpn.no_nulls==0)) {
		dpn.pack_mode = PACK_MODE_TRIVIAL;					// trivial data (only nulls or all values are the same), no physical pack
		dpn.is_stored = false;
	}
	else if(dpn.pack_file == PF_NO_OBJ) {
		dpn.pack_mode = PACK_MODE_EMPTY;					// empty pack, no objects
		dpn.is_stored = false;
	}
	else {
		dpn.pack_mode = PACK_MODE_UNLOADED;					// non trivial pack - data on disk
		dpn.is_stored = true;
	}

	if(FileFormat() > 9) {
		// restore 37-th byte
	}
}

void RCAttr::RestoreDPN(char* buf, uint pack_no)
{
	RestoreDPN( buf, dpns[pack_no] );
}

//void RCAttr::WriteUnique(RSValue v)
//{
//	IBFile fattr;
//
//	if(v!=RS_UNKNOWN)
//	{
//		SetUniqueUpdated(true);
//		if(v==RS_ALL) SetUnique(true);
//		if(v==RS_NONE) SetUnique(false);
//
//		//char flags = char(nulls_mode)+(declared_unique?4:0)+(is_primary?8:0)+(is_unique?16:0)+(is_unique_updated?32:0);
//		char flags = char(Type().GetNullsMode()) +(IsUnique()?16:0)+(IsUniqueUpdated()?32:0);
//		string fname(AttrFileName(0));
//		fattr.OpenReadWriteWithThreadAffinity(fname);
//		fattr.Seek(25,SEEK_SET);
//		fattr.Write(&flags,1);
//		fattr.CloseWithThreadAffinity(fname);
//	}
//}

PackOntologicalStatus RCAttr::GetPackOntologicalStatus(int pack_no)
{
	LoadPackInfo();
	DPN const* dpn( pack_no >= 0 ? &dpns[pack_no] : NULL );
	if(pack_no < 0 || dpn->pack_file == PF_NULLS_ONLY || dpn->no_nulls-1 == dpn->no_objs)
		return NULLS_ONLY;
	if(PackType() == PackN)
	{
		if(dpn->local_min == dpn->local_max)
		{
			if(dpn->no_nulls == 0)
				return UNIFORM;
			return UNIFORM_AND_NULLS;
		}
	}
	return NORMAL;
}

bool RCAttr::ShouldExist(int pack_no)
{
	LoadPackInfo();
	DPN const& dpn(dpns[pack_no]);
	if(dpn.pack_file == PF_NULLS_ONLY || dpn.pack_file == PF_NO_OBJ || dpn.no_nulls - 1 == dpn.no_objs)
		return false;
	if(PackType() == PackN) {
		if(dpn.no_nulls == 0 && dpn.local_min == dpn.local_max)
			return false;
	} else
		return dpn.pack_mode == PACK_MODE_UNLOADED;
	return true;
}


//old name GetTable_S
RCBString RCAttr::GetValueString(const _int64 obj)
{
	if(obj == NULL_VALUE_64)
		return RCBString();
	int pack = (int) (obj >> 16);
	if(PackType() == PackS) {
		DPN const& dpn( dpns[pack] );
		if(dpn.pack_mode == PACK_MODE_TRIVIAL || dpn.pack_file == PF_NULLS_ONLY)
			return RCBString();
		assert(dpn.pack->IsLocked());
		AttrPackS *cur_pack = (AttrPackS*)dpn.pack.get();

		if(cur_pack->IsNull((int) (obj & 65535)))
			return RCBString();

		int len = cur_pack->GetSize((int) (obj & 65535));
		if(len)
			return RCBString(cur_pack->GetVal((int) (obj & 65535)), len);
		else
			return ZERO_LENGTH_STRING;
	}
	_int64 v = GetValueInt64(obj);
	return DecodeValue_S(v);
}

RCBString RCAttr::GetNotNullValueString(const _int64 obj)
{
	int pack = (int) (obj >> 16);
	if(PackType() == PackS) {
		BHASSERT(pack <= dpns.size(), "Reading past the end of DPNs");
		DPN const& dpn( dpns[pack] );
		BHASSERT(dpn.pack->IsLocked(), "Access unlocked pack");
		AttrPackS *cur_pack = (AttrPackS*)dpn.pack.get();
		BHASSERT(cur_pack!=NULL, "Pack ptr is null");
		int len = cur_pack->GetSize((int) (obj & 65535));
		if(len)
			return RCBString(cur_pack->GetVal((int) (obj & 65535)), len);
		else
			return ZERO_LENGTH_STRING;
	}
	_int64 v = GetNotNullValueInt64(obj);
	return DecodeValue_S(v);
}

void RCAttr::GetValueBin(_int64 obj, int& size, char* val_buf)		// original 0-level value (text, string, date, time etc.)
{
	if(obj == NULL_VALUE_64)
		return;
	AttributeType a_type = TypeName();
	size = 0;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
	LoadPackInfo();
	int pack = (int)(obj >> 16);	// simplified version: all packs are full
	DPN const& dpn( dpns[pack] );
	if(dpn.pack_file == PF_NULLS_ONLY)
		return;
	if(ATI::IsStringType(a_type)) {
		if(PackType() == PackN) {
			_int64 res = GetValueInt64(obj);
			if(res == NULL_VALUE_64)
				return;
			size = dic->ValueSize((int)res);
			memcpy(val_buf, dic->GetBuffer((int)res), size);
			return;
		} else {			// no dictionary
			if(dpn.pack_mode == PACK_MODE_TRIVIAL)
				return;
			assert(dpn.pack->IsLocked());
			size = ((AttrPackS*)dpn.pack.get())->GetSize((int)(obj & 65535));
			memcpy(val_buf, ((AttrPackS*)dpn.pack.get())->GetVal((int)(obj & 65535)), size);
			return;
		}
	} else if(ATI::IsInteger32Type(a_type)) {
		size = 4;
		_int64 v = GetValueInt64(obj);
		if(v == NULL_VALUE_64)
			return;
		*(int*)val_buf = int(v);
		val_buf[4] = 0;
		return;
	} else if(a_type == RC_NUM || a_type == RC_BIGINT || ATI::IsRealType(a_type) || ATI::IsDateTimeType(a_type)) {
		size = 8;
		_int64 v = GetValueInt64(obj);
		if(v == NULL_VALUE_64)
			return;
		*(_int64*)(val_buf) = v;
		val_buf[8] = 0;
		return;
	}
	return;
}

extern bool IsTimeStampZero(MYSQL_TIME& t);

RCValueObject RCAttr::GetValue(_int64 obj, bool lookup_to_num)
{
	if(obj == NULL_VALUE_64)
		return RCValueObject();
	AttributeType a_type = TypeName();
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
	RCValueObject ret;
	if(!IsNull(obj))
	{
		if(ATI::IsTxtType(a_type) && !lookup_to_num)
			ret = GetNotNullValueString(obj);
		else if(ATI::IsBinType(a_type))
		{
			int tmp_size = GetLength(obj);
			RCBString rcbs(NULL, tmp_size, true);
			GetValueBin(obj, tmp_size, rcbs.val);
			rcbs.null = false;
			ret = rcbs;
		}
		else if(ATI::IsIntegerType(a_type))
			ret = RCNum(GetNotNullValueInt64(obj), -1, false, a_type);
		else if(a_type == RC_TIMESTAMP) {
#ifdef PURE_LIBRARY
	BHERROR("NOT IMPLEMENTED! Depends on MySQL code.");
#else
			// needs to convert UTC/GMT time stored on server to time zone of client
			RCBString s = GetValueString(obj);
			MYSQL_TIME myt;
			int not_used;
			// convert UTC timestamp given in string into TIME structure
			str_to_datetime(s.Value(), s.len, &myt, TIME_DATETIME_ONLY, &not_used);
			return RCDateTime(myt.year, myt.month, myt.day, myt.hour, myt.minute, myt.second, RC_TIMESTAMP);			
#endif
		} else if(ATI::IsDateTimeType(a_type))
			ret = RCDateTime(this->GetNotNullValueInt64(obj), a_type);
		else if(ATI::IsRealType(a_type))
			ret = RCNum(this->GetNotNullValueInt64(obj), 0, true, a_type);
		else if(lookup_to_num || a_type == RC_NUM)
			ret = RCNum((_int64)GetNotNullValueInt64(obj), Type().GetScale());
	}
	return ret;
}

RCDataType& RCAttr::GetValue(_int64 obj, RCDataType& value, bool lookup_to_num)
{
	if(obj == NULL_VALUE_64 || IsNull(obj))
		value = ValuePrototype(lookup_to_num);
	else {
		AttributeType a_type = TypeName();
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
		if(ATI::IsTxtType(a_type) && !lookup_to_num)
			((RCBString&)value) = GetNotNullValueString(obj);
		else if(ATI::IsBinType(a_type)) {
			int tmp_size = GetLength(obj);
			((RCBString&)value) = RCBString(NULL, tmp_size, true);
			GetValueBin(obj, tmp_size, ((RCBString&)value).val);
			value.null = false;
		} else if(ATI::IsIntegerType(a_type))
			((RCNum&)value).Assign(GetNotNullValueInt64(obj), -1, false, a_type);
		else if(ATI::IsDateTimeType(a_type)) {
			((RCDateTime&)value) = RCDateTime(this->GetNotNullValueInt64(obj), a_type);
		} else if(ATI::IsRealType(a_type))
			((RCNum&)value).Assign(this->GetNotNullValueInt64(obj), 0, true, a_type);
		else
			((RCNum&)value).Assign(this->GetNotNullValueInt64(obj), Type().GetScale());
	}
	return value;
}

_int64 RCAttr::GetNoNulls(int pack)
{
	LoadPackInfo();
	if(pack == -1)
		return NoNulls();
	return dpns[pack].no_nulls;
}

unsigned int RCAttr::GetNoValues(int pack)
{
	LoadPackInfo();
	return dpns[pack].GetNoObj();
}

ushort RCAttr::GetActualSize(int pack)
{
	if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
		return 0;
	ushort ss = (ushort)dpns[pack].sum_size;
	if(Type().IsLookup() || PackType() != PackS || ss == 0)
		return Type().GetPrecision();
	return ss;
}

_int64 RCAttr::GetSum(int pack, bool &nonnegative)
{
	LoadPackInfo();
	DPN const& dpn( dpns[pack] );
	if(GetPackOntologicalStatus(pack) == NULLS_ONLY || dpns.empty() || Type().IsString())
		return NULL_VALUE_64;
	if(!Type().IsFloat() && (dpn.local_min < (MINUS_INF_64 / 65536) || dpn.local_max > (PLUS_INF_64 / 65536)))
		return NULL_VALUE_64;								// conservative overflow test for int/decimals
	nonnegative = (dpn.local_min >= 0);
	return dpn.sum_size;
}

_int64 RCAttr::GetMinInt64(int pack)
{
	LoadPackInfo();
	if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
		return MINUS_INF_64;
	return dpns[pack].local_min;
}

_int64 RCAttr::GetMaxInt64(int pack)
{
	LoadPackInfo();
	if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
		return PLUS_INF_64;
	return dpns[pack].local_max;
}


RCBString RCAttr::GetMaxString(int pack)
{
	LoadPackInfo();
	if(GetPackOntologicalStatus(pack) == NULLS_ONLY || pack_type != PackS)
		return RCBString();
	char* s = (char*)&dpns[pack].local_max;
	int max_len = GetActualSize(pack);
	if(max_len > 8)
		max_len = 8;
	int min_len = max_len - 1;
	while(min_len >= 0 && s[min_len] != '\0') 
		min_len--;
	return RCBString(s, min_len >= 0 ? min_len : max_len, true);
}

RCBString RCAttr::GetMinString(int pack)
{
	LoadPackInfo();
	if(GetPackOntologicalStatus(pack) == NULLS_ONLY || pack_type != PackS)
		return RCBString();
	char* s = (char*)&dpns[pack].local_min;
	int max_len = GetActualSize(pack);
	int min_len = (max_len > 8 ? 8 : max_len);
	while(min_len > 0 && s[min_len - 1] == '\0') 
		min_len--;
	return RCBString(s, min_len, true);
}


std::auto_ptr<DPN> RCAttr::GetDPNWithoutDataPack(const DataPackId& dpid)
{
	LoadPackInfo();
	return dpns[(int)dpid].CloneWithoutPack();
}

std::auto_ptr<DataPack> RCAttr::GetDataPack(const DataPackId& dpid, ConnectionInfo& conn)
{
	LoadPackInfo();
	std::auto_ptr<DataPack> dp;
	if(ATI::IsStringType(TypeName()))
		dp = auto_ptr<DataPack>(new DataPackImpl<RCBString>(dpns[(int)dpid].GetNoObj()));
	else if(ATI::IsDateTimeType(TypeName()))
		dp = auto_ptr<DataPack>(new DataPackImpl<RCDateTime>(dpns[(int)dpid].GetNoObj()));
	else
		dp = auto_ptr<DataPack>(new DataPackImpl<RCNum>(dpns[(int)dpid].GetNoObj()));

	shared_ptr<DataPackLock> dp_lock(new DataPackLock(*this, dpid));
	if(ATI::IsStringType(TypeName())) {
		dp->dp_lock = dp_lock;
		if(!Type().IsLookup()) {
			if(ShouldExist((int)dpid)) {
				dp->SetDecomposerID(((AttrPackS&)(*dpns[(int)dpid].pack)).GetDecomposerID());
				dp->outliers = ((AttrPackS&)(*dpns[(int)dpid].pack)).GetNoOutliers();
			} else
				dp->SetDecomposerID((uint)dpns[(int)dpid].local_min);
		}
	}

	_uint64 i = (((_uint64)dpid - packs_omitted) << 16);
	_uint64 last = i + GetNoValues(dpid - packs_omitted);
	ushort id = 0;
	for(; i < last; i++)
		GetValue(i, (*dp)[id++]);

	return dp;
}

int	RCAttr::GetLength(_int64 obj)		// size of original 0-level value (text/binary, not null-terminated)
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(NoObj()>=(_int64)obj);
	LoadPackInfo();
	int pack = (int)(obj >> 16);									// simplified version: all packs are full
	DPN const& dpn( dpns[pack] );
	if(dpn.pack_file == PF_NULLS_ONLY)	
		return 0;
	if(PackType() != PackS) 
		return Type().GetDisplaySize();
	return ((AttrPackS*)dpn.pack.get())->GetSize((int)(obj&65535));
}

RCBString RCAttr::DecodeValue_S(_int64 code)			// original 0-level value for a given 1-level code
{
	if(code == NULL_VALUE_64) {
		return RCBString();
	}
	if(Type().IsLookup()) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(PackType() == PackN);
		return dic->GetRealValue((int)code);
	}
	AttributeType a_type = TypeName();
	if(ATI::IsIntegerType(a_type)) {
		RCNum rcn(code, -1, false, a_type);
		RCBString local_rcb = rcn.ToRCString();
		local_rcb.MakePersistent();
		return local_rcb;
	} else if(ATI::IsRealType(a_type)) {
		RCNum rcn(code, -1, true, a_type);
		RCBString local_rcb = rcn.ToRCString();
		local_rcb.MakePersistent();
		return local_rcb;
	} else if(a_type == RC_NUM) {
		RCNum rcn(code, Type().GetScale(), false, a_type);
		RCBString local_rcb = rcn.ToRCString();
		local_rcb.MakePersistent();
		return local_rcb;
	} else if(ATI::IsDateTimeType(a_type)) {
		RCDateTime rcdt(code, a_type);
		if(a_type == RC_TIMESTAMP) {			
			RCDateTime::AdjustTimezone(rcdt);
		}
		RCBString local_rcb = rcdt.ToRCString();
		local_rcb.MakePersistent();
		return local_rcb;
	}
	return RCBString();
}

int RCAttr::EncodeValue_T(const RCBString& rcbs, int new_val, BHReturnCode* bhrc)	// 1-level code value for a given 0-level (text) value
													// if new_val=1, then add to dictionary if not present
{
	if(bhrc)
		*bhrc = BHRC_SUCCESS;
	if(rcbs.IsNull()) return NULL_VALUE_32;
	if(ATI::IsStringType(TypeName())) {
		BHASSERT_WITH_NO_PERFORMANCE_IMPACT(PackType() == PackN);
		LoadPackInfo();
		int vs;
		if(new_val ==0 )
			vs = dic->GetEncodedValue(rcbs);
		else
			vs=dic->Add(rcbs);
		if(vs < 0) 
			return NULL_VALUE_32;
		return vs;
	}
	char const* val = rcbs.val;
	if(val == 0)
		val = ZERO_LENGTH_STRING;
	if(ATI::IsDateTimeType(TypeName()) || TypeName() == RC_BIGINT) {
		BHASSERT(0, "Wrong data type!");
	} else {
		RCNum rcn;
		BHReturnCode tmp_bhrc = RCNum::Parse(rcbs, rcn, TypeName());
		if(bhrc)
			*bhrc = tmp_bhrc;
		return (int)(_int64)rcn;
	}
	return NULL_VALUE_32;
}

// transform a RCNum value into 1-level code, take into account the precision etc.
// no changes for REAL; rounded=true iff v has greater precision than the column and the returned result is rounded down
_int64 RCAttr::EncodeValue64(RCDataType *v, bool &rounded, BHReturnCode* bhrc)
{
	rounded = false;
	if(bhrc)
		*bhrc = BHRC_SUCCESS;
	if(!v || v->IsNull())
		return NULL_VALUE_64;

	if((Type().IsLookup() && v->Type() != RC_NUM)) {
		return EncodeValue_T(v->ToRCString(), 0, bhrc);
	} else if(ATI::IsDateTimeType(TypeName()) || ATI::IsDateTimeNType(TypeName())) {
		return ((RCDateTime*) v)->GetInt64();
	}
	BHASSERT(PackType() == PackN, "Pack type must be numeric!");

	_int64 vv = ((RCNum*) v)->Value();
	int vp = ((RCNum*) v)->Scale();
	if(ATI::IsRealType(TypeName())) {
		if(((RCNum*)v)->IsReal())
			return vv; // already stored as double
		double res = double(vv);
		res /= Uint64PowOfTen(vp);
		//for(int i=0;i<vp;i++) res*=10;
		return *(_int64*) (&res); // encode
	}
	if(((RCNum*)v)->IsReal()) { // v is double
		double vd = *(double*)(&vv);
		vd *= Uint64PowOfTen(Type().GetScale());				// translate into _int64 of proper precision
		if(vd > PLUS_INF_64)
			return PLUS_INF_64;
		if(vd < MINUS_INF_64)
			return MINUS_INF_64;
		_int64 res = _int64(vd);
		if(fabs(vd - double(res)) > 0.01)
			rounded = true; // ignore errors which are 2 digits less than declared precision
		return res;
	}
	unsigned char dplaces = Type().GetScale();
	while(vp < dplaces) {
		if(vv < MINUS_INF_64 / 10)
			return MINUS_INF_64;
		if(vv > PLUS_INF_64 / 10)
			return PLUS_INF_64;
		vv *= 10;
		vp++;
	}
	while(vp > dplaces) {
		if(vv % 10 != 0)
			rounded = true;
		vv /= 10;
		vp--;
	}
	return vv;
}

_int64 RCAttr::EncodeValue64(const RCValueObject& v, bool& rounded, BHReturnCode* bhrc)
{
	return EncodeValue64(v.Get(), rounded, bhrc);
}

RCBString RCAttr::GetPrefix(int pack)
{
	LoadPackInfo();

	if(GetPackOntologicalStatus(pack) == NULLS_ONLY)
		return 0;

	DPN const& dpn( dpns[pack] );
	char* min_s = (char*)&dpn.local_min;
	char* max_s = (char*)&dpn.local_max;


	int dif_pos = 0;
	for(; ( dif_pos < sizeof(_uint64) )
		&& min_s[dif_pos]
		&& max_s[dif_pos]
		&& ( min_s[dif_pos] == max_s[dif_pos] ); ++ dif_pos )
		;

	if ( dif_pos == 0 )
		return ZERO_LENGTH_STRING;

	RCBString s(min_s, dif_pos, true);
	return s;
}

uint AttributeTypeInfo::ExternalSize(RCAttr* attr, EDF edf)
{
	AttributeType attrt = attr->TypeName();
	return AttributeTypeInfo::ExternalSize(attrt, attr->Type().GetPrecision(), attr->Type().GetScale(), edf);
}

uint AttributeTypeInfo::ExtrnalSize(EDF edf) const
{
	return AttributeTypeInfo::ExternalSize(attrt, precision, scale, edf);
}

uint AttributeTypeInfo::ExternalSize(AttributeType attrt, int precision, int scale, EDF edf)
{
	return DataFormat::GetDataFormat(edf)->ExtrnalSize(attrt, precision, scale);
}

int AttributeTypeInfo::TextSize()
{
	return DataFormat::GetDataFormat(TXT_VARIABLE)->ExtrnalSize(attrt, precision, scale, &collation);
}

bool AttributeTypeInfo::IsFixedSize(AttributeType attr_type, EDF edf)
{
	return !DataFormat::GetDataFormat(edf)->IsVariableSize(attr_type);
}

void RCAttr::LockPackForUse(unsigned pack_no, ConnectionInfo& conn)
{
	if(conn.killed())
		throw KilledRCException();
	LockPackForUse(pack_no, *conn.GetTransaction(), boost::bind(&ConnectionInfo::NotifyPackLoad, &conn));
}

void RCAttr::LockPackForUse(unsigned pack_no, Transaction& trans, boost::function0<void> notify_pack_load)
{
	assert((int)pack_no < NoPack());
	LoadPackInfo(trans);
	{
		IBGuard m_dpns_guard(m_dpns_mutex);
		DPN& dpn( dpns[pack_no] );
		if(dpn.no_pack_locks) {
			dpn.no_pack_locks++;
			dpn.pack->Lock();
			return;
		} else if(!dpn.is_stored)
			return;
	}

	AttrPackPtr p = trans.GetAttrPack(PackCoordinate(table_number, attr_number, pack_no + packs_omitted,0,0), *m_allocator);

	{
		IBGuard m_dpns_guard(m_dpns_mutex);
		DPN& dpn( dpns[pack_no] );
		if(dpn.no_pack_locks) {
			dpn.no_pack_locks++;
			// GetAttrPack already does the lock
			//dpn.pack->Lock();
			return;
		}
		dpn.no_pack_locks++;
		if(notify_pack_load)
			notify_pack_load();
		//if(!dpn.pack) {
			assert(!dpn.pack);
			dpn.pack = p;
		//}
	}
}


void RCAttr::LockPackForUse(unsigned pack_no)
{
	LockPackForUse(pack_no, ConnectionInfoOnTLS.Get());
}

void RCAttr::LockLastPackForUse(Transaction& trans)
{
	if(NoPack() > 0)
		LockPackForUse(NoPack() - 1, trans);
}

void RCAttr::UnlockLastPackFromUse()
{
	if(NoPack() > 0)
		UnlockPackFromUse(NoPack() - 1);
}

void RCAttr::UnlockPackFromUse(unsigned pack_no)
{
	MEASURE_FET("RCAttr::UnlockPackFromUse(...)");
	assert((int)pack_no<NoPack());
	LoadPackInfo();
	IBGuard m_dpns_guard(m_dpns_mutex);
	DPN& dpn( dpns[pack_no] );
	if(dpn.no_pack_locks) {
		dpn.no_pack_locks--;
		if (dpn.no_pack_locks==0) {
			if (dpn.pack->IsEmpty() || CachingLevel == 0)
				ConnectionInfoOnTLS->GetTransaction()->ResetAndUnlockOrDropPack(dpn.pack);
			else
				TrackableObject::UnlockAndResetOrDeletePack(dpn.pack);
			if(dpn.pack_mode == PACK_MODE_IN_MEMORY)
				dpn.pack_mode = PACK_MODE_UNLOADED;
		}
		else
			dpn.pack->Unlock();
	}
}

bool RCAttr::OldFormat()
{
	return (FileFormat() < 10);
}

/******************************************
 * ABSwitcher implementation
 ******************************************/

ABSwitch ABSwitcher::GetState(std::string const&  path)
{
		string name;
    GenerateName(name, path);
    return (DoesFileExist(name)) ? ABS_B_STATE : ABS_A_STATE;
}

int ABSwitcher::FlipState(std::string const& path)
{
		string name;
    bool is_file_exist;

    GenerateName(name, path);

    is_file_exist = DoesFileExist(name);
    if (is_file_exist) {
        // File exists. Delete it now.
        try {
            RemoveFile(name);
        } catch(...) {
            rclog << lock << "ERROR: Failed to delete AB switch file " << name << unlock;
            return -1;
        }
    }
    else {
        // File does not exist. Create it now.
        try {
            IBFile ibfl;
            ibfl.OpenCreate(name);
        } catch(...) {
            rclog << lock << "ERROR: Failed to create AB switch file " << name << unlock;
            return -1;
        }
    }

    return 0;
}

void ABSwitcher::GenerateName(std::string& name, std::string const& path)
{
		name = path;
		name += "/";
		name += AB_SWITCH_FILE_NAME;
}

/*static*/
const char* ABSwitcher::SwitchName( ABSwitch value )
{
    if (value == ABS_A_STATE) return "ABS_A_STATE";
    return "ABS_B_STATE";
}

/////// RCAttr metadata methods


void RCAttr::Load(string const& cmd_file)
{
	uint bytes_processed;
	boost::shared_array<char> tmp_buf(new char [10000]);
	try {
		IBFile fattr_read;
		fattr_read.OpenReadOnly(cmd_file);
		fattr_read.ReadExact(tmp_buf.get(), CLMD_HEADER_LEN);

		bytes_processed = LoadHeader(tmp_buf.get());

		uint bufsize = fattr_read.Read(tmp_buf.get(), 10000);

		bytes_processed = LoadColumnInfo(tmp_buf.get());
		LoadSessionInfo(tmp_buf.get() + bytes_processed, bufsize, &fattr_read);

	} catch(DatabaseRCException& e) {
		rclog << lock << "Error while opening file " << cmd_file << " : " << e.what() << unlock;
		throw;
	}
}

uint RCAttr::LoadHeader(const char* a_buf)
{
	natural_size_saved = 0;
	compressed_size = 0;

	bool is_lookup;
	NullMode nulls_mode;
	int scale = 0;
	int precision = 0;
	AttributeType	type;

	file_format = FILEFORMAT(a_buf);
	if (file_format == CLMD_FORMAT_INVALID_ID)
		return RCATTR_BADFORMAT;

	if ((uchar)a_buf[8] < 128) {
		type = (AttributeType)a_buf[8];
		is_lookup = true;
	} else {
		type = (AttributeType)((uchar)a_buf[8]-128);
		is_lookup = false;
	}

	if (((type == RC_VARCHAR ||
	  type == RC_STRING) && is_lookup) ||
	  ATI::IsNumericType(type) ||
	  ATI::IsDateTimeType(type))
		pack_type = PackN;
	else
		pack_type = PackS;

	no_obj = *((_int64*)(a_buf + 9));
	no_nulls = *((_int64*)(a_buf + 17));

	nulls_mode = (NullMode)(a_buf[25] % 4);

	SetUnique(((a_buf[25] & 0x10) ? true : false));
	SetUniqueUpdated(((a_buf[25] & 0x20) ? true : false));

	if(a_buf[25] & 0x40)
		inserting_mode = 1;
	else
		inserting_mode = 0;

	precision = *((ushort*)(a_buf + 26));
	scale = a_buf[28];

	ct.Initialize(type, nulls_mode, is_lookup, precision, scale);

	no_pack = *((int*)(a_buf + 30));

	packs_offset = *((uint*)(a_buf + 34));
	dict_offset = *((uint*)(a_buf + 38));
	special_offset = *((uint*)(a_buf + 42));

	i_min = 0;
	i_max = 0;

	last_pack_index = no_pack != 0 ? no_pack - 1 : 0;
	no_pack_ptr = 0;

    return 46;  // 42 + 4, where 42 is an offset for "special_offset" and 4 is a size of of "special_offset"
}

uint RCAttr::LoadColumnInfo(const char* a_buf)
{
	const char* buf = a_buf;
//	size_t const name_ct(strlen(buf) + 1);

//	char name_tmp [name_ct];

	SetName(buf);
//	strcpy(name, buf);
	buf += strlen(buf) + 1;

	SetDescription(buf);
//	size_t const desc_ct(strlen(buf)+1);
//	desc = new char [desc_ct];
//	strcpy(desc, buf);
	buf += strlen(buf) + 1;

	total_p_f =* ((int*)buf);
	buf += 4;

	// the remainings after table of files' locations (if not default)
	int p_f = *((int*)buf);
	if (p_f != PF_END)
	{
		char const eMsg[] = "Error in attribute file: bad format";
		rclog << lock << eMsg << unlock;
		throw DatabaseRCException( eMsg );
	}
	buf += 4;
	return (uint)(a_buf - buf);
}

uint RCAttr::LoadSessionInfo(const char* cur_buf, uint actual_size, IBFile* fcmd_file)
{
	char* tmp_buf = new char [10000];

	// internal representation (which PackX class to use): numerical, text or binary?
	pack_info_collapsed = (dict_offset || no_pack);

	// load special blocks
	if(actual_size < special_offset + 27 + 2 * sizeof(_uint64)) {
		fcmd_file->Seek(special_offset, SEEK_SET);
		fcmd_file->Read(tmp_buf, 27 + 2 * sizeof(_uint64));
		cur_buf = tmp_buf;
	}
	else
		cur_buf = cur_buf + special_offset;

	if (special_offset>0 && *(uchar*)(cur_buf+5))
		fcmd_file->Seek(special_offset, SEEK_SET);

	while (special_offset > 0) {
		int b_len = *(int*)cur_buf;
		uchar b_type = *(uchar*)(cur_buf + 4);
		uchar b_next = *(uchar*)(cur_buf + 5);

		if(b_type == 0)	{ 								// type 0: the first special block, rollback and session info
			savefile_loc[0] = *((int*)(cur_buf + 6));		// the first special block: rollback and session info
			savefile_loc[1] = *((int*)(cur_buf + 10));
			savepos_loc[0] = *((uint*)(cur_buf + 14));
			savepos_loc[1] = *((uint*)(cur_buf + 18));
			current_save_loc = cur_buf[22];
			//if (open_session_type == 0 || open_session_type == 3)	// only in case of read (normal/backup) sessions, otherwise read_session_id remains 0
			//	last_save_session_id = *((uint*)(cur_buf + 23));
			natural_size_saved = *((_uint64*)(cur_buf + 23 + sizeof(uint)));
			compressed_size = *((_uint64*)(cur_buf + 23 + sizeof(uint) + sizeof(_uint64)));
		}
		else
			rclog << lock << "Warning: special block (unknown type " << int(b_type) << ") ignored in attribute " << Name() << unlock;

		if (b_next == 0)
			special_offset = 0;
		else {
			fcmd_file->Seek(b_len, SEEK_CUR);
			fcmd_file->Read(&b_len, 4);
			fcmd_file->Seek(-4, SEEK_CUR);
			fcmd_file->Read(tmp_buf, b_len);
			cur_buf = tmp_buf;
		}
	}
	current_read_loc = current_save_loc;
	delete [] tmp_buf;
	return 1;
}

bool RCAttr::Save(string const& cmd_file, FTree* dic, _int64 comprs_size)
{
	IBFile fattr;
	fattr.OpenCreateEmpty(cmd_file);

	const int buf_size = 10000;
	char buf[buf_size];

	strcpy(buf, CLMD_FORMAT_RSC10);

	// Reserved for security encryption info
	*((uint*)(buf + 4)) = 0;

	// The first bit (0x80) indicates string interpretation: 0 for dictionary, 1 for long text (>=RSc4)
	buf[8] = (char)((char)TypeName() + (Type().IsLookup() ? 0 : 0x80));
	*((_int64*)(buf + 9)) = no_obj;
	*((_int64*)(buf + 17)) = no_nulls;

	// buf[25] stores attribute's flags: null_mode + 4*declared_unique + 8*is_primary  + 16*is_unique + 32*is_unique_updated
	// Now declared_unique, is_primary are obsolete. Can be removed in future file format
	buf[25] = char(Type().GetNullsMode()) + (IsUnique() ?16:0) + (IsUniqueUpdated() ?32:0) + (inserting_mode == 1? 0x40:0);
	*((ushort*)(buf + 26)) = Type().GetPrecision();
	buf[28] = Type().GetScale();

	// Now it is obsolete field. Can be removed in future file format
	buf[29] = (uchar)0;
	*((int*)(buf + 30)) = no_pack;

	// Actual values will be written at the end
	packs_offset = 0;
	dict_offset = 0;
	special_offset = 0;

	// Actual values will be written at the end
	*((uint*)(buf+34)) = packs_offset;
	*((uint*)(buf+38)) = dict_offset;
	*((uint*)(buf+42)) = special_offset;

	// Temporary, because there is no offset values yet
	fattr.WriteExact(buf, 46);

	fattr.WriteExact(Name(), (int)strlen(Name()) + 1);
	if (Description())
		fattr.WriteExact(Description(),(int)strlen(Description()) + 1);
	else
		fattr.WriteExact("\0", 1);

	// List of all nontrivial path names
	fattr.WriteExact((char*)(&total_p_f),4);

	int p_f=PF_END;
	fattr.WriteExact((char*)(&p_f),4);

	packs_offset = (uint)fattr.Tell();
	dict_offset = (uint)fattr.Tell();

	AttributeType a_type = TypeName();
	if(Type().IsLookup()){
		*((_uint64*)(buf)) = i_min;
		*((_uint64*)(buf + 8)) = i_max;
		fattr.WriteExact(buf, 16);
		uchar *dic_buf = new uchar [dic->ByteSize()];
		if (!dic_buf)
			rclog << lock << "Error: out of memory (" << dic->ByteSize() << " bytes failed). (23)" << unlock;
		uchar *p_dic_buf = dic_buf;
		dic->SaveData(p_dic_buf);
		fattr.WriteExact((char*)dic_buf, dic->ByteSize());
		delete [] dic_buf;
	}
	else if(ATI::IsNumericType(a_type)  ||
		ATI::IsDateTimeType(a_type) || ATI::IsDateTimeNType(a_type)){
		*((_uint64*)(buf)) = i_min;
		*((_uint64*)(buf+8)) = i_max;
		fattr.WriteExact(buf,16);
	}
	// ... other types of dictionary ...
	else {
		// e.g. string_no_dict==1
		dict_offset = 0;
	}

	special_offset = (uint)fattr.Tell();

	// saving session and rollback info
	uint bs = 27 + 2*sizeof(_uint64);
	*((uint*)(buf)) = bs;
	buf[4] = 0;	// = block type
	buf[5] = 0;	// = last block

	*((int*)(buf+6)) = savefile_loc[0];
	*((int*)(buf+10)) = savefile_loc[1];

	*((uint*)(buf+14)) = savepos_loc[0];
	*((uint*)(buf+18)) = savepos_loc[1];

	buf[22] = current_save_loc;
	*((uint*)(buf+23)) = save_session_id;

	*((_uint64*)(buf + 23 + sizeof(uint))) = natural_size_saved;
	*((_uint64*)(buf + 23 + sizeof(uint) + sizeof(_uint64))) = compressed_size;
	fattr.WriteExact(buf, bs);

	// Update compressed data size
	compressed_size = comprs_size + fattr.Tell() * 2;	// *2, because column header (and lookup dict.) is stored in TA00... and TB00...
	fattr.Seek(-(long)sizeof(_uint64), SEEK_CUR);
	fattr.WriteExact(&compressed_size, sizeof(_uint64));

	// Update addresses of sections
	*((uint*)(buf)) = packs_offset;
	*((uint*)(buf+4)) = dict_offset;
	*((uint*)(buf+8)) = special_offset;

	//seekp(34,ios::beg);
	fattr.Seek(34, SEEK_SET);
	fattr.WriteExact(buf, 12);
	fattr.Close();
	return true;
}

IBFile *
RCAttr::GetPackFile(int pack_no)
{
  IBFile *fh = new IBFile();

  fh->OpenReadOnly(AttrPackFileName(pack_no - packs_omitted));
  fh->Seek(dpns[pack_no - packs_omitted].pack_addr,SEEK_SET);

  return fh;
}

int
RCAttr::Collapse()
{
	IBGuard guard(dpns_load_mutex);
	
	DeleteRSIs();
	if(!!dic) {
		dic->Release();
		dic.reset();
	}
	GlobalDataCache::GetGlobalDataCache().ReleaseColumnDNPs(_logical_coord.co.rcattr[0],_logical_coord.co.rcattr[1]);
	dpns.clear();
	SetPackInfoCollapsed(true);	
	return 0;
}

void
RCAttr::Release()

{
	Collapse();
}

bool 
PackAllocator::isLoadable( int pack )
{
	attr->LoadPackInfo();
	return attr->dpns[pack].pack_mode == PACK_MODE_UNLOADED && attr->dpns[pack].is_stored;
}

AttrPackPtr
PackAllocator::lockedAlloc(const PackCoordinate& pack_coord)
{
	AttrPackPtr p;
	if(pt == PackS) {
		p = AttrPackPtr(new AttrPackS(pack_coord, attr->TypeName(), attr->GetInsertingMode(), attr->GetNoCompression(), 0));
	} else {
		p = AttrPackPtr(new AttrPackN(pack_coord, attr->TypeName(), attr->GetInsertingMode(), 0));
	}
	return p;
}

IBFile *
PackAllocator::getFile( int pack )
{
    return attr->GetPackFile(pack);
}

AttrPackPtr AttrPackLoad(const PackCoordinate& coord, PackAllocator &al) {
	AttrPackPtr pack = al.lockedAlloc(coord);
	boost::scoped_ptr<IBFile> f(al.getFile( pc_dp( coord ) ));
	pack->LoadData( f.get() );
	f->Close();
	pack->Uncompress(al.GetDomainInjectionManager());
	return ( pack );
}
