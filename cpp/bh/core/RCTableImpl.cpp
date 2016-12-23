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

#include <boost/assign/std/vector.hpp>

#include "edition/local.h"
#include "system/Channel.h"
#include "system/IBFileSystem.h"
#include "core/RCAttr.h"
#include "RCTableImpl.h"
#include "RCEngine.h"
#include "handler/BrighthouseHandler.h"
#include "domaininject/DomainInjectionsDictionary.h"

using namespace std;
using namespace boost;
using namespace boost::assign;

extern const char *ha_rcbase_exts[];

RCTableImpl::RCTableImpl(string const& a_path, vector<DTCollation> charsets, char *nm, int t_n, uint s_id) :
	waiting_for_drop(0), db_name(), path(), path_no_ext(), charsets(charsets), m_state(0), obsolete(false)
{
	assert(t_n >= 0);
	check_integrity = true;
	tab_num = t_n;
	no_attr = 0;
	int strl = (int)strlen(nm) + 1;
	name = new char [strl];
	desc = NULL;
	strcpy(name, nm);
	a = NULL;
	// current path (terminated by '/'), determining the directory where Table.ctb and TAxxxxx.ctb may be found
	if ( !a_path.empty() )
		SetPath(a_path);
	else
		SetPath("./");
	session = s_id;
	conn_mode = -1;

#ifdef __GNUC__
	//pthread_mutexattr_t mattr;
	//pthread_mutexattr_init(&mattr);
	//pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
	//pthread_mutex_init(&synchr, &mattr);
#else
	//InitializeCriticalSection(&this->synchr);
#endif
}

/////////////////////////////////////////////////////////////////////////////////////////////////
// Table description file format ("Table.ctb"):
//
//	"RScT"				4		- file identifier
//	<format_id>			1		- "0", "1" - obsolete formats, "2" - current
//	<security>			4		- reserved for security info
//	<no_all_obj>		8		- reserved for total no. of objects (including deleted); currently not used
//	<no_cur_obj>		8		- reserved for current (excluding deleted) no. of objects; currently not used
//	<no_attr>			4		- number of attributes
//	<block_offset>		4		- offset of the first special block (0 means no such block)
//
//	<attr_mode>			no_attr	- one-byte attribute parameter; 0 for deleted attribute, !=0 otherwise
//	<tab_name>			...		- table name, null-terminated
//	<tab_desc>			...		- table description, null-terminated
//
//	<block_1>			...		- special blocks
//	<block_2>			...
//	...
//	<block_n>			...
//
//	general sctructure of a block:		<blk_len>	4	- including 6 bytes of header
//										<blk_type>	1	- reserved (0 is assumed as interpretable now)
//										<is_next>	1	- 0 for the last block, 1 otherwise
//
//
RCTableImpl::RCTableImpl(string const& a_path, vector<DTCollation> cs, int current_state, OpenMode::open_mode_t openMode_)
	:	waiting_for_drop(0), db_name(), path(), path_no_ext(), charsets(cs), m_state(current_state), obsolete(false)
{
	check_integrity = !(openMode_ & OpenMode::FORCE);
	conn_mode = 0;
	session = 0;
	tab_num=-1; // index of this table in database (-1 means we should load it from file)
	no_attr=0;
	a=NULL;
	name=NULL;
	desc=NULL;

	// set path to the table directory
	if ( !a_path.empty() )
		SetPath(a_path);
	else
		SetPath("./");
	string bufX( path );

	int res = (int)GetFileCreateTime(path);
	if (res < 0) {
		throw NoTableFolderRCException(path);
	}
	m_create_time = (time_t) res;

	// open table definition file ("...<base>.../D000000000/Table.ctb")

	bufX += "/Table.ctb";
	IBFile ftbl;

	try
	{
		ftbl.OpenReadOnly(bufX);
		static int const CTB_FILE_SIZE( 8192 );
		char buf[CTB_FILE_SIZE] = {0};

		uint block_offset = 0;
		uint fsize = ftbl.Read(buf, CTB_FILE_SIZE);
		// header (33 bytes):
		if (fsize < 33 || strncmp(buf, "RScT", 4) != 0 || buf[4] != '2')
			throw DatabaseRCException(string("Bad format of table definition in ") + path + "/Table.ctb");
		no_attr = *((int*)(buf+25));
		block_offset = *((int*)(buf + 29));

		// attribute parameters table
		if(no_attr > 0) {
			a = new RCAttr * [no_attr];
			if(!a)
				throw OutOfMemoryRCException();
			//memcpy(a_mode,b+33,no_attr);	// copy the vector of attributes' parameters
		}

		// table name and description
		uint pos = 33 + no_attr;
		if (no_attr < 0 || pos >= fsize || memchr(buf + pos, 0, fsize - pos) == NULL)
			throw DatabaseRCException(string("Bad format of table definition in ") + path + "/Table.ctb");
		int i( (int)strlen(buf + pos) + 1 );
		name = new char [i];
		if (!name)
			throw OutOfMemoryRCException();
		//strcpy_s(name, i, b);
		strcpy(name, buf + pos);
		pos += i;
		if (pos >= fsize || memchr(buf + pos, 0, fsize - pos) == NULL)
			throw DatabaseRCException(string("Bad format of table definition in ") + path + "/Table.ctb");
		i = (int)strlen(buf + pos) + 1;
		desc = new char [i];
		if (!desc)
			throw OutOfMemoryRCException();
		//strcpy_s(desc, i, b);
		strcpy(desc, buf + pos);

		// blocks
		while(block_offset > 0) {
			if(block_offset + 6 > fsize)
				throw DatabaseRCException(string("Bad format of table definition in ")+path+"/Table.ctb");
			char* b = (char*)buf + block_offset; // block offset from the beginning of the file
			int b_len = *(int*)b;
			uchar b_type = *(uchar*)(b + 4);
			uchar b_next = *(uchar*)(b + 5);
			if(b_len < 6 || block_offset + b_len > fsize)
				throw DatabaseRCException(string("Bad format of table definition in ")+path+"/Table.ctb");

			// block types
			if(b_type == 1 ) {// MySQL special block: internal table number
				if (b_len != 10)
					throw DatabaseRCException(string("Bad format of table definition in ")+path+"/Table.ctb");
				//				assert(tab_num==-1);
				tab_num = *(int*)(b + 6);
			} else if(b_type == 66) {
				// place for block interpretation
			} else {
				rclog << lock << "Warning: special block (unknown type " << int(b_type) << ") ignored in table " << name << unlock;
			}
			if(b_next == 0) {
				if(block_offset+b_len != fsize)
					throw DatabaseRCException(string("Bad format of table definition in ")+path+"/Table.ctb");
				block_offset = 0;
			} else 
				block_offset += b_len;
		}
		ftbl.Close();

		if (tab_num < 0)
			throw DatabaseRCException(string("Bad format of table definition in ")+path+"/Table.ctb");
	}
	catch (...)
	{
		delete[] a;
		delete[] name;
		delete[] desc;
		no_attr = 0;
		throw;
	}

	// load definitions of non-deleted attributes
	for(int i(0); i < no_attr; ++i)
		a[i] = NULL;

	if(charsets.size() == 0)
		charsets.assign(no_attr, DTCollation());

	if(openMode_& OpenMode::FOR_LOADER)
		return;

#ifdef __GNUC__
	//pthread_mutex_init(&synchr, NULL);
#else
	//InitializeCriticalSection(&synchr);
#endif

	if(no_attr > 0) {
		try {
			if(RCTableImpl::OpenMode::FOR_DROP != openMode_) {
				LockPackInfoForUse(); // refresh all column information, check metadata integrity
				m_update_time = a[0]->UpdateTime();
			}
		} catch (...) {
			CleanUp();
			throw;
		}
	} else
		m_update_time = m_create_time;

}

RCTableImpl::RCTableImpl(int noattr) :
	waiting_for_drop(0), db_name(), path(), path_no_ext(), m_state(0), obsolete(false)
{
	check_integrity = false;
	tab_num = 0;
	no_attr = noattr;
	name = NULL;
	desc = NULL;
	a = NULL;
	session = INVALID_TRANSACTION_ID;
	conn_mode = 1;

	a=new RCAttr * [no_attr];
	for(int i = 0; i < no_attr; i++)
		a[i] = NULL;
}

void RCTableImpl::CleanUp()
{
#ifdef __GNUC__
	//pthread_mutex_destroy(&synchr);
#else
	//DeleteCriticalSection(&synchr);
#endif
	for(int i=0; i < no_attr; i++) {
		if( a[i] == NULL ) continue;
		a[i]->StopAccessTracking();
		delete a[i];
	}
	no_attr = 0;
	delete [] a;
	a = NULL;
	delete [] name;
	name = NULL;
	delete [] desc;
	desc = NULL;
}

RCTableImpl::~RCTableImpl()
{
	CleanUp();
}

void RCTableImpl::SetPath( string const& a_path )
{
	path = a_path;
	path_no_ext.assign( path, 0, path.length() - strlen(ha_rcbase_exts[0]) );
	string::size_type end( path_no_ext.find_last_of( "/\\" ) );
	if ( end != string::npos ) {
		string::size_type beg( path_no_ext.find_last_of( "/\\", end - 1 ) );
		if ( beg != string::npos ) {
			db_name.assign( path_no_ext, beg + 1, ( end - beg ) - 1 );
		}
	}
}

void RCTableImpl::LockPackInfoForUse()
{
	for(int att=0; att<no_attr; att++) {
		LoadAttribute(att);
		a[att]->Lock();
		a[att]->TrackAccess();
	}
}

void RCTableImpl::UnlockPackInfoFromUse()
{
	for(int att=0; att<no_attr; att++) {
		a[att]->Unlock();
	}
}

void RCTableImpl::LockPackForUse(unsigned attr, unsigned pack_no, ConnectionInfo& conn)
{
	if( pack_no == 0xFFFFFFFF )		// "not used" pack number
		return;
	LoadAttribute(attr);			// TODO: some kind of locking between these two lines? (But performance...)
	a[attr]->LockPackForUse(pack_no, conn);
}

void RCTableImpl::UnlockPackFromUse(unsigned attr, unsigned pack_no)
{
	if( pack_no == 0xFFFFFFFF )
		return;
	LoadAttribute(attr);
	a[attr]->UnlockPackFromUse(pack_no);
}

void RCTableImpl::LockLastPacks(Transaction& trans)
{
	for (int attr=0; attr<no_attr; attr++)
		LoadAttribute(attr);
	for_each(a, a + no_attr, boost::bind(&RCAttr::LockLastPackForUse, _1, boost::ref(trans)));
}

void RCTableImpl::LockLastPacks(Transaction& trans, const std::set<int>& columns)
{
	for(std::set<int>::const_iterator it =columns.begin(); it!=columns.end(); ++it) {
		LoadAttribute(*it);
		a[*it]->LockLastPackForUse(trans);
	}
}

void RCTableImpl::UnlockLastPacks()
{
	for(int attr = 0; attr < no_attr; attr++) {
		if(a[attr])
			a[attr]->UnlockLastPackFromUse();
	}
}

void RCTableImpl::UnlockLastPacks(const std::set<int>& columns)
{
	for(std::set<int>::const_iterator it =columns.begin(); it!=columns.end(); ++it)
		a[*it]->UnlockLastPackFromUse();
}

void RCTableImpl::UnlockLastPacksForOthers(const std::set<int>& columns)
{
	int prev = -1;
	for(std::set<int>::const_iterator it =columns.begin(); it!=columns.end(); ++it) {
		for (int attr = prev+1; attr < *it; attr++)
			a[attr]->UnlockLastPackFromUse();
		prev = *it;
	}
	for (int attr = prev+1; attr < no_attr; attr++)
		a[attr]->UnlockLastPackFromUse();
}

void RCTableImpl::AddAttribute(char* nm, AttributeType a_type, int a_field_size, int a_dec_places /*= 0*/, unsigned int param /*= 0*/, DTCollation collation /*= DTCollation()*/)
{
	no_attr++;
	RCAttr** a_new = new RCAttr * [no_attr]; // warning C6211: Leaking memory 'a_new' due to an exception.
	for(int i = 0; i < no_attr - 1; i++)
		a_new[i] = a[i];
	delete [] a;
	a = a_new;
	a[no_attr - 1] = new RCAttr(nm, no_attr - 1, tab_num, a_type, a_field_size, a_dec_places, path, param, session, collation);
}


void RCTableImpl::SaveHeader()
{
	try {
		if(IsAbsolutePath(path))
			CreateDir( path ); //temporary table
		else
			CreateDir( infobright_data_dir + path );
	} catch(DatabaseRCException& e) {
			rclog << lock << e.what() << " DatabaseRCException" << unlock;
			throw;
	}

	int res = (int)GetFileCreateTime(path);
	if (res < 0) {
			rclog << "Error: " << path << " directory status can not be obtained." << endl;
	} else {
			m_create_time = (time_t) res;
	}

	string filename( path );
	filename += "/Table.ctb";

	int i;
	char* b = 0;
	IBFile ftbl;
	ftbl.OpenCreateEmpty( filename );
	char buf[8192] = {0};
	b = (char*)buf;
	memset(b, 0, 33);
	//strcpy_s(b, (8192 - (int)(b-buf)), "RScT2");
	strcpy(b, "RScT2");
	*(uint*)(b+5)=0; //	<security>, for the future use. 0 means no security encoding.
	_int64 no_obj=0;
	for(i=0; i<no_attr; i++)
		if(a[i]) {
			no_obj=a[i]->NoObj();
			break;
		}
	*(_int64*)(b+9)=no_obj;
	*(int*)(b+25)=no_attr;
	*(int*)(b+29)=0; // block offset
	b+=33;

	// attr. parameters
	//memcpy(b,a_mode,no_attr);
	memset(b, 1, no_attr);
	b+=no_attr;

	// name and description
	//strcpy_s(b, (8192 - (int)(b-buf)), name);
	strcpy(b, name);
	b+=strlen(name)+1;
	if(desc) {
		//strcpy_s(b, (8192 - (uint)(b-buf)), desc);
		strcpy(b, desc);
		b+=strlen(desc)+1;
	} else {
		*b='\0';
		b++;
	}
	//////////////// special blocks ////////////////////

	*(int*)(buf+29)=int(b-buf); // offset of the first special block (from the beginning of the file)

	// MySQL special block: internal table number

	*(int*)b = 10; // length: 6 for header, 4 for table number
	*(b+4) = 1; // block type
	*(b+5) = 0; // next?
	*(int*)(b+6) = tab_num;
	b+=10;

	////////////////////////////////////////////////////
	ftbl.WriteExact(buf, (uint)(b - buf));
	ftbl.Close();
}

void RCTableImpl::Save()
{
	SaveHeader();
	for(int i=0; i<no_attr; i++)
		if(a[i])
			a[i]->SaveHeader();
	if(no_attr > 0) {
		LoadAttribute(0);
		m_update_time = a[0]->UpdateTime();
	} else
		m_update_time = m_create_time;
}

//void RCTableImpl::Drop()
//{
//	if(rsi_manager) {
//		rsi_manager->DeleteIndex(tab_num); // delete all indexes connected with this table
//	}
//	for(int i = 0; i < no_attr; i++) {
//		delete a[i];
//		a[i] = 0;
//	}
//	DeleteDirectory(path);
//}

void RCTableImpl::Drop()
{
	DoDrop();
}

void RCTableImpl::DoDrop()
{
	if(rsi_manager) {
		for(int j = 0; j < no_attr; j++) {
			rsi_manager->DeleteIndex(tab_num, j); // delete all indexes connected with this table
		}
	} else {
		// KNs may have been created when KNLevel was > 0, they cannot stay
		InitRSIManager(infobright_data_dir, true);
		for(int j = 0; j < no_attr; j++)
			rsi_manager->DeleteIndex(tab_num, j); // delete all indexes connected with this table
		rsi_manager.reset();
	}


	for(int i = 0; i < no_attr; i++) {
		if(a[i] == NULL)
			continue;
		a[i]->StopAccessTracking();
		delete a[i];
		a[i] = 0;
	}
	DeleteDirectory(path);
}

void RCTableImpl::LoadAttribute(int attr_no)
{
	if(!a[attr_no]) {
        IBGuard load_attribute_guard(load_attribute_mutex);
		if(!a[attr_no]) {
			a[attr_no] = new RCAttr(attr_no, tab_num, path, conn_mode, session, charsets[attr_no]);
			if(a[attr_no]->OldFormat()) {
				a[attr_no]->UpgradeFormat();
				delete a[attr_no];
				a[attr_no] = new RCAttr(attr_no, tab_num, path, conn_mode, session, charsets[attr_no]);
			}
			// Checking consistency
			if(process_type != ProcessType::DIM && check_integrity) {
				for(int i = 0; i < no_attr; i++)
					if(i != attr_no && a[i]) {
						if(a[i]->NoObj() != a[attr_no]->NoObj()) {
							rccontrol.lock(-1) << "Data integrity error. 1" << unlock;
							throw DatabaseRCException("Data integrity error. 1");
						}
						break;
					}
			}
			// next, needs to extract character set form MySql structures
			if(ATI::IsTxtType(a[attr_no]->TypeName()) && ConnectionInfoOnTLS.IsValid()) {
				if(attr_no < charsets.size())
					a[attr_no]->SetCollation(charsets[attr_no]);
			}
			// RCAttr should start unlocked so MM can call Collapse()
			a[attr_no]->Unlock();
		}
	}
}

std::vector<AttrInfo> RCTableImpl::GetAttributesInfo()
{
	std::vector<AttrInfo> info(NoAttrs());
	Verify();
	for(int j = 0; j < (int) NoAttrs(); j++) {
		LoadAttribute(j);
		//info[j].name = AttrName(j);
		//info[j].desc = a[j]->Description();
		info[j].type = a[j]->TypeName();
		info[j].size = a[j]->Type().GetPrecision();
		info[j].precision = a[j]->Type().GetScale();
		if(a[j]->Type().GetNullsMode() == NO_NULLS)
			info[j].no_nulls = true;
		else
			info[j].no_nulls = false;
		info[j].lookup = a[j]->Type().IsLookup();
		//info[j].unique = a[j]->IsDeclaredUnique();
		info[j].actually_unique = (a[j]->PhysicalColumn::IsDistinct() == RS_ALL);
		//info[j].primary = a[j]->IsPrimary();
		info[j].current_trans = a[j]->GetSaveSessionId();
		if(info[j].current_trans == 0xFFFFFFFF)
			info[j].current_trans = a[j]->GetSessionId();
		info[j].uncomp_size = a[j]->NaturalSize();
		info[j].comp_size = a[j]->CompressedSize();
	}
	return info;
}

std::vector<ATI> RCTableImpl::GetATIs(bool orig)
{
	vector<ATI> deas;
	for(uint j = 0; j < NoAttrs(); j++) {
		LoadAttribute(j);
		deas += ATI(a[j]->TypeName(), a[j]->Type().GetNullsMode() == NO_NULLS, a[j]->Type().GetPrecision(), a[j]->Type().GetScale(), a[j]->Type().IsLookup(), false, a[j]->Type().GetCollation());
	}
	return deas;
}

void RCTableImpl::FillDecompositions(vector<std::string>& decompositions)
{
	for (int attr=0; attr<no_attr; attr++) {
		LoadAttribute(attr);
		decompositions.push_back(DomainInjectionsDictionary::Instance().get_rule(db_name, name, a[attr]->Name()));
	}
}

const char* RCTableImpl::AttrName(int n_a)
{
	LoadAttribute(n_a);
	return a[n_a]->Name();
}

bool RCTableImpl::IsLookup(int n_a)
{
	LoadAttribute(n_a);
	return a[n_a]->Type().IsLookup();
}

const ColumnType& RCTableImpl::GetColumnType(int n_a)
{
	LoadAttribute(n_a);
	return a[n_a]->Type();
}


RCAttr* RCTableImpl::GetAttr(int n_a)
{
	LoadAttribute(n_a);
	a[n_a]->TrackAccess();
	return a[n_a];
}

int RCTableImpl::Verify()
{
	int res=1;
	uint s_id=0xFFFFFFFF;
	int i;
	_int64 n_o = 0xFFFFFFFF;
	for(i=0; i<no_attr; i++)
		if(a[i]) {
			LoadAttribute(i);
			if(n_o==0xFFFFFFFF)
				n_o=a[i]->NoObj();
			if(s_id==0xFFFFFFFF)
				s_id=a[i]->GetSessionId();
			if(n_o!=a[i]->NoObj())
				res=0;
			if(s_id!=a[i]->GetSessionId())
				res=0;
		}
	if(res==0) {
		rclog << lock << "Error:  columns in table " << name << " are inconsistent:" << unlock;
		for(i=0; i<no_attr; i++)
			if(a[i]) {
				rclog << "  - " << a[i]->Name() << " \t no.obj.= " << a[i]->NoObj() << " \t sess.id= "
						<< a[i]->GetSessionId() << endl;
			}
		rclog << unlock;
	}
	return res;
}

uint RCTableImpl::CommitSaveSession(uint s_id)
{
	uint result=INVALID_TRANSACTION_ID;

	//////////////// Commit data ////////////////////

	for(int i = 0; i < no_attr; i++) {
		LoadAttribute(i);
		if(a[i]) {
			result = a[i]->GetSaveSessionId();
			if(result == INVALID_TRANSACTION_ID || (s_id != 0 && s_id != result))
				return INVALID_TRANSACTION_ID; // result;  AB Switch change
		}

		if(a[i]->NoObj() != 0 && a[i]->NoObj() != (((int64)a[i]->NoPack() - 1) * MAX_PACK_ROW_SIZE + a[i]->GetNoValues(a[i]->NoPack() - a[i]->packs_omitted - 1))) {
			stringstream msg;
			msg << "Data integrity error : Number of values in last DP and global number of values in table : '";
			msg <<  GetPathNoExt() << "', column : '" << a[i]->Name() << "' does not match. ";
			msg << "Number of DPs : " << a[i]->NoPack() << ". ";
			msg << "Number of values in last DP : " << a[i]->GetNoValues(a[i]->NoPack() - a[i]->packs_omitted - 1) << ". ";
			msg << "Number of values in column : " << a[i]->NoObj();
			throw DatabaseRCException(msg.str());
		}
	}

	for(int i = 0; i < no_attr; i++)
		if(a[i])
			a[i]->CommitSaveSession();

	return result;
}

void RCTableImpl::CommitSwitch(uint save_result)
{
	//////////////// Finishing ////////////////////

    if (save_result != INVALID_TRANSACTION_ID) {
        // If there were changes in columns than session id is not equal to INVALID_TRANSACTION_ID.
        // It means RCAttr created TS* file and renamed it to TA*/TB* file in CommitSaveSession() method above.
        // In this case the system flips AB Switch, which changes "current set" of TA*/TB* files.
        ABSwitcher absw;
        int res = absw.FlipState(path);
        if (res) {
            // throw here
        }
    }

	m_state = 0;
	conn_mode = 0;
}

uint RCTableImpl::GetSaveSessionId()
{
	uint result = INVALID_TRANSACTION_ID;
	for(int i = 0; i < no_attr; i++) {
		LoadAttribute(i);
		if(a[i])
			return a[i]->GetSaveSessionId();
	}
	return result;
}

void RCTableImpl::Rollback(uint s_id, bool)
{
	for(int i = 0; i < no_attr; i++) {
		LoadAttribute(i);
		if(a[i]) {
			a[i]->Rollback(s_id);
		}
	}

	SetMode(0, s_id);
	if(no_attr > 0) {
		LoadAttribute(0);
		m_update_time = a[0]->UpdateTime();
	}
}

void RCTableImpl::SetMode(int conn_mode, uint s_id)
{
	session = s_id;

	this->conn_mode = conn_mode;
	for(int i=0; i<no_attr; i++) {
		if( a[i] == NULL ) continue;
		a[i]->StopAccessTracking();
		delete a[i];
		a[i] = 0;
	}
}

void RCTableImpl::LoadDirtyData(unsigned int session_id)
{
	DTCollation collation;
	for(int i = 0; i < no_attr; i++) {
		collation = DTCollation();
		if(a[i]) {
			collation = a[i]->GetCollation();
			a[i]->StopAccessTracking();
			delete a[i];
		}
		a[i] = new RCAttr(i, tab_num, path, 1, session_id, collation);
		a[i]->TrackAccess();
	}
	if(no_attr > 0)
		m_update_time = a[0]->UpdateTime();
}

void RCTableImpl::DisplayRSI()
{
	rccontrol << lock << "--- RSIndices for " << name << " (tab.no. " << tab_num << ", " << NoPacks() << " packs) ---"
			<< unlock;
	rccontrol << lock << "Name               Triv.packs\tSpan\tHist.dens." << unlock;
	rccontrol << lock << "-----------------------------------------------------------" << unlock;
	for(int i=0; i<no_attr; i++) {
		if(!a[i])
			LoadAttribute(i);
		if(a[i]) {
			double hd = 0;
			double span = 0;
			int trivials = 0;
			a[i]->RoughStats(hd, trivials, span);
			rccontrol << lock;
			rccontrol << a[i]->Name();
			int display_limit = int(20) - (int)strlen(a[i]->Name());
			for(int j=0; j<display_limit; j++)
				rccontrol << " ";
			rccontrol << trivials << "\t\t";
			if(span!=-1)
				rccontrol << int(span*10000)/100.0 << "%\t";
			else
				rccontrol << "\t";
			if(hd!=-1)
				rccontrol << int(hd*10000)/100.0 << "%\t";
			else
				rccontrol << "\t";
			rccontrol << unlock;
		}
	}
	rccontrol << lock << "-----------------------------------------------------------" << unlock;
}

RCTableImpl::Iterator::Iterator(RCTableImpl& table, FilterPtr filter)
	:	table(&table), filter(filter), it(filter.get()), position(-1), current_record_fetched(false), used_atts_updated(false), conn(NULL)
{
}

void RCTableImpl::Iterator::Initialize(const std::vector<bool>& attrs)
{
	int attr_id = 0;
	this->attrs.clear();
	record.clear();
	values_fetchers.clear();

	for(std::vector<bool>::const_iterator iter = attrs.begin(), end = attrs.end(); iter != end; ++iter, ++attr_id) {
		if(*iter) {
			RCAttr* attr = table->GetAttr(attr_id);
			this->attrs.push_back(attr);
			this->record.push_back(RCDataTypePtr(attr->ValuePrototype(false).Clone()));
			this->values_fetchers.push_back(boost::bind(&RCAttr::GetValue, attr, _1, ref(*this->record[attr_id]), false));
		} else {
			this->record.push_back(RCDataTypePtr(table->GetAttr(attr_id)->ValuePrototype(false).Clone()));
		}
	}
}

/*void  RCTableImpl::Iterator::UpdateUsedAttrs(const std::vector<bool>& attrs)
{
	current_record_fetched = false;
	UnlockPacks(-1);

	Initialize(attrs);

	LockPacks();
	FetchValues();
	used_atts_updated = true;
}*/


bool RCTableImpl::Iterator::operator==(const Iterator& iter)
{
	return position == iter.position;
}

void RCTableImpl::Iterator::operator++(int)
{
	++it;
	int64 new_pos;
	if(it.IsValid())
		new_pos = *it;
	else
		new_pos = -1;
	UnlockPacks(new_pos);
	position = new_pos;
	current_record_fetched = false;
}


void RCTableImpl::Iterator::MoveToRow(int64 row_id)
{
	UnlockPacks(row_id);
	it.RewindToRow(row_id);
	if(it.IsValid())
		position = row_id;
	else
		position = -1;
	current_record_fetched = false;
}

void RCTableImpl::Iterator::FetchValues()
{
	if(!current_record_fetched) {
		LockPacks();
		BOOST_FOREACH(values_fetchers_t::value_type& func, values_fetchers)
			func(position);
		current_record_fetched = true;
	}
}

void RCTableImpl::Iterator::UnlockPacks(int64 new_row_id)
{
	if(position != -1 && (new_row_id == -1 || (position >> 16) != (new_row_id >> 16)))
		dp_locks.clear();
}

void RCTableImpl::Iterator::LockPacks()
{
	if(dp_locks.empty() && position != -1) {
		for(std::vector<RCAttr*>::iterator iter = attrs.begin(), end = attrs.end(); iter != end; ++iter) {
			table->PrefetchPack((*iter)->AttrNo(), it.Lookahead(3), *conn);
			dp_locks.push_back(boost::shared_ptr<DataPackLock>(new DataPackLock(*(*iter), int(position >> 16))));
		}
	}
}

RCTableImpl::Iterator RCTableImpl::Iterator::CreateBegin(RCTableImpl& table, FilterPtr filter, const std::vector<bool>& attrs)
{
	RCTableImpl::Iterator ret(table, filter);
	ret.Initialize(attrs);
	ret.it.Rewind();
	if(ret.it.IsValid())
		ret.position = *(ret.it);
	else
		ret.position = -1;
	ret.conn = &ConnectionInfoOnTLS.Get();
	return ret;
}

RCTableImpl::Iterator RCTableImpl::Iterator::CreateEnd()
{
	return RCTableImpl::Iterator();
}

/*void RCTableImpl::UnlockAttr(int n_a)
{
	a[n_a]->Unlock();
}*/
