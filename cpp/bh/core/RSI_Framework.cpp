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

#include <fstream>
#include <boost/bind.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/assign.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/shared_array.hpp>
#include <boost/tokenizer.hpp>
#include <boost/format.hpp>

#include "edition/local.h"
#include "system/RCException.h"
#include "RSI_Framework.h"
#include "RSI_Histogram.h"
#include "RSI_CMap.h"
#include "system/RCSystem.h"
#include "system/IBFile.h"
#include "system/IBFileSystem.h"
#include "util/BHString.h"


using namespace boost;
using namespace std;
using namespace boost::filesystem;

const char* RSIndexName[] = { "TEST", "HIST", "JPP", "CMAP" };

RSI_Manager::versions_map_type RSI_Manager::versions_map =
		boost::assign::list_of< pair<string, uint> > ("3.1", 1)(RSI_MANAGER_VERSION, 2);


RSI_Manager::RSI_Manager(const char *_path, int _index_l)
	: path(_path)
{

	if((path[path.length() - 1] != '/') && (path[path.length() - 1] != '\\')) // put slash at the end of directory name
		path += DIR_SEPARATOR_STRING;

	// load RSI definition

	pool = shared_ptr<RSIndexPool>(new RSIndexPool());
	index_level = _index_l;
}

RSI_Manager::~RSI_Manager()
{
	IBGuard guard(rsi_mutex);
}

RSIndex* RSI_Manager::GetIndex(RSIndexID id,int read_loc)
{
	//LockCS _cs(&CS);
    IBGuard guard(rsi_mutex);

	int pos = pool->FindID(id);

	RSIndex* rsi;
	if(pos < 0)	{
		rsi = LoadIndex(id,false,read_loc);
		if(!rsi) {						// loading failed
			return NULL;
		}
		if(pos < 0) pos = pool->NewID(id);
		pool->PutForRead(pos, rsi);
		return rsi;
	}

	bool valid(false);
	rsi = pool->GetForRead(pos, valid);
	if(rsi) {
		if(rsi->IsValid())
			return rsi;
		assert(0);
		rsi = NULL;
	}

	assert(rsi == NULL);			// index doesn't exist or ID is invalid (old and waiting for destruction) or ID is not created yet
	if(!valid) {
		return NULL;			// ID is invalid or not created
	}

	// load index from file
	rsi = LoadIndex(id,false,read_loc);
	if(!rsi) {						// loading failed
		pool->LoadFailed(pos);
		return NULL;
	}
	if(!rsi->IsValid()) {
		assert(0);
		return NULL;
	}
	pool->PutForRead(pos, rsi);
	return rsi;
}

bool RSI_Manager::IndexExists(RSIndexID id)
{
	IBGuard guard(rsi_mutex);
	if(pool->FindID(id) >= 0)
		return true;	
	if(DoesFileExist(id.GetFileName(path)))
		return true;
	return false;
}

void RSI_Manager::ReleaseIndex(RSIndex *ptr)
{
	//LockCS _cs(&CS);
    IBGuard guard(rsi_mutex);
    pool->ReleaseRead(ptr);
}

RSIndex* RSI_Manager::GetIndexForUpdate(RSIndexID id,int read_loc)
{
	//LockCS _cs(&CS);
    IBGuard guard(rsi_mutex);
	RSIndex* rsi = NULL;

	int pos = pool->FindID(id);
	if(pos < 0) pos = pool->NewID(id);
	if(pos < 0) return NULL;
	if(pool->IsLocked(pos)) return NULL;		// the index is already locked for update by this process

	// try to lock the index on disk
	int lock = 0;//TryLock(id);
	if(lock < 0) return NULL;					// already locked by another process

	// load the newest version of the index from file
	rsi = LoadIndex(id, true, read_loc);
	if(!rsi) {
		//UnLock(lock);
		return NULL;
	}

	// register the index as being updated
	pool->PutForWrite(pos, rsi, lock);
	if(!rsi->IsValid()) { assert(0); return NULL; }
	return rsi;
}

int RSI_Manager::UpdateIndex(RSIndex *ptr,int write_loc,bool do_not_save)
{
	//LockCS _cs(&CS);
    IBGuard guard(rsi_mutex);
	if(!ptr) { assert(0); return 1; }				// incorrect 'ptr'

	RSIndexID id = ptr->GetID();
	int pos = pool->FindID(id);
	if(pos < 0) { assert(0); return 1; }			// incorrect 'ptr'

	// move 'rsi' to group 'read'; delete unused from this group; update RSI def
	int lock;
	ptr = pool->Commit(pos, ptr, lock);

	try {
		if(ptr && !do_not_save)
			SaveIndex(id, ptr, write_loc);		// save 'rsi' to file, if ID and the object is not deleted
	} catch (...) {
		ptr->Invalidate();
		throw;
	}
	ptr->Invalidate();
	//UnLock(lock);
	return 0;
}

//void RSI_Manager::CollapseIndex(RSIndex* ptr)
//{
//	//LockCS _cs(&CS);
//    IBGuard guard(rsi_mutex);
//	pool->CollapseIndex(ptr);
//}

void RSI_Manager::DeleteIndex(int tab, int col)
{
	//LockCS _cs(&CS);
    IBGuard guard(rsi_mutex);
	RSIndexID id;
	bool changed = false;

	int pos = pool->FirstID(id);
	while(pos >= 0) {
		if(id.Contain(tab,col)) {
			//DeleteIndexFile(id);
			if(id.IsType2()) {
				pool->DeleteFromDef(pos);
				changed = true;
			} else
				pool->DeleteID(pos);
		}
		pos = pool->NextID(id);
	}

	RSIndexType t[] = { RSI_HIST, RSI_CMAP }; // P2P were considered in while(pos >= 0)...
	int numberKNtypes = sizeof(t) / sizeof(RSIndexType);
	for(int i = 0; i < numberKNtypes; i++)
		DeleteIndexFile( RSIndexID(t[i], tab, col) );
	if(changed)
		pool->Cleaning();
}

void RSI_Manager::UpdateDefForTable(int table_id)
{
	IBGuard guard(rsi_mutex);
	pool->InvalidateRead(table_id);
	pool->Cleaning();
}

RSIndex* RSI_Manager::LoadIndex(RSIndexID id, bool write,int read_loc){
	// This method should:
	//		- load an existing file (read only or read/write),
	//		- if the file cannot be opened (e.g. does not exist):
	//			- if write=true create an empty object
	//			- else return NULL


	std::string file_path = id.GetFileName(path);
	// open .rsi file for shared reading
	int tries = 0, err = 0;
	IBFile frs_index;
	bool file_exists = DoesFileExist(file_path);

	if (file_exists) {
		try	{
			frs_index.OpenReadOnly(file_path);
		} catch (DatabaseRCException&) {
			return NULL;
		}
	}

	// create index object
	RSIndex* rsi = NULL;
	switch(id.type) {
		case RSI_TEST:	rsi = new RSIndex_Test(); break;
		case RSI_HIST:	rsi = new RSIndex_Hist(); break;
		case RSI_CMAP:	rsi = new RSIndex_CMap(); break;
		default:		assert(0); break;
	}
	if(rsi) {
		rsi->SetID(id);
		if (file_exists) {
			try	{
				if(write)
					rsi->LoadLastOnly(&frs_index, read_loc);
				else
					rsi->Load(&frs_index, read_loc);
			} catch (std::exception&) {
				err = 1;
			}
		} else
			err = 1;
		if(err) {			// error occurred while loading
			if(write) rsi->Clear();
			else {
				delete rsi;
				rsi = NULL;
			}
		}
	}
	return rsi;
}
void RSI_Manager::SaveIndex(RSIndexID id, RSIndex* rsi,int save_loc)
{
	string file_path = id.GetFileName(path);

	// open .rsi file for exclusive writing
	// NOTE: check whether other threads may still read from it
	int tries = 0, err = 0;
	IBFile frs_index;

	while(true) {
		try	{
			frs_index.OpenCreate(file_path);
			break;
		} catch (DatabaseRCException&) {
			err = errno;
			if ((err == EACCES) && (++tries < RSI_FLOCK_TRY_W))
				SleepNow(RSI_FLOCK_WAIT_W);
			else
				throw;
		}
	}
	rsi->Save(&frs_index,save_loc);
	frs_index.Close();
}

void RSI_Manager::DeleteIndexFile(RSIndexID id)
{
	std::string file_path = id.GetFileName(path);
	try {
		RemoveFile(file_path);
	} catch (std::exception& e) {
		rclog << lock << "Failed to delete RSI file : " << file_path  << ". " << e.what() << unlock;
	} catch (...) {
		rclog << lock << "Failed to delete RSI file : " << file_path  << "." << unlock;
	}
}

int RSI_Manager::GetRSIVersionCode(const string& ib_version)
{
	versions_map_type::iterator iter = find_if(versions_map.begin(), versions_map.end(),
			bind(&versions_map_type::value_type::first, _1) == ib_version);
	if(iter != versions_map.end())
		return iter->second;
	return -1;
}

string RSI_Manager::GetRSIVersionName(int version_code)
{
	versions_map_type::iterator iter = find_if(versions_map.begin(), versions_map.end(),
			bind(&versions_map_type::value_type::second, _1) == version_code);
	BHASSERT(iter != versions_map.end(), "Wrong IB version code.");
	return iter->first;
}

int RSI_Manager::GetRSIVersion(string metaDataFileFolder)
{
	string metaDataFilePath = metaDataFileFolder + RSI_METADATAFILE ;
	int version = -1;
	string p = infobright_data_dir + metaDataFilePath;
	ifstream versionFile(p.c_str());
	try {
		//versionFile.open;
		string st;
		getline(versionFile, st);
		size_t pos = st.find(':');
		if(pos != string::npos && st.length() - 1 > pos) {
			st = st.substr(pos + 1);
			trim(st);
			version = GetRSIVersionCode(st);
		}

	} catch(std::exception&) {
		//TODO
	}
	versionFile.close();
	return version;
}

int RSI_Manager::CreateMetadataFile(const string& metadata_file_path, int version_to)
{
	try {
		string p = infobright_data_dir + metadata_file_path;
		ofstream vf(p.c_str());
		vf << "Version: " << RSI_Manager::GetRSIVersionName(version_to) << endl;
		vf.close();
	} catch(std::exception& e) {
		throw FileRCException(string("Unable to create metadata file. ") + e.what());
	}
	return 1;
}

RSIndexType	RSI_Manager::RSIndexNameToType(std::string name) throw(SystemRCException){
	if(name=="JPP")
		return RSI_JPP;
	else if (name=="HIST")
		return RSI_HIST;
	else if (name=="CMAP")
		return RSI_CMAP;
	else if (name=="TEST")
		return RSI_TEST;
	else
		throw SystemRCException("Unrecognized type of kn");
}

//////////////////////////////////////////////////////////////

RSIndexPool::RSIndexPool()
	: last_i(-1)
{
}

RSIndexPool::~RSIndexPool()
{
	// delete all stored indexes (deleting at the same time all RSI objects contained in 'info')
	for(size_t i = 0; i < index.size(); i++)
		delete index[i].info;
}

void RSIndexPool::InvalidateRead(int table_id)
{
	for(size_t i = 0; i < index.size(); i++) {
		if(table_id < 0 || index[i].id.Contain(table_id)) {
			vector<RSIndex*>& read = index[i].info->read;

			for(int i = 0; i < (int)read.size(); i++)
				if(read[i])
					read[i]->Invalidate();
		}
	}
}


int RSIndexPool::NewID(RSIndexID id)
{
	// find place to insert
	int pos = (int)index.size();
	while(pos && (id < index[pos-1].id))	pos--;
	assert((pos == 0) || !(id == index[pos-1].id));

	InsertID(id, pos);
	index[pos].info->isnew = true;			// this ID will have to be added to RSI definition file
	return pos;
}

inline void RSIndexPool::InsertID(RSIndexID id, int pos)
{
	size_t n = index.size();
	index.resize(n + 1);
	while(n > (size_t)pos) {
		index[n] = index[n-1];
		n--;
	}
	index[pos].id = id;
	index[pos].info = new IndexInfo(); //&infos[n];
}

inline void RSIndexPool::DeleteID(int pos)
{
	delete index[pos].info;
	index.erase(index.begin() + pos);
}

void RSIndexPool::DeleteFromDef(int pos)
{
	// mark ID as old and invalidate all index objects
	if(pos < 0) return;
	index[pos].info->delfromdef = true;
	index[pos].info->isold = true;
	index[pos].info->isnew = false;
	if(index[pos].info->write) index[pos].info->write->Invalidate();
	vector<RSIndex*>& read = index[pos].info->read;
	for(int i = 0; i < (int)read.size(); i++)
		if(read[i]) read[i]->Invalidate();
}

inline int RSIndexPool::FirstID(RSIndexID& id) {
	if(index.empty()) return -1;
	curpos = 0;
	id = index[curpos].id;
	return curpos;
}

inline int RSIndexPool::NextID(RSIndexID& id) {
	if(curpos < 0) return -1;
	++ curpos;
	if(curpos >= (int)index.size()) return -1;
	id = index[curpos].id;
	return curpos;
}

inline int RSIndexPool::FindID(RSIndexID id)
{
	int i_size = int(index.size());
	if(last_i != -1 && last_i < i_size && index[last_i].id == id) 
		return last_i;
	last_i = -1;
	for(size_t i = 0; i < i_size; i++) {
		if(index[i].id == id) {
			last_i = (int)i;
			break;
		}
	}
	return last_i;
}

inline bool RSIndexPool::IsLocked(int pos)
{
	return index[pos].info->write != NULL;
}

inline void RSIndexPool::LoadFailed(int pos)
{
	index[pos].info->nfail++;
}

inline RSIndex* RSIndexPool::GetForRead(int pos, bool& valid)
{
	index[pos].info->nreq ++;

	if(index[pos].id.IsType1())
		valid = true;
	else {
		if(index[pos].info->isold || index[pos].info->isnew)
			valid = false;
		else
			valid = true;
	}
	vector<RSIndex*>& read = index[pos].info->read;
	if(!valid || read.empty() || (read[0] == NULL) || !read[0]->IsValid())
		return NULL;
	return read[0]->GetForRead();
}

inline void RSIndexPool::PutForRead(int pos, RSIndex* rsi)
{
	vector<RSIndex*>& read = index[pos].info->read;
	assert(read.empty() || (read[0]==NULL) || !read[0]->IsValid());
	if(read.empty()) read.resize(1);
	else if(read[0]) read.push_back(read[0]);
	read[0] = rsi->GetForRead();
	assert(read[0] == rsi);
}

void RSIndexPool::ReleaseRead(RSIndex *rsi)
{
	if(!rsi) { assert(0); return; }

	// find 'rsi' in 'index'
	int pos1 = FindID(rsi->GetID());
	if(pos1 < 0) { assert(0); return; }					// invalid 'rsi'
	vector<RSIndex*>& read = index[pos1].info->read;
	size_t pos2 = 0;
	for(; pos2 < read.size(); pos2++)
		if(read[pos2] == rsi) break;

	if(pos2 == read.size()) { assert(0); return; }		// invalid 'rsi'
	int err = read[pos2]->ReleaseRead();
	assert(!err);
	//if(!read[pos2]->IsLocked() && ((pos2 > 0) || !read[pos2]->IsValid()))
	//	// 'rsi' is no longer used and it's invalid or there is newer version of the index
	//	DestroyRSI((size_t)pos1, pos2);

	Cleaning(pos1);
}

inline void RSIndexPool::PutForWrite(int pos, RSIndex* rsi, int lock)
{
	assert(pos >= 0);
	assert(index[pos].info->write == NULL);
	index[pos].info->write = rsi;
	index[pos].info->write_lock = lock;
	if(index[pos].info->isold) {			// in the case that ID is during destruction, already deleted from RSI def
		index[pos].info->isold = false;
		index[pos].info->isnew = true;
	}
	rsi = rsi->GetForWrite();		assert(rsi);
}

RSIndex* RSIndexPool::Commit(int pos, RSIndex* rsi, int& lock)
{
	// unlock the index
	assert(rsi == index[pos].info->write);
	index[pos].info->write = NULL;
	int err = rsi->ReleaseWrite();
	assert(!err);

	// move 'rsi' to the list of indexes for reading; delete older indexes
	if(rsi->IsValid()) {
		assert(!index[pos].info->isold);
		vector<RSIndex*>& read = index[pos].info->read;
		if(read.size() > 0) {
			if(read[0] != NULL) {
				if (read[0]->IsLocked())
					read.push_back(read[0]);
				else
					delete read[0];
			}
		} else
			read.resize(1);
		read[0] = rsi;
	}
	else {
		delete rsi;
		rsi = NULL;
	}

	lock = index[pos].info->write_lock;
	if(index[pos].info->isold)
		Cleaning(pos);				// if 'isold' -> do cleaning
	else if(index[pos].info->isnew && index[pos].id.IsType2())
		Cleaning();
	return rsi;
}

//int RSIndexPool::Rollback(int pos, RSIndex* rsi)
//{
//	assert(rsi == index[pos].info->write);
//	int err = rsi->ReleaseWrite();				assert(!err);
//	delete rsi;
//	index[pos].info->write = NULL;
//	int lock = index[pos].info->write_lock;
//	if(index[pos].info->isnew) {
//		assert(index[pos].info->isold == false);
//		index[pos].info->isnew = false;
//		index[pos].info->isold = true;
//	}
//	Cleaning(pos);
//	return lock;
//}

inline void RSIndexPool::Cleaning(int pos)
{
	if(pos < 0) {
		for(int i = 0; i < (int)index.size(); i++)
			Cleaning(i);
		return;
	}

	// remove unnecessary index objects;
	// there can be many NULLs in 'read' list - all of them will be removed but the one at read[0]
	if(index[pos].info->write) { assert(index[pos].info->write->IsLocked()); }
	vector<RSIndex*>& read = index[pos].info->read;
	if((read.size() > 0) && read[0] && !read[0]->IsLocked() && !read[0]->IsValid()) {
		delete read[0];
		read[0] = NULL;
	}
	for(int i = 1; i < (int)read.size(); i++) {
		if(read[i] && read[i]->IsLocked()) continue;
		if(read[i]) delete read[i];
		if(i+1 < (int)read.size())
			read[i] = read.back();
		read.pop_back();
	}
	if((read.size() == 1) && (read[0] == NULL))
		read.pop_back();

	// is the ID old and should be removed from the pool
	if(index[pos].info->isold && !index[pos].info->write && !read.size()) {
		delete index[pos].info;
		pos++;
		while(pos < (int)index.size()) {
			index[pos-1] = index[pos];
			pos++;
		}
		index.pop_back();
	}
}

//int RSIndexPool::CollapseIndex(RSIndex* rsi)
//{
//	if(!rsi) { assert(0); return 2; }
//	if(rsi->IsLocked()) return 1;
//
//	int pos1 = FindID(rsi->GetID());
//	if(pos1 < 0) { assert(0); return 2; }
//	assert(index[pos1].info->write != rsi);
//
//	vector<RSIndex*>& read = index[pos1].info->read;
//	size_t pos2 = 0;
//	for(; pos2 < read.size(); pos2++)
//		if(read[pos2] == rsi) break;
//	if(pos2 == read.size()) { assert(0); return 2; }		// invalid 'rsi'
//
//	delete read[pos2];
//	read[pos2] = NULL;
//
//	Cleaning(pos1);
//	return 0;
//}

//////////////////////////////////////////////////////////////

RSIndexID::RSIndexID(string fname){

}

inline int RSIndexID::Load(FILE* f)
{
	int typeIn = 0;
	if(1 != fscanf(f, "%18d ", &typeIn)) return 1;
	type = static_cast<RSIndexType>( typeIn ); /* *CAUTION* */
	if(!IsType1() && !IsType2()) return 1;
	if(2 != fscanf(f, "%18d %18d ", &tab, &col)) return 1;
	if(IsType2() &&
	  (2 != fscanf(f, "%18d %18d ", &tab2, &col2))) return 1;
	if(!IsCorrect()) return 1;
	return 0;
}

inline int RSIndexID::Save(FILE* f)
{
	if(0 > fprintf(f, "%d", type)) return 1;
	if(0 > fprintf(f, "\t%d\t%d", tab, col)) return 1;
	if(IsType2() &&
	  (0 > fprintf(f, "\t%d\t%d", tab2, col2))) return 1;
	if(0 > fprintf(f, "\n")) return 1;
	return 0;
}

std::string RSIndexID::GetFileName(string const& s)
{
	stringstream name;
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(IsType1() || IsType2());
	if(IsType1())
		name << s << RSIndexName[type] << "." << tab << "." << col << ".rsi";
	else if(IsType2())
		name << s << RSIndexName[type] << "." << tab << "." << col << "." << tab2 << "." << col2 << ".rsi";
	return name.str();
}

bool RSIndexID::operator==(const RSIndexID& id) const
{
	return (col==id.col)&&(tab==id.tab)&&(type==id.type)&& (IsType1() || ((tab2==id.tab2)&&(col2==id.col2)));
}


bool RSIndexID::operator<(const RSIndexID& id) const
{
	if(type < id.type) return true; else
	if(type > id.type) return false;
	if(tab < id.tab) return true; else
	if(tab > id.tab) return false;
	if(col < id.col) return true; else
	if(col > id.col) return false;
	if(IsType1()) return false;
	assert(IsType2());
	if(tab2 < id.tab2) return true; else
	if(tab2 > id.tab2) return false;
	if(col2 < id.col2) return true;
	return false;
}

inline bool RSIndexID::Contain(int t, int c)
{
	if(c < 0) return (tab==t) || (IsType2() && (tab2==t));
	return ((tab==t)&&(col==c)) || (IsType2() && (tab2==t)&&(col2==c));
}

std::ostream& operator<<(std::ostream& outstr, const RSIndexID& rsi)
{
	switch(rsi.type) {
		case RSI_CMAP 	:
		case RSI_HIST 	:	outstr << rsi.type << "\t" << rsi.tab << "\t" << rsi.col ;
							break;
		case RSI_JPP	: 	outstr << rsi.type << "\t" << rsi.tab << "\t" << rsi.col << "\t" << rsi.tab2 << "\t" << rsi.col2 ;
							break;
		default			: 	break;
	}
	return outstr;
}

inline RSIndexPool::IndexInfo::~IndexInfo()
{
	delete write;
	for(size_t i=0; i < read.size(); i++)
		delete read[i];
}

vector<RSIndexID> RSI_Manager::GetAll(int tab)		// get a list of all existing RSIndices concerning the given table
{	
	vector<RSIndexID> list;
	RSIndexID id;
	int unpars = 0;
	bool valid = false;
	// scanning kn_folder
	directory_iterator end_itr;
	for( directory_iterator itr_db(path); itr_db != end_itr; ++itr_db) {
		string fn_loc = itr_db->path().string();
		typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
		boost::char_separator<char> sep(".", "", boost::keep_empty_tokens);
		tokenizer tok(fn_loc, sep);			
		tokenizer::iterator it_end = tok.end();		
		vector<string> vs;
		for(tokenizer::iterator it = tok.begin(); it != it_end; ++it)
			vs.push_back(*it);

		int vssize = int(vs.size());		
		if(vssize == 4 || vssize == 6){
			int t1, c1, t2, c2;		
			if((vssize == 4) && (vs[0] == "CMAP" || vs[0] == "HIST")) {
				try {
					t1 = boost::lexical_cast<int>(vs[1]);
					c1 = boost::lexical_cast<int>(vs[2]);
					if(tab == t1){
						if(vs[0] == "CMAP") {							
							list.push_back(*(new RSIndexID(RSI_CMAP, t1, c1)));
						}
						else if(vs[0] == "HIST"){							
							list.push_back(*(new RSIndexID(RSI_HIST, t1, c1)));
						}						
					} 	
					valid = true;
				} catch(bad_lexical_cast const &) {
				}
			} else { //vssize == 6
				if(vs[0] == "JPP") {
					try {
						t1 = boost::lexical_cast<int>(vs[1]);
						c1 = boost::lexical_cast<int>(vs[2]);
						t2 = boost::lexical_cast<int>(vs[3]);
						c2 = boost::lexical_cast<int>(vs[4]);
						if(tab == t1 || tab == t2) {							
							list.push_back(*(new RSIndexID(RSI_JPP, t1, c1, t2, c2)));	
						}
						valid = true;
					} catch(bad_lexical_cast const &) {
					}
				} 
			} 
		}
		if(!valid)
			unpars++;
		valid = false;
	}
	if( unpars > 2 ){
		rclog << lock << "WARNING: There were found " << unpars << " filenames in KNFolder that could not be parsed." << unlock;
	}		
	return list;
}

vector<RSIndexID> RSI_Manager::GetAll2(RCTable & tab)		// get a list of all existing RSIndices concerning the given table
{	
	vector<RSIndexID> list;
	int tid = tab.GetID();	
	int const RSIndexNameSize = sizeof( RSIndexName )/sizeof (char*);	
	directory_iterator end_itr;
	for(uint an = 0; an < tab.NoAttrs(); an++ )
		for(int i = 0; i < RSIndexNameSize; i++){		
			std::string act_n(RSIndexName[i]);
			if(act_n == "JPP") 
				continue;		
			RSIndexType typ = RSI_Manager::RSIndexNameToType(act_n);						
			RSIndexID ind(typ, tid, an);			
			IBFile ibf;
			if (exists(ind.GetFileName((rsi_manager->GetKNFolderPath()).c_str())))
				list.push_back(ind);			
		}
	return list;
}

int InitRSIManager(string datadir_path, bool force)
{
	if(!rsi_manager) {
		char rsi_path[2048];
		int index_level = 99; // 0 to switch off RSIndex
		try {
			index_level = Configuration::GetProperty(Configuration::KNLevel);
		} catch (...) {
			index_level = 99;
		}
		if(index_level > 0 || force) {
			try {
				strcpy(rsi_path, Configuration::GetProperty(Configuration::KNFolder).c_str());
			} catch (...) {
				strcpy(rsi_path, "BH_RSI_Repository");
			}
			try {
				if(!DoesFileExist(rsi_path)) {
					if(MakeDirs(rsi_path) < 0) {
						rclog << lock << "WARNING: KN directory not present/creation failed" << unlock;
						return -1;
					}
					try {
						RSI_Manager::CreateMetadataFile(MakeDirectoryPathOsSpecific(rsi_path) + RSI_METADATAFILE, RSI_Manager::GetRSIVersionCode((RSI_MANAGER_VERSION)));
					} catch(std::exception&) {
						rclog << lock << "WARNING: Metadata file creation failed" << unlock;
						return -1;
					}
				}
				rsi_manager.reset(new RSI_Manager(rsi_path, index_level));
			} catch (...) {
				// just run without RSI...
			}
		} else {
			rccontrol << lock << "KNLevel set to 0, no Knowledge Grid in use." << unlock;
		}
	}
	return 0;
}


int RunRSIUpdater(const string& datadir_path, const string& kn_folder)
{
#ifndef PURE_LIBRARY
	char updater_app[1048] = "";
// GA
	size_t len = 0;
	dirname_part(updater_app, my_progname, &len);
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT(updater_app[len] == 0);

// 	dirname_part(updater_app, my_progname);


	strcat(updater_app, "updater");

	vector<string> params;

#ifdef __GNUC__
	params.push_back(string("--datadir=") + datadir_path);
#else
	params.push_back(string("--datadir=\"") + datadir_path + "\"");
#endif
	if(IsAbsolutePath(kn_folder))
		params.push_back(string("--knfolder=") + kn_folder);
	else
#ifdef __GNUC__
		params.push_back(string("--knfolder=") + datadir_path + kn_folder);
#else
		params.push_back(string("--knfolder=\"") + datadir_path + kn_folder + "\"");
#endif
	params.push_back(string("--migrate-to=") + RSI_MANAGER_VERSION);
	params.push_back(string("--log-file=updater.log"));

	IBProcess ibpr;
	int return_code = -128;
	try {
		ibpr.Start(updater_app, params);
		ibpr.Wait(return_code);
	} catch (IBSysException& e) {
   	format syserr("System error(%1%) on %2% execution: %3%.");
   	syserr % e.GetErrorCode(); syserr % updater_app; syserr % e.what();
		rclog << lock << syserr.str() << unlock;
		my_message(BHERROR_UNKNOWN, syserr.str().c_str(), MYF(0));
		return -1;
	} catch(...) {
		return_code = -128;
	}

	if(return_code == -128)
		rclog << lock << "Brighthouse: Fatal error: Unable to start IB Updater (" << updater_app << ")." << unlock;
	else if(return_code < 0) {
		rclog << lock <<	string("Brighthouse: Fatal error: Update process failed.") +
							string(" For details on the error please see the updater.log file in the " + datadir_path + " directory.")
		<< unlock;
	}
	return min(0, return_code);
#else
	BHERROR("NOT IMPLEMENTED");
	return 0;
#endif
}
