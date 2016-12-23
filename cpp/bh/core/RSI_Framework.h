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

#ifndef __RSI_FRAMEWORK_H
#define __RSI_FRAMEWORK_H

#ifdef MSC_VER
#include <io.h>
#endif

#include <stdio.h>
#include <ostream>
#include <vector>
#include <string>
#include "bintools.h"
#include "system/RCException.h"
#include "system/MemoryManagement/TrackableObject.h"
#include "system/IBFile.h"
#include "system/ib_system.h"

#define RSI_METADATAFILE	"metadata.rsd"
#define RSI_MANAGER_VERSION "3.2GA"
#define RSI_FLOCK_TRY_DEFR	50			// how many times to try to lock definition file when reading
#define RSI_FLOCK_WAIT_DEFR	500			// time delay [ms] between tries, for definition file when reading
#define RSI_FLOCK_TRY_DEFW	20			// how many times to try to lock definition file when writing
#define RSI_FLOCK_WAIT_DEFW	500			// time delay [ms] between tries, for definition file when writing
#define RSI_FLOCK_TRY_R		10			// how many times to try to lock .rsi file when reading
#define RSI_FLOCK_WAIT_R	500			// time delay [ms] between tries, when reading
#define RSI_FLOCK_TRY_W		100			// how many times to try to lock .rsi file when writing
#define RSI_FLOCK_WAIT_W	500			// time delay [ms] between tries, when writing
#define RSI_DELETE_TRY		20			// how many times to try to delete .rsi file
#define RSI_DELETE_WAIT		200			// time delay [ms] between tries

//////////////////////////////////////////////////////////////
#ifdef __GNUC__
enum RSIndexType	{	RSI_TEST = 0,			// used for testing of RSI_Manager
							RSI_HIST = 1,			// histogram data on packs
							RSI_JPP = 2,			// Pack-pack joining index
							RSI_CMAP = 3			// character maps on packs
						};
#else
enum RSIndexType:int	{	RSI_TEST = 0,			// used for testing of RSI_Manager
							RSI_HIST = 1,			// histogram data on packs
							RSI_JPP = 2,			// Pack-pack joining index
							RSI_CMAP = 3			// character maps on packs
						};
#endif

class RSIndex;
class KN
{
public:
	KN() : rsi(0), pack(-1) {}
	RSIndex* rsi;
	int pack;
};

struct RSUsageStats
{
	RSUsageStats():all_packs(0),used_packs(0),pack_rsr(0),pack_rsi(0)
	{}
	_int64 all_packs;		// upper limit: all packs in a column / pair
	_int64 used_packs;		// number of packs still to be (roughly) checked
	_int64 pack_rsr;			// number of packs excluded because of rough set representation (statistics)
	_int64 pack_rsi;			// number of packs excluded because of rough set indexes of various types
	void Clear()
	{
		all_packs=used_packs=pack_rsi=pack_rsr=0;
	}
};

struct RSIndexID			// index identifier: index type plus index-dependent parameters (can be extended when needed)
{
	RSIndexType type;

	// table and column numbers: (tab,col) - 1st column, (tab2,col2) - 2nd column (if used; depends on 'type')
	int tab, col, tab2, col2;

	bool IsType1() const						{ return (type <= RSI_HIST || type == RSI_CMAP); }
																						// one-column index
	bool IsType2() const						{ return type == RSI_JPP;  }			// two-column index
	bool IsCorrect()							{ return type <= RSI_CMAP && (tab>=0)&&(col>=0) &&
														 (IsType1() || ((tab2>=0)&&(col2>=0))); }
	void Check()								{ assert(IsCorrect()); }

	RSIndexID()									{ type = (RSIndexType)-1; tab = col = tab2 = col2 = -1; }
	RSIndexID(RSIndexType t, int tb, int c)		{ type = t; tab = tb; col = c; tab2 = col2 = -1; Check(); }
	RSIndexID(RSIndexType t, int tb, int c, int tb2, int c2)
												{ type = t; tab = tb; col = c; tab2 = tb2; col2 = c2; Check(); }
	RSIndexID(std::string filename);

	int Load(FILE* f);
	int Save(FILE* f);
	std::string GetFileName(std::string const& s);

	bool operator==(const RSIndexID& id) const;
	bool operator< (const RSIndexID& id) const;
	bool Contain(int t, int c = -1);			// does this ID contain column (t,c) or table (t)
};

std::ostream& operator<<(std::ostream& o, const RSIndexID& rsi);

class RSIndex;
class RSIndexPool;

//////////////////////////////////////////////////////////////

class RSI_Manager	// implements its own memory management based on requests of the general Memory Manager
{
public:
	RSI_Manager(const char *_path, int _index_l);
						// load RSI Repository information for the current database (_path is a directory path);
						// throw exception on error (e.g. when '_path' is not a name of existing directory)

	~RSI_Manager();		// update statistics, delete all RSIndexes etc.

	// Manipulating on indexes:

	RSIndex* GetIndex( RSIndexID id, int read_loc );
						// Get index for read only.
						// return: NULL if no such index exists or it is not accessible,
						// otherwise a pointer to be cast to the proper index type;
						// note that this pointer is managed by RSI_Manager and must be released by ReleaseIndex after use
	void ReleaseIndex( RSIndex *ptr );
						// Give back an object 'ptr' taken for reading with GetIndex().
						// Annotate object 'ptr' as no longer used; may be deleted or stored for future use

	RSIndex* GetIndexForUpdate(	RSIndexID id, int read_loc );
						// Get index for read and write.
						// If index doesn't exist - create an empty index of certain type,
						// or lock an existing index to be updated.
						// If rollback copy of index file doesn't exist, make it.
						// Return NULL if the index can't be locked (e.g. is already locked).
						// The newest version of index is loaded from file before update.
						// If the file is successfully opened but loading fails, an empty index is returned
						// (so the index will have to be built from scratch!!!)
						// The index is then filled in (or updated) by type-dependent index functions.
						// The index must be released by UpdateIndex once creation or update is finished.

	int UpdateIndex( RSIndex *ptr, int write_loc,bool do_not_save = false);
						// after GetIndexForUpdate, save (or not) updates and unlock, do NOT commit; return 0 when success

	//void CollapseIndex( RSIndex* ptr );		// delete index object from memory; invoked by MemoryManager (assert: index exists and is not being used)

	void DeleteIndex( int tab, int col);	// delete all indexes concerning the given column or table (when col<0)
	std::vector<RSIndexID> GetAll(int tab);		// get a list of all existing RSIndices concerning the given table
	std::vector<RSIndexID> GetAll2(RCTable & tab);		// get a list of all existing RSIndices concerning the given table without scanning kn folder

	//void UpdateDef( bool reload = true );	// read current RSI DEF file; 'reload' - mark all indexes as invalid (not up to date), to be deleted

	void UpdateDefForTable(int table_id);

	int IndexLevel()		{ return index_level; }
	bool IndexExists(RSIndexID id);

private:

	std::string path;
	int index_level;		// 0 - no KNs (but the manager is NULL in this case), 1 - normal, but no KNs in group by, 2 - full

	boost::shared_ptr<RSIndexPool> pool;		// definitions of all available indexes, with info which of them are loaded and/or locked
    IBMutex 	rsi_mutex;
	//pthread_mutex_t CS;
	int TryLock(RSIndexID id);
	void UnLock(int lock);
	RSIndex* LoadIndex(RSIndexID id, bool write, int read_loc);		// when write=true, file is created if needed; return NULL if file can't be loaded (e.g. already locked)
	void SaveIndex(RSIndexID id, RSIndex* rsi, int write_loc);		// return 0 if OK
	void DeleteIndexFile(RSIndexID id);

public:
	static int			GetRSIVersionCode(const std::string& ib_version);
	static int			GetRSIVersion(std::string metaDataFileFolder);
	static std::string 	GetRSIVersionName(int version_code);
	static int 			CreateMetadataFile(const std::string& metadata_file_path, int version_to);
	std::string			GetKNFolderPath() {return std::string(path);};
	static RSIndexType	RSIndexNameToType(std::string name) throw(SystemRCException);

	typedef std::vector<std::pair<std::string, uint> > versions_map_type;
	static	versions_map_type versions_map;
};

//////////////////////////////////////////////////////////////

/*
	Format of RSI definition file:
	- one line for each index
	- line:  <index_type/int> [<table/int> <column/int> ...<other_params>]
*/

class RSIndexPool
{
	friend class RSI_Manager;
public:
	RSIndexPool();
	~RSIndexPool();
		
	void InvalidateRead(int table_id);			// mark 'read' index for table as invalid

	int curpos;									// used in FirstID and NextID
	int FirstID(RSIndexID& id);					// for iterating along 'index'; returns 'id' and position of 'id'
	int NextID(RSIndexID& id);

	int FindID(RSIndexID id);
	int NewID(RSIndexID id);					// keeps array 'index' sorted by id
	void DeleteFromDef(int pos);				// mark id as old

	RSIndex* GetForRead(int pos, bool& valid);	// if the index exists, its no. of readers is incremented
	void PutForRead(int pos, RSIndex* rsi);		// used after loading index from file
	void ReleaseRead(RSIndex *rsi);

	bool IsLocked(int pos);						// is an index locked for writing
	void PutForWrite(int pos, RSIndex* rsi, int lock);
	RSIndex* Commit(int pos, RSIndex* rsi, int& lock);			// moves 'rsi' to group of readable indexes
	//int Rollback(int pos, RSIndex* rsi);		// returns lock descriptor; deletes 'rsi'
	//int FindWrite(RSIndex* rsi, RSIndexID* id = NULL);

	//int CollapseIndex(RSIndex* rsi);			// find 'rsi' in pool and delete it from memory; return: 0 - success, 1 - 'rsi' is locked, 2 - another error

	void Cleaning(int pos = -1);				// remove old RSI objects and old IDs
	void LoadFailed(int pos);					// nfail++

private:

	//struct IndexObj {
	//	RSIndex* rsi;
	//	int readers;				// no. of users currently using 'ptr' for reading
	//};
	struct IndexInfo {
		//std::vector<IndexObj> read;
		std::vector<RSIndex*> read;	// Array of RSIndex objects being used for reading (it's not enough to hold only 1 !!!)
									// The newest object is always in read[0]. If read[0]==NULL then the newest object
									// had to be deleted and must be loaded for next reading request.
		RSIndex* write;				// The RSIndex being updated now (can be at most 1) or NULL
		int write_lock;				// Descriptor of .lock file associated with the file being updated
		int nreq, nfail;			// Total no. of requests for reading (nreq) and how many of them failed (nfail)
									// due to problems with loading
		bool isnew;					// ID not in DEF file, should be added asap (now or after construction, depending on 'write')
		bool isold;					// ID not in DEF file, should be removed from memory asap
		bool delfromdef;			// ID is in DEF file, but should be deleted from DEF (implies isold==true)
		void Init()			{ write = NULL; nreq = nfail = 0; write_lock = -1; isnew = isold = delfromdef = false; read.reserve(2); }
		IndexInfo()			{ Init(); }
		~IndexInfo();
	};

	struct ID_Info {
		RSIndexID id;
		IndexInfo* info;
	};

	std::vector<ID_Info> index;		// indexes in 'index' and in definition file are sorted by ID
	int last_i;						// index of the last seen ID_info (for optimization)

	static const size_t _PATHLEN = 1024;

	void InsertID(RSIndexID id, int pos);
	void DeleteID(int pos);
	void DestroyRSI(size_t pos1, size_t pos2);

};


//////////////////////////////////////////////////////////////

class RSIndex : public TrackableObject
{
	friend class IBCFormat;
private:
	RSIndexID id;
	int readers;		// when positive: number of readers; -1: used by a writer; 0: currently not used (can be deleted)
	bool valid;			// object is invalidated when index is deleted, so the object should be also deleted asap

public:

	// Internal Representation for external compressed format

	virtual ushort GetRepLength() = 0;
	virtual void CopyRepresentation(void *buf, int pack) = 0;

	///// I/O methods, to be implemented in descendants
	// Return value: positive for error, 0 when OK
	virtual void Load(IBFile* frs_index, int current_loc) = 0;		// fd - file descriptor, open for binary reading (the file can be newly created and empty - this shouldn't cause an error)
	virtual void LoadLastOnly(IBFile* frs_index, int current_loc) = 0;		// read structure for update (do not create a full object)
	virtual void Save(IBFile* frs_index, int current_loc) = 0;		// fd - file descriptor, open for binary writing
	virtual void Clear() = 0;			// reset to empty index - to be called when Load() fails, to assure index validity

	///// From TrackableObject
	virtual TRACKABLEOBJECT_TYPE TrackableType() const { return TO_RSINDEX; }
	virtual bool IsLocked()					{ return readers != 0; }
	//virtual bool IsLockedByWrite()			{ return readers < 0; }

	///// To be called by RSI_Manager. Assumption: mutual exclusion is provided by critical section in RSI_Manager
	void SetID(RSIndexID id_)							{ id = id_; }
	RSIndexID GetID()									{ return id; }

	// Return value: positive for error, 0 when OK
	RSIndex* GetForRead()				{ if(readers < 0) return NULL;	 	readers++; return this; }
	RSIndex* GetForWrite()				{ if(readers != 0) return NULL;		readers = -1; return this; }
	int ReleaseRead()					{ if(readers <= 0) return 1;		readers--; return 0; }
	int ReleaseWrite()					{ if(readers >= 0) return 1;		readers = 0; return 0; }

	bool IsValid()						{ return valid; }
	void Invalidate()					{ valid = false; }

	RSIndex()							{ readers = 0; valid = true; }
	virtual ~RSIndex()					{}
protected:
	virtual char* GetRepresentation(int pack) = 0;
};


//////////////////////////////////////////////////////////////

/* only for testing purposes */
class RSIndex_Test : public RSIndex
{
public:
	int time;
	static const int SZ = 128;
	char buf[SZ];
	void Load(IBFile* frs_index,int current_loc)	{ buf[frs_index->Read(buf, SZ-1)] = 0; sscanf(buf, "%d", &time); }
	void LoadLastOnly(IBFile* frs_index,int current_loc)	{ buf[frs_index->ReadExact(buf, SZ-1)] = 0; sscanf(buf, "%d", &time); }
	void Save(IBFile* frs_index,int current_loc)	{ frs_index->WriteExact(buf, sprintf(buf, "%d", time)); }
	virtual void Clear()		{ time = 0; memset(buf, 0, SZ); }
	void Update()				{ time++; }
	RSIndex_Test()				{ Clear(); }
	char *GetRepresentation(int pack) { return NULL; }
	ushort GetRepLength() { return 0; }	
	void CopyRepresentation(void *buf, int pack) {}
};

int	InitRSIManager(std::string datadir_path = "", bool force = false);
int RunRSIUpdater(const std::string& datadir_path, const std::string& kn_folder);

#endif


