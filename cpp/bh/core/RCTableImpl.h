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

#ifndef _RCTABLE_H_
#define _RCTABLE_H_

#include <string>
#include <boost/shared_ptr.hpp>
#include <boost/utility.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "common/CommonDefinitions.h"
#include "core/JustATable.h"
#include "core/RCAttr.h"
#include "system/ib_system.h"

class BufferForInsert;
class IndexTable;
class Updater;

struct AttrInfo
{
	//const char* name;
	//const char* desc;
	AttributeType type;
	int size;
	int precision;
	bool no_nulls;
	bool lookup;
	bool actually_unique;
	unsigned int current_trans;
	_int64 uncomp_size;
	_int64 comp_size;
	int column_size;
};

class RCTableImpl : public JustATable ,public boost::noncopyable
{
public:
	struct OpenMode
		{
		typedef enum
			{
			DEFAULT = 0,
			FOR_WRITE = 1,
			FOR_DROP = 2,
			FOR_LOADER = 4,
			FORCE = 8
			} open_mode_t;
		};
public:
	RCTableImpl( std::string const& path, std::vector<DTCollation> charsets, char* nm, int t_n, unsigned int s_id = 0);
	RCTableImpl( std::string const& path, std::vector<DTCollation> charsets, int current_state, OpenMode::open_mode_t = OpenMode::DEFAULT);
	RCTableImpl(int noattr);
	virtual ~RCTableImpl();

	/////////////////////// Synchronization //////////////////////////////////////
	int waiting_for_drop;

	//void Refresh(int current_state); // inter-process synchronization

	void LockPackInfoForUse(); // lock attribute data against memory manager
	void UnlockPackInfoFromUse(); // return attribute data to memory manager
	void LockPackForUse(unsigned attr, unsigned pack_no, ConnectionInfo& conn);
	void UnlockPackFromUse(unsigned attr, unsigned pack_no);
	void LockLastPacks(Transaction& trans);
	void LockLastPacks(Transaction& trans, const std::set<int>& columns);
	void UnlockLastPacks();
	void UnlockLastPacks(const std::set<int>& columns);
	void UnlockLastPacksForOthers(const std::set<int>& columns);

	///////////////////////////// Table definition & information //////////////////////////////
	void AddAttribute(char* nm, AttributeType a_type, int a_field_size, int a_dec_places = 0, unsigned int param = 0, DTCollation collation = DTCollation());

	virtual void Save(); // save table definition and data
	void SaveHeader(); // save table definition
	void Drop(); // drop this table

	const char		*Name() const 			{ return name; }
	const char		*Description() const 	{ return desc; }
	int 			GetID() const 			{ return tab_num; }
	TType 			TableType() const 		{ return RC_TABLE; }

	uint NoAttrs() const { return no_attr; }
	uint NoDisplaybleAttrs() const { return NoAttrs(); }
	std::vector<AttrInfo> GetAttributesInfo(); // attributes info
	//int AttrNum(const char *a_name); // return the number of an attribute (given by name) or -1 if attribute not found
	const char *AttrName(int n_a);
	//AttributeType AttrType(int n_a);
	std::vector<ATI> GetATIs(bool orig = false);
	//int GetAttrSize(int n_a);
	//int GetFieldSize(int n_a);
	//int GetInternalSize(int n_a);
	//NullMode GetAttrNullMode(int n_a);
	//int GetAttrScale(int n_a);
	bool IsLookup(int n_a);
	const ColumnType& GetColumnType(int a) ;
	PhysicalColumn* GetColumn(int col_no) { return a[col_no]; }
	RCAttr* GetAttr(int n_a);
	//void UnlockAttr(int n_a);
	time_t GetCreateTime() const { return m_create_time; }
	time_t GetUpdateTime() const { return m_update_time; }

	//////////////////////////// Transaction management ////////////////////////
	int Verify(); // verify table integrity (number of objects in each attribute)
	unsigned int GetSaveSessionId();
	//unsigned int GetLastSaveSessionId();
	virtual unsigned int CommitSaveSession(unsigned int sess_id);
	virtual void CommitSwitch(uint save_result);
	virtual void Rollback(unsigned int s_id = 0, bool = false );
	bool IsRLCPossible();
	//void RollbackLastCommitted(uint s_id, int current_state);
	//bool Committed();

	//////////////////////////// Data load & export ////////////////////////
	void SetWriteMode(unsigned int s_id) { SetMode(1, s_id); } // reopen all attributes information in the write mode
	void LoadDirtyData(unsigned int session_id);
	virtual bool NeedRefresh() { return false; }

	/////////////////////// Data access & information //////////////////////////
	virtual _int64 NoObj();
	virtual _int64 NoValues() { return NoObj(); }
	virtual _int64 NoNulls(int attr);

	void GetTable_S(RCBString& s, _int64 obj, int attr);
	_int64 GetTable64(_int64 obj, int attr); // value from table in 1-level numerical form
	void GetTable_B(_int64 obj, int attr, int &size, char *val_buf); // value from table in string form
	bool IsNull(_int64 obj, int attr); // return true if the value of attr. is null
	RCValueObject GetValue(_int64 obj, int attr, ConnectionInfo *conn = NULL);

	/////////////////////// Query execution ///////////////////////////////////

	_int64 RoughMin(int n_a, Filter *f = NULL);		// for numerical: best rough approximation of min for a given filter (or global min if filter is NULL)
	_int64 RoughMax(int n_a, Filter *f = NULL);		// for numerical: best rough approximation of max for a given filter (or global max if filter is NULL)
	ushort MaxStringSize(int n_a, Filter *f = NULL);
	/////////////////////////// Rough set part ////////////////////////////////////////////

	void DisplayRSI();
	std::vector<DTCollation> GetCharsets() { return charsets; }
	void SetCharsets(std::vector<DTCollation> _charsets) { charsets = _charsets; } 
	void FillDecompositions(std::vector<std::string>& decompositions);
protected:
	char* name;
	std::string db_name;
	char* desc; // table description (comments)
	int tab_num; // Index (unique) of the table in database. Used e.g. for determining folder name, identifying RSI etc.
	std::string path;
	std::string path_no_ext;

	std::vector<DTCollation> charsets; // charset definition for each column

	int no_attr;
	RCAttr** a;

    IBMutex  load_attribute_mutex;

	int m_state;
	bool check_integrity;			// if false, then do not check no. of obj. integrity in open

	time_t m_create_time, m_update_time;

	int conn_mode;
	unsigned int session;

	bool			obsolete;

	// concatenate db path 'a_path' with folder name of the table (\D00...) and store in 'path'
	void SetPath( std::string const& a_path );
	virtual void LoadAttribute(int attr_no);

	void SetMode(int conn_mode, unsigned int s_id); // reopen all attributes information in the new mode (see above)

	/////////////////////// Housekeeping //////////////////////////////////////
	void CleanUp();
	virtual void DoDrop();
public:
	int 		DeleteRow(_uint64 row_id);
	std::string const& GetPath() const { return path; }
	std::string const& GetPathNoExt() const { return path_no_ext; }
	std::string const&	GetDBName() const { return db_name; }
	virtual void MakeObsolete() { obsolete = true; }

	class Iterator {
		USE_THIS_MACRO_TO_DO_RC_TABLE_A_FRIEND
	public:
		Iterator()	:	table(0), position(-1), current_record_fetched(false), used_atts_updated(false), conn(NULL)	{}
	private:
		Iterator(RCTableImpl& table, FilterPtr filter);
		void Initialize(const std::vector<bool>& attrs);

	public:
		//bool UsedAttrsUpdated() const { return used_atts_updated; }
		//void UpdateUsedAttrs(const std::vector<bool>& attrs);

		bool operator==(const Iterator& iter);
		bool operator!=(const Iterator& iter) { return !(*this == iter); }
		void operator++(int);
		NewRecord& operator*() { FetchValues(); return record; }


		void MoveToRow(int64 row_id);
		int64 GetCurrentRowId() const { return position; }

	private:
		void FetchValues();
		void UnlockPacks(int64 new_row_id);
		void LockPacks();

	private:
		RCTableImpl*	table;
		FilterPtr		filter;
		FilterOnesIterator it;
		int64 			position;
		NewRecord 		record;
		bool 			current_record_fetched;
		bool			used_atts_updated;
		typedef std::vector<boost::function1<void, uint64> > values_fetchers_t;
		values_fetchers_t values_fetchers;
		std::vector<boost::shared_ptr<DataPackLock> > dp_locks;
		std::vector<RCAttr*> attrs;
		ConnectionInfo* conn;


	private:
		static Iterator CreateBegin(RCTableImpl& table, FilterPtr filter, const std::vector<bool>& attrs);
		static Iterator CreateEnd();
	};

	Iterator Begin(const std::vector<bool>& attrs, Filter& filter)	{ return Iterator::CreateBegin(*this,	FilterPtr(new Filter(filter)), attrs); }
	virtual Iterator Begin(const std::vector<bool>& attrs)			{ return Iterator::CreateBegin(*this,	FilterPtr(new Filter(NoObj(), true)), attrs); }
	Iterator End() 													{ return Iterator::CreateEnd(); }
};

#endif

