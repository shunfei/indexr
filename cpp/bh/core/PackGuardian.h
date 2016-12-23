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

#ifndef PACKGUARDIAN_H_INCLUDED
#define PACKGUARDIAN_H_INCLUDED 1

#include <vector>

#include "fwd.h"
#include "Descriptor.h"
#include "ValueOrNull.h"

class VirtualColumn;
class ConnectionInfo;
class RCAttr;
class MIIterator;


enum PG_Status {	PG_UNKNOWN = 0,		// status unknown (pack not used yet) - it is never returned by PackRequest() as a result
					PG_READY	= 1,	// the pack is ready to be used (and it is locked)
					PG_LATER	= 2,	// try later: no room for more packs
					PG_DONE	= 3			// the pack is already used and unlocked, no need to return to it
				};

//////////////////////////////////////////////////////////////////////////////////////

class VCPackGuardian
{
public:
	VCPackGuardian(VirtualColumn *vc);
	~VCPackGuardian();

	virtual PG_Status LockPackrow(const MIIterator &mit);
	void UnlockAll();

protected:
	// Different strategies of work of PackGuardian:
	enum { 	
		LOCK_ONE,				// lock the desired pack, unlock the previous one (for sequential access)
		LOCK_ALL,				// keep all locked packs in memory, unless unlocked by hand

		// NOT IMPLEMENTED:
		UNLOCK_OLDEST,			// remember when the pack was requested
								// and unlock the oldest used if required; always one-pass scanning
		LOCK_OLDEST				// lock all packs till the end of limit, then lock/unlock only one;
		// may require manual unlocking of packs no more needed
	} current_strategy;

	void Initialize(const MIIterator &mit, int no_th = 1);		// mit used only for determining dimension sizes

	VirtualColumn &my_vc;
	bool 	initialized;	// false if the object was not initialized yet.
							// To be done at the first use, because it is too risky to init in constructor (multiindex may not be valid yet)
	int		no_dims;		// number of dimensions (used)
	int		*dim_size;		// size of used dimensions (no. of packs)

	// Structures used for LOCK_ONE
	int** 	last_pack;

	int threads;		// number of parallel threads using the guardian

	// Structures used for LOCK_ALL
	// Performance note: it is assumed that dimensions involved in LOCK_ALL are rather small, thus the whole pack status tables are initialized
	char**	pack_locked;	// pack_locked[i] is a table of (one-byte) pack stata for dimension i: 0 - unlocked, 1 - locked
};

/////////////////////////////////////////////////////////////////////////////
// PackGuardian    - a simple pack manager, responsible for locking them for
//						   reuse in longer time perspective
//
// Usage:
//
//		PackGuardian pg(cur_tab, cur_attr);
//		do {
//			pg.Rewind();			// here we unlock all packs (and flag anything_left)
//			for(int pack = 0; pack < no_pack; pack++)
//			{
//				...
//				if(...pack needed... && pg.PackRequest(pack) == PG_READY)
//				{ ... use pack, it is locked now ...}
//			}
//		} while(pg.AnythingLeft());
//		pg.Rewind();			// here we unlock all packs; alternatively we may do this in descructor
//
/*
class PackGuardian
{
public:
	PackGuardian(	JustATable* _t,			// table to be guarded (switched off if the table is TempTable)
						int _n_a,			// attribute number (-1 means a deactivated (fake) PackGuardian)
						ConnectionInfo* _conn,
						int divide_by = 1	// number of pack guardians in memory - for better approximation
						);
	PackGuardian(	JustATable* _t,			// table to be guarded (switched off if the table is TempTable)
						std::vector<int>& v_a,	// attributes to be guarded (all locked at the same time)
						ConnectionInfo* _conn
						);
	PackGuardian(	JustATable* _t,			// table to be guarded (switched off if the table is TempTable)
						const Descriptor& desc, 	// attributes used in this descriptor will be guarded (all locked at the same time)
						ConnectionInfo* _conn
						);
	PackGuardian(	std::vector<std::pair<JustATable*,int> >& v_a		// tables/attributes to be guarded (all locked at the same time)
						);			// NOT WORKING YET
	~PackGuardian();

	PG_Status PackRequest(int pack);
									// try to lock the pack (if not already locked) for all guarded columns

	bool AnythingLeft()				{ return anything_left; }		// was any pack set to PG_LATER mode?

	void UnlockOne(int pack);					// manual unlocking, may be used for some strategies
	void Rewind();								// reset all pack status PG_READY -> PG_DONE, unlock packs
	void Clear();								// reset all pack status to PG_UNKNOWN, unlock packs

	void SetStrategyUnlockAllOnRewind()	{ current_strategy = UNLOCK_ALL_ON_REWIND; }	// NOTE: usually needs also Clear()
	void SetStrategyUnlockOldest()	{ current_strategy = UNLOCK_OLDEST; }
	void SetStrategyLockOne()		{ current_strategy = LOCK_ONE; }
	void SetStrategyLockOldest()	{ current_strategy = LOCK_OLDEST; }

	// Different strategies of work of PackGuardian:
	enum { 	UNLOCK_OLDEST,			// remember when the pack was requested
									// and unlock the oldest used if required; always one-pass scanning
			UNLOCK_ALL_ON_REWIND,	// deny access to a pack not in memory, if no place for loading it
			LOCK_ONE,				// lock the desired pack, unlock the previous one (for sequential access)
			LOCK_OLDEST				// lock all packs till the end of limit, then lock/unlock only one;
									// may require manual unlocking of packs no more needed
	} current_strategy;

private:
	RCTable *t;			// a pointer to a table to be guarded (or NULL for TempTable)
	std::vector<int> n_a;	// table of attribute numbers
	int no_attr;		// number of attributes to be guarded
	int no_pack;		// number of packs for a column

	_int64 MemoryLimit();			// how much memory we can allocate for data packs
	void ReconsiderStrategy();		// possible automatic strategy switch
	void AddAttrsFromCQTerm(const CQTerm& cq, std::vector<int>& attrno);	//extract attr numbers used in a CQTerm, add them to attrno.
	_int64 old_pack_requests;		// a counter of requesting packs

	bool anything_left;
	PG_Status *pstat;		// the current table of pack status: PG_UNKNOWN if a pack is not used yet (may be locked), PG_READY for locked packs, PG_DONE for previously used
	_int64 *requested_at;	// the freshest request number for this pack
	_int64 request_counter;	// count only newly realized requests (new locks)
	int pack_counter;		// the counter of locked packs
	int pack_limit;			// above this limit we will not lock more packs
	int last_locked_pack;	// the last pack locked in LOCK_ONE strategy (or -1)
	ConnectionInfo& conn;	// an external pointer to the current session info (for e.g. pack statistics)
};
*/
#endif /* not PACKGUARDIAN_H_INCLUDED */

