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

#ifndef DIMENSIONGROUP_H_INCLUDED
#define DIMENSIONGROUP_H_INCLUDED

#include "bintools.h"
#include "Filter.h"
#include "IndexTable.h"

///////////////////////////////////////////////////////////////////////////////////

class DimensionVector
{
public:
	DimensionVector();
	DimensionVector(int no_dims);
	DimensionVector(const DimensionVector &sec);
	~DimensionVector();
	DimensionVector &operator=(const DimensionVector &sec);
	bool operator==(const DimensionVector &d2) const;
	bool operator!=(const DimensionVector &d2) {return !operator==(d2);}
	void Resize(int no_dims);				// create the vector wider, set all new values as false

	bool &operator[](int i)					{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i >= 0 && i < size); return v[i]; }
	bool Get(int i) const					{ BHASSERT_WITH_NO_PERFORMANCE_IMPACT(i >= 0 && i < size); return v[i]; }	// to allow const usage

	void Clean();							// set all dimensions to false
	void SetAll();							// set all dimensions to true (present)
	void Complement();						// set all false to true (and opposite)
	bool Intersects(DimensionVector &sec);	// true if any common dimension present
	bool Includes(DimensionVector &sec);	// true if all dimensions from sec are present in *this
	void Minus(DimensionVector &sec);		// exclude from *this all dimensions present in sec
	void Plus(DimensionVector &sec);		// include in *this all dimensions present in sec

	int NoDimsUsed() const;						// return a number of present dimensions
	int GetOneDim();							// return the only existing dim, or -1 if more or less than one
	bool IsEmpty() const;						// is any dimension set?
	int Size()	const							{ return size; }	// return a number of all dimensions (present or not)

private:
	int size;
	bool *v;
};

///////////////////////////////////////////////////////////////////////////////////

class DimensionGroup
{
public:
	enum DGType	{ DG_FILTER, DG_INDEX_TABLE, DG_VIRTUAL, DG_NOT_KNOWN } 
		dim_group_type;

	///////////////////////////////////////////////////////////////////////////
	DimensionGroup() : dim_group_type(DG_NOT_KNOWN), no_obj(0), locks(0)		{}
	virtual DimensionGroup* Clone(bool shallow) = 0;					// create a new group with the same (copied) contents
	virtual ~DimensionGroup()													{}
	
	_int64 NoTuples()					{ return no_obj; }
	virtual void UpdateNoTuples()		{}								// may be not needed for some group types
	DGType Type()						{ return dim_group_type; }		// type selector
	virtual Filter* GetFilter(int dim) const	{ return NULL; }		// Get the pointer to a filter attached to a dimension. NOTE: will be NULL if not applicable
	virtual Filter* GetUpdatableFilter(int dim) const { return NULL; }	// Get the pointer to a filter, if it may be changed. NOTE: will be NULL if not applicable
	virtual bool DimUsed(int d) = 0;									// true if the dimension is involved in this group
	virtual bool DimEnabled(int d) = 0;									// true if the dimension is used and has values (is not forgotten)
	virtual bool NullsPossible(int d) = 0;								// true if there are any outer nulls possible in the dimension
	virtual void Empty() = 0;											// make the group empty (0 tuples)

	virtual void Lock(int dim, int n = 1)		{ locks += n; }			// lock for getting value (if needed), or just remember the number of locks
	virtual void Unlock(int dim)		{ locks--; }
	virtual int NoLocks(int dim)		{ return locks; }
	virtual bool IsThreadSafe() = 0;									// true if the dimension group may be used in parallel for reading
	virtual bool IsOrderable() = 0;										// true if the dimension group may provide an ordered (nontrivial) iterator

	// Temporary code, for rare cases when we add a dimension after other joins (smk_33):
	virtual void AddDimension()			{}

	//////////////////////////////////////////////////////////////////////////
	class Iterator
	{
	public:
		// note: retrieving a value depends on DimensionGroup type
		Iterator()								{ valid = false; }
		Iterator(const Iterator& sec)			{ valid = sec.valid; }
		virtual ~Iterator() {};

		virtual void operator++() = 0;
		virtual void Rewind() = 0;
		virtual bool NextInsidePack() = 0;
		bool IsValid() const								{ return valid; }
		virtual _int64 GetPackSizeLeft() = 0;
		virtual bool WholePack(int dim) = 0;					// True, if the current packrow contain exactly the whole pack for a dimension (no repetitions, no null objects)
		virtual bool InsideOnePack() = 0;						// true if there is only one packrow possible for dimensions in this iterator
		virtual bool NullsExist(int dim) = 0;					// return true if there exist any 0 value (always false for virtual dimensions)
		virtual void NextPackrow() = 0;
		virtual _int64 GetCurPos(int dim) = 0;
		virtual int GetCurPackrow(int dim) = 0;
		virtual int GetNextPackrow(int dim, int ahead) = 0;		// ahead = 1 => the next packrow after this one, etc., return -1 if not known
		virtual bool BarrierAfterPackrow() = 0;					// true, if we must synchronize threads before NextPackrow()

		// Updating, if possible:
		virtual bool InternallyUpdatable()		{ return false; }	// true if the subclass implements in-group updating functions, like below:
		virtual void ResetCurrent()				{}				// reset the current position
		virtual void ResetCurrentPackrow()		{}				// reset the whole current packrow
		virtual void CommitUpdate()				{}				// commit the previous resets
		virtual void SetNoPacksToGo(int n)		{}
		virtual void RewindToRow(_int64 n)		{}
		virtual bool RewindToPack(int pack)		{ return false; }	// true if the pack is nonempty
		virtual int NoOnesUncommited(uint pack)	{ return -1; }
	protected:
		bool valid;
	};

	virtual DimensionGroup::Iterator* NewIterator(DimensionVector&) = 0;		// create a new iterator (to be deleted by user)
	virtual DimensionGroup::Iterator* NewOrderedIterator(DimensionVector&, PackOrderer *po)		{ return NULL; }	// create a new ordered iterator, if possible
	virtual DimensionGroup::Iterator* CopyIterator(DimensionGroup::Iterator*) = 0;

	//////////////////////////////////////////////////////////////////////////

	virtual void FillCurrentPos(DimensionGroup::Iterator *it, _int64 *cur_pos, int *cur_pack, DimensionVector &dims) = 0;			// fill only relevant dimensions basing on a local iterator

protected:
	_int64 no_obj;
	int locks;
};


//////////////////////////////////////////////////////////////////////////////////

class DimensionGroupFilter : public DimensionGroup
{
public:
	DimensionGroupFilter(int dim, _int64 size);
	DimensionGroupFilter(int dim, Filter *f_source, int copy_mode = 0);		// copy_mode: 0 - copy filter, 1 - ShallowCopy filter, 2 - grab pointer
	virtual ~DimensionGroupFilter();
	virtual DimensionGroup* Clone(bool shallow)										{ return new DimensionGroupFilter(base_dim, f, (shallow ? 1 : 0)); }

	virtual void FillCurrentPos(DimensionGroup::Iterator *it, _int64 *cur_pos, int *cur_pack, DimensionVector &dims)	
	{
		if(dims[base_dim]) { 
			cur_pos[base_dim] = it->GetCurPos(base_dim); 
			cur_pack[base_dim] = it->GetCurPackrow(base_dim);
		}
	}
	virtual void UpdateNoTuples()													{ no_obj = f->NoOnes(); }
	virtual Filter* GetFilter(int dim)	const										{ assert(dim == base_dim || dim == -1); return f; }
	// For this type of filter: dim == -1 means the only existing one
	virtual Filter* GetUpdatableFilter(int dim) const								{ assert(dim == base_dim || dim == -1); return f; }
	virtual bool DimUsed(int dim)													{ return base_dim == dim; }
	virtual bool DimEnabled(int dim)												{ return base_dim == dim; }
	virtual bool NullsPossible(int d)												{ return false; }
	virtual void Empty()															{ no_obj = 0; f->Reset(); }
	virtual bool IsThreadSafe()														{ return true; }
	virtual bool IsOrderable()														{ return true; }

	////////////////////////////////////////////////////////////////////
	class DGFilterIterator : public DimensionGroup::Iterator
	{
	public:
		DGFilterIterator(Filter *f_to_iterate) : fi(f_to_iterate), f(f_to_iterate) { valid = !f->IsEmpty(); }
		DGFilterIterator(const Iterator& sec);

		void operator++()							{ assert(valid); ++fi; valid = fi.IsValid(); }
		virtual void Rewind()						{ fi.Rewind(); valid = fi.IsValid(); }
		virtual bool NextInsidePack()				{ bool r = fi.NextInsidePack(); valid = fi.IsValid(); return r; }
		virtual _int64 GetPackSizeLeft() 			{ return fi.GetPackSizeLeft(); }
		virtual bool WholePack(int dim)				{ return f->IsFull(fi.GetCurrPack()); }
		virtual bool InsideOnePack()				{ return fi.InsideOnePack(); }
		virtual bool NullsExist(int dim)			{ return false; }
		virtual void NextPackrow()					{ assert(valid); fi.NextPack(); valid = fi.IsValid(); }
		int GetCurPackrow(int dim)					{ return fi.GetCurrPack(); }
		_int64 GetCurPos(int dim)					{ return (*fi); }
		virtual int GetNextPackrow(int dim, int ahead)	{ return fi.Lookahead(ahead); }
		virtual bool BarrierAfterPackrow()			{ return false; }
		// Updating
		virtual bool InternallyUpdatable()		{ return true; }
		virtual void ResetCurrent()				{ fi.ResetDelayed(); }
		virtual void ResetCurrentPackrow()		{ fi.ResetCurrentPackrow(); }
		virtual void CommitUpdate()				{ f->Commit(); }
		virtual void SetNoPacksToGo(int n)		{ fi.SetNoPacksToGo(n); }
		virtual void RewindToRow(_int64 n)		{ fi.RewindToRow(n); valid = fi.IsValid(); }
		virtual bool RewindToPack(int pack)		{ bool r = fi.RewindToPack(pack); valid = fi.IsValid(); return r; }
		virtual int NoOnesUncommited(uint pack)	{ return f->NoOnesUncommited(pack); }

		virtual bool Ordered()					{ return false; }		// check if it is an ordered iterator

	private:
		FilterOnesIterator fi;
		// external pointer:
		Filter *f;
	};
	////////////////////////////////////////////////////////////////////
	class DGFilterOrderedIterator : public DimensionGroup::Iterator
	{
	public:
		DGFilterOrderedIterator(Filter *f_to_iterate, PackOrderer *po) : fi(f_to_iterate, po), f(f_to_iterate) { valid = !f->IsEmpty(); }
		DGFilterOrderedIterator(const Iterator& sec);

		virtual void operator++()					{ assert(valid); ++fi; valid = fi.IsValid(); }
		virtual void Rewind()						{ fi.Rewind(); valid = fi.IsValid(); }
		virtual bool NextInsidePack()				{ bool r = fi.NextInsidePack(); valid = fi.IsValid(); return r; }
		virtual _int64 GetPackSizeLeft() 			{ return fi.GetPackSizeLeft(); }
		virtual bool WholePack(int dim)				{ return f->IsFull(fi.GetCurrPack()); }
		virtual bool InsideOnePack()				{ return fi.InsideOnePack(); }
		virtual bool NullsExist(int dim)			{ return false; }
		virtual void NextPackrow()					{ assert(valid); fi.NextPack(); valid = fi.IsValid(); }
		virtual int GetCurPackrow(int dim)			{ return fi.GetCurrPack(); }
		virtual _int64 GetCurPos(int dim)			{ return (*fi); }
		virtual int GetNextPackrow(int dim, int ahead)	{ return fi.Lookahead(ahead); }
		virtual bool BarrierAfterPackrow()			{ return (fi.NaturallyOrdered() == false); }
		// Updating
		virtual bool InternallyUpdatable()		{ return true; }
		virtual void ResetCurrent()				{ fi.ResetDelayed(); }
		virtual void ResetCurrentPackrow()		{ fi.ResetCurrentPackrow(); }
		virtual void CommitUpdate()				{ f->Commit(); }
		virtual void SetNoPacksToGo(int n)		{ fi.SetNoPacksToGo(n); }
		virtual void RewindToRow(_int64 n)		{ fi.RewindToRow(n); valid = fi.IsValid(); }
		virtual bool RewindToPack(int pack)		{ bool r = fi.RewindToPack(pack); valid = fi.IsValid(); return r; }
		virtual int NoOnesUncommited(uint pack)	{ return f->NoOnesUncommited(pack); }

		virtual bool Ordered()					{ return true; }		// check if it is an ordered iterator

	private:
		FilterOnesIteratorOrdered fi;
		// external pointer:
		Filter *f;
	};

	virtual DimensionGroup::Iterator* NewIterator(DimensionVector&);			// create a new iterator (to be deleted by user)
	virtual DimensionGroup::Iterator* NewOrderedIterator(DimensionVector&, PackOrderer *po);		// create a new ordered iterator, if possible
	virtual DimensionGroup::Iterator* CopyIterator(DimensionGroup::Iterator* s);

private:
	int base_dim;
	Filter *f;
};

/////////////////////////////////////////////////////////////////////////////////

class DimensionGroupMaterialized : public DimensionGroup
{
public:
	// NOTE: works also for "count only" (all t[i] are NULL, only no_obj set)
	DimensionGroupMaterialized(DimensionVector &dims);
	virtual ~DimensionGroupMaterialized();
	virtual DimensionGroup* Clone(bool shallow);

	void NewDimensionContent(int dim, IndexTable *tnew, bool nulls);		// tnew will be added (as a pointer to be deleted by destructor) on a dimension dim
	void SetNoObj(_int64 _no_obj)										{ no_obj = _no_obj; }
	virtual bool DimUsed(int dim)										{ return dims_used[dim]; }
	virtual bool DimEnabled(int dim)									{ return (t[dim] != NULL); }
	virtual bool NullsPossible(int dim)									{ return nulls_possible[dim]; }
	virtual void Empty();

	virtual void FillCurrentPos(DimensionGroup::Iterator *it, _int64 *cur_pos, int *cur_pack, DimensionVector &dims);

	virtual void Lock(int dim, int n = 1)								{ if(t[dim]) for(int i = 0; i < n; i++) t[dim]->Lock(); }
	virtual void Unlock(int dim)										{ if(t[dim]) t[dim]->Unlock(); }
	virtual int NoLocks(int dim)										{ return (t[dim] ? t[dim]->NoLocks() : 0); }
	virtual bool IsThreadSafe()											{ return true; }		// BarrierAfterPackrow() must be used for parallel execution
	virtual bool IsOrderable()											{ return false; }

	////////////////////////////////////////////////////////////////////
	class DGMaterializedIterator : public DimensionGroup::Iterator
	{
	public:
		// NOTE: works also for "count only" (all t[i] are NULL)
		DGMaterializedIterator(_int64 _no_obj, DimensionVector& dims, IndexTable **_t, bool *nulls);
		DGMaterializedIterator(const Iterator& sec);
		~DGMaterializedIterator();

		_int64 CurPos()								{ return cur_pos; }

		virtual void operator++()					{ cur_pos++; pack_size_left--; if(pack_size_left == 0) InitPackrow(); }
		virtual void Rewind();
		virtual bool NextInsidePack();				
		virtual _int64 GetPackSizeLeft()			{ return pack_size_left; }
		virtual bool WholePack(int dim)				{ return false; }
		virtual bool InsideOnePack()				{ return inside_one_pack; }
		virtual bool NullsExist(int dim)			{ return nulls_found[dim]; }
		virtual void NextPackrow()					{ cur_pos += pack_size_left; InitPackrow(); }
		virtual _int64 GetCurPos(int dim)			{ _int64 v = t[dim]->Get64(cur_pos); return (v == 0 ? NULL_VALUE_64 : v - 1); }
		virtual int GetCurPackrow(int dim)			{ return cur_pack[dim]; }
		virtual int GetNextPackrow(int dim, int ahead);
		virtual bool BarrierAfterPackrow();
	private:
		void InitPackrow();
		void FindPackEnd(int dim);

		_int64 pack_size_left;
		_int64 cur_pack_start;
		_int64 cur_pos;
		_int64 no_obj;
		int no_dims;
		bool inside_one_pack;	// all dimensions contain values from one packrow only (determined on InitPackrow())

		bool *one_packrow;		// dimensions containing values from one packrow only (no nulls). Approximation: false => still may be one packrow
		bool *nulls_found;		// in these dimensions nulls were found in this pack
		int *cur_pack;			// the number of the current pack (must be stored separately because there may be nulls on the beginning of pack
		_int64 *next_pack;		// beginning of the next pack (or no_obj if there is no other pack)
		_int64 *ahead1, *ahead2, *ahead3;		// beginning of the three next packs after next_pack, or -1 if not determined properly

		// External pointers:
		IndexTable **t;			// table is local, pointers are from DimensionGroupMaterialized, NULL for not used (not iterated)
		bool *nulls_possible;
	};
	virtual DimensionGroup::Iterator* NewIterator(DimensionVector&);			// create a new iterator (to be deleted by user)
	virtual DimensionGroup::Iterator* CopyIterator(DimensionGroup::Iterator* s)		{ return new DGMaterializedIterator(*s); }

private:
	DimensionVector dims_used;
	int no_dims;				// number of all possible dimensions (or just the last used one)
	IndexTable **t;				// NULL for not used (natural numbering)
	bool *nulls_possible;
};

#endif

