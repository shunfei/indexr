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

#ifndef DIMENSIONGROUPVIRTUAL_H_INCLUDED
#define DIMENSIONGROUPVIRTUAL_H_INCLUDED

#include "DimensionGroup.h"

///////////////////////////////////////////////////////////////////////////////////

class DimensionGroupVirtual : public DimensionGroup
{
public:
	DimensionGroupVirtual(DimensionVector &dims, int base_dim, Filter *f_source, int copy_mode = 0);		// copy_mode: 0 - copy filter, 1 - ShallowCopy filter, 2 - grab pointer
	virtual ~DimensionGroupVirtual();
	virtual DimensionGroup* Clone(bool shallow);

	virtual void FillCurrentPos(DimensionGroup::Iterator *it, _int64 *cur_pos, int *cur_pack, DimensionVector &dims);	
	virtual void UpdateNoTuples();
	virtual Filter* GetFilter(int dim)	const										{ return (base_dim == dim || dim == -1 ? f : NULL); }
	// For this type of filter: dim == -1 means the only existing one
	// Note: GetUpdatableFilter remains default (NULL)
	virtual bool DimUsed(int dim)													{ return (base_dim == dim || dims_used[dim]); }
	virtual bool DimEnabled(int dim)												{ return (base_dim == dim || t[dim] != NULL); }
	virtual bool NullsPossible(int dim)												{ return nulls_possible[dim]; }
	virtual void Empty();
	void NewDimensionContent(int dim, IndexTable *tnew, bool nulls);		// tnew will be added (as a pointer to be deleted by destructor) on a dimension dim

	virtual void Lock(int dim, int n = 1)								
	{ 
		if(t[dim]) { 
			for(int i = 0; i < n; i++) 
				t[dim]->Lock(); 
		} else if(base_dim == dim) 
			locks += n; 
	}
	virtual void Unlock(int dim)										
	{ 
		if(t[dim]) 
			t[dim]->Unlock(); 
		else if(base_dim == dim) 
			locks--; 
	}
	virtual int NoLocks(int dim)										
	{ 
		if(t[dim])
			return t[dim]->NoLocks();
		if(base_dim == dim)
			return locks; 
		return 0;
	}
	virtual bool IsThreadSafe()			{ return true; }
	virtual bool IsOrderable();

	////////////////////////////////////////////////////////////////////
	class DGVirtualIterator : public DimensionGroup::Iterator
	{
	public:
		DGVirtualIterator(Filter *f_to_iterate, int b_dim, DimensionVector &dims, IndexTable **tt);
		DGVirtualIterator(const Iterator& sec);
		~DGVirtualIterator();

		virtual void operator++();
		virtual void Rewind()						{ fi.Rewind(); valid = fi.IsValid(); dim_pos = 0; cur_pack_start = 0; }
		virtual _int64 GetPackSizeLeft() 			{ return fi.GetPackSizeLeft(); }
		virtual bool NextInsidePack();
		virtual bool WholePack(int dim)				{ return (dim == base_dim ? f->IsFull(fi.GetCurrPack()) : false); }
		virtual bool InsideOnePack()				{ return fi.InsideOnePack(); }
		virtual bool NullsExist(int dim)			{ return nulls_found[dim]; }
		virtual void NextPackrow();
		int GetCurPackrow(int dim)					{ return (dim == base_dim ? fi.GetCurrPack() : cur_pack[dim]); }
		_int64 GetCurPos(int dim);
		virtual int GetNextPackrow(int dim, int ahead)	{ return (dim == base_dim ? fi.Lookahead(ahead) : cur_pack[dim]); }
		virtual bool BarrierAfterPackrow();

		virtual bool Ordered()						{ return false; }		// check if it is an ordered iterator

	private:
		FilterOnesIterator fi;
		_int64 dim_pos;
		_int64 no_obj;
		_int64 cur_pack_start;	// for dim
		int no_dims;
		int base_dim;
		bool *nulls_found;		// in these dimensions nulls were found in this pack
		int *cur_pack;			// the number of the current pack (must be stored separately because there may be nulls on the beginning of pack
		// external pointers:
		Filter *f;
		IndexTable **t;
	};
	////////////////////////////////////////////////////////////////////
	class DGVirtualOrderedIterator : public DimensionGroup::Iterator
	{
	public:
		DGVirtualOrderedIterator(Filter *f_to_iterate, int b_dim, DimensionVector &dims, _int64 *ppos, IndexTable **tt, PackOrderer *po);
		DGVirtualOrderedIterator(const Iterator& sec);
		~DGVirtualOrderedIterator();

		virtual void operator++();
		virtual void Rewind();
		virtual _int64 GetPackSizeLeft() 			{ return fi.GetPackSizeLeft(); }
		virtual bool NextInsidePack();
		virtual bool WholePack(int dim)				{ return (dim == base_dim ? f->IsFull(fi.GetCurrPack()) : false); }
		virtual bool InsideOnePack()				{ return fi.InsideOnePack(); }
		virtual bool NullsExist(int dim)			{ return nulls_found[dim]; }
		virtual void NextPackrow();
		int GetCurPackrow(int dim)					{ return (dim == base_dim ? fi.GetCurrPack() : cur_pack[dim]); }
		_int64 GetCurPos(int dim);
		virtual int GetNextPackrow(int dim, int ahead)	{ return (dim == base_dim ? fi.Lookahead(ahead) : cur_pack[dim]); }
		virtual bool BarrierAfterPackrow()			{ return false; }

		virtual bool Ordered()						{ return true; }		// check if it is an ordered iterator

	private:
		FilterOnesIteratorOrdered fi;
		_int64 dim_pos;
		_int64 cur_pack_start;	// for dim
		int no_dims;
		int base_dim;
		bool *nulls_found;		// in these dimensions nulls were found in this pack
		int *cur_pack;			// the number of the current pack (must be stored separately because there may be nulls on the beginning of pack
		// external pointers:
		_int64 *pack_pos;
		Filter *f;
		IndexTable **t;
	};

	virtual DimensionGroup::Iterator* NewIterator(DimensionVector&);			// create a new iterator (to be deleted by user)
	virtual DimensionGroup::Iterator* NewOrderedIterator(DimensionVector&, PackOrderer *po);		// create a new ordered iterator, if possible
	virtual DimensionGroup::Iterator* CopyIterator(DimensionGroup::Iterator* s);

private:
	int base_dim;				// dim for filter
	Filter *f;
	DimensionVector dims_used;
	int no_dims;				// number of all possible dimensions (or just the last used one)
	IndexTable **t;				// NULL for not used (natural numbering) and for base_dim
	_int64 *pack_pos;			// table of size = number of packs in base_dim; the first position of a given pack in all IndexTables
	bool *nulls_possible;
};

#endif

