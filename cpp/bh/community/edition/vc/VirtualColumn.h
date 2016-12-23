/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * VirtualColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in VirtualColumn object may not exist physically, they can be computed on demand.
 *
 */


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


#ifndef VIRTUALCOLUMN_H_INCLUDED
#define VIRTUALCOLUMN_H_INCLUDED

#include "vc/VirtualColumnBase.h"
#include "core/PackGuardian.h"

class MIIterator;

/*! \brief A column defined by an expression (including a subquery) or encapsulating a PhysicalColumn
 * VirtualColumn is associated with an MultiIndex object and cannot exist without it.
 * Values contained in VirtualColumn object may not exist physically, they can be computed on demand.
 *
 */

class VirtualColumn : public VirtualColumnBase
{
public:
	VirtualColumn(ColumnType const& col_type, MultiIndex* mind) : VirtualColumnBase(col_type, mind), pguard(this) {}
	VirtualColumn(VirtualColumn const& vc) : VirtualColumnBase(vc), pguard(this) {}
	~VirtualColumn();
	virtual void AssignFromACopy(const VirtualColumn* vc);
	void LockSourcePacks(const MIIterator& mit);
	void UnlockSourcePacks();

	void InitPrefetching(MIIterator& mit, int depth = -1) {}
	void SetPrefetchFilter(RSValue* r_filter = NULL) {}
	void StopPrefetching() {}

protected:
	VCPackGuardian pguard;		//IEE version

};

#endif /* not VIRTUALCOLUMN_H_INCLUDED */


