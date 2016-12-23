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

#include "edition/vc/VirtualColumn.h"


VirtualColumn::~VirtualColumn()
	{
		pguard.UnlockAll();
	//	for(ComplexTerm::SetOfVars::iterator iter = vars.begin(); iter != vars.end(); iter++) {
	//		delete var_buf[*iter] ;
	//	}
	}

void VirtualColumn::AssignFromACopy(const VirtualColumn*v)
{
	assert(this!=v);
	//VirtualColumn* vc_casted = dynamic_cast<VirtualColumn*>(v);
	//assert(vc_casted);
	pguard.UnlockAll();
	VirtualColumnBase::AssignFromACopy(v);
}


void VirtualColumn::LockSourcePacks(const MIIterator& mit)
{
	pguard.LockPackrow(mit);		// TODO: do something with a return code, when it becomes nontrivial
}

void VirtualColumn::UnlockSourcePacks()
{
	pguard.UnlockAll();
}
