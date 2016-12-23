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

#include "common/CommonDefinitions.h"
#include "system/Channel.h"
#include "system/RCException.h"
#include "edition/local.h"
#include "PackGuardian.h"
#include "RCAttr.h"
#include "edition/vc/VirtualColumn.h"

using namespace std;

_int64 RCTableImpl::NoObj()
{
	for(int i=0;i<no_attr;i++) {
		LoadAttribute(i);
		if(a[i]) return (_int64)a[i]->NoObj();
	}
	return 0;
}

_int64 RCTableImpl::NoNulls(int attr)
{
	assert(attr <= no_attr);
	if(!a[attr])
		LoadAttribute(attr);
	return a[attr]->NoNulls();
}

void RCTableImpl::GetTable_S(RCBString& s, _int64 obj, int attr)
{
	assert(attr <= no_attr);
	if(!a[attr])
		LoadAttribute(attr);
	assert((_int64)obj <= a[attr]->NoObj());
	s = a[attr]->GetValueString(obj);
}

void RCTableImpl::GetTable_B(_int64 obj, int attr, int &size, char *val_buf)
{
	assert(attr <= no_attr);
	if(!a[attr]) LoadAttribute(attr);
	assert((_int64)obj <= a[attr]->NoObj());
	a[attr]->GetValueBin(obj, size, val_buf);
}

_int64 RCTableImpl::GetTable64(_int64 obj,int attr)
{
	assert(attr <= no_attr);
	if(!a[attr]) LoadAttribute(attr);
	assert((_int64)obj <= a[attr]->NoObj());
	return a[attr]->GetValueInt64(obj);
}

bool RCTableImpl::IsNull(_int64 obj,int attr)
{
	assert(attr <= no_attr);
	if(!a[attr]) LoadAttribute(attr);
	assert((_int64)obj <= a[attr]->NoObj());
	return ( a[attr]->IsNull(obj) ? true : false );
}

RCValueObject RCTableImpl::GetValue(_int64 obj,int attr, ConnectionInfo *conn)
{
	assert(attr <= no_attr);
	if(!a[attr]) LoadAttribute(attr);
	assert((_int64)obj <= a[attr]->NoObj());
	return a[attr]->GetValue(obj, false);
}

ushort RCTableImpl::MaxStringSize(int n_a, Filter *f)
{
	assert(n_a >= 0 && n_a <= no_attr);
	if(NoObj() == 0)
		return 1;
	LoadAttribute(n_a);
	if(a[n_a])
		return a[n_a]->MaxStringSize(f);
	return 1;
}

