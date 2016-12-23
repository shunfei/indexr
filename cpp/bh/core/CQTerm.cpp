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

#include "CQTerm.h"
#include "bintools.h"
#include "RCAttr.h"
#include "system/TextUtils.h"
#include "common/CommonDefinitions.h"
#include "ValueSet.h"
#include "types/ValueParserForText.h"
#include "edition/vc/VirtualColumn.h"
#include "core/QueryOperator.h"

CQTerm::CQTerm() :
	type(RC_UNKNOWN), vc(NULL), vc_id(NULL_VALUE_32), is_vc_owner(false)
{
}

CQTerm::CQTerm(int v) :
	type(RC_UNKNOWN), vc(NULL), vc_id(v), is_vc_owner(false)
{
}

CQTerm::CQTerm(const CQTerm & t)
{
	type = t.type;
	vc_id = t.vc_id;
	vc = t.vc;  // deep copy is not necessary, as vc are managed by Query
	is_vc_owner = false;
}

CQTerm::~CQTerm()
{
	if(is_vc_owner)
		delete vc;
}


CQTerm & CQTerm::operator=(const CQTerm &t)
{
	if(this == &t)
		return *this;
	type = t.type;
	vc_id = t.vc_id;

	vc = t.vc;  // deep copy is not necessary, as vc are managed by Query
	is_vc_owner = false;
	return *this;
}

//obsolete
bool CQTerm::operator==(const CQTerm &t) const
{
	if(type == t.type && vc_id == t.vc_id && vc == t.vc)
		return true;
	else
		return false;
}

char * CQTerm::ToString(char *buf, int tab_id) const
{
	if(vc_id != NULL_VALUE_32 && tab_id != 0)
		sprintf(buf + strlen(buf), "VC:%d.%d ", tab_id, vc_id);
	else if(vc_id != NULL_VALUE_32)
		sprintf(buf + strlen(buf), "VC:%d ", vc_id);

	int long len = (int)strlen(buf);
	if((len > 0) && (buf[len - 1] == ' '))
		buf[len - 1] = '\0';

	return buf;
}

char * CQTerm::ToString(char p_buf[], size_t buf_ct, int tab_id) const
{
	size_t buf_len = strlen(p_buf);
	size_t rem_buf = buf_ct - ( buf_len + 1 );
	char *buf = p_buf + buf_len;
	if(IsNull())
		snprintf(buf, rem_buf, "<null> ");
	if(vc_id != NULL_VALUE_32) {
		if(tab_id == 0) {
			char val_buf[100];
			val_buf[0] = '\0';
			if(vc)
				vc->ToString(val_buf, 99);
			if(val_buf[0] == '\0')
				snprintf(buf, rem_buf, "VC:%d ", vc_id);
			else
				snprintf(buf, rem_buf, "VC:%d(%s) ", vc_id, val_buf);
		} else {
			char val_buf[100];
			val_buf[0] = '\0';
			if(vc)
				vc->ToString(val_buf, 99);
			if(val_buf[0] == '\0')
				snprintf(buf, rem_buf, "VC:%d.%d ", tab_id, vc_id);
			else
				snprintf(buf, rem_buf, "VC:%d.%d(%s) ", tab_id, vc_id, val_buf);
		} 
	}
	if(buf[strlen(buf) - 1] == ' ')
		buf[strlen(buf) - 1] = '\0';
	return p_buf;
}

