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

#include "CompiledQuery.h"
#include "PhysicalColumn.h"

using namespace std;

PhysicalColumn::~PhysicalColumn()
{
	if (name)
		delete [] name;
	if (desc)
		delete [] desc;
}

void PhysicalColumn::SetName(const char *a_name)
{
	if (name)
		delete [] name;

	if(a_name == NULL)
		name = NULL;
	else {
		size_t const name_ct = strlen(a_name) + 1;
		name = new char [name_ct];
		strcpy(name, a_name);
	}
}


void PhysicalColumn::SetDescription(const char *a_desc)
{
	if (desc)
		delete [] desc;

	if(a_desc == NULL)
		desc = NULL;
	else {
		size_t const desc_ct = strlen(a_desc) + 1;
		desc = new char [desc_ct];
		strcpy(desc, a_desc);
	}
}



