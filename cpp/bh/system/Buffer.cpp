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

#include "Buffer.h"

int Buffer::Write(uchar c)
{
	char* ptr = BufAppend(1);
	if (ptr) {
		*ptr = c;
		return 1;
	}
	else
		return -1;
}


int Buffer::WriteIfNonzero(uchar c)
{
	if(c != 0)
		return Write(c);
	else
		return -1;
}
