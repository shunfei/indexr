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

#ifndef _MEMORY_BLOCK_H_E5FF7A96_7174_421b_9FE2_043859D08D15
#define _MEMORY_BLOCK_H_E5FF7A96_7174_421b_9FE2_043859D08D15

#include <cstddef>

#ifdef __GNUC__
enum BLOCK_TYPE
{
	BLOCK_FREE,
	BLOCK_COMPRESSED,
	BLOCK_UNCOMPRESSED,
	BLOCK_TEMPORARY,
	BLOCK_FIXED //Used in rc_realloc when pointer != NULL
};
#else
enum BLOCK_TYPE: char
{
	BLOCK_FREE,
	BLOCK_COMPRESSED,
	BLOCK_UNCOMPRESSED,
	BLOCK_TEMPORARY,
	BLOCK_FIXED //Used in rc_realloc when pointer != NULL
};
#endif

#endif
