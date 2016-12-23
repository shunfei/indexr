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

#ifndef _COMMON_H_30F2F326_684F_455a_A4BE_0F25E5FD5532
#define _COMMON_H_30F2F326_684F_455a_A4BE_0F25E5FD5532

#ifdef __GNUC__
enum HEAP_STATUS
{
	HEAP_SUCCESS,
	HEAP_OUT_OF_MEMORY,
	HEAP_CORRUPTED,
	HEAP_ERROR
};
#else
enum HEAP_STATUS:char
{
	HEAP_SUCCESS,
	HEAP_OUT_OF_MEMORY,
	HEAP_CORRUPTED,
	HEAP_ERROR
};
#endif

typedef void* MEM_HANDLE_MP ;

#define COMPRESSED_HEAP_RELEASE 104857600 //100MB
#define UNCOMPRESSED_HEAP_RELEASE 209715200 // 200MB  // 20971520 // 20MB

#define COMP_SIZE	419430400 // 400MB
#define UNCOMP_SIZE 1073741824 // 1 GB // 104857600 // 100MB

//Function pointers for memory release evaluation functions
typedef int *cmp_mem_release_eval_func (short, short);
typedef int *uncmp_mem_release_eval_func (short, short, short);

#define __STR2__(x) #x
#define __STR1__(x) __STR2__(x)
#define __LOC__ __FILE__ "("__STR1__(__LINE__)") : Info: "
#define __LOC2__ __FILE__ "("__STR1__(__LINE__)") : "

#endif

