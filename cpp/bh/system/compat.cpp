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

#ifdef __GNUC__
#include <cstdio>
#include <sys/fcntl.h>
#include <dirent.h>
#include "IBFileSystem.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

extern "C"
DIR* __wrap_opendir( char const* const path )
{
#ifdef DEBUG
	printf( "=== wrapped opendir() called ===\n" );
#endif /* DEBUG */

/* MPOPOV  SOLPORT */
#ifdef SOLARIS
	return ( fdopendir( open( path, O_RDONLY | O_NONBLOCK ) ) );
#else
	return ( fdopendir( open( path, O_RDONLY | O_NONBLOCK | O_DIRECTORY ) ) );
#endif
}

extern "C"
int __real_unlink( char const* );

extern "C"
int __wrap_unlink( char const* path )
{
#ifdef DEBUG
	printf( "=== wrapped unlink() called ===\n" );
#endif /* DEBUG */
#ifdef SOLARIS
	int dfp = open( path, O_RDONLY | O_NONBLOCK );
#else
	int dfp = open( path, O_RDONLY | O_NONBLOCK | O_DIRECTORY );
#endif
	int err = 0;
	if ( dfp >= 0 ) {
		try	{
			DeleteDirectory( path );
		} catch ( ... )	{
			close( dfp );
			err = -1;
		}
	}	else
		err = __real_unlink( path );
	return ( err );
}
#endif

