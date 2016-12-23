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

#ifndef _IBFile_H_
#define _IBFile_H_

#include "system/RCException.h"
#include "system/IBFileSystem.h"
#include "system/ib_system.h"
#include "system/IBStream.h"
#ifdef __GNUC__
#include <sys/file.h>
#else
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>
#include <stdlib.h>
#endif

#ifdef __GNUC__

#define ib_read         read
#define ib_open         open
#define ib_write        write
#define ib_close        close
#define ib_seek         lseek
#define ib_tell(fd)     lseek(fd, 0, SEEK_CUR);
typedef mode_t          ib_pmode;
#define O_BINARY        0
#define O_SEQUENTIAL    0
#define O_RANDOM        0

typedef off_t           ib_off_t;
#else

typedef __int64         ib_off_t;

// I/O functions
#define ib_open         _open
#define ib_read         _read
#define ib_write        _write
#define ib_seek         _lseeki64
#define ib_tell(fd)     _telli64(fd)
#define ib_close        _close

// I/O flags
#define O_CREAT         _O_CREAT
#define O_RDONLY        _O_RDONLY
#define O_WRONLY        _O_WRONLY
#define O_RDWR          _O_RDWR
#define O_TRUNC         _O_TRUNC
#define O_APPEND        _O_APPEND

#define O_BINARY        _O_BINARY
#define O_TEXT          _O_TEXT

// I/O access permission
#define S_IRUSR         _S_IREAD
#define S_IWUSR         _S_IWRITE
#define S_IXUSR         0
#define S_IRGRP         0
#define S_IWGRP         0
#define S_IXGRP         0
// Already defined in MySQL
//#define S_IROTH         _S_IREAD
#define S_IWOTH         0
#define S_IXOTH         0

// I/O access method
#define O_SEQUENTIAL    _O_SEQUENTIAL
#define O_RANDOM        _O_RANDOM

// May be required or not
#define O_LARGEFILE     0
typedef unsigned int    ib_pmode;
#endif

#define ib_umask        S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP
#define IBFILE_OPEN_NO_TIMEOUT UINT64_MAX
#define IBFILE_OPEN_SLEEP_STEP 100

/* A portable wrapper class for low level file I/O.
  Note - Most of the functions throws exception if it fails. So caller must handle
  exception appropriately.
*/


class IBFile : public IBStream {

	int          fd;
	//std::string  name;

	// Throws error as exception
	//void ThrowError(int errnum) throw(DatabaseRCException);
	//void ThrowError(std::string serror)	throw(DatabaseRCException);

public:

	IBFile() { fd = -1; };
	~IBFile() { if (fd != -1) Close(); }
	int	GetFileId() { return fd; }

	int Open(std::string const& file, int flags, ib_pmode mode, uint64 timeout = IBFILE_OPEN_NO_TIMEOUT) throw(DatabaseRCException); //timeout in miniseconds
	uint Read(void* buf, uint count) throw(DatabaseRCException);
	//uint Write(const void* buf, uint count) throw(DatabaseRCException);
	ib_off_t Seek(ib_off_t pos, int whence) throw(DatabaseRCException);
	ib_off_t Tell() throw(DatabaseRCException);
	int Flush();


	// Variants for Open call
	// O_CREAT|O_RDWR|O_LARGEFILE|O_BINARY
	int OpenCreate(std::string const& file) throw(DatabaseRCException);
	// O_CREAT|O_EXCL|O_RDWR|O_LARGEFILE|O_BINARY
	int OpenCreateNotExists(std::string const& file) throw(DatabaseRCException);
	// O_CREAT|O_WRONLY|O_TRUNC|O_LARGEFILE|O_BINARY
	int OpenCreateEmpty(std::string const& file, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	// O_CREAT|O_RDONLY|O_LARGEFILE|O_BINARY
	int OpenCreateReadOnly(std::string const& file) throw(DatabaseRCException);
	// O_RDONLY|O_LARGEFILE|O_BINARY
	int OpenReadOnly(std::string const& file, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	// O_RDWR|O_LARGEFILE|O_BINARY
	int OpenReadWrite(std::string const& file, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	// O_WRONLY|O_LARGEFILE|O_APPEND|O_BINARY
	int OpenWriteAppend(std::string const& file, int create = 0, uint64 timeout = 0 ) throw(DatabaseRCException);
	int	OpenReadWriteWithThreadAffinity(std::string const& file) throw(DatabaseRCException);

	// Variant for Read and Write calls
	// Throws exception if can't read exact number of bytes
	uint ReadExact(void* buf, uint count) throw(DatabaseRCException);
	// Throws exception if can't read exact number of bytes
	uint WriteExact(const void* buf, uint count) throw(DatabaseRCException);

	bool IsOpen() const;
	int  Close();

	void MakeThreadAffinityName(std::string const&, std::string&) throw(DatabaseRCException);
	int  CloseWithThreadAffinity(std::string const& file) throw(DatabaseRCException);
};

#endif //_IBFile_H_
