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


#ifndef IBPIPE_H_
#define IBPIPE_H_

#include <string>
#ifdef _MSC_VER
#include <winsock2.h>
#include <windows.h>
#else
#include <fstream>
#endif

#include "system/IBStream.h"
#include "system/RCException.h"
#include "system/IBFileSystem.h"
#include "system/ib_system.h"
#include "system/LargeBuffer.h"

typedef unsigned int    ib_pmode;


class IBFile;

class IBPipe : public IBStream
{
public:
	struct MODE {
		typedef enum {
			READ = 1,
			WRITE = 2,
			CLIENT = 4,
			SERVER = 8,
			INVALID = 0
		} mode_t;
	};
	/*! \brief Open pipe.
	 * \param timeout_ - timeout in miliseconds.
	 */
	void open( std::string const&, MODE::mode_t, int long timeout_);
	void open_force();
	void retry_open_force();
	void close();
	bool operator!() const;
	int long read( void*, int long );
	int long write( void const* const, int long );
	bool IsOpen() const;
	int Open() 	throw(DatabaseRCException);
	int OpenForWriting(std::string const& filename);
	int Close();
	/*! \brief Open pipe read only.
	 * \param timeout - timeout in miliseconds.
	 */
	int OpenReadOnly(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	/*! \brief Open pipe for reading and writting.
	 * \param timeout - timeout in miliseconds.
	 */
	int OpenReadWrite(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	int OpenCreateEmpty(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	int OpenWriteAppend(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException);
	uint WriteExact(const void* buf, uint count) 	throw(DatabaseRCException);
	uint Read(void* buf, uint count) 	throw(DatabaseRCException);
	//uint Write(const void* buf, uint count) throw(DatabaseRCException);
	uint ReadExact(void* buf, uint count) throw(DatabaseRCException);

	int WaitForData(int timeout);
	int GetFileId();
	bool HasMoreData();
private:
	void delayed_open();
	std::string    _path;
	MODE::mode_t   _mode;
	int            _timeout;
	bool            open_on_close;
#ifdef _MSC_VER
	HANDLE          curpipe;
	OVERLAPPED     _overlapped;
#else
	std::auto_ptr<IBFile> file;
	bool unlink_on_close;
#endif

public:
	IBPipe(bool unlink_on_close = true, bool openOnclose_ = false );
	virtual ~IBPipe();
};

#endif /* IBPIPE_H_ */
