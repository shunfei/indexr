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

#include <poll.h>

#include "common/bhassert.h"
#include "system/RCSystem.h"
#include "system/IBPipe.h"
#include "system/IBFile.h"

using namespace std;

#define max_attempt 10

IBPipe::IBPipe(bool unlink_on_close, bool openOnClose_)
	:	_path(), _mode( MODE::INVALID ), _timeout( 0 ), open_on_close(openOnClose_), file(new IBFile()), unlink_on_close(unlink_on_close)
{
}

IBPipe::~IBPipe() 
{
	if ( IsOpen() )
		close();
}

int IBPipe::OpenForWriting(std::string const& filename)
{
	_path = filename;
	file->Open(filename, O_WRONLY | O_BINARY, ib_umask);
	return 0;
}

bool IBPipe::IsOpen() const
{
	return file->IsOpen();
}

int IBPipe::Close()
{
	close();
	return 0;
}

uint IBPipe::WriteExact(const void* buf, uint count)
	throw(DatabaseRCException)
{ 
	uint current_write = write( buf, count );
	if (count && (int) current_write < count) {
		stringstream ss;
		ThrowError( static_cast<stringstream&>( ss << "IBPipe::WriteExact, write count mismatch: count and actually written count: " << count << " " << current_write).str() );
	}
	return current_write;
}

void IBPipe::open( std::string const& path_, MODE::mode_t mode_, int long timeout_ )
{
	if ( ( mode_ & MODE::SERVER ) && mkfifo( path_.c_str(), 0600 ) != 0 )
		throw RCException( path_ + ": " + ::strerror( errno ) );
	_path = path_;
	_mode = mode_;
	_timeout = timeout_;
}

void IBPipe::open_force()
{
	if ( ! IsOpen() )
		delayed_open();
}

void IBPipe::retry_open_force()
{
	if ( ! IsOpen() )
		delayed_open();
}

void IBPipe::close()
{
	if(open_on_close && !IsOpen())
		delayed_open();
	file->Close();
	if (unlink_on_close)
		unlink( _path.c_str() );
}

void IBPipe::delayed_open()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( ! IsOpen() );
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( _path != "" );

	try {
		if(_mode & MODE::WRITE)
			file->OpenWriteAppend(_path, 0, _timeout);
		else
			file->OpenReadOnly(_path);
	} catch (RCException&) {
		open_on_close = false;
		throw;
	}
}

bool IBPipe::operator !() const
{
	return ( ! IsOpen() );
}

int long IBPipe::read( void* const data_, int long size_ )
{
	if ( ! IsOpen() )
		delayed_open();
	int wtres = 1;
	if (_timeout)
		wtres = WaitForData(_timeout);
	if (wtres > 0)
		return ( file->Read(static_cast<char*>( data_ ), size_ ) );
	return wtres;
}

int long IBPipe::write( void const* const data_, int long size_ )
{
	if ( ! IsOpen() )
		delayed_open();
	return file->WriteExact( data_, size_);
}

/*uint IBPipe::Write(const void* buf, uint count)
	throw(DatabaseRCException)
{
	return write( buf, count );
}*/

int IBPipe::OpenReadOnly(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
	_path = filename;
	_timeout = timeout;
	file->Open(filename, O_RDONLY | O_BINARY, ib_umask);
	return 0;
}

int IBPipe::OpenReadWrite(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
	_path = filename;
	_timeout = timeout;
	file->Open(filename, O_RDWR | O_NONBLOCK, ib_umask);
	return 0;
};

int IBPipe::OpenCreateEmpty(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
	BHERROR("Not implemented!");
	return -1;
}

int IBPipe::OpenWriteAppend(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
	BHERROR("Not implemented!");
	return -1;
}

uint IBPipe::Read(void* buf, uint count) throw(DatabaseRCException)
{
	return ( read( buf, count ) );
}

uint IBPipe::ReadExact(void* buf, uint count)
	throw(DatabaseRCException)
{
	uint read_bytes = 0;
	uint current_read = 0;
	int attempt = 0;

	while (current_read < count /*&& attempt < max_attempt*/) {
		read_bytes = Read((char*)buf + current_read, count - current_read);
		if (read_bytes > 0)
			current_read += read_bytes;
		else {
			break;
		}
		attempt ++;
	}
	if (count && (int) current_read < count) {
		stringstream ss;
		ThrowError( static_cast<stringstream&>( ss << "read mismatch: read count and expected count did not match: " << count << " " << read_bytes ).str() );
	}
	return current_read;
}

int IBPipe::GetFileId()
{
	BHASSERT_WITH_NO_PERFORMANCE_IMPACT( IsOpen() );
	return file->GetFileId();
}

int IBPipe::WaitForData(int timeout)
{
	struct pollfd pfd;
	pfd.fd = GetFileId();
	pfd.events = POLLIN;
	int r = poll(&pfd, 1, timeout );
	if((pfd.revents & POLLHUP) && !(pfd.revents & POLLIN))
		return 0;
	return r;
}

bool IBPipe::HasMoreData()
{
	return (WaitForData(_timeout) > 0);
}
