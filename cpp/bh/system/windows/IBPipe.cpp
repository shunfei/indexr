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

#ifdef _MSC_VER

#include <boost/lexical_cast.hpp>
#include <winsock2.h>
#include <Aclapi.h>

#include "system/RCSystem.h"
#include "system/IBPipe.h"

using namespace std;
using namespace boost;

IBPipe::IBPipe(bool unlink_on_close, bool openOnClose_)
: _path(), _mode(MODE::INVALID), _timeout( 0 ),
	curpipe(INVALID_HANDLE_VALUE), _overlapped(),
	open_on_close( openOnClose_ )
{
	_overlapped.hEvent = ::CreateEvent(0, true, false, 0);
	if (!_overlapped.hEvent)
		ThrowError( GetLastError() );
}

IBPipe::~IBPipe() 
{
	// TODO Auto-generated destructor stub
	if ( IsOpen() )
		close();
	if (!_overlapped.hEvent)
		CloseHandle(_overlapped.hEvent);
}

void IBPipe::open( std::string const& path_, MODE::mode_t mode_, int long timeout_ )
{
	_mode = mode_;
	_path = path_;
	_timeout = timeout_;
}

void IBPipe::open_force()
{
	if ( ! IsOpen() )
		delayed_open();
}

void IBPipe::retry_open_force()
{
	BHASSERT( (_mode & MODE::WRITE) && (_mode & MODE::SERVER), "Internal error: IBPipe::retry_open_force() used for MODE::READ or MODE::CLIENT");
	int up( ::WaitForSingleObject( _overlapped.hEvent, _timeout ) );
	if ( up != WAIT_OBJECT_0 )
		ThrowError( "Timeout on pipe. No signal from pipe client." );
}

void WaitForPipe(LargeBuffer::Params* params)
{	
	while(params->pipe == INVALID_HANDLE_VALUE && !params->finish)
		params->pipe = CreateFileA(params->path, params->dw_desired_access, 0, NULL , OPEN_EXISTING, 0, NULL);
}

void IBPipe::delayed_open() {
	if ( _mode & MODE::READ )
		OpenReadOnly( _path, _mode & MODE::SERVER, _timeout );
	else {
		if (_mode & MODE::SERVER) {
			OpenForWriting( _path );
			int err( ConnectNamedPipe( curpipe, &_overlapped ) );
			int up( ::WaitForSingleObject( _overlapped.hEvent, _timeout ) );
			if ( up != WAIT_OBJECT_0 )
				ThrowError( "Timeout on pipe. No signal from pipe client." );
		}
		else {
			LargeBuffer::Params p;
			size_t path_ct = strlen(_path.c_str()) + 1; 
			p.path = new char [path_ct];
			p.dw_desired_access = GENERIC_WRITE;
			strcpy_s(p.path, path_ct, _path.c_str());
			p.pipe = curpipe;
			p.finish = false;
			unsigned long threadId;
			HANDLE thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)WaitForPipe, &p,	0, &threadId);      // returns thread ID 
			if(WaitForSingleObject(thread, _timeout) == WAIT_OBJECT_0) {
				curpipe = p.pipe;
				CloseHandle(thread);
				delete[] p.path;
			} else {
				p.finish = true;
				WaitForSingleObject(thread, INFINITE);
				CloseHandle(thread);
				if(p.pipe != INVALID_HANDLE_VALUE)
					CloseHandle(p.pipe);
				delete[] p.path;
				curpipe = INVALID_HANDLE_VALUE;					
				ThrowError( "Unable to open named pipe." );
			}		
		}
	}
}

void IBPipe::close() {
	Close();
}

bool IBPipe::operator !() const {
	return ( ! IsOpen() );
}

int long IBPipe::read( void* const data_, int long size_ ) {
	if ( ! IsOpen() )
		delayed_open();
	return ( Read( data_, size_ ) );
}

int long IBPipe::write( void const* const data_, int long size_ ) {
	if ( ! IsOpen() )
		delayed_open();
	DWORD nWritten( 0 );
	if ( ! ::WriteFile( curpipe, data_, size_, &nWritten, &_overlapped ) && GetLastError() != ERROR_IO_PENDING )
		ThrowError( "WriteFile failed" );
	if ( ! ::GetOverlappedResult( curpipe, &_overlapped, &nWritten, true ) )
		ThrowError( "GetOverlappedResult failed" );
	return ( nWritten );
}

int IBPipe::OpenForWriting(std::string const& filename)
{
		name = filename;
		curpipe = CreateNamedPipe(filename.c_str(),
				PIPE_ACCESS_OUTBOUND | FILE_FLAG_OVERLAPPED,
				PIPE_TYPE_BYTE | PIPE_WAIT,
				1,
				PIPE_BUFFER_SIZE,
				0,
				PIPE_TIMEOUT,
				0);
		if(curpipe == INVALID_HANDLE_VALUE)
			ThrowError( "Cannot create named pipe!" );
		return 1;
}

bool IBPipe::IsOpen() const
{
	return curpipe != INVALID_HANDLE_VALUE;
}

int IBPipe::Close()
{
	if(curpipe != INVALID_HANDLE_VALUE) {
		FlushFileBuffers(curpipe);
		if(curpipe != INVALID_HANDLE_VALUE)
			CloseHandle(curpipe);
		curpipe = INVALID_HANDLE_VALUE;
		return 1;
	} else if ( open_on_close ) {
		delayed_open();
		CloseHandle(curpipe);
		curpipe = INVALID_HANDLE_VALUE;
	}
	return 0;
}

void WaitForPipeClient(HANDLE curpipe)
{	
	ConnectNamedPipe(curpipe, 0);
}

int IBPipe::OpenReadOnly(std::string const& filename, int create, uint64 timeout)
	throw(DatabaseRCException)
{	
	if(create) {
		curpipe = CreateNamedPipe(filename.c_str(),
			PIPE_ACCESS_INBOUND,
			PIPE_TYPE_BYTE | PIPE_WAIT,
			1,			
			0,
			PIPE_BUFFER_SIZE,
			DWORD(timeout),
			0);
		if(curpipe == INVALID_HANDLE_VALUE){					
			return 0;			
		} else {
			unsigned long threadId;
			HANDLE thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)WaitForPipeClient, (LPVOID) curpipe, 0, &threadId);
			if(WaitForSingleObject(thread, DWORD(timeout * 1000)) == WAIT_OBJECT_0) { 
				return 1;						
			} else {
				TerminateThread(thread, 0);	// warning C6258: Using TerminateThread does not allow proper thread clean up
				CloseHandle(thread);
				if(curpipe != INVALID_HANDLE_VALUE)
					CloseHandle(curpipe);
				curpipe = INVALID_HANDLE_VALUE;
				return 0;
			}
		}
	} else {
		LargeBuffer::Params p;
		size_t path_ct = strlen(filename.c_str()) + 1; 
		p.path = new char [path_ct];
		p.dw_desired_access = GENERIC_READ;
		strcpy_s(p.path, path_ct, filename.c_str());
		p.pipe = curpipe;
		p.finish = false;
		unsigned long threadId;
		HANDLE thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)WaitForPipe, &p,	0, &threadId);      // returns thread ID 
		if(WaitForSingleObject(thread, DWORD(timeout * 1000)) == WAIT_OBJECT_0) {
			curpipe = p.pipe;
			CloseHandle(thread);
			if (p.pipe == INVALID_HANDLE_VALUE)
				ThrowError("Could not open pipe");
			delete[] p.path;
			SetEvent(_overlapped.hEvent);
			return 2;
		} else {
			p.finish = true;
			WaitForSingleObject(thread, INFINITE);
			CloseHandle(thread);
			if(p.pipe != INVALID_HANDLE_VALUE)
				CloseHandle(p.pipe);
			else
				ThrowError("Could not open pipe due to timeout.");
			delete[] p.path;
			curpipe = INVALID_HANDLE_VALUE;					
			return 0;
		}		
	}
	return 0;
};

int IBPipe::OpenReadWrite(std::string const& filename, int create, uint64 timeout) throw(DatabaseRCException)
{
       return 0;
};

int IBPipe::OpenCreateEmpty(std::string const& filename, int create, uint64 timeout)
	throw(DatabaseRCException)
{
	name = filename;	
	if(create) {
		curpipe = CreateNamedPipe(filename.c_str(),
			PIPE_ACCESS_OUTBOUND,			
			PIPE_TYPE_BYTE | PIPE_WAIT,					
			1,	
			PIPE_BUFFER_SIZE,					
			0,					
			DWORD(timeout),	
			0);			

		if(curpipe == INVALID_HANDLE_VALUE)	{			
			return 0;
		} else {
			unsigned long threadId;
			HANDLE thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)WaitForPipeClient,(LPVOID) curpipe, 0, &threadId);
			if(WaitForSingleObject(thread, DWORD(timeout * 1000)) == WAIT_OBJECT_0) { 
				return 1;
				CloseHandle(thread);
			} else {
				TerminateThread(thread, 0); // warning C6258: Using TerminateThread does not allow proper thread clean up
				CloseHandle(thread);
				if(curpipe != INVALID_HANDLE_VALUE)
					CloseHandle(curpipe);
				curpipe = INVALID_HANDLE_VALUE;
				return 0;
			}
		}
	} else {
		LargeBuffer::Params p;
		size_t path_ct = strlen(filename.c_str()) + 1; 
		p.path = new char [path_ct];
		strcpy_s(p.path, path_ct, filename.c_str());
		p.pipe = curpipe;
		p.finish = false;
		p.dw_desired_access = GENERIC_WRITE;
		unsigned long threadId;
		HANDLE thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)WaitForPipe, &p,	0, &threadId);      // returns thread ID 
		if(WaitForSingleObject(thread, DWORD(timeout * 1000)) == WAIT_OBJECT_0) {
			curpipe = p.pipe;
			CloseHandle(thread);
			delete[] p.path;
			return 2;
		} else {
			p.finish = true;
			WaitForSingleObject(thread, INFINITE);
			CloseHandle(thread);
			if(p.pipe != INVALID_HANDLE_VALUE)
				CloseHandle(p.pipe);
			delete[] p.path;
			curpipe = INVALID_HANDLE_VALUE;								
			return 0;
		}
	}
	return 0;
}


int IBPipe::OpenWriteAppend(std::string const& filename, int create, uint64 timeout)
	throw(DatabaseRCException)
{	
	return OpenCreateEmpty(filename, create, timeout);
}

uint IBPipe::Read(void* buf, uint count)
	throw(DatabaseRCException)
{
	unsigned long no_read_bytes = 0;	
//	int ret = ReadFile(curpipe, buf, count, &no_read_bytes, 0);
	ReadFile(curpipe, buf, count, &no_read_bytes, 0);

	//if ( ret == 0)
	//	ThrowError(errno);
	return no_read_bytes;
}

/*uint IBPipe::Write(const void* buf, uint count)
throw(DatabaseRCException)
{
	return write(buf,count);
}*/

uint IBPipe::WriteExact(const void* buf, uint count)
	throw(DatabaseRCException)
{ 
	int long written_bytes = write(buf, count);
	if ( written_bytes != count)
		ThrowError("write mismatch: write count and expected count did not match");
	return written_bytes;
}

int IBPipe::GetFileId()
{
	return -1;
}

int IBPipe::WaitForData(int timeout)
{
	int hasData( 0 );
	int waited( 0 );
	char buf[2];
	do {
		DWORD nAvail( 0 );
		if ( ::PeekNamedPipe( curpipe, buf, 1, NULL, &nAvail, NULL ) ) {
			if ( nAvail > 0 ) {
				hasData = 1;
				break;
			}
		} else {
			hasData = -1;
			break;
		}
		::Sleep( 10 );
		waited += 10;
	} while ( waited < timeout );
	return hasData;
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

	if (count && current_read < count)
		ThrowError( string( "read mismatch: read count and expected count did not match: " ) + lexical_cast<string>( current_read ) + " vs " + lexical_cast<string>( count ) );
	return current_read;
}

bool IBPipe::HasMoreData()
{
	return WaitForData(_timeout) > 0;
}

#endif
