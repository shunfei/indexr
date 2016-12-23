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

#include <boost/lexical_cast.hpp>

#include "IBFile.h"

#ifndef __BH_COMMUNITY__
#include "enterprise/edition/system/IBResourceMon.h"
#endif
#if defined( __FreeBSD__ )
static int const O_LARGEFILE = 0;
#endif

using namespace std;
using namespace boost;

int IBFile::Open(std::string const& file, int flags, ib_pmode mode, uint64 timeout)
	throw(DatabaseRCException)
{
	assert(file.length());
	name = file;

#ifdef _MSC_VER
	//BHASSERT_WITH_NO_PERFORMANCE_IMPACT(timeout == IBFILE_OPEN_NO_TIMEOUT);
	fd = ib_open(file.c_str(), flags, mode);
#else
	if(timeout == IBFILE_OPEN_NO_TIMEOUT) {
		fd = ib_open(file.c_str(), flags, mode);
	} else {
		uint64 elapsed = 0;
		do {
			fd = ib_open(file.c_str(), flags | O_NONBLOCK, mode);
			if(fd == -1) {
				SleepNow(IBFILE_OPEN_SLEEP_STEP);
				elapsed += IBFILE_OPEN_SLEEP_STEP;
			}
		} while (elapsed < timeout && fd == -1);
		if(fd != -1) {
			int arg = fcntl(fd, F_GETFL, 0);
			BHASSERT_WITH_NO_PERFORMANCE_IMPACT(arg & O_NONBLOCK);
			arg &= ~O_NONBLOCK;
			fcntl(fd, F_SETFL, arg);
		} else
			throw DatabaseRCException("Failed to open '" + file + "' file");
	}
#endif
	if(fd == -1)
		ThrowError(errno);

	return fd;
}

uint IBFile::Read(void* buf, uint count)
	throw(DatabaseRCException)
{
//	std::cerr <<  "read " << name << " from " << Tell() << " to " << count+ Tell() << std::endl;

	uint read_bytes;
	#ifdef __GNUC__
	errno = 0;					/* Linux doesn't reset this */
	#endif
	assert(fd != -1);
	read_bytes = (uint)ib_read(fd, buf, count);
	if ((int) read_bytes == -1)
		ThrowError(errno);
#ifndef __BH_COMMUNITY__
	IBResourceMon::readbytes += read_bytes;
	IBResourceMon::readcount ++;
#endif
	return read_bytes;
}

int IBFile::Flush()
{
	int ret;
	#ifdef __GNUC__

	#ifdef HAVE_FDATASYNC
        ret = fdatasync(fd);
	#else
        ret = fsync(fd);
	#endif	

	#else
        ret = _commit(fd);
	#endif
	return ret;

}

/*uint IBFile::Write(const void* buf, uint count)
	throw(DatabaseRCException)
{
//	std::cerr <<  "write " << name << " from " << Tell() << " to " << count+ Tell() << std::endl;
	uint writen_bytes;
	assert(fd != -1);
	writen_bytes = ib_write(fd, buf, count);
	if ((int) writen_bytes == -1)
		ThrowError(errno);
#ifndef __BH_COMMUNITY__
	IBResourceMon::writebytes += writen_bytes;
	IBResourceMon::writecount ++;
#endif
	return writen_bytes;
}*/

int IBFile::OpenCreate(std::string const& file)
	throw(DatabaseRCException)
{
	return Open(file, O_CREAT|O_RDWR|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenCreateNotExists(std::string const& file)
	throw(DatabaseRCException)
{
	return Open(file, O_CREAT|O_EXCL|O_RDWR|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenCreateEmpty(std::string const& file, int create, uint64 timeout)
	throw(DatabaseRCException)
{
	return Open(file, O_CREAT|O_RDWR|O_TRUNC|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenCreateReadOnly(std::string const& file)
	throw(DatabaseRCException)
{
	return Open(file, O_CREAT|O_RDONLY|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenReadOnly(std::string const& file, int create, uint64 timeout)
	throw(DatabaseRCException)
{
	return Open(file, O_RDONLY|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenReadWrite(std::string const& file, int create, uint64 timeout)
	throw(DatabaseRCException)
{
	return Open(file, O_RDWR|O_LARGEFILE|O_BINARY, ib_umask);
};

int IBFile::OpenWriteAppend(std::string const& file, int create, uint64 timeout)
	throw(DatabaseRCException)
{
	return Open(file, O_WRONLY|O_LARGEFILE|O_APPEND|O_BINARY, ib_umask, timeout);
};

int IBFile::OpenReadWriteWithThreadAffinity(std::string const& file) 
	throw(DatabaseRCException)
{
	assert(file.length());
	string temp_file_name;
	MakeThreadAffinityName(file, temp_file_name);
	CopyFile (file, temp_file_name);
	return OpenReadWrite (temp_file_name);
}

int IBFile::CloseWithThreadAffinity(std::string const& file)
	throw(DatabaseRCException)
{
	assert(file.length());
	int res = Close();
	if (res == 0) {
		string temp_file_name;
		MakeThreadAffinityName(file, temp_file_name);
		RenameFile(temp_file_name, file);
	}
	return res;
}

void IBFile::MakeThreadAffinityName(std::string const& prefix, std::string& buf )
	throw(DatabaseRCException)
{
	const size_t MAX_FILE_NAME_SIZE = 1024;
	char thread_id [32];

	assert(prefix.length());
	if ( (prefix.length()+sizeof(thread_id)+1 ) >= MAX_FILE_NAME_SIZE) {
		throw DatabaseRCException ("MakeThreadAffinityName: file name too long");
	}
	int id = static_cast<int>(get_raw_thread_id( this_thread::get_id()));
	sprintf (thread_id, ".%u", id);

	buf = prefix + thread_id;
}

uint IBFile::ReadExact(void* buf, uint count)
	throw(DatabaseRCException)
{
	uint read_bytes=0,rb=0;
	// Enable this in future. Right now it breaks rev 2340 tests. Need to find the root cause.
	//assert (count != 0);
	while( read_bytes < count ) {
		rb = Read((char *)buf+read_bytes, count-read_bytes);
		if(rb == 0) break;
		read_bytes += rb;
	}
	//if ((int) read_bytes == 0 && count != 0)
		//ThrowError("read only 0 bytes");
	//else if ((int) read_bytes != count)
	if ((int) read_bytes != count) {
		stringstream ss;
		ThrowError( static_cast<stringstream&>( ss << "read mismatch: read count and expected count did not match: " << count << " " << read_bytes ).str() );
	}
	return read_bytes;
}

uint IBFile::WriteExact(const void* buf, uint count)
	throw(DatabaseRCException)
{
	assert(fd != -1);
	uint total_writen_bytes = 0;
	while (total_writen_bytes < count) {
		uint writen_bytes = ib_write(fd, ((char*)buf) + total_writen_bytes, count - total_writen_bytes);
		if ((int)writen_bytes == -1)
			ThrowError(errno);
		total_writen_bytes += writen_bytes;
	}
#ifndef __BH_COMMUNITY__
	IBResourceMon::writebytes += total_writen_bytes;
	IBResourceMon::writecount ++;
#endif
	return total_writen_bytes;
}

ib_off_t IBFile::Seek(ib_off_t pos, int whence)
	throw(DatabaseRCException)
{
	ib_off_t new_pos = -1;
	assert(fd != -1);

	new_pos = ib_seek(fd, pos, whence);

	if (new_pos == (ib_off_t) -1)
		ThrowError(errno);
	return new_pos;
}

ib_off_t IBFile::Tell()
	throw(DatabaseRCException)
{
	ib_off_t pos;
	assert(fd != -1);
	pos = ib_tell(fd);
	if (pos == (ib_off_t) -1)
		ThrowError(errno);
	return pos;
}

bool IBFile::IsOpen() const
{
	return (fd != -1 ? true: false);
}

int IBFile::Close()
{
	int bh_err = 0;
	if (fd != -1) {
		bh_err = ib_close(fd);
		name = "";
	#ifdef __GNUC__
		//flock( fd, LOCK_UN );
	#endif
		fd = -1;
	}
	return bh_err;
}

//void IBFile::ThrowError(int errnum)
//	throw(DatabaseRCException)
//{
//	ThrowError(GetErrorMessage(errnum));
//};

//void IBFile::ThrowError(std::string serror)
//	throw(DatabaseRCException)
//{
//	std::string errmsg = "I/O operation on ";
//	errmsg += name.c_str();
//	errmsg += " failed with error - ";
//	errmsg += serror;
//	if (name != "") {
//		errmsg += " File name ";
//		errmsg += name;
//	}
//	#ifdef THROW_ERROR_FOR_IO_FAIL
//	throw DatabaseRCException(errmsg);
//	#else
//	printf("IBFile error: %s", errmsg.c_str());
//	#endif
//};
