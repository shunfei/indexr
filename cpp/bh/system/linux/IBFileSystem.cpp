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

#include <string>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "system/ib_system.h"
#include "system/IBFileSystem.h"
#include "system/IBFile.h"
#include <unistd.h>
#include <string.h>

#ifdef __FreeBSD__
#define dirent64 dirent
#define readdir64 readdir
#endif

using namespace std;

static const char* FILE_SYSTEM_ERROR = "FileSystem Error : ";

int ClearDirectory(std::string const& path);

static void DeleteFile(std::string const& path, struct dirent64 *file)
{
	const char *fname;
	fname = file->d_name;
	if((strcasecmp(fname,".")!=0) && (strcasecmp(fname,"..")!=0) && (strcasecmp(fname,"") !=0))
	{
		string file_path(path);
		file_path += DIR_SEPARATOR_STRING;
		file_path += fname;

        	bool is_dir = false;
	        struct stat stat_info;
        	int ret = stat(file_path.c_str(), &stat_info);
	        if (ret == 0) {
        	    is_dir = S_ISDIR(stat_info.st_mode);
	        }

		if(! is_dir)
			RemoveFile(file_path);
		else
		{
			ClearDirectory(file_path);
			rmdir(file_path.c_str());
		}
	}
}

int ClearDirectory(std::string const& path)
{
	string tmp_path(path);
	tmp_path += DIR_SEPARATOR_STRING;

	DIR *dfp;
	struct	dirent64 *dinfo;
	dfp = opendir(tmp_path.c_str());
	if (dfp == NULL) {
		return 0;
	}
	do
	{
		if ((dinfo = readdir64(dfp)) == NULL)
			break;
		DeleteFile(path, dinfo);
	} while(true);
	closedir(dfp);
	return 0;
}

int IsDir(std::string const& path)
{
    bool is_dir = false;
    struct stat stat_info;
    int ret = stat(path.c_str(), &stat_info);
    if (ret == 0) {
        is_dir = S_ISDIR(stat_info.st_mode);
    }
    return is_dir;
}

// find a file name that matches with a substr
int FindFirst(const char* srcdir,
    const char* substr,            // match file name with substr 
    int depth,
    char* filefirst)
{
    char    tmp_path[256];
    DIR*    dfp;
    struct  dirent64 *dinfo;

    filefirst[0] = '\0';

    strcpy(tmp_path, srcdir);
    strcat(tmp_path, DIR_SEPARATOR_STRING);

    dfp = opendir(tmp_path);
    if (dfp == NULL)
        return 0;
    do {
        if ((dinfo = readdir64(dfp)) == NULL)
            break;
        std::string fname = dinfo->d_name;
        if ((fname != ".") && (fname != "..") && (fname != "")) {
            //printf(" fname =%s", fname.c_str());
            // substr match
            if (strstr(fname.c_str(), substr)) {
                strcpy(filefirst, fname.c_str());
                return 1;
            }
            fname = DIR_SEPARATOR_STRING + fname;
            fname = srcdir + fname;
            //printf(" fname =%s", fname.c_str());
            if (IsDir(fname.c_str()) && depth > 0) {
                if (FindFirst(fname.c_str(), substr, depth -1, filefirst))
                    return 1;
            }
        }
    } while(true);
    closedir(dfp);
    return 0;
}

void DeleteDirectory(std::string const& path)
{
	ClearDirectory(path);
	rmdir(path.c_str());
}

bool GetFileSize(std::string const& path, _int64& size)
{
    size = 0;
    struct stat stat_info;
    int ret = stat(path.c_str(), &stat_info);
    if(ret != 0) return false;
    size += stat_info.st_size;
    return true;
}

time_t GetFileTime(std::string const& path)
{
    struct stat stat_info;
    int ret = stat(path.c_str(), &stat_info);
    if(ret != 0) return -1;
    return stat_info.st_mtime;
}

time_t GetFileCreateTime(std::string const& path)
{
    struct stat stat_info;
    int ret = stat(path.c_str(), &stat_info);
    if(ret != 0) return -1;
    return stat_info.st_ctime;
}

#ifndef NDEBUG
bool DoesFileExistImpl(std::string const& file)
#else /* #ifndef NDEBUG */
bool DoesFileExist(std::string const& file)
#endif /* #else #ifndef NDEBUG */
{
	struct stat stat_info;
	return (0 == stat(file.c_str(), &stat_info));
}
#ifndef NDEBUG
bool DoesFileExist(std::string const& file)
{
	return ( DoesFileExistImpl( file ) );
}
#endif /* #ifndef NDEBUG */

void RenameFile(string const& OldPath, string const& NewPath)
    throw(DatabaseRCException)
{
	assert(OldPath.length() && NewPath.length());
	if (rename(OldPath.c_str(), NewPath.c_str())) {
		throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
	}
};

void VerifyWriteAccess(std::string const& file)
    throw(DatabaseRCException)
{
	assert(file.length());
	if (access(file.c_str(), W_OK|F_OK)) {
		throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
	}
};


#ifndef NDEBUG
void RemoveFileImpl(std::string const& path, int throwerror) throw(DatabaseRCException)
#else /* #ifndef NDEBUG */
void RemoveFile(std::string const& path, int throwerror) throw(DatabaseRCException)
#endif /* #else #ifndef NDEBUG */
{
	assert(path.length());
	if (remove(path.c_str())) {
        if (DoesFileExist(path) && throwerror) {
		    throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
        }
	}
}
#ifndef NDEBUG
void RemoveFile(std::string const& file, int throwerror) throw(DatabaseRCException)
{
	RemoveFileImpl( file, throwerror );
}
#endif /* #ifndef NDEBUG */

void CopyFile (string const& source, string const& dest)
    throw(DatabaseRCException)
{
    assert(source.length());
    assert(dest.length());

    const size_t BUF_SIZE = 4096;
    char buf [ BUF_SIZE ];
    int dst_fd=0, src_fd=0, len, res;

    dst_fd = open (dest.c_str(), O_WRONLY | O_TRUNC);
    if (dst_fd < 0) {
        dst_fd = creat (dest.c_str(), 0660);
        if (dst_fd < 0) goto l_err;
    }

    src_fd = open (source.c_str(), O_RDONLY);
    if (src_fd < 0) goto l_err;

    while ( (len = read (src_fd, buf, BUF_SIZE)) > 0 ) {
        res = write (dst_fd, buf, len);
        if (res < 0) goto l_err;
    }
    if (len < 0) goto l_err;

    close (dst_fd);
    close (src_fd);
    return;
l_err:
    if (dst_fd) close (dst_fd);
    if (src_fd) close (src_fd);
    throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
}

void CopyFileWithSecurity(string const& source, string const& dest)
    throw(DatabaseRCException)
{
    struct stat stat_info;

    CopyFile(source, dest);

    // set original user, group to the newly copied file.
    int ret = stat(source.c_str(), &stat_info);
    if (ret != 0) goto l_err;

    ret = chown(dest.c_str(),  stat_info.st_uid, stat_info.st_gid);
    if (ret != 0) goto l_err;

    ret = chmod(dest.c_str(), stat_info.st_mode);
    if (ret != 0) goto l_err;

    return;
l_err:
    throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
}

void FlushDirectoryChanges(std::string const& path) throw(DatabaseRCException)
{
#ifdef SOLARIS
#else
    DIR* dir;
    int res, fd;

    dir = opendir(path.c_str());
    if(dir==0) goto l_err;

    fd = dirfd(dir);
    if (fd < 0) goto l_err;

    res = fsync(fd);
    if (res < 0) goto l_err;

    closedir(dir);
    return;
l_err:
    if (dir) closedir(dir);
    throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
#endif
}

void FlushFileChanges(std::string const& path)
{
	IBFile fb;
	fb.OpenReadOnly(path);
	fb.Flush();
	fb.Close();
}

void CreateDir(std::string const& path) throw(DatabaseRCException)
{
    int ret = mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    if (ret) {
        if (!DoesFileExist(path)) {
            throw DatabaseRCException( GetErrorMessage(errno, FILE_SYSTEM_ERROR).c_str() );
        }
    }
}

bool IsReadWriteAllowed(std::string const& path)
{
    int ret = access(path.c_str(), R_OK | W_OK);
    return (ret == 0);
}

std::string GetSystemTempPath()
{
	return "/tmp/";
}

std::string GetCurrentWoringDirectory()
{
	char buffer[PATH_MAX+1];
	getcwd(buffer, PATH_MAX);
	return string(buffer);
}

std::string MakeDirectoryPathOsSpecific(const std::string& path, bool add_separator_on_the_end)
{
	std::string r = path;
	if(r.length()) {
		std::replace(r.begin(), r.end(), '\\', DIR_SEPARATOR_CHAR);
		if(add_separator_on_the_end && *r.rbegin() != DIR_SEPARATOR_CHAR)
			r += DIR_SEPARATOR_CHAR;
	}
	return r;
}

bool IsAbsolutePath(const std::string& path)
{
	std::string tmp = MakeDirectoryPathOsSpecific(path);
	return tmp.length() && tmp[0] == DIR_SEPARATOR_CHAR;
}

void TruncateFile(const std::string& path, _int64 new_length)
{
	truncate(path.c_str(), new_length);
}
