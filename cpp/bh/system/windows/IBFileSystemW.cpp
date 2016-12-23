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
#ifndef __GNUC__

#include <string>
#include <winsock2.h>
#include <windows.h>
#include <direct.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include "system/IBFileSystem.h"
#include "system/ib_system.h"
#include "system/IBFile.h"

using namespace std;

static const char* FILE_SYSTEM_ERROR = "FileSystem Error : ";

static void DeleteFile(std::string const& path, WIN32_FIND_DATAA *file)
{
    const char *fname;
    fname = file->cFileName;
    if ((_stricmp(fname,".")!=0) && (_stricmp(fname,"..")!=0) && (_stricmp(fname,"") !=0))
    {
         string file_path(path);
         file_path += DIR_SEPARATOR_STRING;
         file_path += fname;
         if (!(file->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
             remove(file_path.c_str());
         else {
             ClearDirectory(file_path);
             rmdir(file_path.c_str());
         }
    }
}

int ClearDirectory(std::string const& path)
{
    string tmp_path(path);
    tmp_path += DIR_SEPARATOR_STRING;
    tmp_path += "*";

    WIN32_FIND_DATAA FindFileData;
    HANDLE hFind = FindFirstFileA(tmp_path.c_str(), &FindFileData);
    if (hFind != INVALID_HANDLE_VALUE)
        while(FindNextFileA(hFind, &FindFileData) != 0)
	    DeleteFile(path, &FindFileData);
    FindClose(hFind);
    return 0;
}

// find a file name that matches with a substr
int FindFirst(const char* srcdir,
    const char* substr,            // match file name with substr 
    int depth,
    char* filefirst)
{
    char    tmp_path[256];
    filefirst[0] = '\0';

    strcpy(tmp_path, srcdir);
    strcat(tmp_path, DIR_SEPARATOR_STRING);
    strcat(tmp_path, "*");

    WIN32_FIND_DATAA FindFileData;
    HANDLE hFind = FindFirstFileA(tmp_path, &FindFileData);
    if (hFind == INVALID_HANDLE_VALUE) 
        return 0;
    while(FindNextFileA(hFind, &FindFileData) != 0) {
        std::string fname = FindFileData.cFileName;
        if ((fname != ".") && (fname != "..") && (fname != "")) {
            //printf(" fname =%s", fname.c_str());
            // substr match
            if (strstr(fname.c_str(), substr)) {
                strcpy(filefirst, fname.c_str());
                return 1;
            }
            fname = DIR_SEPARATOR_STRING + fname;
            fname = srcdir + fname;
            if (FindFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY && depth > 0) {
                if (FindFirst(fname.c_str(), substr, depth -1, filefirst))
                    return 1;
            }
        }
    }
    FindClose(hFind);
    return 0;
}

void DeleteDirectory(std::string const& path)
{
    ClearDirectory(path);
    rmdir(path.c_str());
}

bool GetFileSize(std::string const&name, _int64& size)
{
    size = 0;
    struct ibstat stat_info;
    int ret = ibstat(name.c_str(), & stat_info);
    if (ret != 0) return false;
        size += stat_info.st_size;
    return true;
}

time_t GetFileTime(std::string const&name)
{
    struct ibstat stat_info;
    int ret = ibstat(name.c_str(), & stat_info);
    if (ret != 0) return -1;
    return stat_info.st_mtime;
}

time_t  GetFileCreateTime(std::string const&name)
{
    struct ibstat stat_info;
    int ret = ibstat(name.c_str(), & stat_info);
    if (ret != 0) return -1;
    return stat_info.st_ctime;
}


#ifndef NDEBUG
bool DoesFileExistImpl(std::string const& file)
#else /* #ifndef NDEBUG */
bool DoesFileExist(std::string const& file)
#endif /* #else #ifndef NDEBUG */
{
    struct ibstat stat_info;
    return (0 == ibstat(file.c_str(), &stat_info));
}
#ifndef NDEBUG
bool DoesFileExist(std::string const& file)
{
	return ( DoesFileExistImpl( file ) );
}
#endif /* #ifndef NDEBUG */

void RenameFile(std::string const&OldPath, std::string const& NewPath)
    throw(DatabaseRCException)
{
    assert(OldPath.length() && NewPath.length());
    if (!MoveFileEx(OldPath.c_str(), NewPath.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        throw DatabaseRCException(GetErrorMessage(GetLastError(), FILE_SYSTEM_ERROR).c_str());
    }
};

void VerifyWriteAccess(std::string const&file)
    throw(DatabaseRCException)
{
    assert(file.length());
    if (access(file.c_str(), W_OK|F_OK)) {
	throw DatabaseRCException(GetErrorMessage(GetLastError(), FILE_SYSTEM_ERROR).c_str());
    }
};

#ifndef NDEBUG
void RemoveFileImpl(std::string const& file, int throwerror) throw(DatabaseRCException)
#else /* #ifndef NDEBUG */
void RemoveFile(std::string const& file, int throwerror) throw(DatabaseRCException)
#endif /* #else #ifndef NDEBUG */
{
    assert(file.length());

    // WINPORT
    BOOL b = ::DeleteFileA(file.c_str());
    if (!b) {
        if (ERROR_FILE_NOT_FOUND != GetLastError() && throwerror) {
            throw DatabaseRCException(GetErrorMessage(GetLastError(), FILE_SYSTEM_ERROR).c_str());
        }
    }
};
#ifndef NDEBUG
void RemoveFile(std::string const& file, int throwerror) throw(DatabaseRCException)
{
	RemoveFileImpl( file, throwerror );
}
#endif /* #ifndef NDEBUG */

void CopyFile (std::string const& source, std::string const& dest)
    throw(DatabaseRCException)
{
    assert(source.length() && dest.length());
    if (!::CopyFileA(source.c_str(), dest.c_str(), FALSE))
        throw DatabaseRCException(GetErrorMessage(GetLastError(), FILE_SYSTEM_ERROR).c_str());
}

void CopyFileWithSecurity(string const& source, string const& dest)
    throw(DatabaseRCException)
{
    // TODO: add enhancement 
    CopyFile(source, dest);
}

void FlushDirectoryChanges(std::string const& path) throw(DatabaseRCException)
{
    // not implemented
    return;
}

void CreateDir(std::string const& path) throw(DatabaseRCException)
{
    int ret = mkdir(path.c_str());
    if (ret) {
        if (!DoesFileExist(path)) {
            throw DatabaseRCException(GetErrorMessage(GetLastError(), FILE_SYSTEM_ERROR).c_str() );
        }
    }
}

void FlushFileChanges(std::string const& path)
{
	IBFile fb;
	fb.OpenWriteAppend(path, 0, IBFILE_OPEN_NO_TIMEOUT);
	fb.Flush();
	fb.Close();
}


bool IsReadWriteAllowed(std::string const& path)
{
    int ret = access(path.c_str(), R_OK | W_OK);
    return (ret == 0);
}

std::string GetSystemTempPath()
{
    unsigned int bufsize=512;
    unsigned int retval;
    char tpath[512];
    retval = GetTempPathA(bufsize, tpath);
    if (retval > bufsize || (retval == 0))
        return "";
       return tpath;
}

std::string GetCurrentWoringDirectory()
{
	char* buffer = _getcwd( NULL, 0 );
	std::string path(buffer);
	free(buffer);
	return path;
}

std::string MakeDirectoryPathOsSpecific(const std::string& path, bool add_separator_on_the_end)
{
	std::string r = path;
	if(r.length()) {
		std::replace(r.begin(), r.end(), '/', DIR_SEPARATOR_CHAR);
		if(add_separator_on_the_end && *r.rbegin() != DIR_SEPARATOR_CHAR)
			r += DIR_SEPARATOR_CHAR;
	}
	return r;
}

bool IsAbsolutePath(const std::string& path)
{
	std::string tmp = MakeDirectoryPathOsSpecific(path);
	return tmp.length() >= 2 && tmp[1] == ':' && tmp[2] == DIR_SEPARATOR_CHAR;
}

void TruncateFile(const std::string& path, _int64 new_length)
{	
	HANDLE hfile = CreateFile(path.c_str(), 
								GENERIC_READ | GENERIC_WRITE,
								0, 
								NULL,
								OPEN_EXISTING, 
								FILE_ATTRIBUTE_NORMAL, 
								NULL);	
	//assert(new_length > 0);
	SetFilePointer(hfile, LONG(new_length), 0, FILE_BEGIN);
	SetEndOfFile(hfile);
	CloseHandle(hfile);
	return;

}

#endif
