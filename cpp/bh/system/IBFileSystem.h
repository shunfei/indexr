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

#ifndef _SYSTEM_IBFILESYSTEM_H_
#define _SYSTEM_IBFILESYSTEM_H_

#include "common/stdafx.h"
#include "system/RCException.h"
#include <string>

#ifdef __GNUC__
#define DIR_SEPARATOR_STRING   "/"
#define DIR_SEPARATOR_CHAR     '/'

#define ibstat    stat
#else
#define DIR_SEPARATOR_STRING   "\\"
#define DIR_SEPARATOR_CHAR     '\\'

#define F_OK      0
#define W_OK      2
#define R_OK      4

// CRT _stati64 mapping. Windows has different
// CRT function for stat on large file.
#define ibstat    _stati64
//#define _CRT_SECURE_NO_WARNINGS
//#define _CRT_SECURE_NO_DEPRECATE
#endif

/** \brief Deletes a directory defined by path
 *  \param path Directory path
 *  The delete operation is recursive. It deletes all existing files and subdirectories
 */
void DeleteDirectory(std::string const& path);

/** \brief Provides a file size
 *  \param name  Path to the file
 *  \param size  Out parameter. Set as a size of a file
 *  \return True if file size is set. False in case of an error
 */
bool GetFileSize(std::string const& path, _int64& size);

/** \brief Returns file's last content modification time
 *  \param name Path to the file
 *  \return Time typecasted to int
 */
time_t GetFileTime(std::string const& path);

/** \brief Returns file's last metadata modification time
 *  \param name Path to the file
 *  \return Time typecasted to int
 *  \see chown
 */
time_t GetFileCreateTime(std::string const& path);

/** \brief Checks file existense
 *  \param name Path to the file
 *  \return True if a file exists. False if a file doesn't exist
 */
bool DoesFileExist(std::string const&);
#ifndef NDEBUG
bool DoesFileExistImpl(std::string const&);
#endif /* #ifndef NDEBUG */

/** \brief Rename file
 *  \param OldPath Original file path
 *  \param NewPath New file path
 *  Function throws an exception in case it fails to rename a file
 */
void RenameFile(std::string const& OldPath, std::string const& NewPath) throw(DatabaseRCException);

/** \brief Delete file
 *  \param file  Path to the file
 *  Function throws an exception in case it fails to remove a file
 *  If a file does not exist, no exceptions are thrown
 */
void RemoveFile(std::string const& file, int throwerror=true) throw(DatabaseRCException);
#ifndef NDEBUG
void RemoveFileImpl(std::string const& file, int throwerror=true) throw(DatabaseRCException);
#endif /* #ifndef NDEBUG */

/** \brief Copy file
 *  \param OldPath  Path to the source file
 *  \param NewPath  Path to the destination file
 *  Function throws an exception in case it fails to copy a file
 */
void CopyFile(std::string const& OldPath, std::string const& NewPath) throw(DatabaseRCException);

void CopyFileWithSecurity(std::string const& OldPath, std::string const& NewPath) throw(DatabaseRCException);

/** \brief Flush to the disk kernel buffers with directory entries changes
 *  \param path  Directory path
 *  Function throws an exception in case it fails to perform
 */
void FlushDirectoryChanges(std::string const& path) throw(DatabaseRCException);

void FlushFileChanges(std::string const& path);

/** \brief Create a new directory
 *  \param path  Directory path
 *  Function throws an exception in case it fails to perform
 */
void CreateDir(std::string const& path) throw(DatabaseRCException);

/** \brief Check file permissions
 *  \param path  File path
 *  \return True if a process is allowed to read and write a file. False otherwise
 */
bool IsReadWriteAllowed(std::string const& path);

/** \brief Give system temp folder path
 *  \return temp path
 */
std::string GetSystemTempPath();

/** \brief Clear the content of directory
 *  \return
 */
int ClearDirectory(std::string const& path);

std::string GetCurrentWoringDirectory();

int FindFirst(const char* srcdir,
    const char* substr,            // match file name with substr 
    int depth,
    char* filefirst);

std::string MakeDirectoryPathOsSpecific(const std::string& path, bool add_separator_on_the_end = true);

bool IsAbsolutePath(const std::string& path);

/* change the end of file for given file size
*/

void TruncateFile(const std::string& path, _int64 new_length);

#endif //_SYSTEM_IBFILESYSTEM_H_
