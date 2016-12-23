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

#include <fstream>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

#include "RCException.h"
#include "IBFileSystem.h"
#include "RCSystem.h"
#include "DatadirVersion.h"

using namespace std;
using namespace boost;

DatadirVersion::DatadirVersion(const string& datadir)
	:	datadir(datadir), data_version_file(datadir + DIR_SEPARATOR_STRING + "ib_data_version"), my_data_ver(2), my_server_ver("4.0.6")
{
	if(!filesystem::is_directory(datadir) || !filesystem::is_regular_file(datadir + DIR_SEPARATOR_STRING + "brighthouse.ini"))
		throw RCException("Specified path: `" + datadir + "' is not Infobright data directory.");
    ObtainDataDirVersion();
}

void DatadirVersion::ObtainDataDirVersion()
{
    if(DoesFileExist(data_version_file)) {
        ifstream ver_file(data_version_file.c_str());
        if(!ver_file)
            throw RCException(string("Fatal error: can not to read ") + data_version_file + ".");

        string data_ver_text;
        getline(ver_file, data_ver_text, '\n');
        if(ver_file)
            getline(ver_file, my_server_ver, '\n');

        if(!ver_file || ver_file.get() != ifstream::traits_type::eof())
            throw RCException(string("Fatal error: wrong format of ") + data_version_file + " file.");

        try {
            my_data_ver = lexical_cast<int>(data_ver_text);
        } catch(...) {
            throw RCException(string("Fatal error: wrong format of ") + data_version_file + " file.");
        }
        if(my_data_ver < 0)
            throw RCException(string("Fatal error: wrong format of ") + data_version_file + " file.");
    }
}

void DatadirVersion::UpdateTo(int data_ver, const std::string& server_ver)
{
	BHASSERT(data_ver >= DATADIR_VERSION_PRIOR_TO_4_0_7, "An attempt to upgrade to previous version in DatadirVersion class!");
	BHASSERT(!server_ver.empty(), "An attempt to set server version to empty string.");

	if(data_ver != my_data_ver || server_ver != my_server_ver) {
		if(data_ver == DATADIR_VERSION_PRIOR_TO_4_0_7) {
			RemoveFile(data_version_file);
		} else {
			string tmp_file_name(data_version_file);
			MakeStringUnique(tmp_file_name);
			ofstream ver_file(tmp_file_name.c_str());
			if(ver_file)
				ver_file << data_ver << '\n' << server_ver << '\n';
			if(!ver_file)
				throw RCException(string("Fatal error: can not create or write to ") + data_version_file + " file.");
			ver_file.close();
			FlushFileChanges(tmp_file_name);
			RenameFile(tmp_file_name, data_version_file);
			FlushDirectoryChanges(datadir);
		}
		my_data_ver = data_ver;
		my_server_ver = server_ver;
	}
}
