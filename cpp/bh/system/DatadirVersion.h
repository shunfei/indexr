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

#ifndef _SYSTEM_DATADIRVERSION_H_
#define _SYSTEM_DATADIRVERSION_H_

#include <string>

class DatadirVersion
{
public:
	DatadirVersion(const std::string& datadir);

public:
	void		UpdateTo(int data_ver, const std::string& server_ver);
	int			GetDataVersion() const	{ return my_data_ver; }
	std::string	GetServerVersion() const	{ return my_server_ver; }

private:
    void ObtainDataDirVersion();

private:
    std::string datadir;
	std::string data_version_file;
	int my_data_ver;
	std::string my_server_ver;

public:
	static const int DATADIR_VERSION_PRIOR_TO_4_0_7 = 2;

};

#endif //_SYSTEM_DATADIRVERSION_H_
