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

#include "system/RCSystem.h"
#include "Loader.h"
#include "core/RSI_Framework.h"

using namespace std;
using namespace bh;

// This is mysqld server id.
extern ulong server_id;

int Loader::LoaderSaveThreadNumber = 16;

Loader::Loader()
  : mode(false), rs_owner(-1), table_id(-1)
{
    memset (& params, 0, sizeof(params));
    params.packrow_size = MAX_PACK_ROW_SIZE;
}

Loader::~Loader()
{
	shared_file.Close();
}

void Loader::Init(string bhload_app, IOParameters* io_params)
{
	if(!io_params || bhload_app.length() == 0)
		throw InternalRCException("Unable to start Loader.");
	this->bhload_app = bhload_app;
    iop = *io_params;

	string err_msg= "";
	mode= false;
	try {
		Configuration::LoadSettings();
	} catch (const Configuration::Exception& e) {
		rclog << lock << e.what() << " Configuration::Exception" << __FILE__ << __LINE__ << unlock;
	}

	if (!ConfMan.IsLoaded())
		Loader::LoaderSaveThreadNumber = 16;
	else
		Loader::LoaderSaveThreadNumber = ConfMan.GetValueInt("brighthouse/LoaderSaveThreadNumber", err_msg);

	shared_obj_name= "bhload-info-";
	MakeStringUnique(shared_obj_name);

	rs_owner= RS_CREATOR;
}

void Loader::Init(string shared_obj_name, string shared_obj_path)
{
	string err_msg = "";
	mode= false;
	try {
		Configuration::LoadSettings();
	} catch (const Configuration::Exception& e) {
		rclog << lock << e.what() << " Configuration::Exception" << __FILE__ << __LINE__ << unlock;
	}

	mode= true;

	try {
		shared_file.OpenReadWrite(shared_obj_path.c_str());
	} catch(DatabaseRCException& e)	{
		err_msg = "Unable to open a shared file ";
		err_msg += shared_obj_path + ". Error : ";
		err_msg += e.what();
		throw InternalRCException(err_msg.c_str());
	}

    ReLoadParameters();
	rs_owner= RS_USER;

	if (infobright_home_dir.empty())
		infobright_home_dir = iop.InstallPath();
	if (infobright_data_dir.empty())
		infobright_data_dir = iop.BasePath();

	if (!ConfMan.IsLoaded() && ConfMan.LoadConfig(err_msg)) {
		//rclog << lock << err_msg.c_str() << __FILE__ << __LINE__ << unlock;
		Loader::LoaderSaveThreadNumber = 16;
	} else
		Loader::LoaderSaveThreadNumber = ConfMan.GetValueInt("brighthouse/LoaderSaveThreadNumber", err_msg);

}

void Loader::FlushParameters()
{
	shared_file.Seek(0, SEEK_SET);
	shared_file.WriteExact(&params, sizeof(params));
	try {
		iop.PutParameters(shared_file);
	} catch(const DatabaseRCException& e)	{
		throw InternalRCException(string("Loader::FlushParameters: Failed to flush parameters. ") + e.what());
	}

}

void Loader::ReLoadParameters()
{
	shared_file.Seek(0, SEEK_SET);
	shared_file.ReadExact(&params, sizeof(params));
	try {
		iop.GetParameters(shared_file);
	} catch(const DatabaseRCException& e)	{
        throw InternalRCException(string("Loader::ReLoadParameters: Failed to reload parameters. ") + e.what());
    }
}

void Loader::DeInit(bool cleanup)
{
	shared_file.Close();
	if (cleanup) {
		string shared_obj;
		shared_obj = GetSystemTempPath();
		shared_obj += shared_obj_name;
		RemoveFile(shared_obj.c_str(), false);
	}
}
