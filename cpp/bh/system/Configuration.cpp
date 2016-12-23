/* Copyright (C) 2005-2008 Infobright Inc.

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
#include <boost/program_options.hpp>
#include "RCSystem.h"
#include "Configuration.h"

using namespace std;
using namespace boost::program_options;

#define INIT_BOOST_PROPERTY(name, datatype, seed) \
	Configuration::PropertyDescriptor<datatype> Configuration::name(#name, seed)
INIT_BOOST_PROPERTY(CacheFolder, string, "cache");
INIT_BOOST_PROPERTY(LicenseFile, string, "<unknown>");
INIT_BOOST_PROPERTY(ControlMessages, int, 0);
INIT_BOOST_PROPERTY(InternalMessages, bool, false);
INIT_BOOST_PROPERTY(KNFolder, string, "BH_RSI_Repository");
INIT_BOOST_PROPERTY(KNLevel, int, 99);
INIT_BOOST_PROPERTY(LoaderMainHeapSize, int, 320);
INIT_BOOST_PROPERTY(PushDown, bool, true);
INIT_BOOST_PROPERTY(ServerMainHeapSize, int, 600);
//INIT_BOOST_PROPERTY(LastPackCompression, bool, true);
INIT_BOOST_PROPERTY(UseMySQLImportExportDefaults, bool, false);
INIT_BOOST_PROPERTY(AutoConfigure, bool, false);

// Depricated parameters. They are still here so that mysqld can start while they
// may present in old brighthouse.ini file. They are unused and will be removed from here in future.
INIT_BOOST_PROPERTY(BufferingLevel, int, 2);
INIT_BOOST_PROPERTY(CachingLevel, int, 1);
INIT_BOOST_PROPERTY(ClusterSize, int, 2000);
INIT_BOOST_PROPERTY(HugefileDir, std::string, string());
INIT_BOOST_PROPERTY(LoaderSaveThreadNumber, int, 16);
INIT_BOOST_PROPERTY(ServerCompressedHeapSize, int, 320);
INIT_BOOST_PROPERTY(PrefetchThreads,int, 0);
INIT_BOOST_PROPERTY(PrefetchQueueLength,int, 0);
INIT_BOOST_PROPERTY(PrefetchDepth,int, 0);

#undef INIT_BOOST_PROPERTY

std::string Configuration::default_configuration_filename = "brighthouse.ini";
bool Configuration::_loaded = false;
boost::program_options::variables_map Configuration::properties;
boost::program_options::options_description Configuration::definitions("Brighthouse Configuration Properties");

char bh_sysvar_refresh_sys_infobright = TRUE;

// Variables to show config through mysql 'show variables' command
int bh_sysvar_allowmysqlquerypath = 1;
char* bh_sysvar_cachefolder = NULL;
char* bh_sysvar_licensefile = NULL;
int bh_sysvar_controlmessages = 0;
char bh_sysvar_internalmessages = false;
char* bh_sysvar_knfolder = NULL;
int bh_sysvar_knlevel = 99;
int bh_sysvar_loadermainheapsize = 320;
char bh_sysvar_pushdown = true;
int bh_sysvar_servermainheapsize = 600;
char bh_sysvar_usemysqlimportexportdefaults = false;
char bh_sysvar_autoconfigure = false;

template<typename _DataType>
struct property_logger
{
	string _name;
	property_logger( string const& name ) : _name( name ) {}
	void operator()( _DataType const& val ) {
		if ( Configuration::IsLoaded() ) {
			// Do not display depricated parameters
			if (_name != "BufferingLevel" &&
			  _name != "CachingLevel"  &&
			  _name != "ClusterSize"   &&
			  _name != "HugefileDir"   &&
			  _name != "LoaderSaveThreadNumber"    && 
			  _name != "ServerCompressedHeapSize"  && 
			  _name != "PrefetchThreads"      &&
			  _name != "PrefetchQueueLength"  &&
			  _name != "PrefetchDepth" )
			rccontrol << lock << "Option: " << _name << ", value: " << val << "." << unlock;
		}
	}
};

template <typename _DataType>
void Configuration::PropertyDescriptor<_DataType>::RegisterDefinition(options_description_easy_init& initializer) const
{
	initializer(name.c_str(), value<_DataType>()->default_value(seed)->notifier( property_logger<_DataType>( name  ) ) );
}

void Configuration::LoadSettings(std::string configuration_filename)
{
    static int once = 0;
    if (once) return;
    once = 1;

	try {
	    options_description_easy_init initializer = definitions.add_options();
	    AllowMySQLQueryPath.RegisterDefinition(initializer);
	    CacheFolder.RegisterDefinition(initializer);
	    LicenseFile.RegisterDefinition(initializer);
	    ControlMessages.RegisterDefinition(initializer);
	    InternalMessages.RegisterDefinition(initializer);
	    KNFolder.RegisterDefinition(initializer);
	    KNLevel.RegisterDefinition(initializer);
	    LoaderMainHeapSize.RegisterDefinition(initializer);
	    PushDown.RegisterDefinition(initializer);
	    ServerMainHeapSize.RegisterDefinition(initializer);
	    //LastPackCompression.RegisterDefinition(initializer);
	    UseMySQLImportExportDefaults.RegisterDefinition(initializer);
	    AutoConfigure.RegisterDefinition(initializer);

	    // Depricated parameters. They are still here so that mysqld can start while they
	    // may present in old brighthouse.ini file. They are unused and will be removed from here in future.
	    BufferingLevel.RegisterDefinition(initializer);
	    CachingLevel.RegisterDefinition(initializer);
	    ClusterSize.RegisterDefinition(initializer);
	    HugefileDir.RegisterDefinition(initializer);
	    LoaderSaveThreadNumber.RegisterDefinition(initializer);
	    ServerCompressedHeapSize.RegisterDefinition(initializer);
	    PrefetchThreads.RegisterDefinition(initializer);
	    PrefetchQueueLength.RegisterDefinition(initializer);
	    PrefetchDepth.RegisterDefinition(initializer);

	    Configuration::InitializeEditionSpecificOptions(initializer);
		
		// This load call on an empty option set is needed to force
		// boost to set the default property values.  Otherwise, a
		// call to fetch a property prior to loading the Brighthouse
		// configuration file will raise a boost::bad_any_cast
		// exception for trying to convert an empty string to an
		// integer ("" != 0).  There may be a better way to do this
		// but it was not obvious from the boost documentation and
		// the notify call by itself is insufficient.  This scenario
		// specifically occurs in the stand-alone brighthouse loader
		// which does not explicitly load the configuration file.
		basic_parsed_options<char> empty(&definitions);
		store(empty, properties);
		notify(properties);
	}
	catch(exception& e) {
		throw Exception(string("ERROR: Failed to initialize the configuration [") + e.what() + "]");
	}

	string p = infobright_data_dir + configuration_filename.c_str();
	ifstream configuration_file(p.c_str());
	if(!configuration_file) {
		rclog << lock << "ERROR: The configuration file " << configuration_filename << " not found" << unlock;
		char buf[1000];
		sprintf (buf, "ERROR: The configuration file %s not found", configuration_filename.c_str());
		throw Exception(buf);
	}
	else {
		try {
			_loaded = true;
			rccontrol << lock << "Loading configuration for Infobright instance ..." << unlock;
			store(parse_config_file(configuration_file, definitions), properties);
			notify(properties);
			rccontrol << lock << "Infobright instance configuration loaded." << unlock;
		}
		catch(exception &e) {
			rclog << lock << "ERROR: Failed to parse \"" << configuration_filename << "\"" << unlock;
			rclog << lock << "Configuration parsing error = \"" << e.what() << "\"" << unlock;
			char buf [1000];
			sprintf(buf, "ERROR: Failed to parse %s, parsing error = %s",  configuration_filename.c_str(), e.what());
			throw Exception(buf);
		}
	}
	PublishConfigsToShowVar();
}

void ConfigureRCControl()
{
    int control_level = Configuration::GetProperty(Configuration::ControlMessages);
    (control_level > 0) ? rccontrol.setOn() : rccontrol.setOff();
    if(control_level >= 2){
        rccontrol.setTimeStamp();
    	if (control_level >= 3) {
        	rccontrol.setResourceMon();
    	}
    }
}

void Configuration::PublishConfigsToShowVar()
{
	bh_sysvar_allowmysqlquerypath = GetProperty(AllowMySQLQueryPath);
	bh_sysvar_cachefolder = const_cast<char *>(GetProperty(CacheFolder).c_str());
	bh_sysvar_licensefile = const_cast<char *>(GetProperty(LicenseFile).c_str());
	bh_sysvar_controlmessages = GetProperty(ControlMessages);
	bh_sysvar_internalmessages = GetProperty(InternalMessages);
	bh_sysvar_knfolder = const_cast<char *>(GetProperty(KNFolder).c_str());
	bh_sysvar_knlevel = GetProperty(KNLevel);
	bh_sysvar_loadermainheapsize = GetProperty(LoaderMainHeapSize);
	bh_sysvar_pushdown = GetProperty(PushDown);
	bh_sysvar_servermainheapsize = GetProperty(ServerMainHeapSize);
	bh_sysvar_usemysqlimportexportdefaults = GetProperty(UseMySQLImportExportDefaults);
	bh_sysvar_autoconfigure = GetProperty(AutoConfigure);
}
