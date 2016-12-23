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

#ifndef _CONFIGURATION_H_
#define _CONFIGURATION_H_

#include <map>
#include <string>
#include <boost/program_options.hpp>
#include "system/RCException.h"

class Configuration
{
	public:
		/**
			\brief A configuration specific exception object.

			This exception is typically raised if the configuration
			file format is incorrect or a requested property is not
			properly defined.
		 */
		class Exception : public RCException
		{
			public:
				/**
 					Constructs the configuration exception.

					\param msg further describes the cause of the exception.
				 */
				Exception(std::string msg) throw() : RCException(msg) {}
		};

		typedef const std::string PropertyName;

		template <typename _DataType>
		class PropertyDescriptor
		{
			public:
				typedef _DataType DataType;

				PropertyDescriptor(PropertyName n, DataType s) : name(n), seed(s) {}

				void RegisterDefinition(boost::program_options::options_description_easy_init&) const;

				PropertyName GetName() const { return name; }

			private:
				PropertyName name;
				DataType seed;
		};

		static void LoadSettings(std::string configuration_filename = default_configuration_filename);

		template <class _Property>
		static typename _Property::DataType GetProperty(const _Property& property)
		{
			std::string message;

			try {
				boost::program_options::variables_map::const_iterator it = properties.find(property.GetName().c_str());
				if(it == properties.end())
					throw Configuration::Exception(std::string( "No such key: " ) + property.GetName());
				const boost::program_options::variable_value& entry = it->second;
				return entry.as<typename _Property::DataType>();
			} catch (boost::bad_any_cast&) {
				message = "The property format is invalid";
			} catch (std::exception& e) {
				message = std::string( "An unknown problem was encountered retrieving the property: " ) + e.what();
			}
			throw Exception(std::string("WARNING: \"") + property.GetName() + "\" - " + message);
		}

		static PropertyDescriptor<int> AllowMySQLQueryPath;
		static PropertyDescriptor<std::string> CacheFolder;
		static PropertyDescriptor<std::string> LicenseFile;
		static PropertyDescriptor<int> ControlMessages;
		static PropertyDescriptor<bool> InternalMessages;
		static PropertyDescriptor<std::string> KNFolder;
		static PropertyDescriptor<int> KNLevel;
		static PropertyDescriptor<int> LoaderMainHeapSize;
		static PropertyDescriptor<bool> PushDown;
		static PropertyDescriptor<int> ServerMainHeapSize;
//		static PropertyDescriptor<bool> LastPackCompression;
		static PropertyDescriptor<bool> UseMySQLImportExportDefaults;
		static PropertyDescriptor<bool> AutoConfigure;

	        // Depricated parameters. They are still here so that mysqld can start while they
	        // may present in old brighthouse.ini file. They are unused and will be removed from here in future.
		static PropertyDescriptor<int> BufferingLevel;
		static PropertyDescriptor<int> CachingLevel;
		static PropertyDescriptor<int> ClusterSize;
		static PropertyDescriptor<std::string> HugefileDir;
		static PropertyDescriptor<int> LoaderSaveThreadNumber;
		static PropertyDescriptor<int> ServerCompressedHeapSize;
		static PropertyDescriptor<int> PrefetchThreads;
		static PropertyDescriptor<int> PrefetchQueueLength;
		static PropertyDescriptor<int> PrefetchDepth;
	private:
		Configuration() {};

		static void InitializeEditionSpecificOptions(boost::program_options::options_description_easy_init&);

		static std::string default_configuration_filename;
		static boost::program_options::variables_map properties;
		static boost::program_options::options_description definitions;
		static bool _loaded;

	public:
		static bool IsLoaded()
			{ return ( _loaded ); }
		static void PublishConfigsToShowVar();
};

void ConfigureRCControl();

#endif // _CONFIGURATION_H_

