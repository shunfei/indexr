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

#include <ctype.h>
#include <string>
#include <vector>
#include <algorithm>

#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>

#include "common/DataFormat.h"
#include "system/RCException.h"

#ifdef test
#undef test
#endif

using namespace std;
using namespace boost;

int DataFormat::no_formats = 0;

DataFormat::DataFormat(string name, EDF edf)
	: name(name), id(no_formats++), edf(edf)
{
}

DataFormat::~DataFormat()
{
}

DataFormatPtr DataFormat::GetDataFormat(const string& name)
{
	map_type::iterator it = DataFormat::GetExternalDataFormats()->find(trim_copy(to_upper_copy(name)));
	return it != DataFormat::GetExternalDataFormats()->end() ? (*it).second : DataFormatPtr();
}

DataFormatPtr DataFormat::GetDataFormat(int id)
{
	map_type::iterator it = find_if(GetExternalDataFormats()->begin(), GetExternalDataFormats()->end(),
			bind(equal_to<int>(), id, bind(&DataFormat::GetId, bind(&map_type::iterator::value_type::second, _1))));
	return it != DataFormat::GetExternalDataFormats()->end() ? it->second : DataFormatPtr();
}

DataFormatPtr DataFormat::GetDataFormat(EDF edf)
{
	return GetDataFormatbyEDF(edf);
}

DataFormatPtr DataFormat::GetDataFormatbyEDF(EDF edf)
{
	map_type::iterator it = find_if(GetExternalDataFormats()->begin(), GetExternalDataFormats()->end(),
				bind(equal_to<int>(), edf, bind(&DataFormat::GetEDF, bind(&map_type::iterator::value_type::second, _1))));
		return it != DataFormat::GetExternalDataFormats()->end() ? it->second : DataFormatPtr();
}

bool DataFormat::CanImport(const IOParameters& iop, BHError& bherror) const
{
	bherror = BHError();
	return true;
}

bool DataFormat::CanExport() const
{
	return true;
}
