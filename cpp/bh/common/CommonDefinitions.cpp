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

#include "CommonDefinitions.h"

using namespace std;
using namespace boost;

string infobright_data_dir;
string infobright_home_dir;

ProcessType::enum_t process_type( ProcessType::INVALID );

BHTribool BHTribool::And(BHTribool a, BHTribool b)
{
	if(a == true && b == true)
		return true;
	if(a == false || b == false)
		return false;
	return BHTRIBOOL_UNKNOWN;
}

BHTribool BHTribool::Or(BHTribool a, BHTribool b)
{
	if(a == true || b == true)
		return true; 
	if(a == BHTRIBOOL_UNKNOWN || b == BHTRIBOOL_UNKNOWN)
		return BHTRIBOOL_UNKNOWN;
	return false; 
}
