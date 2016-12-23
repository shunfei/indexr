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

#ifndef _BHLOADER_H_
#define _BHLOADER_H_

#include <string>

#include "system/RCSystem.h"
#include "loader/Loader.h"
#include "system/IBPipe.h"

#define BHLOADER_REPORT_EXTENSTION    ".bhl_report"

class BHLoader;
class Buffer;

class BHLoader : public Loader
{
public:
	~BHLoader();
	bool Proceed(pid_t& pi);			// return true on success otherwise return false
};

int LoadData(std::string shared_obj_name, std::string shared_obj_path);


#endif //_BHLOADER_H_

