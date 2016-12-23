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

#define THROW_ERROR_FOR_IO_FAIL

#include "IBStream.h"
#include "system/BHToolkit.h"
#include "system/IOParameters.h"
#include "system/IBFile.h"
#include "system/IBPipe.h"

using namespace std;

IBStream::IBStream() 
{
	// TODO Auto-generated constructor stub
}

IBStream::~IBStream() 
{
	// TODO Auto-generated destructor stub
}

auto_ptr<IBStream> IBStream::CreateOpenIBStream(const IOParameters & iop, BufOpenType mode)
	throw(DatabaseRCException)
	{
		auto_ptr<IBStream> res;
		const char* fname = iop.Path();

#ifdef _MSC_VER

		bool ispipe = IsPipe(iop.Path());
		if(ispipe) {
			int create = iop.PipeMode();
			int timeout = iop.Timeout();
			res = auto_ptr<IBStream>(new IBPipe());
			if(mode == READ) {
//				DebugBreak();
				res->OpenReadOnly(fname, create, timeout);	
			} else if(mode == OVERWRITE || mode == APPEND)				
				res->OpenCreateEmpty(fname, create, timeout);
			return res;
		}
#endif

			res = auto_ptr<IBStream> (new IBFile());
			if(mode == READ) {				
				res->OpenReadOnly(fname);
			} else if(mode == OVERWRITE || mode == APPEND) {				
				if(mode == OVERWRITE) {
					res->OpenCreateEmpty(fname);
				} else {
					res->OpenWriteAppend(fname);
				}
			}			
			return res;
}

void IBStream::ThrowError(int errnum)
	throw(DatabaseRCException)
{
	ThrowError(GetErrorMessage(errnum));
};

void IBStream::ThrowError(std::string serror)
	throw(DatabaseRCException)
{
	std::string errmsg = "I/O operation on ";
	errmsg += name.c_str();
	errmsg += " failed with error - ";
	errmsg += serror;
	if (name != "") {
		errmsg += " File name ";
		errmsg += name;
	}
#ifdef THROW_ERROR_FOR_IO_FAIL
	throw DatabaseRCException(errmsg);
#else
	printf("IBStream error: %s", errmsg.c_str());
#endif
};
