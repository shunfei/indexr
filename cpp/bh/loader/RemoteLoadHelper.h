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

#ifndef REMOTELOADHELPER_H_
#define REMOTELOADHELPER_H_

#include <string>
#include <memory>

#include "system/ib_system.h"

class	IBPipe;
class	IOParameters;
struct	st_io_cache;
typedef st_io_cache IO_CACHE;

class RemoteLoadHelper
{
public:
	RemoteLoadHelper(IOParameters& iop);
	~RemoteLoadHelper();
	void operator()(IBProcess&);

private:
	void Push();

	void ReadAllFromCache(IO_CACHE& io);

private:
	typedef std::auto_ptr<IBPipe> pipe_t;

	bool		is_remote;
	std::string infile;
	pipe_t 		pipe;

public:
	static std::string make_pipe_path();
	static std::string make_file_path();
};

#endif /* REMOTELOADHELPER_H_ */
