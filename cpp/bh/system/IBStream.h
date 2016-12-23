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

#ifndef IBSTREAM_H_
#define IBSTREAM_H_

#include <memory>
#include <string>

#include "system/RCException.h"
#include "system/LargeBuffer.h"

class IOParameters;

class IBStream {
public:
	std::string name;


	IBStream();
	virtual ~IBStream();
	virtual bool IsOpen() const = 0;
	virtual int Close() = 0;
	virtual int OpenReadOnly(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException) = 0;
	virtual int OpenReadWrite(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException) = 0;
	virtual int OpenCreateEmpty(std::string const& filename, int create = 0, uint64 timeout = 0) throw(DatabaseRCException) = 0;
	virtual int OpenWriteAppend(std:: string const&, int create = 0 , uint64 timeout = 0) throw(DatabaseRCException) = 0;
	virtual uint WriteExact(const void* buf, uint count) throw(DatabaseRCException) = 0;
	virtual uint Read(void* buf, uint count) throw(DatabaseRCException) = 0;
	//virtual uint Write(const void* buf, uint count) throw(DatabaseRCException) = 0;
	// Variant for Read and Write calls
	// Throws exception if can't read exact number of bytes
	virtual uint ReadExact(void* buf, uint count) throw(DatabaseRCException) = 0;
	static std::auto_ptr<IBStream> CreateOpenIBStream(const IOParameters & iop, BufOpenType mode)
		throw(DatabaseRCException);
	void ThrowError(int errnum) throw(DatabaseRCException);
	void ThrowError(std::string serror) throw(DatabaseRCException);
};

#endif /* IBSTREAM_H_ */
